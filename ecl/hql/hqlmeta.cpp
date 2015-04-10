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
#include "jliball.hpp"
#include "hql.hpp"
#include "eclrtl.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"
#include "hqlpmap.hpp"
#include "hqlfold.hpp"
#include "hqlerrors.hpp"
#include "hqltrans.ipp"
#include "hqlthql.hpp"
#include "deffield.hpp"
#include "workunit.hpp"
#include "jencrypt.hpp"
#include "hqlattr.hpp"
#include "hqlerror.hpp"
#include "hqlmeta.hpp"

//#define OPTIMIZATION2

static IHqlExpression * cacheGroupedElement;
static IHqlExpression * cacheUnknownAttribute;
static IHqlExpression * cacheIndeterminateAttribute;
static IHqlExpression * cacheUnknownSortlist;
static IHqlExpression * cacheIndeterminateSortlist;
static IHqlExpression * cacheMatchGroupOrderSortlist;
static IHqlExpression * cached_omitted_Attribute;
static IHqlExpression * cacheAnyAttribute;
static IHqlExpression * cacheAnyOrderSortlist;
static CHqlMetaProperty * nullMetaProperty;
static CHqlMetaProperty * nullGroupedMetaProperty;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    IAtom * groupedOrderAtom = createAtom("{group-order}");
    IAtom * anyOrderAtom = createAtom("{any}");
    cacheGroupedElement = createAttribute(groupedOrderAtom);
    cacheUnknownAttribute = createAttribute(unknownAtom);
    cacheIndeterminateAttribute = createAttribute(indeterminateAtom);
    cacheAnyAttribute = createAttribute(anyOrderAtom);
    cached_omitted_Attribute = createAttribute(_omitted_Atom);
    cacheUnknownSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheUnknownAttribute));
    cacheIndeterminateSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheIndeterminateAttribute));
    cacheMatchGroupOrderSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheGroupedElement));
    cacheAnyOrderSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheAnyAttribute));
    nullMetaProperty = new CHqlMetaProperty;
    nullGroupedMetaProperty = new CHqlMetaProperty;
    nullGroupedMetaProperty->meta.setUnknownGrouping();
    return true;
}
MODULE_EXIT()
{
    nullGroupedMetaProperty->Release();
    nullMetaProperty->Release();
    cached_omitted_Attribute->Release();
    cacheAnyOrderSortlist->Release();
    cacheMatchGroupOrderSortlist->Release();
    cacheAnyAttribute->Release();
    cacheIndeterminateSortlist->Release();
    cacheUnknownSortlist->Release();
    cacheIndeterminateAttribute->Release();
    cacheUnknownAttribute->Release();
    cacheGroupedElement->Release();
}

/*

This file contains the logic for maintaining the dataset information about the distribution, sort order and grouping of a dataset.
It is called from createDataset() to set up the type information, and from various optimization routines to determine the best way to
implement an activity - e.g, grouping depending on the current distribution.

The following information is maintained:

Distribution
- Either the expression that was used in the DISTRIBUTE() activity, or a sorted attribute with the sort order (no_sortlist) as a child
- or an unknownAttribute if unknown.

Global sort order
- The sort order maintained across all nodes.
- Should never have trailing unknown.

Local ungrouped sort order
- The order of the records on each individual node, excluding anything within a grouping condition
- Can have trailing unknown if a mapping is lost and the dataset is grouped.
- can be set to a special grouping sort list if grouped and couldn't split out the grouping.

Grouping
- Which expressions was the dataset grouped by.
- contains unknown attributes in the grouping elements???

Group sort order
- What is the current order within the group.
- Should never have a trailing unknown attribute

The types are processed with the following in mind:
Projects:
* Projections (or any other transforms) can cause fields referenced in the meta information to be modified
- It can be mapped to an expression.
- It could be mapped to a constant
- The field cannot be mapped.

Distributions:
* A global sort activity ensures that each row with the same sort component fields will end up on the same node.  This means it is important to know
  if we have lost track of one of the sort components, and not just lose it.  Otherwise we might assume the dataset is distributed by (x,y) when it 
  is actually distributed by (x,y,z) - meaning some values of (x,y) may be on different nodes.  
  It may still be more efficient to group by (x,y) though instead of redistributing by (x,y).
* A distribute activity similarly ensures that all rows with the same disrtibute expression end up on the same node.
* For both of these it means that a pre-existing distribution is sufficient if it is a subset of a following required distribution.

Sort orders
* It is fine to lose trailing sort-orders that are unknown, but need to be very careful that the order is correctly preserved when a grouped dataset is ungrouped.
* I assert there is no operation which can modify the local sort order without modifying the global sort order.  (The global order is never going to be better
  than the local order).  This means hthor/roxie only need to look at the local sort order, instead of the global sort order.
* The local and global sort orders are tracked excluding any sort order

Grouping
- All fields that are grouped are maintained, not just the leading ones.  This is so that when a record is ungrouped, all leading sort components can be preserved??

A single row is always sorted and distributed as you would like.

Problems of constants, unknown attributes and duplicates:
- Duplicates are always removed from sort lists, and group lists before comparing.
- Constant percolation can sometimes cause constants to appear in sort lists.  They should also ideally be removed, but that may
  cause us to miss matching sort orders.

=>
- Distributions are either fully mapped, unknown or sorted(list of components, terminated by unknown).  This latter may possibly useful for 
  knowing we could use group to subsort instead of resorting.
- Global sort lists do not include any trailing unknown attributes.
- Local sort lists may contain a trailing unknown attribute if they are partial.
- Grouped sort lists do not include unknown attributes.
- Grouping information contains unknown attributes for each unknown item.
- Constants and duplicates are always removed (both from meta information and before matching)


The following are a sample of some of the key activities and how they affect the meta information:

* CHOOSEN
- Although this is sequential, it actually preserves all of the information since no records are moved.

* GLOBAL SORT(x,y)
- Changes the distribution to sorted([x,y])
- Changes global and local sort orders to [x,y]

* LOCAL SORT(x,y)
- Changes the local sort order to [x,y]
- Changes the global sort order to the leading intersection of the current global sort order and the new local sort order.

* GROUPED SORT(x,y)
- Modifies the grouped sort order
- If the local order is grouped attribute, set it to unknown.
- Truncate the global sort order at the last component which is found in the current grouping.
- May indirectly affect the global/local sort orders since they are derived by combining with the grouped order.

* GROUP (x,y)
- If non local then the distribution is invalidated.
- Global sort order is preserved as-is.
- Local sort order is split in two.  
  The last sort component which is included in the grouping condition (and any trailing constants) marks the end of the local order.  
  The trailing components become the grouped sort order.
- If none of the sort components occur in the grouping condition, then set the local order to a "grouping attribute".

* UNGROUP (x,y)
- Distribution and global sort order are preserved.
- If the local sort order is a grouping attribute, set it to the grouped sort order.
- If the local sort order ends with an unknown attribute, truncate it, otherwise merged with the grouped order.

* DISTRIBUTE (x)
- set distribtion to x
- invalidate local and global sort orders

* DISTRIBUTE (x,MERGE)
- set distribution to x
- invalidate global sort order
- set local sort order to merge criteria.

* PROJECT
- Distribution: If a distribution fails to be mapped fully it is replaced with a unknownAttribute.
- Sortlist: Any components after the first component that fails to be mapped are relaced with an unknownAttribute
- Grouping: All grouping elements that fail to be matched are replaced with an unknownAttribute.  (But trailing components are preserved).


Optimizations:

Grouping:
- A global group can be done locally if
  i) The distribution was a sort, and the trailing component of the sort is included in the grouping criteria.
  ii) The distribution was a non-sort and all fields in the distribution are included in the grouping criteria.
  iii) The dataset was previously grouped by a subset of the new grouping fields.
- A group,all (local or global) can become a group if (folllowing above)
  i) Each of the grouping criteria (reordered match the leading components of the appropriate sort (including extra constants).
  This also has implications for re-ordering join conditions to ensure they are done in the best order.
- Sometimes for an implicit grouping (e.g., group aggregate), if the distribution partially matches it should still be used.

Sort:
- A local sort can potentially optimized to a group sort if
  i) Leading components of the sorts match, and are signficant enough.

codistribute (e.g., cogroup):
- Make use of any existing distributions to minimize the transfer.

*** Implement cogroup() as an example.
COGROUP(a,b)

Check a first, and then b:
If a is sufficiently distributed:
i) If distributed by a sort, generate a cosort, optionally locally sort a, and then local merge.
ii) If keyed distributed then could do a key distr, then local cogroup
iii) If distributed
   a) If a and b are locally sorted use distribute,merge on b, then local merge.
   b) if a is locally sorted, use distribute(b)->localsort->localmerge
   c) If neither sorted use (a+distribute(b))->local sort
NOTE: If one side is distributed(f(x,y)),sorted(x,y); other is distributed(f(x),sorted(x))  It will be best to use the second since we could use a merge distribute.
iv) Take acount of the expected number of rows - ignore if expect to have few rows.

Local cogroup:
if (left and right sufficiently sorted)
   pick the shortest (but not a single row)
   ensure other side is sorted the same
   perform a local merge.
if (either side sufficiently sorted and not a singlerow)
   ensure other side is sorted the same
   perform a local merge.
else
   append and locally sort.

Also should have
NORMALIZE(ds, group, f(rows(left)));
which can be used for the result of a cogroup to perform a similar function to REDUCE in hadoop etc.
Implement at the same time as the child query variety of NORMALIZE

*/

IHqlExpression * queryUnknownAttribute() { return cacheUnknownAttribute; }
IHqlExpression * queryMatchGroupOrderSortlist() { return cacheMatchGroupOrderSortlist; }
IHqlExpression * queryUnknownSortlist() { return cacheUnknownSortlist; }
IHqlExpression * getUnknownAttribute() { return LINK(cacheUnknownAttribute); }
IHqlExpression * getMatchGroupOrderSortlist() { return LINK(cacheMatchGroupOrderSortlist); }
IHqlExpression * getUnknownSortlist() { return LINK(cacheUnknownSortlist); }
IHqlExpression * queryAnyOrderSortlist() { return cacheAnyOrderSortlist; }
IHqlExpression * queryAnyDistributionAttribute() { return cacheAnyAttribute; }
CHqlMetaProperty * queryNullMetaProperty(bool isGrouped) { return isGrouped ? nullGroupedMetaProperty : nullMetaProperty; }

bool hasKnownSortlist(IHqlExpression * sortlist)
{
    if (!sortlist)
        return false;

    unsigned max = sortlist->numChildren();
    if (max == 0)
        return false;

    return (sortlist->queryChild(max-1)->queryName() != unknownAtom);
}


bool CHqlMetaInfo::appearsToBeSorted(bool isLocal, bool ignoreGrouping)
{
    if (isLocal)
        return localUngroupedSortOrder != NULL;
    if (!ignoreGrouping && grouping)
        return groupSortOrder != NULL;
    return globalSortOrder != NULL;
}

void CHqlMetaInfo::clearGrouping()
{
    if (grouping)
    {
        grouping.clear();
        groupSortOrder.clear();
    }
}

void CHqlMetaInfo::ensureAppearsSorted(bool isLocal, bool ignoreGrouping)
{
    if (!appearsToBeSorted(isLocal, false))
    {
        IHqlExpression * unknownOrder = queryUnknownSortlist();
        if (isGrouped())
            applyGroupSort(unknownOrder);
        else if (isLocal)
            applyLocalSort(unknownOrder);
        else
        {
            globalSortOrder.set(unknownOrder);
            localUngroupedSortOrder.set(unknownOrder);
        }
    }
}

bool CHqlMetaInfo::hasKnownSortGroupDistribution(bool isLocal) const
{
    if (!isLocal)
    {
        if (!distribution || (distribution->queryName() == unknownAtom))
            return false;
        if (!hasKnownSortlist(globalSortOrder))
            return false;
    }
    else
    {
        if (!hasKnownSortlist(localUngroupedSortOrder))
            return false;
    }
    if (!grouping)
        return true;
    if (grouping->queryName() == unknownAtom)
        return false;
    if (!hasKnownSortlist(groupSortOrder))
        return false;
    return true;
}

bool CHqlMetaInfo::hasUsefulInformation() const
{
    return (distribution && containsActiveDataset(distribution)) ||
           (globalSortOrder && containsActiveDataset(globalSortOrder)) ||
           (localUngroupedSortOrder && containsActiveDataset(localUngroupedSortOrder)) ||
           (grouping && containsActiveDataset(grouping)) ||
           (groupSortOrder && containsActiveDataset(groupSortOrder));
}

bool CHqlMetaInfo::matches(const CHqlMetaInfo & other) const
{
    return (distribution == other.distribution) &&
           (globalSortOrder == other.globalSortOrder) &&
           (localUngroupedSortOrder == other.localUngroupedSortOrder) &&
           (grouping == other.grouping) &&
           (groupSortOrder == other.groupSortOrder);
}

void CHqlMetaInfo::preserveGrouping(IHqlExpression * dataset)
{
    if (::isGrouped(dataset))
        setUnknownGrouping();
    else
        grouping.clear();
}

void CHqlMetaInfo::removeAllAndUngroup(bool isLocal)
{
    if (!isLocal)
        distribution.clear();
    globalSortOrder.clear();
    localUngroupedSortOrder.clear();
    clearGrouping();
}

void CHqlMetaInfo::removeAllKeepGrouping()
{
    distribution.clear();
    globalSortOrder.clear();
    localUngroupedSortOrder.clear();
    if (grouping)
    {
        grouping.setown(getUnknownSortlist());
        groupSortOrder.clear();
    }
}

void CHqlMetaInfo::removeAllSortOrders()
{
    globalSortOrder.clear();
    localUngroupedSortOrder.clear();
    groupSortOrder.clear();
}

void CHqlMetaInfo::removeDistribution()
{
    distribution.clear();
}

void CHqlMetaInfo::set(const CHqlMetaInfo & other)
{
    distribution.set(other.distribution);
    globalSortOrder.set(other.globalSortOrder);
    localUngroupedSortOrder.set(other.localUngroupedSortOrder);
    grouping.set(other.grouping);
    groupSortOrder.set(other.groupSortOrder);
}

void CHqlMetaInfo::setMatchesAny()
{
    distribution.set(queryAnyDistributionAttribute());
    globalSortOrder.set(queryAnyOrderSortlist());
    localUngroupedSortOrder.set(queryAnyOrderSortlist());
}

void CHqlMetaInfo::setUnknownDistribution()
{
    distribution.setown(getUnknownAttribute());
}


void CHqlMetaInfo::setUnknownGrouping()
{
    grouping.setown(getUnknownSortlist());
}

//---------------------------------------------------------------------------------------------------------------------

class JoinEqualityMapper
{
public:
    inline JoinEqualityMapper(IHqlExpression * joinExpr)
    {
        left = joinExpr->queryChild(0);
        right = joinExpr->queryChild(1);
        selSeq = querySelSeq(joinExpr);
    }

    IHqlExpression * mapEqualities(IHqlExpression * expr, IHqlExpression * cond)
    {
        if (cond->getOperator() == no_assertkeyed)
            cond = cond->queryChild(0);

        if (cond->getOperator() == no_and)
        {
            OwnedHqlExpr mapped = mapEqualities(expr, cond->queryChild(0));
            return mapEqualities(mapped, cond->queryChild(1));
        }
        else if (cond->getOperator() == no_eq)
        {
            IHqlExpression * lhs = cond->queryChild(0);
            IHqlExpression * rhs = cond->queryChild(1);
            if (lhs->queryType() == rhs->queryType())
            {
                IHqlExpression * leftSelect = queryDatasetCursor(lhs);
                IHqlExpression * rightSelect = queryDatasetCursor(rhs);
                if (isLeft(leftSelect) && isRight(rightSelect))
                    return replaceExpression(expr, rhs, lhs);
                if (isRight(leftSelect) && isLeft(rightSelect))
                    return replaceExpression(expr, lhs, rhs);
            }
        }
        return LINK(expr);
    }

protected:
    inline bool isMatch(IHqlExpression * expr, node_operator op, IHqlExpression * side)
    {
        return (expr->getOperator() == op) &&
            (expr->queryRecord()->queryBody() == side->queryRecord()->queryBody()) &&
            (expr->queryChild(1) == selSeq);
    }
    inline bool isLeft(IHqlExpression * expr) { return isMatch(expr, no_left, left); }
    inline bool isRight(IHqlExpression * expr) { return isMatch(expr, no_right, right); }

protected:
    IHqlExpression * left;
    IHqlExpression * right;
    IHqlExpression * selSeq;
};

//---------------------------------------------------------------------------------------------------------------------

inline bool matchesGroupOrder(IHqlExpression * expr) { return expr == cacheMatchGroupOrderSortlist; }

bool hasTrailingGroupOrder(IHqlExpression * expr)
{
    if (expr)
    {
        unsigned max = expr->numChildren();
        if (max)
            return expr->queryChild(max-1) == cacheGroupedElement;
    }
    return false;
}

//---------------------------------------------------------------------------------------------
// Helper functions for processing the basic lists

//return true if identical
bool intersectList(HqlExprArray & target, const HqlExprArray & left, const HqlExprArray & right)
{
    unsigned max = left.ordinality();
    for (unsigned i= 0; i < max; i++)
    {
        if (!right.isItem(i))
            return false;
        IHqlExpression & cur = left.item(i);
        if (&cur != &right.item(i))
            return false;
        target.append(OLINK(cur));
    }
    if (right.isItem(max))
        return false;
    return true;
}


IHqlExpression * createSubSortlist(IHqlExpression * sortlist, unsigned from, unsigned to, IHqlExpression * subsetAttr)
{
    if (from == to)
        return NULL;
    if ((from == 0) && (to == sortlist->numChildren()))
        return LINK(sortlist);

    HqlExprArray components;
    unwindChildren(components, sortlist, from, to);
    if (subsetAttr)
        components.append(*LINK(subsetAttr));
    return createSortList(components);
}

void removeDuplicates(HqlExprArray & components)
{
    unsigned max = components.ordinality();
    if (max == 0)
        return;

    for (unsigned i=max-1; i != 0; i--)
    {
        IHqlExpression & cur = components.item(i);
        unsigned match = components.find(cur);
        if (match != i)
            components.remove(i);
    }
}

static bool hasUnknownComponent(HqlExprArray & components)
{
    if (components.ordinality())
    {
        IHqlExpression & last = components.tos();
        return last.isAttribute() && last.queryName() == unknownAtom;
    }
    return false;
}

void unwindNormalizeSortlist(HqlExprArray & args, IHqlExpression * src, bool removeAttributes)
{
    if (!src)
        return;

    ForEachChild(i, src)
    {
        IHqlExpression * cur = src->queryChild(i);
        if (!cur->queryValue() && !(removeAttributes && cur->isAttribute()))
            args.append(*LINK(cur->queryBody()));
    }
}

void normalizeComponents(HqlExprArray & args, const HqlExprArray & src)
{
    ForEachItemIn(i, src)
    {
        IHqlExpression * cur = &src.item(i);
        if (!cur->queryValue())
            args.append(*LINK(cur->queryBody()));
    }
    removeDuplicates(args);
}

IHqlExpression * getIntersectingSortlist(IHqlExpression * left, IHqlExpression * right, IHqlExpression * subsetAttr)
{
    if (!left || !right)
        return NULL;

    if (left == queryAnyOrderSortlist())
        return LINK(right);
    if (right == queryAnyOrderSortlist())
        return LINK(left);

    ForEachChild(i, left)
    {
        //This test also covers the case where one list is longer than the other...
        if (left->queryChild(i) != right->queryChild(i))
            return createSubSortlist(left, 0, i, subsetAttr);
    }
    return LINK(left);
}


//Find the intersection between left and (localOrder+groupOrder)
IHqlExpression * getModifiedGlobalOrder(IHqlExpression * globalOrder, IHqlExpression * localOrder, IHqlExpression * groupOrder)
{
    if (!globalOrder || !localOrder)
        return NULL;

    unsigned max1=0;
    if (!matchesGroupOrder(localOrder))
    {
        ForEachChild(i1, localOrder)
        {
            //This test also covers the case where one list is longer than the other...
            IHqlExpression * curLocal = localOrder->queryChild(i1);
            if (globalOrder->queryChild(i1) != curLocal)
            {
                if (curLocal == cacheGroupedElement)
                    break;
                return createSubSortlist(globalOrder, 0, i1, NULL);
            }
        }
        max1 = localOrder->numChildren();
    }

    unsigned max2 = 0;
    if (groupOrder)
    {
        ForEachChild(i2, groupOrder)
        {
            //This test also covers the case where one list is longer than the other...
            if (globalOrder->queryChild(i2+max1) != groupOrder->queryChild(i2))
                return createSubSortlist(globalOrder, 0, i2+max1, NULL);
        }
        max2 = groupOrder->numChildren();
    }
    return createSubSortlist(globalOrder, 0, max1+max2, NULL);
}


static IHqlExpression * normalizeSortlist(IHqlExpression * sortlist)
{
    if (!sortlist)
        return NULL;

    HqlExprArray components;
    unwindNormalizeSortlist(components, sortlist, false);
    removeDuplicates(components);
    //This never returns NULL if the input was non-null
    return createSortList(components);
}

inline IHqlExpression * normalizeSortlist(IHqlExpression * sortlist, IHqlExpression * dataset)
{
    if (!sortlist)
        return NULL;
    OwnedHqlExpr mapped = replaceSelector(sortlist, dataset, queryActiveTableSelector());
    return normalizeSortlist(mapped);
}

IHqlExpression * normalizeDistribution(IHqlExpression * distribution)
{
    return LINK(distribution);
}

static bool sortComponentMatches(IHqlExpression * curNew, IHqlExpression * curExisting)
{
    //A component of (trim)x is identical to x since spaces are always ignored
    while (isOpRedundantForCompare(curNew))
        curNew = curNew->queryChild(0);
    while (isOpRedundantForCompare(curExisting))
        curExisting = curExisting->queryChild(0);

    IHqlExpression * newBody = curNew->queryBody();
    IHqlExpression * existingBody = curExisting->queryBody();
    if (newBody == existingBody)
        return true;

    ITypeInfo * newType = curNew->queryType();
    ITypeInfo * existingType = curExisting->queryType();

    //A local sort by (string)qstring is the same as by qstring....
    if (isCast(curNew) && (curNew->queryChild(0)->queryBody() == existingBody))
    {
        if (preservesValue(newType, existingType) && preservesOrder(newType, existingType))
            return true;
    }
    // a sort by qstring is the same as by (string)qstring.
    if (isCast(curExisting) && (newBody == curExisting->queryChild(0)->queryBody()))
    {
        if (preservesValue(existingType, newType) && preservesOrder(existingType, newType))
            return true;
    }
    // (cast:z)x should match (implicit-cast:z)x
    if (isCast(curNew) && isCast(curExisting) && (newType==existingType))
        if (curNew->queryChild(0)->queryBody() == curExisting->queryChild(0)->queryBody())
            return true;

    return false;
}

//---------------------------------------------------------------------------------------------

bool isKnownDistribution(IHqlExpression * distribution)
{
    return distribution && (distribution != queryUnknownAttribute());
}

bool isSortedDistribution(IHqlExpression * distribution)
{
    return distribution && (distribution->queryName() == sortedAtom);
}

bool isPersistDistribution(IHqlExpression * distribution)
{
    return isKnownDistribution(distribution) && (distribution->getOperator() == no_bxor);
}

void extractMeta(CHqlMetaInfo & meta, IHqlExpression * expr)
{
    CHqlMetaProperty * match = queryMetaProperty(expr);
    meta.set(match->meta);
}

IHqlExpression * queryGrouping(IHqlExpression * expr)
{
    if (!expr->isDataset())
        return NULL;
    return queryMetaProperty(expr)->meta.grouping;
}

IHqlExpression * queryDistribution(IHqlExpression * expr)
{
    if (!expr->isDataset())
        return NULL;
    return queryMetaProperty(expr)->meta.distribution;
}

IHqlExpression * queryGlobalSortOrder(IHqlExpression * expr)
{
    if (!expr->isDataset())
        return NULL;
    return queryMetaProperty(expr)->meta.globalSortOrder;
}

IHqlExpression * queryLocalUngroupedSortOrder(IHqlExpression * expr)
{
    if (!expr->isDataset())
        return NULL;
    return queryMetaProperty(expr)->meta.localUngroupedSortOrder;
}

IHqlExpression * queryGroupSortOrder(IHqlExpression * expr)
{
    if (!expr->isDataset())
        return NULL;
    return queryMetaProperty(expr)->meta.groupSortOrder;
}



//What is the actual local sort order at the moment - ignoring any grouping.
IHqlExpression * CHqlMetaInfo::getLocalSortOrder() const
{
    IHqlExpression * localOrder = localUngroupedSortOrder;
    if (!isGrouped())
        return LINK(localOrder);

    if (!localOrder)
        return NULL;

    IHqlExpression * groupOrder = groupSortOrder;
    if (matchesGroupOrder(localOrder))
        return LINK(groupOrder);

    HqlExprArray components;
    unwindChildren(components, localOrder);
    if (!hasUnknownComponent(components))
    {
        if (components.length() && (&components.tos() == cacheGroupedElement))
            components.pop();

        if (groupOrder)
        {
            unwindChildren(components, groupOrder);
            if (hasUnknownComponent(components))
                components.pop();
        }
    }
    else
        components.pop();

    if (components.ordinality())
    {
        removeDuplicates(components);
        return createSortList(components);
    }
    return NULL;
}

inline IHqlExpression * getLocalSortOrder(IHqlExpression * expr)
{
    CHqlMetaProperty * metaProp = queryMetaProperty(expr);
    return metaProp->meta.getLocalSortOrder();
}

inline IHqlExpression * getGlobalSortOrder(IHqlExpression * expr)
{
    CHqlMetaProperty * metaProp = queryMetaProperty(expr);
    return LINK(metaProp->meta.globalSortOrder);
}
//---------------------------------------------------------------------------------------------
// Helper functions for handling field projection

extern HQL_API IHqlExpression * mapJoinDistribution(TableProjectMapper & mapper, IHqlExpression * distribution, IHqlExpression * side)
{
    bool doneAll = false;
    IHqlExpression * activeSelector = queryActiveTableSelector();
    OwnedHqlExpr mapped = mapper.collapseFields(distribution, activeSelector, activeSelector, side, &doneAll);
    if (doneAll)
        return mapped.getClear();
    return NULL;
}


extern HQL_API IHqlExpression * mapDistribution(IHqlExpression * distribution, TableProjectMapper & mapper)
{
    if (!distribution) 
        return NULL;

    bool matchedAll = false;
    IHqlExpression * activeSelector = queryActiveTableSelector();
    OwnedHqlExpr mapped = mapper.collapseFields(distribution, activeSelector, activeSelector, &matchedAll);
    if (matchedAll)
        return mapped.getClear();
    return getUnknownAttribute();
}


extern HQL_API IHqlExpression * mapSortOrder(IHqlExpression * order, TableProjectMapper & mapper, bool appendUnknownIfTruncated)
{
    if (!order)
        return NULL;

    IHqlExpression * activeSelector = queryActiveTableSelector();
    HqlExprArray newComponents;
    ForEachChild(idx, order)
    {
        bool matchedAll;
        IHqlExpression * cur = order->queryChild(idx);
        OwnedHqlExpr mapped = mapper.collapseFields(cur, activeSelector, activeSelector, &matchedAll);
        if (!matchedAll)
        {
            //If sorted by x,y,z and grouped by x, need to retain knowledge that it was sorted outside the
            //group, otherwise a subsequent local sort will appear as a gloabal sort after a degroup.
            if (appendUnknownIfTruncated)
                newComponents.append(*getUnknownAttribute());
            break;
        }
        newComponents.append(*mapped.getClear());
    }
    
    if (newComponents.ordinality() == 0)
        return NULL;

    HqlExprArray normalizedComponents;
    normalizeComponents(normalizedComponents, newComponents);
    //?return NULL if no elements??
    return order->clone(normalizedComponents);
}


extern HQL_API IHqlExpression * mapGroup(IHqlExpression * grouping, TableProjectMapper & mapper)
{
    if (!grouping)
        return grouping;

    assertex(grouping->getOperator() == no_sortlist);
    IHqlExpression * activeSelector = queryActiveTableSelector();
    HqlExprArray newGrouping;
    ForEachChild(idx, grouping)
    {
        bool matchedAll;
        IHqlExpression * cur = grouping->queryChild(idx);
        OwnedHqlExpr mapped = mapper.collapseFields(cur, activeSelector, activeSelector, &matchedAll);
        //If the group fields don't translate, replace each with a dummy grouping so still recognised as grouped
        if (!matchedAll)
            newGrouping.append(*getUnknownAttribute());
        else
            newGrouping.append(*mapped.getClear());
    }

    return grouping->clone(newGrouping);
}


//---------------------------------------------------------------------------------------------
// functions used by the creation functions to create a modified type
// They should be optimized to do the minimal work depending on whether the input is grouped.
// any parameters should be mapped so they only refer to active tables

void CHqlMetaInfo::removeGroup()
{
    if (grouping)
    {
        localUngroupedSortOrder.setown(getLocalSortOrder());
        grouping.clear();
        groupSortOrder.clear();
    }
}


static bool matchesGroupBy(IHqlExpression * groupBy, IHqlExpression * cur)
{
    if (sortComponentMatches(groupBy, cur))
        return true;
    if (cur->getOperator() == no_negate)
        return sortComponentMatches(groupBy, cur->queryChild(0));
    return false;
}

static bool withinGroupBy(const HqlExprArray & groupBy, IHqlExpression * cur)
{
    ForEachItemIn(i, groupBy)
    {
        if (matchesGroupBy(&groupBy.item(i), cur))
            return true;
    }
    return false;
}

static bool groupByWithinSortOrder(IHqlExpression * groupBy, IHqlExpression * order)
{
    ForEachChild(i, order)
    {
        if (matchesGroupBy(groupBy, order->queryChild(i)))
            return true;
    }
    return false;
}

//NB: This does not handle ALL groups that is handled in createDataset()
void CHqlMetaInfo::applyGroupBy(IHqlExpression * groupBy, bool isLocal)
{
    removeGroup();

    OwnedHqlExpr newGrouping = normalizeSortlist(groupBy);

    IHqlExpression * localOrder = localUngroupedSortOrder;
    OwnedHqlExpr newLocalOrder;
    OwnedHqlExpr newGroupOrder;

    if (localOrder)
    {
        HqlExprArray groupBy;
        if (newGrouping)
            unwindChildren(groupBy, newGrouping);

        //The local sort order is split into two.
        //Where depends on whether all the grouping conditions match sort elements.
        //MORE: Is there a good way to accomplish this withit iterating both ways round?
        bool allGroupingMatch = true;
        ForEachItemIn(i, groupBy)
        {
            IHqlExpression * groupElement = &groupBy.item(i);
            if (!groupByWithinSortOrder(groupElement, localOrder))
            {
                allGroupingMatch = false;
                break;
            }
        }

        unsigned max = localOrder->numChildren();
        unsigned firstGroup;
        if (allGroupingMatch)
        {
            //All grouping conditions match known sorts.  Therefore the last local order component that is included in
            //the grouping condition is important.  The order of all elements before that will be preserved if the
            //group is sorted.
            firstGroup = 0;
            for (unsigned i=max;i--!= 0;)
            {
                IHqlExpression * cur = localOrder->queryChild(i);
                if (withinGroupBy(groupBy, cur))
                {
                    firstGroup = i+1;
                    break;
                }
            }
        }
        else
        {
            //If one of the grouping conditions is not included in the sort order, and if the group is subsequently
            //sorted then the the state of the first element that doesn't match the grouping condition will be unknown.
            firstGroup = max;
            for (unsigned i=0;i<max;i++)
            {
                IHqlExpression * cur = localOrder->queryChild(i);
                if (!withinGroupBy(groupBy, cur))
                {
                    firstGroup = i;
                    break;
                }
            }
        }

        if (firstGroup == 0)
        {
            //mark the local ungrouped sort order with a special value so we can restore if order doesn't change.
            newLocalOrder.set(queryMatchGroupOrderSortlist());
            newGroupOrder.set(localOrder);
        }
        else
        {
            //Add a marker to the end of the first order if it the rest will become invalidated by a group sort
            IHqlExpression * subsetAttr = (!allGroupingMatch && (firstGroup != max)) ? cacheGroupedElement : NULL;
            newLocalOrder.setown(createSubSortlist(localOrder, 0, firstGroup, subsetAttr));
            newGroupOrder.setown(createSubSortlist(localOrder, firstGroup, max, NULL));
        }
    }

    if (!isLocal)
        distribution.clear();
    localUngroupedSortOrder.setown(newLocalOrder.getClear());
    grouping.setown(newGrouping.getClear());
    groupSortOrder.setown(newGroupOrder.getClear());
}

void CHqlMetaInfo::applyGlobalSort(IHqlExpression * sortOrder)
{
    OwnedHqlExpr newSortOrder = normalizeSortlist(sortOrder);
    distribution.setown(createExprAttribute(sortedAtom, LINK(newSortOrder)));    //, createUniqueId());
    globalSortOrder.set(newSortOrder);
    localUngroupedSortOrder.set(newSortOrder);
}


void CHqlMetaInfo::applyLocalSort(IHqlExpression * sortOrder)
{
    clearGrouping();
    localUngroupedSortOrder.setown(normalizeSortlist(sortOrder));
    //The global sort order is maintained as the leading components that match.
    globalSortOrder.setown(getIntersectingSortlist(globalSortOrder, localUngroupedSortOrder, NULL));
}

void CHqlMetaInfo::applyGroupSort(IHqlExpression * sortOrder)
{
    assertex(isGrouped());
    OwnedHqlExpr groupedOrder = normalizeSortlist(sortOrder);
//    if (groupedOrder == queryGroupSortOrder(prev))
//        return LINK(prev);

    IHqlExpression * globalOrder = globalSortOrder;
    IHqlExpression * localUngroupedOrder = localUngroupedSortOrder;
    //Group sort => make sure we no longer track it as the localsort
    OwnedHqlExpr newLocalUngroupedOrder;
    if (localUngroupedOrder && !matchesGroupOrder(localUngroupedOrder))
    {
        if (hasTrailingGroupOrder(localUngroupedOrder))
        {
            HqlExprArray components;
            unwindChildren(components, localUngroupedOrder);
            components.pop();
            components.append(*getUnknownAttribute());
            newLocalUngroupedOrder.setown(localUngroupedOrder->clone(components));
        }
        else
            newLocalUngroupedOrder.set(localUngroupedOrder);
    }

    OwnedHqlExpr newGlobalOrder = getModifiedGlobalOrder(globalOrder, newLocalUngroupedOrder, groupedOrder);
    globalSortOrder.setown(newGlobalOrder.getClear());
    localUngroupedSortOrder.setown(newLocalUngroupedOrder.getClear());
    groupSortOrder.setown(groupedOrder.getClear());
}


void CHqlMetaInfo::removeActiveSort()
{
    if (isGrouped())
        applyGroupSort(NULL);
    else
        removeAllSortOrders();
}

void CHqlMetaInfo::applyDistribute(IHqlExpression * newDistribution, IHqlExpression * optMergeOrder)
{
    distribution.setown(normalizeDistribution(newDistribution));
    localUngroupedSortOrder.setown(optMergeOrder ? normalizeSortlist(optMergeOrder) : NULL);
    //Theoretically a keyed distribute may create a global sort order if merging also specified.
}

//Used when there is an alternative - either left or right.
//As long as the identical cases fall out fairly well it is probably not worth spending lots of time
//getting it very accurate.
void CHqlMetaInfo::getIntersection(const CHqlMetaInfo & rightMeta)
{
    IHqlExpression * rightDist = rightMeta.distribution;
    if ((distribution == rightDist) || (rightDist == queryAnyDistributionAttribute()))
    {
        //keep existing distribution
    }
    else if (distribution == queryAnyDistributionAttribute())
        distribution.set(rightDist);
    else if (distribution && rightDist)
        distribution.set(queryUnknownAttribute());
    else
        distribution.clear();

    globalSortOrder.setown(getIntersectingSortlist(globalSortOrder, rightMeta.globalSortOrder, NULL));

    IHqlExpression * leftLocalOrder = localUngroupedSortOrder;
    IHqlExpression * rightLocalOrder = rightMeta.localUngroupedSortOrder;
    IHqlExpression * leftGrouping = grouping;
    IHqlExpression * rightGrouping = rightMeta.grouping;
    if (leftGrouping == queryAnyOrderSortlist())
        leftGrouping = rightGrouping;
    else if (rightGrouping == queryAnyOrderSortlist())
        rightGrouping = leftGrouping;

    OwnedHqlExpr newLocalOrder;
    OwnedHqlExpr newGrouping = (leftGrouping || rightGrouping) ? getUnknownSortlist() : NULL;
    if (leftGrouping == rightGrouping)
        newGrouping.set(leftGrouping);

    OwnedHqlExpr newGroupSortOrder;
    if (leftLocalOrder == rightLocalOrder)
    {
        newLocalOrder.set(leftLocalOrder);
        if (leftGrouping == rightGrouping)
        {
            newGroupSortOrder.setown(getIntersectingSortlist(groupSortOrder, rightMeta.groupSortOrder, NULL));
        }
        else
        {
            //Don't intersect the grouping - that may cause false results, and ignore any group ordering.
        }
    }
    else
    {
        //intersect local order - not worth doing anything else
        if (!matchesGroupOrder(leftLocalOrder) && !matchesGroupOrder(rightLocalOrder))
        {
            IHqlExpression * extraAttr = newGrouping ? queryUnknownAttribute() : NULL;
            newLocalOrder.setown(getIntersectingSortlist(leftLocalOrder, rightLocalOrder, extraAttr));
        }
    }

    //MORE: This could be cleaned up
    localUngroupedSortOrder.setown(newLocalOrder.getClear());
    grouping.setown(newGrouping.getClear());
    groupSortOrder.setown(newGroupSortOrder.getClear());
}

//Distribute is all or nothing
//Global sort retains as main as significant
//Local sort needs a trailing unknown marker if dataset is grouped
//Grouping retains attributes in place of grouping elements
//Group sort retains as much as possible.

void CHqlMetaInfo::applyProject(TableProjectMapper & mapper)
{
    distribution.setown(mapDistribution(distribution, mapper));
    globalSortOrder.setown(mapSortOrder(globalSortOrder, mapper, false));
    localUngroupedSortOrder.setown(mapSortOrder(localUngroupedSortOrder, mapper, (grouping != NULL)));
    if (grouping)
    {
        grouping.setown(mapGroup(grouping, mapper));
        groupSortOrder.setown(mapSortOrder(groupSortOrder, mapper, false));
    }
}

void extractMetaFromMetaAttr(CHqlMetaInfo & meta, IHqlExpression * attr, unsigned firstChild)
{
    meta.distribution.set(queryRemoveOmitted(attr->queryChild(firstChild+0)));
    meta.globalSortOrder.set(queryRemoveOmitted(attr->queryChild(firstChild+1)));
    meta.localUngroupedSortOrder.set(queryRemoveOmitted(attr->queryChild(firstChild+2)));
    meta.grouping.set(queryRemoveOmitted(attr->queryChild(firstChild+3)));
    meta.groupSortOrder.set(queryRemoveOmitted(attr->queryChild(firstChild+4)));
}

void CHqlMetaInfo::applySubSort(IHqlExpression * groupBy, IHqlExpression * sortOrder, bool isLocal)
{
    applyGroupBy(groupBy, isLocal);
    applyGroupSort(sortOrder);
    removeGroup();
}

//---------------------------------------------------------------------------------------------

bool appearsToBeSorted(IHqlExpression * expr, bool isLocal, bool ignoreGrouping)
{
    CHqlMetaProperty * metaProp = queryMetaProperty(expr);
    return metaProp->meta.appearsToBeSorted(isLocal, ignoreGrouping);
}

//---------------------------------------------------------------------------------------------
// Helper functions for optimizing grouping operations

bool isSortedForGroup(IHqlExpression * table, IHqlExpression *sortList, bool isLocal)
{
    assertex(sortList->getOperator()==no_sortlist);

    OwnedHqlExpr existingOrder = isLocal ? getLocalSortOrder(table) : getGlobalSortOrder(table);
    OwnedHqlExpr normalizedGroupList = normalizeSortlist(sortList, table);
    unsigned numToGroup = normalizedGroupList->numChildren();
    if (numToGroup == 0)
        return true;
    if (!existingOrder)
        return false;
    unsigned numExistingOrder = existingOrder->numChildren();
    if (numToGroup > numExistingOrder)
        return false;

    bool allowReordering = false;
    if (!allowReordering)
    {
        // Each of the leading components of the sort criteria need to match the elements in the group.
        ForEachChild(i, normalizedGroupList)
        {
            if (existingOrder->queryChild(i) != normalizedGroupList->queryChild(i))
                return false;
        }
        return true;
    }

    //The leading elements of the sort criteria need to match the leading elements of the group, but the order can be changed.
    //NOTE: The lists cannot contain any duplicates => only need to check for existance.
    HqlExprCopyArray existingComponents;
    unwindChildren(existingComponents, existingOrder);

    ForEachChild(i, normalizedGroupList)
    {
        IHqlExpression * cur = normalizedGroupList->queryChild(i);
        unsigned match = existingComponents.find(*cur);
        if ((match == NotFound) || (match >= numToGroup))
            return false;
    }
    return true;
}


IHqlExpression * ensureSortedForGroup(IHqlExpression * table, IHqlExpression *sortList, bool isLocal, bool alwaysLocal, bool allowSubSort)
{
    if (isSortedForGroup(table, sortList, isLocal||alwaysLocal))
        return LINK(table);

    IHqlExpression * attr = isLocal ? createLocalAttribute() : NULL;
    IHqlExpression * ds = LINK(table);
    if (isGrouped(ds))
        ds = createDataset(no_group, ds, NULL);
    return createDatasetF(no_sort, ds, LINK(sortList), attr, NULL);
}


//If this gets too complex we would need to make sure we don't explode traversing the expression tree.
static bool includesFieldsOutsideGrouping(IHqlExpression * distribution, const HqlExprCopyArray & groups)
{
    if (groups.find(*distribution->queryBody()) != NotFound)
        return false;

    switch (distribution->getOperator())
    {
    case no_hash:
    case no_hash32:
    case no_hash64:
    case no_hashmd5:
    case no_add:
    case no_xor:
    case no_bxor:
    case no_sortlist:
    case no_cast:
    case no_implicitcast:
    case no_negate:
        break;
    case no_field:
    case no_select:
    case no_sortpartition:
        return true;
    case no_constant:
        return false;
    case no_trim:
        if (distribution->hasAttribute(leftAtom) || distribution->hasAttribute(allAtom))
            return false;
        return includesFieldsOutsideGrouping(distribution->queryChild(0), groups);
    case no_attr:
        //may be flags on hash32,trim etc.
        return (distribution->queryName() == unknownAtom);
    default:
        return true;
    }
    ForEachChild(idx, distribution)
    {
        if (includesFieldsOutsideGrouping(distribution->queryChild(idx), groups))
            return true;
    }
    return false;
}


bool isPartitionedForGroup(IHqlExpression * table, IHqlExpression *grouping, bool isGroupAll)
{
    IHqlExpression * distribution = queryDistribution(table);
    if (!isKnownDistribution(distribution) || !distribution->isPure())
        return false;

    OwnedHqlExpr normalizedGrouping = normalizeSortlist(grouping, table);
    unsigned numToGroup = normalizedGrouping->numChildren();
    if (numToGroup == 0)
        return false;           // since they all need transferring to a single node!

    HqlExprCopyArray groupingElements;
    unwindChildren(groupingElements, normalizedGrouping);

    // MORE: Could possibly check if the trailing field of the previous grouping lies in the new grouping fields.
    // If so it implies the new grouping will already be split between nodes, so it can be done locally.
    // But it may have been grouped locally so assumption isn't correct

    if (isSortDistribution(distribution))
    {
        IHqlExpression * sortlist = distribution->queryChild(0);
        if (!isGroupAll)
        {
            //The distribution was a sort, ok if the trailing component of the sort is included in the grouping criteria.
            //If a trailing component is a constant we can test the preceding one since a single valued field can't
            //have been split over multiple nodes.
            unsigned numElements = sortlist->numChildren();
            while (numElements != 0)
            {
                IHqlExpression * element = sortlist->queryChild(numElements-1)->queryBody();
                if (!element->isConstant())
                    return groupingElements.contains(*element);
                numElements--;
            }
            return false;
        }
        //For a group,all all the fields in the sort need to be in the grouping condition.
        distribution = sortlist;
    }

    //The distribution was a non-sort, so ok if all fields in the distribution are included in the grouping criteria.
    return !includesFieldsOutsideGrouping(distribution, groupingElements);
}


bool isPartitionedForGroup(IHqlExpression * table, const HqlExprArray & grouping, bool isGroupAll)
{
    OwnedHqlExpr sortlist = createValueSafe(no_sortlist, makeSortListType(NULL), grouping);
    return isPartitionedForGroup(table, sortlist, isGroupAll);
}



//---------------------------------------------------------------------------
// Helper functions for optimizing sorting...

IHqlExpression * getExistingSortOrder(IHqlExpression * dataset, bool isLocal, bool ignoreGrouping)
{
    if (isLocal)
        return getLocalSortOrder(dataset);
    if (ignoreGrouping || !isGrouped(dataset))
        return getGlobalSortOrder(dataset);
    return LINK(queryGroupSortOrder(dataset));
}


static bool isCorrectDistributionForSort(IHqlExpression * dataset, IHqlExpression * normalizedSortOrder, bool isLocal, bool ignoreGrouping)
{
    if (isLocal || (isGrouped(dataset) && !ignoreGrouping))
        return true;
    IHqlExpression * distribution = queryDistribution(dataset);
    if (distribution == queryAnyDistributionAttribute())
        return true;
    if (!isSortDistribution(distribution))
        return false;
    IHqlExpression * previousOrder = distribution->queryChild(0);           // Already normalized when it was created.
    //MORE: We should possibly loosen this test to allow compatible casts etc.
    //return isCompatibleSortOrder(existingOrder, normalizedSortOrder)
    return (previousOrder == normalizedSortOrder);
}

//--------------------------------------------------------------------------------------------------------------------

static bool isCompatibleSortOrder(IHqlExpression * existingOrder, IHqlExpression * normalizedOrder)
{
    if (normalizedOrder->numChildren() == 0)
        return true;
    if (!existingOrder)
        return false;
    if (existingOrder == queryAnyOrderSortlist())
        return true;
    if (normalizedOrder->numChildren() > existingOrder->numChildren())
        return false;
    ForEachChild(i, normalizedOrder)
    {
        if (!sortComponentMatches(normalizedOrder->queryChild(i), existingOrder->queryChild(i)))
            return false;
    }
    return true;
}

static bool normalizedIsAlreadySorted(IHqlExpression * dataset, IHqlExpression * normalizedOrder, bool isLocal, bool ignoreGrouping, bool requireDistribution)
{
#ifdef OPTIMIZATION2
    if (hasNoMoreRowsThan(dataset, 1))
        return true;
#endif
    if (requireDistribution && !isCorrectDistributionForSort(dataset, normalizedOrder, isLocal, ignoreGrouping))
        return false;

    //Constant items and duplicates should have been removed already.
    OwnedHqlExpr existingOrder = getExistingSortOrder(dataset, isLocal, ignoreGrouping);
    return isCompatibleSortOrder(existingOrder, normalizedOrder);
}


bool isAlreadySorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool requireDistribution)
{
#ifdef OPTIMIZATION2
    if (hasNoMoreRowsThan(dataset, 1))
        return true;
#endif

    OwnedHqlExpr normalizedOrder = normalizeSortlist(order, dataset);
    return normalizedIsAlreadySorted(dataset, normalizedOrder, isLocal, ignoreGrouping, requireDistribution);
}


//Elements in the exprarray have already been mapped;
bool isAlreadySorted(IHqlExpression * dataset, const HqlExprArray & newSort, bool isLocal, bool ignoreGrouping, bool requireDistribution)
{
    HqlExprArray components;
    normalizeComponents(components, newSort);
    OwnedHqlExpr normalizedOrder = createSortList(components);
    return normalizedIsAlreadySorted(dataset, normalizedOrder, isLocal, ignoreGrouping, requireDistribution);
}


//--------------------------------------------------------------------------------------------------------------------

static unsigned numCompatibleSortElements(IHqlExpression * existingOrder, IHqlExpression * normalizedOrder)
{
    if (!existingOrder)
        return 0;
    unsigned numExisting = existingOrder->numChildren();
    unsigned numRequired = normalizedOrder->numChildren();
    unsigned numToCompare = (numRequired > numExisting) ? numExisting : numRequired;
    for (unsigned i=0; i < numToCompare; i++)
    {
        if (!sortComponentMatches(normalizedOrder->queryChild(i), existingOrder->queryChild(i)))
            return i;
    }
    return numToCompare;
}

static unsigned normalizedNumSortedElements(IHqlExpression * dataset, IHqlExpression * normalizedOrder, bool isLocal, bool ignoreGrouping)
{
#ifdef OPTIMIZATION2
    if (hasNoMoreRowsThan(dataset, 1))
        return true;
#endif
    if (!isCorrectDistributionForSort(dataset, normalizedOrder, isLocal, ignoreGrouping))
        return false;

    //Constant items and duplicates should have been removed already.
    OwnedHqlExpr existingOrder = getExistingSortOrder(dataset, isLocal, ignoreGrouping);
    return numCompatibleSortElements(existingOrder, normalizedOrder);
}

static unsigned numElementsAlreadySorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping)
{
#ifdef OPTIMIZATION2
    if (hasNoMoreRowsThan(dataset, 1))
        return order->numChildren();
#endif

    OwnedHqlExpr normalizedOrder = normalizeSortlist(order, dataset);
    return normalizedNumSortedElements(dataset, normalizedOrder, isLocal, ignoreGrouping);
}

//Elements in the exprarray have already been mapped;
static unsigned numElementsAlreadySorted(IHqlExpression * dataset, const HqlExprArray & newSort, bool isLocal, bool ignoreGrouping)
{
    HqlExprArray components;
    normalizeComponents(components, newSort);
    OwnedHqlExpr normalizedOrder = createSortList(components);
    return normalizedNumSortedElements(dataset, normalizedOrder, isLocal, ignoreGrouping);
}

bool isWorthShuffling(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping)
{
    //MORE: Should this look at the cardinality of the already-sorted fields, and not transform if below a certain threshold?
    return numElementsAlreadySorted(dataset, order, isLocal, ignoreGrouping) != 0;
}

bool isWorthShuffling(IHqlExpression * dataset, const HqlExprArray & newSort, bool isLocal, bool ignoreGrouping)
{
    //MORE: Should this look at the cardinality of the already-sorted fields, and not transform if below a certain threshold?
    return numElementsAlreadySorted(dataset, newSort, isLocal, ignoreGrouping) != 0;
}

//--------------------------------------------------------------------------------------------------------------------

//Convert SUBSORT(ds, <sort>, <grouping>, ?LOCAL, options) to
//g := GROUP(ds, grouping, ?LOCAL); s := SORT(g, <sort>, options); GROUP(s);
IHqlExpression * convertSubSortToGroupedSort(IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * newOrder = expr->queryChild(1);
    IHqlExpression * grouping = expr->queryChild(2);

    assertex(!isGrouped(dataset) || expr->hasAttribute(globalAtom));
    OwnedHqlExpr attr = isLocalActivity(expr) ? createLocalAttribute() : NULL;
    OwnedHqlExpr grouped = createDatasetF(no_group, LINK(dataset), LINK(grouping), LINK(attr), NULL);

    HqlExprArray args;
    args.append(*grouped.getClear());
    args.append(*LINK(newOrder));
    unwindChildren(args, expr, 3);
    removeAttribute(args, localAtom);
    OwnedHqlExpr sorted = createDataset(no_sort, args);
    return createDataset(no_group, sorted.getClear());
}

static IHqlExpression * createSubSorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal)
{
    bool isGroupedSubSort = !ignoreGrouping && isGrouped(dataset);
    unsigned sortedElements = numElementsAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping);
    if ((sortedElements == 0) || isGroupedSubSort)
        return NULL;

    HqlExprArray components;
    unwindNormalizeSortlist(components, order, false);
    removeDuplicates(components);
    if (components.ordinality() == sortedElements)
        return LINK(dataset);

    OwnedHqlExpr alreadySorted = createValueSafe(no_sortlist, makeSortListType(NULL), components, 0, sortedElements);
    OwnedHqlExpr newOrder = createValueSafe(no_sortlist, makeSortListType(NULL), components, sortedElements, components.ordinality());

    const bool removeGrouping = ignoreGrouping && isGrouped(dataset);
    OwnedHqlExpr attr = isLocal ? createLocalAttribute() : (isGrouped(dataset) && ignoreGrouping) ? createAttribute(globalAtom) : NULL;
    OwnedHqlExpr input = removeGrouping ? createDataset(no_group, LINK(dataset)) : LINK(dataset);
    OwnedHqlExpr subsort = createDatasetF(no_subsort, LINK(input), LINK(newOrder), LINK(alreadySorted), LINK(attr), NULL);
    //Grouped subsorts never generated, global subsorts (if generated) get converted to a global group
    if (!isLocal && !alwaysLocal)
        subsort.setown(convertSubSortToGroupedSort(subsort));

    assertex(isAlreadySorted(subsort, order, isLocal||alwaysLocal, ignoreGrouping, false));
    return subsort.getClear();
}

IHqlExpression * getSubSort(IHqlExpression * dataset, const HqlExprArray & order, bool isLocal, bool ignoreGrouping, bool alwaysLocal)
{
    if (isAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping, true))  // could possible have requireDistribution = false
        return NULL;

    OwnedHqlExpr sortlist = createValueSafe(no_sortlist, makeSortListType(NULL), order);
    OwnedHqlExpr mappedSortlist = replaceSelector(sortlist, queryActiveTableSelector(), dataset);
    return createSubSorted(dataset, mappedSortlist, isLocal, ignoreGrouping, alwaysLocal);
}

IHqlExpression * getSubSort(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal)
{
    if (isAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping, true))  // could possible have requireDistribution = false
        return NULL;

    return createSubSorted(dataset, order, isLocal, ignoreGrouping, alwaysLocal);
}

//--------------------------------------------------------------------------------------------------------------------

IHqlExpression * ensureSorted(IHqlExpression * dataset, IHqlExpression * order, IHqlExpression * parentExpr, bool isLocal, bool ignoreGrouping, bool alwaysLocal, bool allowSubSort, bool requestSpilling)
{
    if (isAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping, true))
        return LINK(dataset);

    if (allowSubSort && (isLocal || alwaysLocal))
    {
        if (isWorthShuffling(dataset, order, isLocal||alwaysLocal, ignoreGrouping))
        {
            OwnedHqlExpr subsorted = createSubSorted(dataset, order, isLocal, ignoreGrouping, alwaysLocal);
            if (subsorted)
                return subsorted.getClear();
        }
    }
    HqlExprArray args;
    args.append(OLINK(*dataset));
    args.append(OLINK(*order));
    if (isLocal)
        args.append(*createLocalAttribute());
    else if (isGrouped(dataset) && ignoreGrouping)
        args.append(*createAttribute(globalAtom));
    if (requestSpilling)
        args.append(*createAttribute(spillAtom));
    unwindHintAttrs(args, parentExpr);
    return createDataset(no_sort, args);
}

//-------------------------------
// Join optimization - can the join order be changed so there is no need to resort.

bool reorderMatchExistingLocalSort(HqlExprArray & sortedLeft, HqlExprArray & reorderedRight, IHqlExpression * dataset, const HqlExprArray & left, const HqlExprArray & right)
{
    OwnedHqlExpr existingOrder = getLocalSortOrder(dataset);
    unsigned maxLeft = left.ordinality();
    if (!existingOrder || (existingOrder->numChildren() < maxLeft))
        return false;

    for (unsigned i1=0; i1 < maxLeft; i1++)
    {
        IHqlExpression * search = existingOrder->queryChild(i1);
        unsigned matched = NotFound;
        ForEachItemIn(i2, left)
        {
            if (sortComponentMatches(&left.item(i2), search))
            {
                matched = i2;
                break;
            }
        }
        if (matched == NotFound)
            return false;
        //Play very safe on joins on same field, or expressions that could match the same sort order.
        if (sortedLeft.contains(left.item(matched)) || reorderedRight.contains(right.item(matched)))
            return false;
        sortedLeft.append(OLINK(left.item(matched)));
        reorderedRight.append(OLINK(right.item(matched)));
    }
    return true;
}

bool matchDedupDistribution(IHqlExpression * distn, const HqlExprArray & equalities)
{
    //Could probably use reinterpret cast, but that would be nasty
    HqlExprCopyArray cloned;
    appendArray(cloned, equalities);
    return !includesFieldsOutsideGrouping(distn, cloned);
}

bool matchesAnyDistribution(IHqlExpression * distn)
{
    return distn == queryAnyDistributionAttribute();
}

//---------------------------------------------------------------------------

/*
 For a join to be able to be optimized to a local join we need:
 a) The distribution function to have exactly the same form on each side.
 b) All references to fields from the dataset must match the join element
*/

static bool checkDistributedCoLocally(IHqlExpression * distribute1, IHqlExpression * distribute2, const HqlExprArray & sort1, const HqlExprArray & sort2)
{
    unsigned match1 = sort1.find(*distribute1->queryBody());
    unsigned match2 = sort2.find(*distribute2->queryBody());
    if ((match1 != NotFound) || (match2 != NotFound))
        return match1 == match2;

    node_operator op = distribute1->getOperator();
    if (op != distribute2->getOperator())
        return false;

    unsigned max = distribute1->numChildren();
    if (max != distribute2->numChildren())
        return false;

    switch (op)
    {
    case no_select:
    case no_field:
        {
            //recurse?
            return false;
        }
    }

    if (max == 0)
        return true;

    for (unsigned idx = 0; idx < max; idx++)
    {
        if (!checkDistributedCoLocally(distribute1->queryChild(idx), distribute2->queryChild(idx), sort1, sort2))
            return  false;
    }
    return true;
}


//Convert a function of fields referenced in oldSort, to fields referenced in newSort.
IHqlExpression * createMatchingDistribution(IHqlExpression * expr, const HqlExprArray & oldSort, const HqlExprArray & newSort)
{
    unsigned match = oldSort.find(*expr->queryBody());
    if (match != NotFound)
        return LINK(&newSort.item(match));

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_hash:
    case no_hash32:
    case no_hash64:
    case no_hashmd5:
    case no_add:
    case no_xor:
    case no_bxor:
    case no_sortlist:
    case no_cast:
    case no_implicitcast:
    case no_negate:
    case no_trim:
        break;
    case no_field:
    case no_select:
    case no_sortpartition:
        return NULL;
    case no_constant:
        break;
    case no_attr:
    case no_attr_expr:
        {
            IAtom * name = expr->queryName();
            if (name == internalAtom)
            {
                //HASH,internal - only valid if the types of the old and new sorts match exactly
                ForEachItemIn(i, oldSort)
                {
                    if (oldSort.item(i).queryType() != newSort.item(i).queryType())
                        return NULL;
                }
            }
            else if (expr == cacheAnyAttribute)
                return NULL;
            break;
        }
    default:
        return NULL;
    }

    unsigned max = expr->numChildren();
    if (max == 0)
        return LINK(expr);

    HqlExprArray args;
    args.ensure(max);
    ForEachChild(i, expr)
    {
        IHqlExpression * mapped = createMatchingDistribution(expr->queryChild(i), oldSort, newSort);
        if (!mapped)
            return NULL;
        args.append(*mapped);
    }
    return expr->clone(args);
}


static IHqlExpression * queryColocalDataset(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_filter:
            break;
        default:
            return expr->queryBody();
        }
        expr = expr->queryChild(0);
    }
}

//Check if the distribution functions are essentially identical, except for the places 
bool isDistributedCoLocally(IHqlExpression * dataset1, IHqlExpression * dataset2, const HqlExprArray & sort1, const HqlExprArray & sort2)
{
    IHqlExpression * distribute1 = queryDistribution(dataset1);
    IHqlExpression * distribute2 = queryDistribution(dataset2);
    //Check the distribution functions are equivalent - by walking through in parallel, and don't contain any
    //references to fields not in the join conditions
    if (isKnownDistribution(distribute1) && distribute1->isPure() &&
        isKnownDistribution(distribute2) && distribute2->isPure())
    {
        //If sorted they are only going to be codistributed if they came from the same sort
        //We could only determine that by making the sort orders unique - by appending a uid
        //But that hits problems with the values going out of sync.  (needs more investigation why)
        if (isSortedDistribution(distribute1) || isSortedDistribution(distribute2))
        {
            if (distribute1 != distribute2)
                 return false;

            //Self join is guaranteed to be from the same sort
            //NOTE: IF/NONEMPTY return sitribution if the same - but since still only
            //one or the other the dataset will be distributed ok.
            if (queryColocalDataset(dataset1) != queryColocalDataset(dataset2))
                return false;
        }

        if (checkDistributedCoLocally(distribute1, distribute2, sort1, sort2))
            return true;
    }
    return false;
}


//---------------------------------------------------------------------------

inline IHqlExpression * ensureNonNull(IHqlExpression * expr)
{
    if (expr)
        return LINK(expr);
    return LINK(cached_omitted_Attribute);
}

ITypeInfo * createDatasetType(ITypeInfo * recordType, bool isGrouped)
{
    ITypeInfo * rowType = makeRowType(LINK(recordType));
    Owned<ITypeInfo> type = makeTableType(rowType);
    if (isGrouped)
        return makeGroupedTableType(type.getClear());
    return type.getClear();
}

static IHqlExpression * createPreserveTableInfo(IHqlExpression * newTable, IHqlExpression * original, bool loseDistribution, IHqlExpression * persistName)
{
    CHqlMetaInfo meta;
    if (original->isDataset())
        extractMeta(meta, original);
    LinkedHqlExpr distribution = loseDistribution ? NULL : meta.distribution;
    IHqlExpression * globalSort = meta.globalSortOrder;
    IHqlExpression * localSort = meta.localUngroupedSortOrder;
    IHqlExpression * grouping = meta.grouping;
    IHqlExpression * groupSort = meta.groupSortOrder;
    if (persistName && isKnownDistribution(distribution))
    {
        if (!distribution->isAttribute())
        {
            //Cluster size may not match so generate a unique modifier.  Needs to modify enough distribute no longer a nop,
            //but not too much to not get hoisted, or introduce extra dependencies.
            //At the moment bxor with a sequence number since I can't see anyone ever doing that.
            __int64 seq = getExpressionCRC(persistName);
            OwnedHqlExpr uid = createConstant(distribution->queryType()->castFrom(true, seq));
            distribution.setown(createValue(no_bxor, distribution->getType(), LINK(distribution), LINK(uid)));
        }
        else if (isSortDistribution(distribution))
        {
            
            //Sort distribution is still ok to preserve - since the assumption that trailing elements are not
            //split over nodes still holds.
        }
        else
        {
            //keyed - assume the worst
            distribution.clear();
        }
    }
    
    LinkedHqlExpr ret = newTable;
    if (distribution || globalSort || localSort || grouping || groupSort)
        ret.setown(createDatasetF(no_preservemeta, LINK(newTable), ensureNonNull(distribution), ensureNonNull(globalSort), ensureNonNull(localSort),  ensureNonNull(grouping), ensureNonNull(groupSort), NULL));
    return original->cloneAllAnnotations(ret);
}


//Convert preservemeta(wuread) to wuread(,preservemeta), primarily because the optimizer moves datasets
//over the metadata, causing items not to be commoned up in child queries (e.g.,  jholt20.xhql).
//Really needs a better solution
static IHqlExpression * optimizePreserveMeta(IHqlExpression * expr)
{
    if (expr->getOperator() != no_preservemeta)
        return LINK(expr);
    IHqlExpression * ds = expr->queryChild(0);
    switch (ds->getOperator())
    {
    case no_workunit_dataset:
    case no_getgraphresult:
        break;
    default:
        return LINK(expr);
    }

    HqlExprArray args;
    unwindChildren(args, expr, 1);

    //Could be an exprAttribute, but if so, I would need to make sure the activeSelector() is removed from the active tables
    OwnedHqlExpr metaAttr = createAttribute(_metadata_Atom, args);
    OwnedHqlExpr ret = appendOwnedOperand(ds, metaAttr.getClear());
    return expr->cloneAllAnnotations(ret);
}


IHqlExpression * preserveTableInfo(IHqlExpression * newTable, IHqlExpression * original, bool loseDistribution, IHqlExpression * persistName)
{
    OwnedHqlExpr preserved = createPreserveTableInfo(newTable, original, loseDistribution, persistName);
    return optimizePreserveMeta(preserved);
}

//---------------------------------------------------------------------------------------------------------------------

static void getMetaIntersection(CHqlMetaInfo & meta, IHqlExpression * other)
{
    CHqlMetaProperty * otherMetaProp = queryMetaProperty(other);
    meta.getIntersection(otherMetaProp->meta);
}


static void calculateProjectMeta(CHqlMetaInfo & meta, IHqlExpression * parent, IHqlExpression * transform, IHqlExpression * selSeq)
{
    OwnedHqlExpr leftSelect = createSelector(no_left, parent, selSeq);
    TableProjectMapper mapper;
    mapper.setMapping(transform, leftSelect);

    extractMeta(meta, parent);
    meta.applyProject(mapper);
}

CHqlMetaProperty * querySimpleDatasetMeta(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();

    switch (op)
    {
    case no_field:
    case no_selectnth:
    case no_inlinetable:
    case no_dataset_from_transform:
    case no_xmlproject:
    case no_temptable:
    case no_id2blob:
    case no_embedbody:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
        return nullMetaProperty;
    case no_rowsetindex:
    case no_rowsetrange:
    case no_translated:
        return queryNullMetaProperty(isGrouped(expr->queryChild(0)));
    case no_param:
        return queryNullMetaProperty(isGrouped(expr));
    case no_pipe:
        return queryNullMetaProperty(expr->hasAttribute(groupAtom));
    case no_alias_project:
    case no_alias_scope:
    case no_cachealias:
    case no_cloned:
    case no_globalscope:
    case no_comma:
    case no_filter:
    case no_keyed:
    case no_nofold:
    case no_nohoist:
    case no_section:
    case no_sectioninput:
    case no_sub:
    case no_thor:
    case no_nothor:
    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
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
    case no_metaactivity:
    case no_split:
    case no_spill:
    case no_readspill:
    case no_commonspill:
    case no_writespill:
    case no_throughaggregate:
    case no_limit:
    case no_catchds:
    case no_keyedlimit:
    case no_compound_fetch:
    case no_preload:
    case no_alias:
    case no_catch:
    case no_activerow:
    case no_newrow:
    case no_assert_ds:
    case no_spillgraphresult:
    case no_cluster:
    case no_forcenolocal:
    case no_thisnode:
    case no_forcelocal:
    case no_filtergroup:
    case no_forcegraph:
    case no_related:
    case no_executewhen:
    case no_outofline:
    case no_fieldmap:
    case no_owned_ds:
    case no_dataset_alias:
    case no_funcdef:
        return queryMetaProperty(expr->queryChild(0));
    case no_compound:
    case no_select:
    case no_mapto:
        return queryMetaProperty(expr->queryChild(1));
    case no_delayedselect:
    case no_libraryselect:
    case no_unboundselect:
        return queryMetaProperty(expr->queryChild(2));
    }
    return NULL;
}

void calculateDatasetMeta(CHqlMetaInfo & meta, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    IHqlExpression * dataset = expr->queryChild(0);

    //Following need to be filled in ready for type creation at the end...
    //gather all the type rules together so we don't get inconsistencies.
    switch (op)
    {
    case no_field:
    case no_selectnth:
    case no_inlinetable:
    case no_dataset_from_transform:
    case no_xmlproject:
    case no_temptable:
    case no_id2blob:
    case no_embedbody:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
        break;
    case no_rowsetindex:
    case no_rowsetrange:
    case no_translated:
        if (isGrouped(expr->queryChild(0)))
            meta.setUnknownGrouping();
        break;
    case no_param:
        if (isGrouped(expr))
            meta.setUnknownGrouping();
        break;
    case no_pipe:
        if (expr->queryAttribute(groupAtom))
            meta.setUnknownGrouping();
        break;
    case no_alias_project:
    case no_alias_scope:
    case no_cachealias:
    case no_cloned:
    case no_globalscope:
    case no_comma:
    case no_filter:
    case no_keyed:
    case no_nofold:
    case no_nohoist:
    case no_section:
    case no_sectioninput:
    case no_sub:
    case no_thor:
    case no_nothor:
    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
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
    case no_metaactivity:
    case no_split:
    case no_spill:
    case no_readspill:
    case no_commonspill:
    case no_writespill:
    case no_throughaggregate:
    case no_limit:
    case no_catchds:
    case no_keyedlimit:
    case no_compound_fetch:
    case no_preload:
    case no_alias:
    case no_catch:
    case no_activerow:
    case no_newrow:
    case no_assert_ds:
    case no_spillgraphresult:
    case no_cluster:
    case no_forcenolocal:
    case no_thisnode:
    case no_forcelocal:
    case no_filtergroup:
    case no_forcegraph:
    case no_related:
    case no_executewhen:
    case no_outofline:
    case no_fieldmap:
    case no_owned_ds:
    case no_dataset_alias:
    case no_funcdef:
        extractMeta(meta, dataset);
        break;
    case no_compound:
    case no_select:
    case no_mapto:
        extractMeta(meta, expr->queryChild(1));
        break;
    case no_delayedselect:
    case no_libraryselect:
    case no_unboundselect:
        extractMeta(meta, expr->queryChild(2));
        break;
    case no_table:
        {
            IHqlExpression * grouping = expr->queryAttribute(groupedAtom);
            if (grouping)
                meta.grouping.set(queryUnknownSortlist());
            break;
        }
    case no_null:
    case no_fail:
    case no_anon:
    case no_pseudods:
    case no_skip:
    case no_all:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_getresult:
    case no_rows:
    case no_internalselect:
    case no_purevirtual:
    case no_libraryinput:
        {
            IHqlExpression * metadata = expr->queryAttribute(_metadata_Atom);
            if (!metadata)
            {
                IHqlExpression * distributed = expr->queryAttribute(distributedAtom);
                IHqlExpression * distribution = distributed ? distributed->queryChild(0) : NULL;
                meta.distribution.set(distribution);

                IHqlExpression * grouped = expr->queryAttribute(groupedAtom);
                if (grouped)
                {
                    IHqlExpression * groupExpr = grouped->queryChild(0);
                    assertex(!groupExpr || groupExpr->getOperator() == no_sortlist);
                    if (!groupExpr)
                        groupExpr = queryUnknownSortlist();
                    meta.grouping.set(groupExpr);
                }
            }
            else
                extractMetaFromMetaAttr(meta, metadata, 0);

            if ((op == no_null) || (op == no_fail))
            {
                meta.setMatchesAny();
                if (meta.grouping)
                    meta.grouping.set(queryAnyOrderSortlist());
            }
            break;
        }
    case no_combine:
    case no_combinegroup:
        {
            calculateProjectMeta(meta, expr->queryChild(0), expr->queryChild(2), expr->queryAttribute(_selectorSequence_Atom));
            //Not at all sure about this wierd case
            break;
        }
    case no_process:
        {
            calculateProjectMeta(meta, expr->queryChild(0), expr->queryChild(2), expr->queryAttribute(_selectorSequence_Atom));
            break;
        }
    case no_fetch:
        {
            //Currently fetch is assumed to preserve nothing (since I suspect thor doesn't)
            break;
        }
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_selfjoin:
    case no_joincount:
        {
            IHqlExpression * transform = expr->queryChild(3);

            //JOIN does not preserve sort order or grouping.
            //It does preserve distribution if distribution fields are projected
            bool isLookupJoin = expr->queryAttribute(lookupAtom) != NULL;
            bool isAllJoin = expr->queryAttribute(allAtom) != NULL;
            bool isHashJoin = expr->queryAttribute(hashAtom) != NULL;
            bool isSmartJoin = expr->queryAttribute(smartAtom) != NULL;
            bool isStreamedJoin = expr->queryAttribute(streamedAtom) != NULL;
            bool isKeyedJoin = !isAllJoin && !isLookupJoin && !isSmartJoin && !isStreamedJoin && (expr->queryAttribute(keyedAtom) || isKey(expr->queryChild(1)));
            bool isLocal = (expr->queryAttribute(localAtom) != NULL);
            bool fo = expr->queryAttribute(fullonlyAtom) || expr->queryAttribute(fullouterAtom);
            bool createDefaultLeft = fo || expr->queryAttribute(rightonlyAtom) || expr->queryAttribute(rightouterAtom);
            bool createDefaultRight = fo || expr->queryAttribute(leftonlyAtom) || expr->queryAttribute(leftouterAtom);

            OwnedHqlExpr leftSelect = createSelector(no_left, expr->queryChild(0), expr->queryAttribute(_selectorSequence_Atom));
            if (isKeyedJoin || isAllJoin || isLookupJoin)
            {
                CHqlMetaInfo & parentMeta = queryMetaProperty(dataset)->meta;
                //If default left hand records created, then can't preserve distn etc.
                meta.set(parentMeta);
                if (!createDefaultLeft)
                {
                    bool preservesOrder = !expr->queryAttribute(unorderedAtom);
                    if (isKeyedJoin)
                        preservesOrder = expr->queryAttribute(_ordered_Atom) != NULL;
                    if (!preservesOrder)
                        meta.removeAllSortOrders();

                    LinkedHqlExpr mapTransform = transform;
                    // If there is a join equality LEFT.x = RIGHT.x, and the transform contains a reference to RIGHT.x
                    // substitute LEFT.x instead.
                    // The modified transform is used for mapping the sort/grouping information
                    // which means that information about sort orders are more likely to be preserved (for keyed joins).
                    if (!createDefaultRight && ((op == no_join) || (op == no_selfjoin)))
                    {
                        //Only bother to modify the transform if something useful is going to be mapped in
                        //the meta information - otherwise this can be expensive for no gain.
                        if (meta.hasUsefulInformation())
                        {
                            JoinEqualityMapper mapper(expr);
                            mapTransform.setown(mapper.mapEqualities(transform, expr->queryChild(2)));
                        }
                    }
                    TableProjectMapper mapper;
                    mapper.setMapping(mapTransform, leftSelect);
                    meta.applyProject(mapper);

                    //For no_denormalize information is only preserved if it is the same whether or not the transform was called.
                    if (op == no_denormalize)
                        meta.getIntersection(parentMeta);
                }
                else
                    meta.removeAllKeepGrouping();

                //The grouping fields could be mapped using the transform to provide more information, but it is
                //unlikely to provide scope for other optimizations, and it will soon be replaced with the expanded
                //implementation which will track map the information.
                if (expr->queryAttribute(groupAtom))
                    meta.setUnknownGrouping();
            }
            else if (isLocal)
            {
                CHqlMetaInfo ungroupedMeta;
                CHqlMetaInfo parentMeta = queryMetaProperty(dataset)->meta;
                ungroupedMeta.set(parentMeta);
                ungroupedMeta.removeGroup();

                //local operation so try and preserve the current distribution, no clue about the following sort order,
                //and result is never grouped.
                if (expr->queryAttribute(_lightweight_Atom) && !createDefaultLeft)
                {
                    //Implementation detail: lightweight joins preserve the lhs sort order
                    //Can be very useful for converting subsequent joins to lightweight joins.
                    TableProjectMapper mapper;
                    mapper.setMapping(transform, leftSelect);

                    meta.set(ungroupedMeta);
                    if (expr->hasAttribute(unorderedAtom))
                        meta.removeAllSortOrders();
                    meta.applyProject(mapper);
                }
                else
                {
                    IHqlExpression * leftDistributeInfo = queryDistribution(expr->queryChild(0));
                    IHqlExpression * rightDistributeInfo = queryDistribution(expr->queryChild(1));

                    IHqlExpression * newDistributeInfo = NULL;
                    if (isKnownDistribution(leftDistributeInfo) || isKnownDistribution(rightDistributeInfo))
                    {
                        TableProjectMapper mapper;
                        mapper.setMapping(transform, NULL);

                        if (isKnownDistribution(leftDistributeInfo) && !createDefaultLeft)
                            newDistributeInfo = mapJoinDistribution(mapper, leftDistributeInfo, leftSelect);
                        if (!newDistributeInfo && isKnownDistribution(rightDistributeInfo) && !createDefaultRight)
                        {
                            OwnedHqlExpr rightSelect = createSelector(no_right, expr->queryChild(1), expr->queryAttribute(_selectorSequence_Atom));
                            newDistributeInfo = mapJoinDistribution(mapper, rightDistributeInfo, rightSelect);
                        }
                    }
                    if (!newDistributeInfo)
                        newDistributeInfo = getUnknownAttribute();
                    meta.distribution.setown(newDistributeInfo);
                }

                //For no_denormalize information is only preserved if it is the same whether or not the transform was called.
                if (op == no_denormalize)
                    meta.getIntersection(ungroupedMeta);
            }
            else if (isHashJoin)
            {
                meta.distribution.setown(getUnknownAttribute());
            }
            else
            {
                //Nothing known
            }
            break;
        }
    case no_dedup:
        {
            extractMeta(meta, dataset);
            bool isLocal = expr->hasAttribute(localAtom);
            bool hasAll = expr->hasAttribute(hashAtom) || expr->hasAttribute(allAtom);

            if (!meta.isGrouped())
            {
                //dedup,all kills the sort order, and global removes the distribution
                if (hasAll)
                {
                    meta.removeAllAndUngroup(isLocal);
                }
                else if (!isLocal)
                    meta.removeDistribution();
            }
            else
            {
                //Some implementations of Dedup all within a group can kill the group sort order
                if (hasAll)
                    meta.removeActiveSort();
            }
            break;
        }
    case no_group:
    case no_grouped:
    case no_assertgrouped:
        {
            extractMeta(meta, dataset);
            IHqlExpression * grouping = queryRealChild(expr, 1);
            if (grouping)
            {
                OwnedHqlExpr mappedGrouping = replaceSelector(grouping, dataset, queryActiveTableSelector());
                bool isLocal = expr->queryAttribute(localAtom) != NULL;
                if (expr->queryAttribute(allAtom))
                {
                    //group,all destroys any previous sort order, may destroy distribution.
                    meta.removeAllAndUngroup(isLocal);
                }
                meta.applyGroupBy(mappedGrouping, isLocal);
            }
            else
                meta.removeGroup();
            break;
        }
    case no_distribute:
    case no_distributed:
    case no_assertdistributed:
        {
            if (expr->queryAttribute(skewAtom))
            {
                meta.setUnknownDistribution();
            }
            else
            {
                OwnedHqlExpr mappedDistributeInfo = replaceSelector(expr->queryChild(1), dataset, queryActiveTableSelector());
                IHqlExpression * sorted = expr->queryAttribute(mergeAtom);
                if (sorted)
                {
                    OwnedHqlExpr mappedSorted = replaceSelector(sorted->queryChild(0), dataset, queryActiveTableSelector());
                    meta.applyDistribute(mappedDistributeInfo, mappedSorted);
                }
                else
                    meta.applyDistribute(mappedDistributeInfo, NULL);
            }
            break;
        }
    case no_preservemeta:
        {
            meta.distribution.set(queryRemoveOmitted(expr->queryChild(1)));
            meta.globalSortOrder.set(queryRemoveOmitted(expr->queryChild(2)));
            meta.localUngroupedSortOrder.set(queryRemoveOmitted(expr->queryChild(3)));
            meta.grouping.set(queryRemoveOmitted(expr->queryChild(4)));
            meta.groupSortOrder.set(queryRemoveOmitted(expr->queryChild(5)));
            break;
        }
    case no_keyeddistribute:
        {
            //destroy grouping and sort order, sets new distribution info
            //to be usable this needs to really save a reference to the actual index used.
            OwnedHqlExpr newDistribution = createAttribute(keyedAtom, replaceSelector(expr->queryChild(1), dataset, queryActiveTableSelector()));
            meta.applyDistribute(newDistribution, NULL);
            break;
        }
    case no_subsort:
        {
            bool isLocal = expr->queryAttribute(localAtom) != NULL;
            OwnedHqlExpr normalizedSortOrder = replaceSelector(expr->queryChild(1), dataset, queryActiveTableSelector());
            OwnedHqlExpr mappedGrouping = replaceSelector(expr->queryChild(2), dataset, queryActiveTableSelector());
            extractMeta(meta, dataset);
            meta.applySubSort(mappedGrouping, normalizedSortOrder, isLocal);
            break;
        }
    case no_cosort:
    case no_sort:
    case no_sorted:
    case no_assertsorted:
    case no_topn:
    case no_stepped:            // stepped implies the sort order matches the stepped criteria
        {
            OwnedHqlExpr normalizedSortOrder;
            bool isLocal = expr->hasAttribute(localAtom);
            bool hasGlobal = expr->hasAttribute(globalAtom);
            IHqlExpression * sortOrder = queryRealChild(expr, 1);
            if (sortOrder)
                normalizedSortOrder.setown(replaceSelector(sortOrder, dataset, queryActiveTableSelector()));

            if (!isLocal && (hasGlobal || !isGrouped(dataset)))
            {
                meta.applyGlobalSort(normalizedSortOrder);
                if ((op == no_topn) || (op == no_sorted))
                    meta.removeDistribution();
            }
            else
            {
                extractMeta(meta, dataset);
                if (isLocal || expr->queryAttribute(globalAtom))
                    meta.removeGroup();
                if (meta.isGrouped())
                    meta.applyGroupSort(normalizedSortOrder);
                else if (isLocal)
                    meta.applyLocalSort(normalizedSortOrder);
                else
                    throwUnexpected(); // should have been handled by the global branch aboce
            }
            break;
        }
    case no_quantile:
        //It is not be safe to assume that all implementations of the quantile activity will generate their results in order.
        if (expr->hasAttribute(localAtom) || isGrouped(dataset))
        {
            IHqlExpression * transform = expr->queryChild(3);
            OwnedHqlExpr leftSelect = createSelector(no_left, dataset, expr->queryAttribute(_selectorSequence_Atom));
            TableProjectMapper mapper;
            mapper.setMapping(transform, leftSelect);

            extractMeta(meta, dataset);
            meta.removeAllSortOrders();
            meta.applyProject(mapper);  // distribution preserved if local..
        }
        else
            meta.preserveGrouping(dataset);
        break;
    case no_iterate:
    case no_transformebcdic:
    case no_transformascii:
    case no_hqlproject:
    case no_normalize:
    case no_rollup:
    case no_newparse:
    case no_newxmlparse:
    case no_rollupgroup:
        {
            //These may lose the sort order because they may modify the sort fields or change the
            //projected fields.  Grouping also translated, but remains even if no mapping.
            TableProjectMapper mapper;
            IHqlExpression * transform;
            bool globalActivityTransfersRows = false;
            OwnedHqlExpr leftSelect = createSelector(no_left, dataset, expr->queryAttribute(_selectorSequence_Atom));
            switch (op)
            {
            case no_rollup:
                transform = expr->queryChild(2);
                globalActivityTransfersRows = true;
                mapper.setMapping(transform, leftSelect);
                break;
            case no_normalize:
                transform = expr->queryChild(2);
                mapper.setMapping(transform, leftSelect);
                break;
            case no_newxmlparse:
                transform = expr->queryChild(3);
                mapper.setMapping(transform, leftSelect);
                break;
            case no_newparse:
                transform = expr->queryChild(4);
                mapper.setMapping(transform, leftSelect);
                break;
            case no_iterate:
                {
                    OwnedHqlExpr rightSelect = createSelector(no_right, dataset, expr->queryAttribute(_selectorSequence_Atom));
                    transform = expr->queryChild(1);
                    globalActivityTransfersRows = true;
                    mapper.setMapping(transform, rightSelect);          // only if keep from right will it be preserved.
                    break;
                }
            case no_transformebcdic:
            case no_transformascii:
            case no_hqlproject:
            case no_rollupgroup:
                transform = expr->queryChild(1);
                mapper.setMapping(transform, leftSelect);
                break;
            default:
                throwUnexpected();
            }

            extractMeta(meta, dataset);
            if (globalActivityTransfersRows && !expr->queryAttribute(localAtom) && !meta.isGrouped())
                meta.removeDistribution();
            if (op == no_rollupgroup)
                meta.removeGroup();

            meta.applyProject(mapper);
            //Tag a count project as sorted in some way, we could spot which field (if any) was initialised with it.
            if ((op == no_hqlproject) && expr->hasAttribute(_countProject_Atom))
            {
                bool isLocal = expr->hasAttribute(localAtom);
                meta.ensureAppearsSorted(isLocal, false);
            }
            break;
        }
    case no_keyindex:
    case no_newkeyindex:
        {
            if (expr->queryAttribute(sortedAtom))
            {
                IHqlExpression * record = expr->queryChild(1);
                HqlExprArray sortExprs;
                if (expr->queryAttribute(sort_KeyedAtom))
                {
                    IHqlExpression * payloadAttr = expr->queryAttribute(_payload_Atom);
                    bool hasFileposition = getBoolAttribute(expr, filepositionAtom, true);
                    unsigned payloadCount = payloadAttr ? (unsigned)getIntValue(payloadAttr->queryChild(0), 1) : hasFileposition ? 1 : 0;
                    unsigned payloadIndex = firstPayloadField(record, payloadCount);
                    unwindRecordAsSelects(sortExprs, record, queryActiveTableSelector(), payloadIndex);
                }
                else
                    unwindRecordAsSelects(sortExprs, record, queryActiveTableSelector());

                OwnedHqlExpr sortOrder = createSortList(sortExprs);
                if (expr->queryAttribute(noRootAtom))
                    meta.applyLocalSort(sortOrder);
                else
                    meta.applyGlobalSort(sortOrder);
            }
            break;
        }
    case no_soapcall_ds:
    case no_newsoapcall_ds:
        meta.preserveGrouping(dataset);
        break;
    case no_parse:
    case no_xmlparse:
        {
            //Assume we can't work out anything about the sort order/grouping/distribution.
            //Not strictly true, but it will do for the moment.
            meta.preserveGrouping(dataset);
            break;
        }
    case no_selectfields:
    case no_aggregate:
    case no_newaggregate:
    case no_newusertable:
    case no_usertable:
        {
            IHqlExpression * record = expr->queryChild(1);
            if (record->getOperator() == no_null)
            {
                extractMeta(meta, dataset);
                break;
            }

            TableProjectMapper mapper;
            record = record->queryRecord();

            IHqlExpression * grouping = NULL;
            IHqlExpression * mapping = NULL;
            LinkedHqlExpr selector = dataset;
            switch (op)
            {
            case no_usertable:
            case no_selectfields:
                mapping = record;
                grouping = expr->queryChild(2);
                break;
            case no_aggregate:
                selector.setown(createSelector(no_left, dataset, expr->queryAttribute(_selectorSequence_Atom)));
                if (!expr->hasAttribute(mergeTransformAtom))
                    mapping = expr->queryChild(2);
                grouping = expr->queryChild(3);
                break;
            case no_newaggregate:
            case no_newusertable:
                mapping = expr->queryChild(2);
                grouping = expr->queryChild(3);
                break;
            }

            if (!mapping)
                mapper.setUnknownMapping();
            else
                mapper.setMapping(mapping, selector);

            if (grouping && grouping->isAttribute())
                grouping = NULL;

            extractMeta(meta, dataset);
            if (grouping)
            {
                if (expr->hasAttribute(groupedAtom))
                {
                    //A grouped hash aggregate - the sort order within the groups will be lost.
                    meta.removeActiveSort();
                }
                else
                {
                    //grouping causes the sort order (and distribution) to be lost - because it might be done by a hash aggregate.
                    meta.removeAllSortOrders();
                    if (!expr->queryAttribute(localAtom))
                        meta.setUnknownDistribution();      // will be distributed by some function of the grouping fields

                    //Aggregation removes grouping, unless explicitly marked as a grouped operation
                    meta.removeGroup();
                }
            }
            else
            {
                //Aggregation removes grouping
                if (op == no_newaggregate || op == no_aggregate || (mapping && mapping->isGroupAggregateFunction()))
                    meta.removeGroup();
            }
            //Now map any fields that we can.
            meta.applyProject(mapper);
            break;
        }
    case no_nonempty:
        {
            //We can take the intersection of the input types for non empty since only one is read.
            extractMeta(meta, dataset);
            ForEachChildFrom(i, expr, 1)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (!cur->isAttribute())
                    getMetaIntersection(meta, cur);
            }
            break;
        }
    case no_addfiles:
    case no_regroup:
    case no_cogroup:
        {
            // Note Concatenation destroys sort order
            // If all the source files have the same distribution then preserve it, else just mark as distributed...
            bool sameDistribution = true;
            bool allInputsIdentical = true;
            IHqlExpression * distributeInfo = queryDistribution(dataset);
            bool allGrouped = isGrouped(dataset);
            ForEachChildFrom(i, expr, 1)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (!cur->isAttribute())
                {
                    if (cur != dataset)
                        allInputsIdentical = false;
                    IHqlExpression * curDist = queryDistribution(cur);
                    if (!curDist)
                        distributeInfo = NULL;
                    else if (curDist != distributeInfo)
                        sameDistribution = false;

                    if (!isGrouped(cur))
                        allGrouped = false;
                }
            }

            IHqlExpression * newDistribution = NULL;
            if (distributeInfo)
            {
                //sort distributions are not identical - except in the unusual case where
                //the inputs are identical (e.g., x + x).  Can change if uids get created for sorts.
                if (sameDistribution && (!isSortedDistribution(distributeInfo) || allInputsIdentical))
                    newDistribution = LINK(distributeInfo);
                else
                    newDistribution = getUnknownAttribute();
            }

            meta.distribution.setown(newDistribution);
            if (allGrouped || (op == no_cogroup))
                meta.setUnknownGrouping();
            break;
        }
    case no_chooseds:
    case no_datasetlist:
    case no_case:
    case no_map:
        {
            //Activities that pick one of the inputs => the meta is the intersection
            unsigned firstDataset = ((op == no_chooseds) || (op == no_case)) ? 1 : 0;
            ForEachChildFrom(i, expr, firstDataset)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (i == firstDataset)
                    extractMeta(meta, cur);
                else
                    getMetaIntersection(meta, cur);
            }
            break;
        }
    case no_normalizegroup:
        {
            //Not very nice - it is hard to track anything through a group normalize.
            if (queryDistribution(dataset))
                meta.setUnknownDistribution();
            meta.preserveGrouping(dataset);
            break;
        }
    case no_if:
        {
            IHqlExpression * left = expr->queryChild(1);
            IHqlExpression * right = expr->queryChild(2);
            if (left->getOperator() == no_null)
                extractMeta(meta, right);
            else if (right->getOperator() == no_null)
                extractMeta(meta, left);
            else
            {
                extractMeta(meta, left);
                getMetaIntersection(meta, right);
            }
            break;
        }
    case no_merge:
        {
            HqlExprArray components;
            IHqlExpression * order= expr->queryAttribute(sortedAtom);       // already uses no_activetable to refer to dataset
            assertex(order);
            unwindChildren(components, order);
            OwnedHqlExpr sortlist = createSortList(components);
            if (expr->queryAttribute(localAtom))
            {
                extractMeta(meta, dataset);
                meta.applyLocalSort(sortlist);
            }
            else
            {
                meta.globalSortOrder.set(sortlist);
                meta.localUngroupedSortOrder.set(sortlist);
            }
            break;
        }
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        {
            IHqlExpression * order = NULL;
            switch (op)
            {
            case no_mergejoin:
                order = expr->queryChild(2);
                break;
            case no_nwayjoin:
                order = expr->queryChild(3);
                break;
            case no_nwaymerge:
                order = expr->queryChild(1);
                break;
            }
            assertex(order);
            IHqlExpression * selSeq = expr->queryAttribute(_selectorSequence_Atom);
            OwnedHqlExpr selector = createSelector(no_left, expr->queryChild(0), selSeq);
            OwnedHqlExpr normalizedOrder = replaceSelector(order, selector, queryActiveTableSelector());
            HqlExprArray components;
            unwindChildren(components, normalizedOrder);
            OwnedHqlExpr sortlist = createSortList(components);
            //These are all currently implemented as local activities, need to change following if no longer true
            extractMeta(meta, expr->queryChild(0));
            meta.applyLocalSort(sortlist);
            break;
        }
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
        //grouped preserves everything
        //otherwise it preserves distribution, global and local order (no data is transferred even for global), but not the grouping.
        if (expr->queryAttribute(groupedAtom))
        {
            extractMeta(meta, dataset);
        }
        else
        {
            extractMeta(meta, dataset);
            meta.removeGroup();
        }
        break;
    case no_allnodes:
        {
            IHqlExpression * merge = expr->queryAttribute(mergeAtom);
            if (merge)
            {
                //more - sort order defined
            }
            break;
        }
    case no_colon:
        //Persist shouldn't preserve the distribution, since can't guarantee done on same width cluster.
        if (queryOperatorInList(no_persist, expr->queryChild(1)))
        {
            extractMeta(meta, dataset);
            meta.setUnknownDistribution();
            if (isGrouped(dataset))
                meta.setUnknownGrouping();
        }
        else if (queryOperatorInList(no_stored, expr->queryChild(1)))
        {
            meta.preserveGrouping(dataset);
        }
        else
            extractMeta(meta, dataset);
        break;
    case no_loop:
        {
            IHqlExpression * body = expr->queryChild(4);
            extractMeta(meta, dataset);
            getMetaIntersection(meta, body->queryChild(0));
            break;
        }
    case no_graphloop:
        {
            IHqlExpression * body = expr->queryChild(2);
            extractMeta(meta, dataset);
            getMetaIntersection(meta, body->queryChild(0));
            break;
        }
    case no_serialize:
        {
            meta.preserveGrouping(dataset);
            break;
        }
    case no_deserialize:
        {
            meta.preserveGrouping(dataset);
            break;
        }
    case no_call:
        //MORE: ?
        if (isGrouped(expr))
            meta.setUnknownGrouping();
        break;
    case no_externalcall:
    case no_external:
        if (isGrouped(expr))
            meta.setUnknownGrouping();
        //No support for grouping?
        break;
    default:
        if (expr->isDataset())
            UNIMPLEMENTED_XY("Type meta for dataset operator", getOpString(op));
        break;
    }

    assertex(isGrouped(expr) == (meta.grouping != NULL));
#ifdef _DEBUG
    assertex(!meta.grouping || meta.grouping->getOperator() == no_sortlist);
    assertex(!meta.globalSortOrder || meta.globalSortOrder->getOperator() == no_sortlist);
#endif

}

ITypeInfo * calculateDatasetType(node_operator op, const HqlExprArray & parms)
{
    IHqlExpression * dataset = NULL;
    ITypeInfo * datasetType = NULL;
    ITypeInfo * childType = NULL;
    ITypeInfo * recordType = NULL;

    switch (op)
    {
    case no_activetable:
        throwUnexpected();
    case no_table:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_all:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_fail:
    case no_skip:
    case no_datasetfromrow:
    case no_datasetfromdictionary:
    case no_if:
    case no_translated:
    case no_rows:
        break;
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        datasetType = parms.item(0).queryType()->queryChildType();
        break;
    case no_chooseds:
    case no_compound:
    case no_select:
        dataset = &parms.item(1);
        break;
    default:
        dataset = &parms.item(0);
        break;
    }

    if (dataset)
        datasetType = dataset->queryType();

    if (datasetType)
    {
        childType = datasetType->queryChildType();
        ITypeInfo * rowType = NULL;
        switch (datasetType->getTypeCode())
        {
        case type_groupedtable:
            rowType = childType->queryChildType();
            break;
        case type_row:
            rowType = datasetType;
            break;
        case type_record:
            recordType = datasetType;
            break;
        default:
            rowType = childType;
            break;
        }
        if (rowType)
            recordType = rowType->queryChildType();
    }

    //Following need to be filled in ready for type creation at the end...
    //gather all the type rules together so we don't get inconsistencies.
    Owned<ITypeInfo> type;
    Owned<ITypeInfo> newRecordType;
    unsigned recordArg = NotFound;
    bool linkCounted = false;
    bool streamed = false;
    bool nowGrouped = false;
    switch (op)
    {
    case no_table:
        {
            assertex(parms.isItem(1));
            Linked<IHqlExpression> recorddef = &parms.item(1);
            newRecordType.setown(createRecordType(recorddef));
            nowGrouped = hasAttribute(groupedAtom, parms);
            break;
        }
    case no_null:
    case no_fail:
    case no_anon:
    case no_pseudods:
    case no_skip:
    case no_all:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_getresult:
    case no_rows:
    case no_internalselect:
    case no_delayedselect:
    case no_unboundselect:
    case no_libraryselect:
    case no_purevirtual:
    case no_libraryinput:
        {
            IHqlExpression * record = parms.item(0).queryRecord();
            recordArg = 0;
            nowGrouped = hasAttribute(groupedAtom, parms);
            linkCounted = (hasAttribute(_linkCounted_Atom, parms) || recordRequiresLinkCount(record));
            break;
        }
    case no_translated:
        type.setown(parms.item(0).getType());
        assertex(parms.ordinality()>1 || hasStreamedModifier(type));     // should have a count or a length
        break;
    case no_inlinetable:
    case no_dataset_from_transform:
    case no_xmlproject:
    case no_temptable:
    case no_id2blob:
    case no_embedbody:
        newRecordType.setown(createRecordType(&parms.item(1)));
        linkCounted = hasAttribute(_linkCounted_Atom, parms);
        streamed = hasAttribute(streamedAtom, parms);
        break;
    case no_pipe:
        {
            nowGrouped=hasAttribute(groupAtom, parms);
            if (parms.isItem(2) && (parms.item(2).getOperator() == no_record))
                newRecordType.setown(createRecordType(&parms.item(2)));
            else
                newRecordType.set(recordType);
            break;
        }

//Records providing the format, can hopefully combine with the transforms soon.
    case no_keyindex:
    case no_newkeyindex:
        {
            recordArg = 1;
            break;
        }
    case no_selectfields:
    case no_aggregate:
    case no_newaggregate:
    case no_newusertable:
    case no_usertable:
        {
            IHqlExpression * record = &parms.item(1);
            if (record->getOperator() == no_null)
            {
                type.set(datasetType);
                break;
            }

            IHqlExpression * grouping = NULL;
            IHqlExpression * mapping = NULL;
            switch (op)
            {
            case no_usertable:
            case no_selectfields:
                mapping = record;
                if (parms.isItem(2))
                    grouping = &parms.item(2);
                break;
            case no_aggregate:
                if (!hasAttribute(mergeTransformAtom, parms))
                    mapping = &parms.item(2);
                if (parms.isItem(3))
                    grouping = &parms.item(3);
                break;
            case no_newaggregate:
            case no_newusertable:
                mapping = &parms.item(2);
                if (parms.isItem(3))
                    grouping = &parms.item(3);
                break;
            }

            recordArg = 1;
            if (grouping && grouping->isAttribute())
                grouping = NULL;

            if (grouping)
            {
                if (hasAttribute(groupedAtom, parms))
                {
                    nowGrouped = isGrouped(datasetType);
                }
                else
                {
                    nowGrouped = false;
                }
            }
            else
            {
                //Aggregation removes grouping
                if (op == no_newaggregate || op == no_aggregate || (mapping && mapping->isGroupAggregateFunction()))
                    nowGrouped=false;
                else
                    nowGrouped = isGrouped(datasetType);
            }
            break;
        }
    case no_httpcall:
    case no_soapcall:
        recordArg = 3;
        break;
    case no_newsoapcall:
        recordArg = 4;
        break;
    case no_quantile:
        recordArg = 3;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_soapcall_ds:
        recordArg = 4;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_newsoapcall_ds:
        recordArg = 5;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_parse:
        recordArg = 3;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_xmlparse:
        recordArg = 2;
        nowGrouped = isGrouped(datasetType);
        break;
//Transforms providing the format, can hopefully combine with the transforms soon.
    case no_iterate:
    case no_transformebcdic:
    case no_transformascii:
    case no_hqlproject:
        recordArg = 1;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_rollupgroup:
        recordArg = 1;
        break;
    case no_combine:
    case no_combinegroup:
    case no_process:
    case no_normalize:
    case no_rollup:
        recordArg = 2;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_selfjoin:
    case no_joincount:
        {
            bool isLookupJoin = queryAttribute(lookupAtom, parms) != NULL;
            bool isAllJoin = queryAttribute(allAtom, parms) != NULL;
            bool isSmartJoin = queryAttribute(smartAtom, parms) != NULL;
            bool isStreamedJoin = queryAttribute(streamedAtom, parms) != NULL;
            bool isKeyedJoin = !isAllJoin && !isLookupJoin && !isSmartJoin && !isStreamedJoin && (queryAttribute(keyedAtom, parms) || isKey(&parms.item(1)));

            recordArg = 3;
            if (queryAttribute(groupAtom, parms))
                nowGrouped = true;
            else if (isKeyedJoin || isAllJoin || isLookupJoin)
                nowGrouped = isGrouped(datasetType);
            else
                nowGrouped = false;
            break;
        }
    case no_newxmlparse:
        recordArg = 3;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_fetch:
        recordArg = 3;
        nowGrouped = false; // Is this really correct?
        break;
    case no_newparse:
        recordArg = 4;
        nowGrouped = isGrouped(datasetType);
        break;
    case no_addfiles:
    case no_regroup:
    case no_cogroup:
    case no_chooseds:
        {
            unsigned max = parms.ordinality();
            bool allGrouped = isGrouped(datasetType);
            unsigned firstDataset = (op == no_chooseds) ? 1 : 0;
            for (unsigned i=firstDataset+1; i < max; i++)
            {
                IHqlExpression & cur = parms.item(i);
                if (!cur.isAttribute() && !isGrouped(&cur))
                    allGrouped = false;
            }

            newRecordType.set(recordType);
            nowGrouped = (allGrouped || (op == no_cogroup));
            break;
        }
    case no_normalizegroup:
        {
            //Not very nice - it is hard to track anything through a group normalize.
            recordArg = 1;
            nowGrouped = isGrouped(dataset);
            break;
        }
    case no_if:
        {
            recordArg = 1;
            IHqlExpression * left = &parms.item(1);
            IHqlExpression * right = &parms.item(2);
            if (isNull(left))
                nowGrouped = isGrouped(right);
            else if (isNull(right))
                nowGrouped = isGrouped(left);
            else
                nowGrouped = isGrouped(left) || isGrouped(right);
            break;
        }
    case no_case:
    case no_mapto:
        //following is wrong, but they get removed pretty quickly so I don't really care
        type.set(parms.item(1).queryType());
        break;
    case no_map:
        //following is wrong, but they get removed pretty quickly so I don't really care
        type.set(parms.item(0).queryType());
        break;
    case no_merge:
        newRecordType.set(recordType);
        nowGrouped = false;
        break;
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
        newRecordType.set(recordType);
        nowGrouped = false;
        break;
    case no_choosen:
    case no_choosesets:
    case no_enth:
    case no_sample:
        newRecordType.set(recordType);
        if (hasAttribute(groupedAtom, parms))
            nowGrouped = isGrouped(dataset);
        break;
    case no_allnodes:
        newRecordType.set(recordType);
        //grouped not currently supported - ensure this is consistent with the meta.
        break;
    case no_colon:
    case no_alias_project:
    case no_alias_scope:
    case no_cachealias:
    case no_cloned:
    case no_globalscope:
    case no_comma:
    case no_compound:
    case no_filter:
    case no_keyed:
    case no_nofold:
    case no_nohoist:
    case no_section:
    case no_sectioninput:
    case no_sub:
    case no_thor:
    case no_nothor:
    case no_compound_indexread:
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
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
    case no_field:
    case no_metaactivity:
    case no_split:
    case no_spill:
    case no_readspill:
    case no_commonspill:
    case no_writespill:
    case no_throughaggregate:
    case no_limit:
    case no_catchds:
    case no_keyedlimit:
    case no_compound_fetch:
    case no_preload:
    case no_alias:
    case no_catch:
    case no_activerow:
    case no_newrow:
    case no_assert_ds:
    case no_spillgraphresult:
    case no_cluster:
    case no_forcenolocal:
    case no_thisnode:
    case no_forcelocal:
    case no_filtergroup:
    case no_forcegraph:
    case no_related:
    case no_executewhen:
    case no_outofline:
    case no_fieldmap:
    case no_owned_ds:
    case no_dataset_alias:
    case no_dedup:
    case no_assertgrouped:
    case no_preservemeta:
    case no_keyeddistribute:
    case no_subsort:
    case no_select:
        type.setown(dataset->getType());
        break;
    case no_distribute:
    case no_distributed:
    case no_assertdistributed:
        newRecordType.set(recordType);
        break;
    case no_cosort:
    case no_sort:
    case no_sorted:
    case no_assertsorted:
    case no_topn:
    case no_stepped:            // stepped implies the sort order matches the stepped criteria
    case no_nonempty:
        newRecordType.set(recordType);
        if (!hasAttribute(localAtom, parms) && !hasAttribute(globalAtom, parms))
            nowGrouped = isGrouped(dataset);
        break;
    case no_group:
    case no_grouped:
        newRecordType.set(recordType);
        nowGrouped = (parms.isItem(1) && !parms.item(1).isAttribute());
        break;
    case no_loop:
        {
            newRecordType.set(recordType);
            IHqlExpression & body = parms.item(4);
            nowGrouped = isGrouped(dataset) || isGrouped(body.queryChild(0));
            break;
        }
    case no_graphloop:
        {
            newRecordType.set(recordType);
            IHqlExpression & body = parms.item(2);
            nowGrouped = isGrouped(dataset) || isGrouped(body.queryChild(0));
            break;
        }
    case no_serialize:
        {
            assertex(parms.ordinality() >= 2);
            IHqlExpression & form = parms.item(1);
            assertex(form.isAttribute());
            type.setown(getSerializedForm(datasetType, form.queryName()));
            break;
        }
    case no_deserialize:
        {
            assertex(parms.ordinality() >= 3);
            IHqlExpression & record = parms.item(1);
            IHqlExpression & form = parms.item(2);
            assertex(form.isAttribute());
            ITypeInfo * recordType = record.queryType();
            OwnedITypeInfo rowType = makeRowType(LINK(recordType));
            assertex(record.getOperator() == no_record);
            if (isGrouped(datasetType))
            {
                ITypeInfo * childType = datasetType->queryChildType();
                OwnedITypeInfo newChildType = replaceChildType(childType, rowType);
                type.setown(replaceChildType(datasetType, newChildType));
            }
            else
                type.setown(replaceChildType(datasetType, rowType));

            //MORE: The distribution etc. won't be correct....
            type.setown(setLinkCountedAttr(type, true));

#ifdef _DEBUG
            OwnedITypeInfo serializedType = getSerializedForm(type, form.queryName());
            assertex(recordTypesMatch(serializedType, datasetType));
#endif
            break;
        }
    case no_rowsetindex:
    case no_rowsetrange:
        type.set(childType);
        break;
    case no_datasetfromrow:
    case no_datasetfromdictionary:
        recordArg = 0;
        break;
    default:
        UNIMPLEMENTED_XY("Type calculation for dataset operator", getOpString(op));
        break;
    }

    if (!type)
    {
        if (!newRecordType)
        {
            assertex(recordArg != NotFound);
            IHqlExpression * record = parms.item(recordArg).queryRecord();
            newRecordType.setown(createRecordType(record));
        }

        type.setown(createDatasetType(newRecordType, nowGrouped));
        if (linkCounted)
            type.setown(setLinkCountedAttr(type, true));
        if (streamed)
            type.setown(setStreamedAttr(type, true));
    }

    return type.getClear();
}

extern HQL_API bool hasSameSortGroupDistribution(IHqlExpression * expr, IHqlExpression * other)
{
    CHqlMetaInfo & left = queryMetaProperty(expr)->meta;
    CHqlMetaInfo & right = queryMetaProperty(other)->meta;
    return left.matches(right);
}

extern HQL_API bool hasKnownSortGroupDistribution(IHqlExpression * expr, bool isLocal)
{
    return queryMetaProperty(expr)->meta.hasKnownSortGroupDistribution(isLocal);
}

//---------------------------------------------------------------------------------------------------------------------

//Mark all selectors that are fully included in the sort criteria. This may not catch all cases
//but it is preferable to have false negatives than false positives.
void markValidSelectors(IHqlExpression * expr, IHqlExpression * dsSelector)
{
    switch (expr->getOperator())
    {
    case no_sortlist:
        break;
    case no_cast:
    case no_implicitcast:
        if (!castPreservesValue(expr))
            return;
        break;
    case no_typetransfer:
        //Special case the transfer to a variable length data type that is done for a dataset in an index build
        //(it will always preserve the value of any argument)
        {
            ITypeInfo * type = expr->queryType();
            if ((type->getTypeCode() != type_data) || !isUnknownSize(type))
                return;
            break;
        }
    case no_select:
        {
            bool isNew = false;
            IHqlExpression * root = querySelectorDataset(expr, isNew);
            if (!isNew && (root == dsSelector))
                expr->setTransformExtra(expr);
            return;
        }
    default:
        return;
    }

    ForEachChild(i, expr)
        markValidSelectors(expr->queryChild(i), dsSelector);
}

extern HQL_API bool allFieldsAreSorted(IHqlExpression * record, IHqlExpression * sortOrder, IHqlExpression * dsSelector, bool strict)
{
    TransformMutexBlock block;

    //First walk the sort order expression, tagging valid sorted selectors
    markValidSelectors(sortOrder, dsSelector);

    //Now expand all the selectors from the record, and check that they have been tagged
    RecordSelectIterator iter(record, dsSelector);
    ForEach(iter)
    {
        IHqlExpression * select = iter.query();
        if (!select->queryTransformExtra())
            return false;

        if (strict && isUnknownSize(select->queryType()))
        {
            //Comparisons on strings (and unicode) ignore trailing spaces, so strictly speaking sorting by a variable
            //length string field doesn't compare all the information from a field.
            ITypeInfo * type = select->queryType();
            if (type->getTypeCode() != type_data)
            {
                if (isStringType(type) || isUnicodeType(type))
                    return false;
            }
        }
    }
    return true;
}
