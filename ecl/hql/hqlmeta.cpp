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

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    _ATOM groupedOrderAtom = createAtom("{group-order}");
    _ATOM anyOrderAtom = createAtom("{any}");
    cacheGroupedElement = createAttribute(groupedOrderAtom);
    cacheUnknownAttribute = createAttribute(unknownAtom);
    cacheIndeterminateAttribute = createAttribute(indeterminateAtom);
    cacheAnyAttribute = createAttribute(anyOrderAtom);
    cached_omitted_Attribute = createAttribute(_omitted_Atom);
    cacheUnknownSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheUnknownAttribute));
    cacheIndeterminateSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheIndeterminateAttribute));
    cacheMatchGroupOrderSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheGroupedElement));
    cacheAnyOrderSortlist = createValue(no_sortlist, makeSortListType(NULL), LINK(cacheAnyAttribute));
    return true;
}
MODULE_EXIT()
{
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

bool isSortedDistribution(ITypeInfo * type)
{
    return isSortedDistribution(queryDistribution(type));
}

//What is the actual local sort order at the moment - ignoring any grouping.
//?Rename quryLocalSortInfo to queryLocalNonGroupSortInfo????
IHqlExpression * getLocalSortOrder(ITypeInfo * type)
{
    IHqlExpression * localOrder = queryLocalUngroupedSortOrder(type);
    if (!isGrouped(type))
        return LINK(localOrder);

    if (!localOrder)
        return NULL;

    IHqlExpression * groupOrder = queryGroupSortOrder(type);
    if (matchesGroupOrder(localOrder))
        return LINK(groupOrder);

    HqlExprArray components;
    unwindChildren(components, localOrder);
    if (!hasUnknownComponent(components))
    {
        if (components.length() && (&components.tos() == cacheGroupedElement))
            components.pop();

        if (groupOrder)
            unwindChildren(components, groupOrder);
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
    return getLocalSortOrder(expr->queryType());
}

inline IHqlExpression * getGlobalSortOrder(ITypeInfo * type)
{
    return LINK(queryGlobalSortOrder(type));
}

inline IHqlExpression * getGlobalSortOrder(IHqlExpression * expr)
{
    return LINK(queryGlobalSortOrder(expr));
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

ITypeInfo * getTypeUngroup(ITypeInfo * prev)
{
    if (!isGrouped(prev))
        return LINK(prev);
    IHqlExpression * distribution = queryDistribution(prev);
    IHqlExpression * globalOrder = queryGlobalSortOrder(prev);
    OwnedHqlExpr newLocalOrder = getLocalSortOrder(prev);
    ITypeInfo * rowType = queryRowType(prev);
    return makeTableType(LINK(rowType), LINK(distribution), LINK(globalOrder), newLocalOrder.getClear());
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
ITypeInfo * getTypeGrouped(ITypeInfo * prev, IHqlExpression * grouping, bool isLocal)
{
    OwnedITypeInfo prevUngrouped = getTypeUngroup(prev);
    OwnedHqlExpr newGrouping = normalizeSortlist(grouping);

    IHqlExpression * distribution = isLocal ? queryDistribution(prevUngrouped) : NULL;
    IHqlExpression * globalOrder = queryGlobalSortOrder(prevUngrouped);
    IHqlExpression * localOrder = queryLocalUngroupedSortOrder(prevUngrouped);
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

    ITypeInfo * rowType = queryRowType(prevUngrouped);
    ITypeInfo * tableType = makeTableType(LINK(rowType), LINK(distribution), LINK(globalOrder), newLocalOrder.getClear());
    return makeGroupedTableType(tableType, newGrouping.getClear(), newGroupOrder.getClear());
}

//NB: This does not handle ALL groups that is handled in createDataset()
ITypeInfo * getTypeLoseDistributionKeepOrder(ITypeInfo * prev)
{
    OwnedITypeInfo prevUngrouped = getTypeUngroup(prev);

    IHqlExpression * distribution = NULL;
    IHqlExpression * globalOrder = queryGlobalSortOrder(prevUngrouped);

    //If records are moved from one node to the next, the global order will be preserved, and the local order will
    //only be valid if it matches the global order.

    ITypeInfo * rowType = queryRowType(prevUngrouped);
    return makeTableType(LINK(rowType), LINK(distribution), LINK(globalOrder), LINK(globalOrder));
}

ITypeInfo * getTypeGlobalSort(ITypeInfo * prev, IHqlExpression * sortOrder)
{
    OwnedHqlExpr newSortOrder = normalizeSortlist(sortOrder);
    OwnedHqlExpr distribution = createExprAttribute(sortedAtom, LINK(newSortOrder));    //, createUniqueId());
    ITypeInfo * rowType = queryRowType(prev);
    return makeTableType(LINK(rowType), distribution.getClear(), LINK(newSortOrder), LINK(newSortOrder));
}

ITypeInfo * getTypeLocalSort(ITypeInfo * prev, IHqlExpression * sortOrder)
{
    IHqlExpression * distribution = queryDistribution(prev);
    IHqlExpression * globalSortOrder = queryGlobalSortOrder(prev);
    OwnedHqlExpr localSortOrder = normalizeSortlist(sortOrder);
    //The global sort order is maintained as the leading components that match.
    OwnedHqlExpr newGlobalOrder = getIntersectingSortlist(globalSortOrder, localSortOrder, NULL);
    ITypeInfo * rowType = queryRowType(prev);
    return makeTableType(LINK(rowType), LINK(distribution), newGlobalOrder.getClear(), localSortOrder.getClear());
}

ITypeInfo * getTypeGroupSort(ITypeInfo * prev, IHqlExpression * sortOrder)
{
    assertex(isGrouped(prev));
    OwnedHqlExpr groupedOrder = normalizeSortlist(sortOrder);
    if (groupedOrder == queryGroupSortOrder(prev))
        return LINK(prev);

    IHqlExpression * globalOrder = queryGlobalSortOrder(prev);
    IHqlExpression * localUngroupedOrder = queryLocalUngroupedSortOrder(prev);
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
    ITypeInfo * prevTable = prev->queryChildType();
    ITypeInfo * tableType;
    if ((globalOrder != newGlobalOrder) || (localUngroupedOrder != newLocalUngroupedOrder))
    {
        ITypeInfo * rowType = queryRowType(prevTable);
        tableType = makeTableType(LINK(rowType), LINK(queryDistribution(prev)), newGlobalOrder.getClear(), LINK(newLocalUngroupedOrder));
    }
    else
        tableType = LINK(prevTable);

    return makeGroupedTableType(tableType, LINK(queryGrouping(prev)), groupedOrder.getClear());
}

ITypeInfo * getTypeDistribute(ITypeInfo * prev, IHqlExpression * distribution, IHqlExpression * optMergeOrder)
{
    OwnedHqlExpr newDistribution = normalizeDistribution(distribution);
    OwnedHqlExpr order = optMergeOrder ? normalizeSortlist(optMergeOrder) : NULL;
    ITypeInfo * rowType = queryRowType(prev);
    //Theoretically a keyed distribute may create a global sort order if merging also specified.
    return makeTableType(LINK(rowType), newDistribution.getClear(), NULL, order.getClear());
}


//Used when there is an alternative - either left or right.
//As long as the identical cases fall out fairly well it is probably not worth spending lots of time
//getting it very accurate.
ITypeInfo * getTypeIntersection(ITypeInfo * leftType, ITypeInfo * rightType)
{
    if (leftType == rightType)
        return LINK(leftType);

    IHqlExpression * leftDist = queryDistribution(leftType);
    IHqlExpression * rightDist = queryDistribution(rightType);
    OwnedHqlExpr newDistributeInfo;
    if ((leftDist == rightDist) || (rightDist == queryAnyDistributionAttribute()))
        newDistributeInfo.set(leftDist);
    else if (leftDist == queryAnyDistributionAttribute())
        newDistributeInfo.set(rightDist);
    else if (leftDist && rightDist)
        newDistributeInfo.set(queryUnknownAttribute());

    OwnedHqlExpr globalOrder = getIntersectingSortlist(queryGlobalSortOrder(leftType), queryGlobalSortOrder(rightType), NULL);
    IHqlExpression * leftLocalOrder = queryLocalUngroupedSortOrder(leftType);
    IHqlExpression * rightLocalOrder = queryLocalUngroupedSortOrder(rightType);
    IHqlExpression * leftGrouping = queryGrouping(leftType);
    IHqlExpression * rightGrouping = queryGrouping(rightType);
    OwnedHqlExpr localOrder;
    OwnedHqlExpr grouping = (leftGrouping || rightGrouping) ? getUnknownSortlist() : NULL;
    if (leftGrouping == rightGrouping)
        grouping.set(leftGrouping);

    OwnedHqlExpr groupOrder;
    if (leftLocalOrder == rightLocalOrder)
    {
        localOrder.set(leftLocalOrder);
        if (leftGrouping == rightGrouping)
        {
            groupOrder.setown(getIntersectingSortlist(queryGroupSortOrder(leftType), queryGroupSortOrder(rightType), NULL));
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
            IHqlExpression * extraAttr = grouping ? queryUnknownAttribute() : NULL;
            localOrder.setown(getIntersectingSortlist(leftLocalOrder, rightLocalOrder, extraAttr));
        }
    }

    ITypeInfo * rowType = queryRowType(leftType);
    ITypeInfo * tableType = makeTableType(LINK(rowType), newDistributeInfo.getClear(), globalOrder.getClear(), localOrder.getClear());
    if (!grouping)
        return tableType;
    return makeGroupedTableType(tableType, grouping.getClear(), groupOrder.getClear());
}


//Distribute is all or nothing
//Global sort retains as main as significant
//Local sort needs a trailing unknown marker if dataset is grouped
//Grouping retains attributes in place of grouping elements
//Group sort retains as much as possible.
extern HQL_API ITypeInfo * getTypeProject(ITypeInfo * prev, IHqlExpression * newRecord, TableProjectMapper & mapper)
{
    IHqlExpression * distribution = queryDistribution(prev);
    IHqlExpression * globalOrder = queryGlobalSortOrder(prev);
    IHqlExpression * localOrder = queryLocalUngroupedSortOrder(prev);
    IHqlExpression * grouping = queryGrouping(prev);
    OwnedHqlExpr newDistribution = mapDistribution(distribution, mapper);
    OwnedHqlExpr newGlobalOrder = mapSortOrder(globalOrder, mapper, false);
    OwnedHqlExpr newLocalOrder = mapSortOrder(localOrder, mapper, (grouping != NULL));
    OwnedHqlExpr newGrouping;
    OwnedHqlExpr newGroupOrder;
    if (grouping)
    {
        newGrouping.setown(mapGroup(grouping, mapper));
        newGroupOrder.setown(mapSortOrder(queryGroupSortOrder(prev), mapper, false));
    }

    ITypeInfo * tableType = makeTableType(makeRowType(createRecordType(newRecord)), newDistribution.getClear(), newGlobalOrder.getClear(), newLocalOrder.getClear());
    if (!grouping)
        return tableType;
    return makeGroupedTableType(tableType, newGrouping.getClear(), newGroupOrder.getClear());
}


// preserve grouping, but that's it.
extern HQL_API ITypeInfo * getTypeCannotProject(ITypeInfo * prev, IHqlExpression * newRecord)
{
    ITypeInfo * type = makeTableType(makeRowType(createRecordType(newRecord)), NULL, NULL, NULL);
    IHqlExpression * grouping = queryGrouping(prev);
    if (grouping)
        type = makeGroupedTableType(type, getUnknownSortlist(), NULL);
    return type;
}


ITypeInfo * getTypeRemoveDistribution(ITypeInfo * prev)
{
    ITypeInfo * childType = prev->queryChildType();
    if (isGrouped(prev))
    {
        OwnedITypeInfo newChild = getTypeRemoveDistribution(childType);
        return replaceChildType(prev, newChild);
    }

    IHqlExpression * globalSort = queryGlobalSortOrder(prev);
    OwnedHqlExpr localSort =  getLocalSortOrder(prev);
    ITypeInfo * rowType = queryRowType(prev);
    return makeTableType(LINK(rowType), NULL, LINK(globalSort), localSort.getClear());
}

ITypeInfo * getTypeUnknownDistribution(ITypeInfo * prev)
{
    ITypeInfo * childType = prev->queryChildType();
    if (isGrouped(prev))
    {
        OwnedITypeInfo newChild = getTypeUnknownDistribution(childType);
        return replaceChildType(prev, newChild);
    }

    IHqlExpression * globalSort = queryGlobalSortOrder(prev);
    OwnedHqlExpr localSort =  getLocalSortOrder(prev);
    ITypeInfo * rowType = queryRowType(prev);
    return makeTableType(LINK(rowType), getUnknownAttribute(), LINK(globalSort), localSort.getClear());
}

ITypeInfo * getTypeRemoveAllSortOrders(ITypeInfo * prev)
{
    IHqlExpression * grouping = queryGrouping(prev);
    ITypeInfo * childType = prev->queryChildType();
    if (grouping)
    {
        OwnedITypeInfo newChild = getTypeRemoveAllSortOrders(childType);
        return makeGroupedTableType(newChild.getClear(), LINK(grouping), NULL);
    }

    IHqlExpression * distribution = queryDistribution(prev);
    return makeTableType(LINK(childType), LINK(distribution), NULL, NULL);
}


ITypeInfo * getTypeRemoveActiveSort(ITypeInfo * prev)
{
    IHqlExpression * grouping = queryGrouping(prev);
    if (grouping)
        return getTypeGroupSort(prev, NULL);

    IHqlExpression * distribution = queryDistribution(prev);
    ITypeInfo * childType = prev->queryChildType();
    return makeTableType(LINK(childType), LINK(distribution), NULL, NULL);
}


ITypeInfo * getTypeFromMeta(IHqlExpression * record, IHqlExpression * meta, unsigned firstChild)
{
    IHqlExpression * distribution = queryRemoveOmitted(meta->queryChild(firstChild+0));
    IHqlExpression * globalSort = queryRemoveOmitted(meta->queryChild(firstChild+1));
    IHqlExpression * localSort = queryRemoveOmitted(meta->queryChild(firstChild+2));
    IHqlExpression * grouping = queryRemoveOmitted(meta->queryChild(firstChild+3));
    IHqlExpression * groupSort = queryRemoveOmitted(meta->queryChild(firstChild+4));
    ITypeInfo * recordType = createRecordType(record);
    Owned<ITypeInfo> type = makeTableType(makeRowType(recordType), LINK(distribution), LINK(globalSort), LINK(localSort));
    if (!grouping)
        return type.getClear();

    return makeGroupedTableType(type.getClear(), LINK(grouping), LINK(groupSort));
}


extern HQL_API ITypeInfo * getTypeSubSort(ITypeInfo * prevType, IHqlExpression * grouping, IHqlExpression * sortOrder, bool isLocal)
{
    Owned<ITypeInfo> groupedType = getTypeGrouped(prevType, grouping, isLocal);
    Owned<ITypeInfo> sortedType = getTypeGroupSort(groupedType, sortOrder);
    return getTypeUngroup(sortedType);
}


//---------------------------------------------------------------------------------------------

bool appearsToBeSorted(ITypeInfo * type, bool isLocal, bool ignoreGrouping)
{
    if (isLocal)
        return queryLocalUngroupedSortOrder(type) != NULL;
    if (!ignoreGrouping && isGrouped(type))
        return queryGroupSortOrder(type) != NULL;
    return queryGlobalSortOrder(type) != NULL;
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
        if (distribution->hasProperty(leftAtom) || distribution->hasProperty(allAtom))
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
    ITypeInfo * type = table->queryType();
    IHqlExpression * distribution = queryDistribution(type);
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

static bool normalizedIsAlreadySorted(IHqlExpression * dataset, IHqlExpression * normalizedOrder, bool isLocal, bool ignoreGrouping)
{
#ifdef OPTIMIZATION2
    if (hasNoMoreRowsThan(dataset, 1))
        return true;
#endif
    if (!isCorrectDistributionForSort(dataset, normalizedOrder, isLocal, ignoreGrouping))
        return false;

    //Constant items and duplicates should have been removed already.
    OwnedHqlExpr existingOrder = getExistingSortOrder(dataset, isLocal, ignoreGrouping);
    return isCompatibleSortOrder(existingOrder, normalizedOrder);
}


bool isAlreadySorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping)
{
#ifdef OPTIMIZATION2
    if (hasNoMoreRowsThan(dataset, 1))
        return true;
#endif

    OwnedHqlExpr normalizedOrder = normalizeSortlist(order, dataset);
    return normalizedIsAlreadySorted(dataset, normalizedOrder, isLocal, ignoreGrouping);
}


//Elements in the exprarray have already been mapped;
bool isAlreadySorted(IHqlExpression * dataset, HqlExprArray & newSort, bool isLocal, bool ignoreGrouping)
{
    HqlExprArray components;
    normalizeComponents(components, newSort);
    OwnedHqlExpr normalizedOrder = createSortList(components);
    return normalizedIsAlreadySorted(dataset, normalizedOrder, isLocal, ignoreGrouping);
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
static unsigned numElementsAlreadySorted(IHqlExpression * dataset, HqlExprArray & newSort, bool isLocal, bool ignoreGrouping)
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

bool isWorthShuffling(IHqlExpression * dataset, HqlExprArray & newSort, bool isLocal, bool ignoreGrouping)
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

    assertex(!isGrouped(dataset) || expr->hasProperty(globalAtom));
    OwnedHqlExpr attr = isLocalActivity(expr) ? createLocalAttribute() : NULL;
    OwnedHqlExpr grouped = createDatasetF(no_group, LINK(dataset), LINK(grouping), LINK(attr), NULL);

    HqlExprArray args;
    args.append(*grouped.getClear());
    args.append(*LINK(newOrder));
    unwindChildren(args, expr, 3);
    removeProperty(args, localAtom);
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

    assertex(isAlreadySorted(subsort, order, isLocal||alwaysLocal, ignoreGrouping));
    return subsort.getClear();
}

IHqlExpression * getSubSort(IHqlExpression * dataset, HqlExprArray & order, bool isLocal, bool ignoreGrouping, bool alwaysLocal)
{
    if (isAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping))
        return NULL;

    OwnedHqlExpr sortlist = createValueSafe(no_sortlist, makeSortListType(NULL), order);
    OwnedHqlExpr mappedSortlist = replaceSelector(sortlist, queryActiveTableSelector(), dataset);
    return createSubSorted(dataset, mappedSortlist, isLocal, ignoreGrouping, alwaysLocal);
}

IHqlExpression * getSubSort(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal)
{
    if (isAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping))
        return NULL;

    return createSubSorted(dataset, order, isLocal, ignoreGrouping, alwaysLocal);
}

//--------------------------------------------------------------------------------------------------------------------

IHqlExpression * ensureSorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal, bool allowSubSort)
{
    if (isAlreadySorted(dataset, order, isLocal||alwaysLocal, ignoreGrouping))
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

    IHqlExpression * attr = isLocal ? createLocalAttribute() : (isGrouped(dataset) && ignoreGrouping) ? createAttribute(globalAtom) : NULL;
    return createDatasetF(no_sort, LINK(dataset), LINK(order), attr, NULL);
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


//---------------------------------------------------------------------------

/*
 For a join to be able to be optimized to a local join we need:
 a) The distribution function to have exactly the same form on each side.
 b) All references to fields from the dataset must match the join element
*/

static bool checkDistributedCoLocally(IHqlExpression * distribute1, IHqlExpression * distribute2, HqlExprArray & sort1, HqlExprArray & sort2)
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
bool isDistributedCoLocally(IHqlExpression * dataset1, IHqlExpression * dataset2, HqlExprArray & sort1, HqlExprArray & sort2)
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

static IHqlExpression * createPreserveTableInfo(IHqlExpression * newTable, IHqlExpression * original, bool loseDistribution, IHqlExpression * persistName)
{
    ITypeInfo * type = original->queryType();
    LinkedHqlExpr distribution = loseDistribution ? NULL : queryDistribution(type);
    IHqlExpression * globalSort = queryGlobalSortOrder(type);
    IHqlExpression * localSort = queryLocalUngroupedSortOrder(type);
    IHqlExpression * grouping = queryGrouping(type);
    IHqlExpression * groupSort = queryGroupSortOrder(type);
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

bool hasUsefulMetaInformation(ITypeInfo * prev)
{
    IHqlExpression * distribution = queryDistribution(prev);
    IHqlExpression * globalOrder = queryGlobalSortOrder(prev);
    IHqlExpression * localOrder = queryLocalUngroupedSortOrder(prev);
    IHqlExpression * grouping = queryGrouping(prev);
    IHqlExpression * groupOrder = static_cast<IHqlExpression *>(prev->queryGroupSortInfo());
    return (distribution && containsActiveDataset(distribution)) ||
           (globalOrder && containsActiveDataset(globalOrder)) ||
           (localOrder && containsActiveDataset(localOrder)) ||
           (grouping && containsActiveDataset(grouping)) ||
           (groupOrder && containsActiveDataset(groupOrder));
}
