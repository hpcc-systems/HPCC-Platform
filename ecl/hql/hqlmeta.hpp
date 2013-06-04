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
#ifndef __HQLMETA_HPP_
#define __HQLMETA_HPP_

class TableProjectMapper;

extern HQL_API IHqlExpression * queryUnknownAttribute();
extern HQL_API IHqlExpression * queryUnknownSortlist();
extern HQL_API IHqlExpression * queryMatchGroupOrderSortlist();
extern HQL_API IHqlExpression * getUnknownAttribute();
extern HQL_API IHqlExpression * getUnknownSortlist();
extern HQL_API IHqlExpression * getMatchGroupOrderSortlist();
extern HQL_API IHqlExpression * queryAnyOrderSortlist();
extern HQL_API IHqlExpression * queryAnyDistributionAttribute();

extern HQL_API IHqlExpression * getExistingSortOrder(IHqlExpression * dataset, bool isLocal, bool ignoreGrouping);

//---------------------------------------------------------------------------------------------

extern HQL_API bool isKnownDistribution(IHqlExpression * distribution);
inline bool isKnownDistribution(ITypeInfo * type) { return isKnownDistribution(queryDistribution(type)); }
extern HQL_API bool isPersistDistribution(IHqlExpression * distribution);
extern HQL_API bool isSortedDistribution(IHqlExpression * distribution);

extern HQL_API ITypeInfo * getTypeUngroup(ITypeInfo * prev);
extern HQL_API ITypeInfo * getTypeGrouped(ITypeInfo * prev, IHqlExpression * grouping, bool isLocal);
extern HQL_API ITypeInfo * getTypeGlobalSort(ITypeInfo * prev, IHqlExpression * sortOrder);
extern HQL_API ITypeInfo * getTypeLocalSort(ITypeInfo * prev, IHqlExpression * sortOrder);
extern HQL_API ITypeInfo * getTypeGroupSort(ITypeInfo * prev, IHqlExpression * sortOrder);
extern HQL_API ITypeInfo * getTypeDistribute(ITypeInfo * prev, IHqlExpression * distribution, IHqlExpression * optMergeOrder);
extern HQL_API ITypeInfo * getTypeFromMeta(IHqlExpression * record, IHqlExpression * meta, unsigned firstChild);
extern HQL_API ITypeInfo * getTypeIntersection(ITypeInfo * leftType, ITypeInfo * rightType);
extern HQL_API ITypeInfo * getTypeLoseDistributionKeepOrder(ITypeInfo * prev);
extern HQL_API ITypeInfo * getTypeProject(ITypeInfo * prev, IHqlExpression * newRecord, TableProjectMapper & mapper);
extern HQL_API ITypeInfo * getTypeSubSort(ITypeInfo * prev, IHqlExpression * grouping, IHqlExpression * sortOrder, bool isLocal);
extern HQL_API ITypeInfo * getTypeCannotProject(ITypeInfo * prev, IHqlExpression * newRecord); // preserve grouping, but that's it.
extern HQL_API ITypeInfo * getTypeUnknownDistribution(ITypeInfo * prev);
extern HQL_API ITypeInfo * getTypeRemoveDistribution(ITypeInfo * prev);
extern HQL_API ITypeInfo * getTypeRemoveActiveSort(ITypeInfo * prev);
extern HQL_API ITypeInfo * getTypeRemoveAllSortOrders(ITypeInfo * prev);
extern HQL_API bool hasUsefulMetaInformation(ITypeInfo * prev);

//---------------------------------------------------------------------------------------------

extern HQL_API IHqlExpression * mapJoinDistribution(TableProjectMapper & mapper, IHqlExpression * distribution, IHqlExpression * side);

//---------------------------------------------------------------------------------------------

extern HQL_API bool isPartitionedForGroup(IHqlExpression * table, IHqlExpression *grouping, bool isGroupAll);
extern HQL_API bool isPartitionedForGroup(IHqlExpression * table, const HqlExprArray & grouping, bool isGroupAll);

//The following only look at the sort order, and not the partitioning
extern HQL_API bool isSortedForGroup(IHqlExpression * table, IHqlExpression *sortList, bool isLocal);
extern HQL_API IHqlExpression * ensureSortedForGroup(IHqlExpression * table, IHqlExpression *sortList, bool isLocal, bool alwaysLocal, bool allowSubSort);

extern HQL_API bool matchDedupDistribution(IHqlExpression * distn, const HqlExprArray & equalities);
extern HQL_API bool matchesAnyDistribution(IHqlExpression * distn);

extern HQL_API bool appearsToBeSorted(ITypeInfo * type, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isAlreadySorted(IHqlExpression * dataset, HqlExprArray & newSort, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isAlreadySorted(IHqlExpression * dataset, IHqlExpression * newSort, bool isLocal, bool ignoreGrouping);
extern HQL_API IHqlExpression * ensureSorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal, bool allowSubSort);

extern HQL_API bool isWorthShuffling(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isWorthShuffling(IHqlExpression * dataset, HqlExprArray & newSort, bool isLocal, bool ignoreGrouping);
extern HQL_API IHqlExpression * getSubSort(IHqlExpression * dataset, HqlExprArray & order, bool isLocal, bool ignoreGrouping, bool alwaysLocal);
extern HQL_API IHqlExpression * getSubSort(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal);
extern HQL_API IHqlExpression * convertSubSortToGroupedSort(IHqlExpression * expr);

extern HQL_API bool reorderMatchExistingLocalSort(HqlExprArray & sortedLeft, HqlExprArray & reorderedRight, IHqlExpression * dataset, const HqlExprArray & left, const HqlExprArray & right);

extern HQL_API IHqlExpression * preserveTableInfo(IHqlExpression * newTable, IHqlExpression * original, bool loseDistribution, IHqlExpression * persistName);
extern HQL_API bool isDistributedCoLocally(IHqlExpression * dataset1, IHqlExpression * dataset2, const HqlExprArray & sort1, const HqlExprArray & sort2);
extern HQL_API IHqlExpression * createMatchingDistribution(IHqlExpression * expr, const HqlExprArray & oldSort, const HqlExprArray & newSort);

inline IHqlExpression * queryRemoveOmitted(IHqlExpression * expr)
{
    if (expr &&  expr->isAttribute() && (expr->queryName() == _omitted_Atom))
        return NULL;
    return expr;
}

#endif
