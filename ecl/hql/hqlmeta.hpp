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
#ifndef __HQLMETA_HPP_
#define __HQLMETA_HPP_

class TableProjectMapper;

extern HQL_API IHqlExpression * queryUnknownAttribute();
extern HQL_API IHqlExpression * queryUnknownSortlist();
extern HQL_API IHqlExpression * queryMatchGroupOrderSortlist();
extern HQL_API IHqlExpression * getUnknownAttribute();
extern HQL_API IHqlExpression * getUnknownSortlist();
extern HQL_API IHqlExpression * getMatchGroupOrderSortlist();

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
extern HQL_API ITypeInfo * getTypeProject(ITypeInfo * prev, IHqlExpression * newRecord, TableProjectMapper & mapper);
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
extern HQL_API IHqlExpression * ensureSortedForGroup(IHqlExpression * table, IHqlExpression *sortList, bool isLocal, bool alwaysLocal);

extern HQL_API bool matchDedupDistribution(IHqlExpression * distn, const HqlExprArray & equalities);

extern HQL_API bool appearsToBeSorted(ITypeInfo * type, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isAlreadySorted(IHqlExpression * dataset, HqlExprArray & newSort, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isAlreadySorted(IHqlExpression * dataset, IHqlExpression * newSort, bool isLocal, bool ignoreGrouping);
extern HQL_API IHqlExpression * ensureSorted(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal);

extern HQL_API bool reorderMatchExistingLocalSort(HqlExprArray & sortedLeft, HqlExprArray & reorderedRight, IHqlExpression * dataset, const HqlExprArray & left, const HqlExprArray & right);

extern HQL_API IHqlExpression * preserveTableInfo(IHqlExpression * newTable, IHqlExpression * original, bool loseDistribution, IHqlExpression * persistName);
extern HQL_API bool isDistributedCoLocally(IHqlExpression * dataset1, IHqlExpression * dataset2, HqlExprArray & sort1, HqlExprArray & sort2);

inline IHqlExpression * queryRemoveOmitted(IHqlExpression * expr)
{
    if (expr &&  expr->isAttribute() && (expr->queryName() == _omitted_Atom))
        return NULL;
    return expr;
}

#endif
