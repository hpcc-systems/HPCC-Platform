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
#ifndef __HQLMETA_HPP_
#define __HQLMETA_HPP_

class TableProjectMapper;

class HQL_API CHqlMetaInfo
{
public:
    inline bool isGrouped() const { return grouping != NULL; }

    bool appearsToBeSorted(bool isLocal, bool ignoreGrouping);
    void applyDistribute(IHqlExpression * newDistribution, IHqlExpression * optMergeOrder);
    void applyGroupSort(IHqlExpression * sortOrder);
    void applyGroupBy(IHqlExpression * groupBy, bool isLocal);
    void applyLocalSort(IHqlExpression * sortOrder);
    void applyGlobalSort(IHqlExpression * sortOrder);
    void applyProject(TableProjectMapper & mapper);
    void applySubSort(IHqlExpression * groupBy, IHqlExpression * sortOrder, bool isLocal);

    void ensureAppearsSorted(bool isLocal, bool ignoreGrouping);
    void getIntersection(const CHqlMetaInfo & other);
    IHqlExpression * getLocalSortOrder() const;
    bool hasUsefulInformation() const;
    bool hasKnownSortGroupDistribution(bool isLocal) const;
    bool matches(const CHqlMetaInfo & other) const;

    void preserveGrouping(IHqlExpression * dataset);

    void removeActiveSort();
    void removeAllKeepGrouping();
    void removeAllSortOrders();
    void removeAllAndUngroup(bool isLocal);
    void removeDistribution();
    void removeGroup();

    void set(const CHqlMetaInfo & other);
    void setMatchesAny();
    void setUnknownDistribution();
    void setUnknownGrouping();

protected:
    void clearGrouping();

public:
    LinkedHqlExpr distribution;
    LinkedHqlExpr globalSortOrder;
    LinkedHqlExpr localUngroupedSortOrder;
    LinkedHqlExpr grouping;
    LinkedHqlExpr groupSortOrder;
};

class CHqlMetaProperty : public CInterfaceOf<IInterface>
{
public:
    CHqlMetaInfo meta;
};

void extractMeta(CHqlMetaInfo & meta, ITypeInfo * type);

extern HQL_API IHqlExpression * queryUnknownAttribute();
extern HQL_API IHqlExpression * queryUnknownSortlist();
extern HQL_API IHqlExpression * queryMatchGroupOrderSortlist();
extern HQL_API IHqlExpression * getUnknownAttribute();
extern HQL_API IHqlExpression * getUnknownSortlist();
extern HQL_API IHqlExpression * getMatchGroupOrderSortlist();
extern HQL_API IHqlExpression * queryAnyOrderSortlist();
extern HQL_API IHqlExpression * queryAnyDistributionAttribute();

extern HQL_API IHqlExpression * getExistingSortOrder(IHqlExpression * dataset, bool isLocal, bool ignoreGrouping);
extern ITypeInfo * calculateDatasetType(node_operator op, const HqlExprArray & parms);

//---------------------------------------------------------------------------------------------

extern HQL_API bool isKnownDistribution(IHqlExpression * distribution);
extern HQL_API bool isPersistDistribution(IHqlExpression * distribution);
extern HQL_API bool isSortedDistribution(IHqlExpression * distribution);

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

extern HQL_API bool appearsToBeSorted(IHqlExpression * dataset, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isAlreadySorted(IHqlExpression * dataset, const HqlExprArray & newSort, bool isLocal, bool ignoreGrouping, bool requireDistribution);
extern HQL_API bool isAlreadySorted(IHqlExpression * dataset, IHqlExpression * newSort, bool isLocal, bool ignoreGrouping, bool requireDistribution);
extern HQL_API IHqlExpression * ensureSorted(IHqlExpression * dataset, IHqlExpression * order, IHqlExpression * parentExpr, bool isLocal, bool ignoreGrouping, bool alwaysLocal, bool allowSubSort, bool requestSpilling);

extern HQL_API bool isWorthShuffling(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping);
extern HQL_API bool isWorthShuffling(IHqlExpression * dataset, const HqlExprArray & newSort, bool isLocal, bool ignoreGrouping);
extern HQL_API IHqlExpression * getSubSort(IHqlExpression * dataset, const HqlExprArray & order, bool isLocal, bool ignoreGrouping, bool alwaysLocal);
extern HQL_API IHqlExpression * getSubSort(IHqlExpression * dataset, IHqlExpression * order, bool isLocal, bool ignoreGrouping, bool alwaysLocal);
extern HQL_API IHqlExpression * convertSubSortToGroupedSort(IHqlExpression * expr);

extern HQL_API bool reorderMatchExistingLocalSort(HqlExprArray & sortedLeft, HqlExprArray & reorderedRight, IHqlExpression * dataset, const HqlExprArray & left, const HqlExprArray & right);

extern HQL_API IHqlExpression * preserveTableInfo(IHqlExpression * newTable, IHqlExpression * original, bool loseDistribution, IHqlExpression * persistName);
extern HQL_API bool isDistributedCoLocally(IHqlExpression * dataset1, IHqlExpression * dataset2, const HqlExprArray & sort1, const HqlExprArray & sort2);
extern HQL_API IHqlExpression * createMatchingDistribution(IHqlExpression * expr, const HqlExprArray & oldSort, const HqlExprArray & newSort);

extern HQL_API void calculateDatasetMeta(CHqlMetaInfo & meta, IHqlExpression * expr);
extern HQL_API CHqlMetaProperty * querySimpleDatasetMeta(IHqlExpression * expr);
extern HQL_API bool hasSameSortGroupDistribution(IHqlExpression * expr, IHqlExpression * other);
extern HQL_API bool hasKnownSortGroupDistribution(IHqlExpression * expr, bool isLocal);

extern HQL_API bool allFieldsAreSorted(IHqlExpression * record, IHqlExpression * sortOrder, IHqlExpression * selector, bool strict);

inline IHqlExpression * queryRemoveOmitted(IHqlExpression * expr)
{
    if (expr &&  expr->isAttribute() && (expr->queryName() == _omitted_Atom))
        return NULL;
    return expr;
}

#endif
