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
#include "hqlexpr.ipp"

#define SIZET_CACHE_SIZE    5001
#define FIXEDATTR_CACHE_SIZE 1001

static ITypeInfo * sizetType;
static ITypeInfo * signedType;
static ITypeInfo * constUnknownVarStringType;
static CriticalSection * sizetCacheCs;
static IHqlExpression * sizetCache[SIZET_CACHE_SIZE];
static IHqlExpression * fixedAttrSizeCache[FIXEDATTR_CACHE_SIZE];
static IHqlExpression * defaultMaxRecordLengthExpr;
static IHqlExpression * cacheAlignedAttr;
static IHqlExpression * cacheEmbeddedAttr;
static IHqlExpression * cacheInlineAttr;
static IHqlExpression * cacheLinkCountedAttr;
static IHqlExpression * cacheReferenceAttr;
static IHqlExpression * cacheSerializedFormAttr;
static IHqlExpression * cacheStreamedAttr;
static IHqlExpression * cacheUnadornedAttr;
static IHqlExpression * matchxxxPseudoFile;
static IHqlExpression * cachedQuotedNullExpr;
static IHqlExpression * cachedGlobalSequenceNumber;
static IHqlExpression * cachedLocalSequenceNumber;
static IHqlExpression * cachedStoredSequenceNumber;
static IHqlExpression * cachedOmittedValueExpr;

static void initBoolAttr(_ATOM name, IHqlExpression * x[2])
{
    x[0] = createExprAttribute(name, createConstant(false));
    x[1] = createExprAttribute(name, createConstant(true));
}


MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    sizetType = makeIntType(sizeof(size32_t), false);
    signedType = makeIntType(sizeof(signed), true);
    sizetCacheCs = new CriticalSection;
    constUnknownVarStringType = makeConstantModifier(makeVarStringType(UNKNOWN_LENGTH));
    defaultMaxRecordLengthExpr = createQuoted("<default-max-length>", makeIntType(sizeof(size32_t), false));
    cacheAlignedAttr = createAttribute(_attrAligned_Atom);
    cacheEmbeddedAttr = createAttribute(embeddedAtom);
    cacheInlineAttr = createAttribute(inlineAtom);
    cacheLinkCountedAttr = createAttribute(_linkCounted_Atom);
    cacheReferenceAttr = createAttribute(referenceAtom);
    cacheSerializedFormAttr = createAttribute(_attrSerializedForm_Atom);
    cacheStreamedAttr = createAttribute(streamedAtom);
    cacheUnadornedAttr = createAttribute(_attrUnadorned_Atom);
    matchxxxPseudoFile = createDataset(no_pseudods, createRecord()->closeExpr(), createAttribute(matchxxxPseudoFileAtom));
    cachedQuotedNullExpr = createValue(no_nullptr, makeBoolType());
    cachedOmittedValueExpr = createValue(no_omitted, makeAnyType());

    cachedGlobalSequenceNumber = createConstant(signedType->castFrom(true, (__int64)ResultSequencePersist));
    cachedLocalSequenceNumber = createConstant(signedType->castFrom(true, (__int64)ResultSequenceInternal));
    cachedStoredSequenceNumber = createConstant(signedType->castFrom(true, (__int64)ResultSequenceStored));
    return true;
}
MODULE_EXIT()
{
    delete sizetCacheCs;
    sizetType->Release();
    signedType->Release();
    defaultMaxRecordLengthExpr->Release();
    for (unsigned i=0; i < SIZET_CACHE_SIZE; i++)
        ::Release(sizetCache[i]);
    for (unsigned i2=0; i2 < FIXEDATTR_CACHE_SIZE; i2++)
        ::Release(fixedAttrSizeCache[i2]);
    cacheAlignedAttr->Release();
    cacheEmbeddedAttr->Release();
    cacheInlineAttr->Release();
    cacheLinkCountedAttr->Release();
    cacheReferenceAttr->Release();
    cacheSerializedFormAttr->Release();
    cacheStreamedAttr->Release();
    cacheUnadornedAttr->Release();
    matchxxxPseudoFile->Release();
    cachedQuotedNullExpr->Release();
    cachedGlobalSequenceNumber->Release();
    cachedLocalSequenceNumber->Release();
    cachedStoredSequenceNumber->Release();
    cachedOmittedValueExpr->Release();
    constUnknownVarStringType->Release();
}

inline int TFI(bool value) { return value ? 0 : 1; }

IHqlExpression * getSizetConstant(unsigned size)
{
    if (size >= SIZET_CACHE_SIZE)
        return createConstant(sizetType->castFrom(false, (__int64)size));

    CriticalBlock block(*sizetCacheCs);
    IHqlExpression * match = sizetCache[size];
    if (!match)
        match = sizetCache[size] = createConstant(sizetType->castFrom(false, (__int64)size));

    return LINK(match);
}

IHqlExpression * createIntConstant(__int64 val)
{
    return createConstant(createMinIntValue(val));
}

IHqlExpression * createUIntConstant(unsigned __int64 val)
{
    return createConstant(createMinIntValue(val));
}

inline _ATOM createMangledName(IHqlExpression * module, IHqlExpression * child)
{
    StringBuffer mangledName;
    mangledName.append(module->queryName()).append(".").append(child->queryName());

    return createIdentifierAtom(mangledName.str());
}

IHqlExpression * queryDefaultMaxRecordLengthExpr()
{
    return defaultMaxRecordLengthExpr;
};

HQL_API IHqlExpression * getFixedSizeAttr(unsigned size)
{
    if (size >= FIXEDATTR_CACHE_SIZE)
    {
        OwnedHqlExpr sizeExpr = getSizetConstant(size);
        return createExprAttribute(_attrSize_Atom, LINK(sizeExpr), LINK(sizeExpr), LINK(sizeExpr));
    }

    CriticalBlock block(*sizetCacheCs);     // reuse the critical section
    IHqlExpression * match = fixedAttrSizeCache[size];
    if (!match)
    {
        OwnedHqlExpr sizeExpr = getSizetConstant(size);
        match = fixedAttrSizeCache[size] = createExprAttribute(_attrSize_Atom, LINK(sizeExpr), LINK(sizeExpr), LINK(sizeExpr));
    }
    return LINK(match);
}


extern HQL_API IHqlExpression * queryQuotedNullExpr()
{
    return cachedQuotedNullExpr;
}


extern HQL_API IHqlExpression * createOmittedValue()
{
    return LINK(cachedOmittedValueExpr);
}

#if 0
IHqlExpression * queryRequiresDestructorAttr(bool value)
{
    return cacheRequiresDestructorAttr[TFI(value)];
}
#endif

IHqlExpression * querySerializedFormAttr()
{
    return cacheSerializedFormAttr;
}

IHqlExpression * queryUnadornedAttr()
{
    return cacheUnadornedAttr;
}


IHqlExpression * queryAlignedAttr()
{
    return cacheAlignedAttr;
}

extern HQL_API IHqlExpression * queryLinkCountedAttr()
{
    return cacheLinkCountedAttr;
}
extern HQL_API IHqlExpression * getLinkCountedAttr()
{
    return LINK(cacheLinkCountedAttr);
}
extern HQL_API IHqlExpression * getStreamedAttr()
{
    return LINK(cacheStreamedAttr);
}
extern HQL_API IHqlExpression * getInlineAttr()
{
    return LINK(cacheInlineAttr);
}
extern HQL_API IHqlExpression * getEmbeddedAttr()
{
    return LINK(cacheEmbeddedAttr);
}
extern HQL_API IHqlExpression * getReferenceAttr()
{
    return LINK(cacheReferenceAttr);
}

extern HQL_API IHqlExpression * queryMatchxxxPseudoFile()
{
    return matchxxxPseudoFile;
}

IHqlExpression * getGlobalSequenceNumber()      { return LINK(cachedGlobalSequenceNumber); }
IHqlExpression * getLocalSequenceNumber()       { return LINK(cachedLocalSequenceNumber); }
IHqlExpression * getStoredSequenceNumber()      { return LINK(cachedStoredSequenceNumber); }
IHqlExpression * getOnceSequenceNumber()        { return createConstant(signedType->castFrom(true, (__int64)ResultSequenceOnce)); }

//---------------------------------------------------------------------------

bool containsAggregate(IHqlExpression * expr)
{
    return expr->isGroupAggregateFunction();
}

bool containsComplexAggregate(IHqlExpression * expr)
{
    unsigned childIndex = (unsigned)-1;
    switch (expr->getOperator())
    {
    case no_record:
        childIndex = 0;
        break;
    case no_newtransform:
    case no_transform:
        childIndex = 1;
        break;
    default:
        UNIMPLEMENTED;
    }

    unsigned num = expr->numChildren();
    unsigned idx;
    for (idx = 0; idx < num; idx++)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        if (cur->isAttribute())
            continue;
        IHqlExpression * value = cur->queryChild(childIndex);
        if (value && value->isGroupAggregateFunction())
        {
            //HOLe can cast aggregate values.
            node_operator op = value->getOperator();
            if ((op == no_cast) || (op == no_implicitcast))
                value = value->queryChild(0);

            switch (value->getOperator())
            {
            case NO_AGGREGATEGROUP:
                break;
            default:
                return true;
            }
        }
    }
    return false;
}


static node_operator containsSingleAggregate(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_assign:
        {
            node_operator rhsOp = expr->queryChild(1)->getOperator();
            switch (rhsOp)
            {
            case NO_AGGREGATEGROUP:
                return rhsOp;
            }
            return no_none;
        }
    case no_assignall:
    case no_transform:
    case no_newtransform:
        {
            node_operator ret = no_none;
            ForEachChild(i, expr)
            {
                node_operator childOp = containsSingleAggregate(expr->queryChild(i));
                if (childOp != no_none)
                {
                    if (ret == no_none)
                        ret = childOp;
                    else
                        return no_all;
                }
            }
            return ret;
        }
    }
    return no_none;
}

node_operator queryTransformSingleAggregate(IHqlExpression * expr)
{
    return containsSingleAggregate(expr);
}
    

static bool containsOnlyLeftTable(IHqlExpression * expr, bool ignoreSelfOrFilepos)
{
    switch (expr->getOperator())
    {
    case no_self:
        return ignoreSelfOrFilepos;
    case no_left:
        return true;
    case no_selectnth:
        return containsOnlyLeftTable(expr->queryChild(0), ignoreSelfOrFilepos) && containsOnlyLeft(expr->queryChild(1), ignoreSelfOrFilepos);
    case no_select:
        return containsOnlyLeftTable(expr->queryChild(0), ignoreSelfOrFilepos);
    }
    return false;
}

bool containsOnlyLeft(IHqlExpression * expr, bool ignoreSelfOrFilepos)
{
    switch (expr->getOperator())
    {
    case no_right:
        return false;
    case no_select:
        return containsOnlyLeftTable(expr, ignoreSelfOrFilepos);
    case no_field:
    case no_table:
        return false;
    case no_filepos:
    case no_file_logicalname:
        return ignoreSelfOrFilepos;
    default:
        {
            unsigned max = expr->numChildren();
            unsigned idx;
            for (idx = 0; idx < max; idx++)
            {
                if (!containsOnlyLeft(expr->queryChild(idx), ignoreSelfOrFilepos))
                    return false;
            }
            return true;
        }
    }
}

IHqlExpression * queryPhysicalRootTable(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_keyindex:
        case no_newkeyindex:
        case no_table:
            return expr;
        }
        switch (getNumChildTables(expr))
        {
        case 1:
            expr = expr->queryChild(0);
            break;
        default:
            return NULL;
        }
    }
}


IHqlExpression * queryTableFilename(IHqlExpression * expr)
{
    IHqlExpression * table = queryPhysicalRootTable(expr);
    if (table)
    {
        switch (table->getOperator())
        {
        case no_keyindex:
            return table->queryChild(2);
        case no_newkeyindex:
            return table->queryChild(3);
        case no_table:
            return table->queryChild(0);
        }
    }
    return NULL;
}


IHqlExpression * createRawIndex(IHqlExpression * index)
{
    IHqlExpression * indexRecord = index->queryRecord();

    HqlExprArray fields;
    unwindChildren(fields, indexRecord);
    fields.pop();
    return createDataset(no_null, createRecord(fields), NULL);
}

//---------------------------------------------------------------------------------------------

IHqlExpression * createRecord(IHqlExpression * field)
{
    HqlExprArray fields;
    fields.append(*LINK(field));
    return createRecord(fields);
}

IHqlExpression * queryLastField(IHqlExpression * record)
{
    unsigned max = record->numChildren();
    while (max--)
    {
        IHqlExpression * cur = record->queryChild(max);
        switch (cur->getOperator())
        {
        case no_field:
            return cur;
        case no_ifblock:
            return queryLastField(cur->queryChild(1));
        case no_record:
            return queryLastField(cur);
        }
    }
    return NULL;
}

bool recordContainsBlobs(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            {
                if (cur->hasProperty(blobAtom))
                    return true;
                IHqlExpression * childRecord = cur->queryRecord();
                if (childRecord && recordContainsBlobs(childRecord))
                    return true;
                break;
            }
        case no_ifblock:
            if (recordContainsBlobs(cur->queryChild(1)))
                return true;
            break;
        case no_record:
            if (recordContainsBlobs(cur))
                return true;
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            UNIMPLEMENTED;
        }
    }
    return false;
}


IHqlExpression * queryVirtualFileposField(IHqlExpression * record)
{
    ForEachChild(idx, record)
    {
        IHqlExpression * cur = record->queryChild(idx);
        IHqlExpression * attr = cur->queryProperty(virtualAtom);
        if (attr)
            return cur;
    }
    return NULL;
}


//---------------------------------------------------------------------------

static IHqlExpression * queryOnlyTableChild(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select: case no_evaluate:
        return NULL;
    }

    IHqlExpression * ret = NULL;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (containsActiveDataset(cur))
        {
            if (ret)
                return NULL;
            ret = cur;
        }
    }
    return ret;
}


//The common bit between upper and lower has to be a function of right, and therefore not table invariant.
//Find it by unwinding all candidates from the lower, and then match from the upper.
static IHqlExpression * findCommonExpression(IHqlExpression * lower, IHqlExpression * upper)
{
    HqlExprCopyArray candidates;
    do
    {
        candidates.append(*lower);
        lower = queryOnlyTableChild(lower);
    } while (lower);

    do
    {
        if (candidates.find(*upper) != NotFound)
            return upper;
        upper = queryOnlyTableChild(upper);
    } while (upper);

    return NULL;
}





class JoinOrderSpotter
{
public:
    JoinOrderSpotter(IHqlExpression * _leftDs, IHqlExpression * _rightDs, IHqlExpression * seq, HqlExprArray & _leftSorts, HqlExprArray & _rightSorts) : leftSorts(_leftSorts), rightSorts(_rightSorts)
    {
        if (_leftDs)
            left.setown(createSelector(no_left, _leftDs, seq));
        if (_rightDs)
            right.setown(createSelector(no_right, _rightDs, seq));
    }

    IHqlExpression * doFindJoinSortOrders(IHqlExpression * condition, HqlExprArray * slidingMatches, HqlExprCopyArray & matched);
    void findImplicitBetween(IHqlExpression * condition, HqlExprArray & slidingMatches, HqlExprCopyArray & matched, HqlExprCopyArray & pending);

protected:
    IHqlExpression * traverseStripSelect(IHqlExpression * expr, node_operator & kind);
    IHqlExpression * cachedTraverseStripSelect(IHqlExpression * expr, node_operator & kind);
    IHqlExpression * doTraverseStripSelect(IHqlExpression * expr, node_operator & kind);
    void unwindSelectorRecord(HqlExprArray & target, IHqlExpression * selector, IHqlExpression * record);

protected:
    OwnedHqlExpr left;
    OwnedHqlExpr right;
    HqlExprArray & leftSorts;
    HqlExprArray & rightSorts;
};


IHqlExpression * JoinOrderSpotter::traverseStripSelect(IHqlExpression * expr, node_operator & kind)
{
    TransformMutexBlock block;
    return cachedTraverseStripSelect(expr, kind);
}

IHqlExpression * JoinOrderSpotter::cachedTraverseStripSelect(IHqlExpression * expr, node_operator & kind)
{
    IHqlExpression * matched = static_cast<IHqlExpression *>(expr->queryTransformExtra());
    if (matched)
        return LINK(matched);
    IHqlExpression * ret = doTraverseStripSelect(expr, kind);
    expr->setTransformExtra(ret);
    return ret;
}

IHqlExpression * JoinOrderSpotter::doTraverseStripSelect(IHqlExpression * expr, node_operator & kind)
{
    if (expr->getOperator()==no_select)
    {
        IHqlExpression * table = expr->queryChild(0);

        node_operator curKind = table->getOperator();
        if (curKind == no_select || expr->hasProperty(newAtom))
        {
            //I'm not sure this is a good idea for elements with newAtom - can end up with weird join conditions
            HqlExprArray args;
            args.append(*cachedTraverseStripSelect(table, kind));
            unwindChildren(args, expr, 1);
            return cloneOrLink(expr, args);
        }
        else if ((table == left) || (table == right))
        {
            if ((kind == no_none) || (kind == curKind))
            {
                kind = curKind;
                //return the unselected id.
                return createSelectExpr(getActiveTableSelector(), LINK(expr->queryChild(1)));
            }
            kind = no_fail;
        }
        //Cope with case when called from the parser and the expression tree isn't normalized.
        else if (!left && !right && ((curKind == no_left) || (curKind == no_right)))
        {
            kind = curKind;
            //return the unselected id.
            return createSelectExpr(getActiveTableSelector(), LINK(expr->queryChild(1)));
        }
    }
    else
    {
        unsigned max = expr->numChildren();
        if (max != 0)
        {
            HqlExprArray args;
            args.ensure(max);

            unsigned idx;
            bool same = true;
            for (idx = 0; idx<max;idx++)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                IHqlExpression * stripped = cachedTraverseStripSelect(cur, kind);
                args.append(*stripped);
                if (cur != stripped)
                    same = false;
            }
            if (!same)
                return expr->clone(args);
        }
    }
    return LINK(expr);
}


void JoinOrderSpotter::unwindSelectorRecord(HqlExprArray & target, IHqlExpression * selector, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            unwindSelectorRecord(target, selector, cur);
            break;
        case no_ifblock:
            unwindSelectorRecord(target, selector, cur->queryChild(1));
            break;
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                target.append(*selected.getClear());
                //MORE: Could expand nested rows
                break;
            }
        }
    }
}

IHqlExpression * JoinOrderSpotter::doFindJoinSortOrders(IHqlExpression * condition, HqlExprArray * slidingMatches, HqlExprCopyArray & matched)
{
    IHqlExpression *l = condition->queryChild(0);
    IHqlExpression *r = condition->queryChild(1);

    switch(condition->getOperator())
    {
    case no_and:
        {
            IHqlExpression *lmatch = doFindJoinSortOrders(l, slidingMatches, matched);
            IHqlExpression *rmatch = doFindJoinSortOrders(r, slidingMatches, matched);
            if (lmatch)
            {
                if (rmatch)
                    return createValue(no_and, lmatch, rmatch);
                else
                    return lmatch;
            }
            else
                return rmatch;
        }
    case no_constant:
        //remove silly "and true" conditions
        if (condition->queryValue()->getBoolValue())
            return NULL;
        return LINK(condition);
    case no_eq:
        {
            node_operator leftSelectKind = no_none;
            node_operator rightSelectKind = no_none;
            OwnedHqlExpr leftStrip = traverseStripSelect(l, leftSelectKind);
            OwnedHqlExpr rightStrip = traverseStripSelect(r, rightSelectKind);

            if ((leftSelectKind == no_left) && (rightSelectKind == no_right))
            {
                leftSorts.append(*leftStrip.getClear());
                rightSorts.append(*rightStrip.getClear());
                return NULL;
            }
            if ((leftSelectKind == no_right) && (rightSelectKind == no_left))
            {
                leftSorts.append(*rightStrip.getClear());
                rightSorts.append(*leftStrip.getClear());
                return NULL;
            }
            if (((l == left) && (r == right)) || ((l == right) && (r == left)))
            {
                unwindSelectorRecord(leftSorts, queryActiveTableSelector(), left->queryRecord());
                unwindSelectorRecord(rightSorts, queryActiveTableSelector(), right->queryRecord());
                return NULL;
            }
        }
        return LINK(condition);
    case no_between:
        if (slidingMatches)
        {
            node_operator leftSelectKind = no_none;
            node_operator rightSelectKind = no_none;
            OwnedHqlExpr leftStrip = traverseStripSelect(l, leftSelectKind);
            OwnedHqlExpr lowerStrip = traverseStripSelect(r, rightSelectKind);
            OwnedHqlExpr upperStrip = traverseStripSelect(condition->queryChild(2), rightSelectKind);

            if ((leftSelectKind == no_left) && (rightSelectKind == no_right))
            {
                //Find the expression of the rhs that is common to lower and upper
                IHqlExpression * common = findCommonExpression(lowerStrip,upperStrip);
                if (common)
                {
                    slidingMatches->append(*createValue(no_between, makeBoolType(), LINK(leftStrip), LINK(lowerStrip), LINK(upperStrip), createExprAttribute(commonAtom, LINK(common))));
                    return NULL;
                }
            }
        }
        return LINK(condition);

    case no_le:
    case no_ge:
        if (matched.find(*condition) != NotFound)
            return NULL;
        return LINK(condition);

    default:
        return LINK(condition);
    }
}

void JoinOrderSpotter::findImplicitBetween(IHqlExpression * condition, HqlExprArray & slidingMatches, HqlExprCopyArray & matched, HqlExprCopyArray & pending)
{
    IHqlExpression *l = condition->queryChild(0);
    IHqlExpression *r = condition->queryChild(1);

    node_operator op = condition->getOperator();
    switch (op)
    {
    case no_and:
        {
            findImplicitBetween(l, slidingMatches, matched, pending);
            findImplicitBetween(r, slidingMatches, matched, pending);
            break;
        }
    case no_ge:
    case no_le:
        {
            node_operator search = (op == no_ge) ? no_le : no_ge;
            ForEachItemIn(idx, pending)
            {
                IHqlExpression & cur = pending.item(idx);
                if ((cur.getOperator() == search) && (cur.queryChild(0) == condition->queryChild(0)))
                {
                    node_operator leftSelectKind = no_none;
                    node_operator rightSelectKind = no_none;
                    IHqlExpression * lower = (op == no_ge) ? condition->queryChild(1) : cur.queryChild(1);
                    IHqlExpression * upper = (op == no_ge) ? cur.queryChild(1) : condition->queryChild(1);
                    OwnedHqlExpr leftStrip = traverseStripSelect(condition->queryChild(0), leftSelectKind);
                    OwnedHqlExpr lowerStrip = traverseStripSelect(lower, rightSelectKind);
                    OwnedHqlExpr upperStrip = traverseStripSelect(upper, rightSelectKind);

                    if ((leftSelectKind == no_left) && (rightSelectKind == no_right))
                    {
                        //Find the expression of the rhs that is common to lower and upper
                        IHqlExpression * common = findCommonExpression(lowerStrip,upperStrip);
                        if (common)
                        {
                            slidingMatches.append(*createValue(no_between, makeBoolType(), LINK(leftStrip), LINK(lowerStrip), LINK(upperStrip), createExprAttribute(commonAtom, LINK(common))));
                            matched.append(*condition);
                            matched.append(cur);
                            pending.zap(cur);
                            return;
                        }
                    }
                }
            }
            pending.append(*condition);
            break;
        }
    }
}

static bool isCommonSubstringRange(IHqlExpression * expr)
{
    if (expr->getOperator() != no_substring)
        return false;
    IHqlExpression * range = expr->queryChild(1);
    return (range->getOperator() == no_rangecommon);
}

static IHqlExpression * getSimplifiedCommonSubstringRange(IHqlExpression * expr)
{
    IHqlExpression * rawSelect = expr->queryChild(0);
    IHqlExpression * range = expr->queryChild(1);
    IHqlExpression * rangeLow = range->queryChild(0);
    if (matchesConstantValue(rangeLow, 1))
        return LINK(rawSelect);
    HqlExprArray args;
    args.append(*LINK(rawSelect));
    args.append(*createValue(no_rangefrom, makeNullType(), LINK(rangeLow)));
    return expr->clone(args);
}


IHqlExpression * findJoinSortOrders(IHqlExpression * condition, IHqlExpression * leftDs, IHqlExpression * rightDs, IHqlExpression * seq, HqlExprArray &leftSorts, HqlExprArray &rightSorts, bool & isLimitedSubstringJoin, HqlExprArray * slidingMatches)
{
    JoinOrderSpotter spotter(leftDs, rightDs, seq, leftSorts, rightSorts);
    HqlExprCopyArray matched;
    if (slidingMatches)
    {
        //First spot any implicit betweens using x >= a and x <= b.  Do it first so that the second pass doesn't
        //reorder the join condition (this still reorders it slightly by moving the implicit betweens before explicit)
        HqlExprCopyArray pending;
        spotter.findImplicitBetween(condition, *slidingMatches, matched, pending);
    }
    OwnedHqlExpr ret = spotter.doFindJoinSortOrders(condition, slidingMatches, matched);
    
    //Check for x[n..*] - a no_rangecommon, and ensure they are tagged as the last sorts.
    unsigned numCommonRange = 0;
    ForEachItemInRev(i, leftSorts)
    {
        IHqlExpression & left = leftSorts.item(i);
        IHqlExpression & right = rightSorts.item(i);
        if (isCommonSubstringRange(&left))
        {
            if (isCommonSubstringRange(&right))
            {
                //MORE: May be best to remove the substring syntax as this point - or modify it if start != 1.
                leftSorts.append(*getSimplifiedCommonSubstringRange(&left));
                leftSorts.remove(i);
                rightSorts.append(*getSimplifiedCommonSubstringRange(&right));
                rightSorts.remove(i);
                numCommonRange++;
            }
            else
                throwError(HQLERR_AtmostSubstringNotMatch);
        }
        else
        {
            if (isCommonSubstringRange(&right))
                throwError(HQLERR_AtmostSubstringNotMatch);
        }
    }
    isLimitedSubstringJoin = numCommonRange != 0;
    if (numCommonRange > 1)
        throwError(HQLERR_AtmostSubstringSingleInstance);

    if ((numCommonRange == 0) && slidingMatches)
    {
        ForEachItemIn(i, *slidingMatches)
        {
            IHqlExpression & cur = slidingMatches->item(i);
            leftSorts.append(*LINK(cur.queryChild(0)));
            rightSorts.append(*LINK(cur.queryChild(3)->queryChild(0)));
        }
    }


    return ret.getClear();
}

extern HQL_API IHqlExpression * findJoinSortOrders(IHqlExpression * expr, HqlExprArray &leftSorts, HqlExprArray &rightSorts, bool & isLimitedSubstringJoin, HqlExprArray * slidingMatches)
{
    IHqlExpression * lhs = expr->queryChild(0);
    return findJoinSortOrders(expr->queryChild(2), lhs, queryJoinRhs(expr), querySelSeq(expr), leftSorts, rightSorts, isLimitedSubstringJoin, slidingMatches);
}

IHqlExpression * createImpureOwn(IHqlExpression * expr)
{
    return createValue(no_impure, expr->getType(), expr);
}

IHqlExpression * getNormalizedFilename(IHqlExpression * filename)
{
    OwnedHqlExpr folded = foldHqlExpression(filename, NULL, HFOloseannotations);
    return lowerCaseHqlExpr(folded);
}

bool canBeSlidingJoin(IHqlExpression * expr)
{
    if (expr->hasProperty(hashAtom) || expr->hasProperty(lookupAtom) || expr->hasProperty(allAtom))
        return false;
    if (expr->hasProperty(rightouterAtom) || expr->hasProperty(fullouterAtom) ||
        expr->hasProperty(leftonlyAtom) || expr->hasProperty(rightonlyAtom) || expr->hasProperty(fullonlyAtom))
        return false;
    if (expr->hasProperty(atmostAtom))
        return false;
    return true;
}

//==============================================================================================================


extern HQL_API bool dedupMatchesWholeRecord(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_dedup);
    unsigned max = expr->numChildren();
    unsigned idx;
    for (idx = 1; idx < max; idx++)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            return false;
        }
    }
    return true;
}

IHqlExpression * getEquality(IHqlExpression * equality, IHqlExpression * left, IHqlExpression * right, IHqlExpression * activeSelector)
{
    IHqlExpression * lhs = equality->queryChild(0);
    IHqlExpression * rhs = equality->queryChild(1);
    if (containsSelector(lhs, left))
    {
        OwnedHqlExpr mappedLeft = replaceSelector(lhs, left, activeSelector);
        OwnedHqlExpr mappedRight = replaceSelector(rhs, right, activeSelector);
        if (mappedLeft == mappedRight)
            return mappedLeft.getClear();
    }
    else if (containsSelector(lhs, right))
    {
        OwnedHqlExpr mappedLeft = replaceSelector(lhs, right, activeSelector);
        OwnedHqlExpr mappedRight = replaceSelector(rhs, left, activeSelector);
        if (mappedLeft == mappedRight)
            return mappedLeft.getClear();
    }
    return NULL;
}


DedupInfoExtractor::DedupInfoExtractor(IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * record = dataset->queryRecord();
    IHqlExpression * selSeq = querySelSeq(expr);
    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr right = createSelector(no_right, dataset, selSeq);
    compareAllRows = false;
    compareAllFields = false;
    isLocal = false;
    keepLeft = true;
    numToKeep.setown(createConstantOne());

    unsigned max = expr->numChildren();
    unsigned idx;
    for (idx = 1; idx < max; idx++)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            {
                _ATOM name = cur->queryName();
                if (name == hashAtom)
                    compareAllRows = true;
                else if (name == localAtom)
                    isLocal = true;
                else if (name == allAtom)
                    compareAllRows = true;
                else if (name == keepAtom)
                    numToKeep.set(cur->queryChild(0));
                else if (name == leftAtom)
                    keepLeft = true;
                else if (name == rightAtom)
                    keepLeft = false;
            }
            break;
        case no_negate:
            {
                IHqlExpression * field = cur->queryChild(0);
                if (field->getOperator() == no_select)
                    field = field->queryChild(1);
                if (!equalities.zap(*field))
                    throwError(HQLERR_DedupFieldNotFound);
            }
            break;
        case no_eq:
            {
                OwnedHqlExpr mapped = getEquality(cur, left, right, dataset->queryNormalizedSelector());
                if (mapped)
                {
                    equalities.append(*mapped.getClear());
                    break;
                }
                //fall through
            }
        default:
            if (containsSelector(cur, left) || containsSelector(cur, right))
                conds.append(*LINK(cur));
            else
                equalities.append(*LINK(cur));
            break;
        }
    }
    if ((equalities.ordinality() == 0) && (conds.ordinality() == 0))
    {
        unwindRecordAsSelects(equalities, record, dataset->queryNormalizedSelector());
        compareAllFields = true;
    }
#ifdef _DEBUG
    //Check to ensure the function stays in sync with the code above
    assertex(compareAllFields == dedupMatchesWholeRecord(expr));
#endif
}



DedupInfoExtractor::DedupKeyCompareKind DedupInfoExtractor::compareKeys(const DedupInfoExtractor & other)
{
    //MORE: These could be coped with a bit better...
    if ((conds.ordinality() != 0) || (other.conds.ordinality() != 0))
        return DedupKeyIsDifferent;

    const HqlExprArray & otherEqualities = other.equalities;
    unsigned num1 = equalities.ordinality();
    unsigned num2 = otherEqualities.ordinality();
    unsigned numMissing = 0;
    ForEachItemIn(i, equalities)
    {
        if (otherEqualities.find(equalities.item(i)) == NotFound)
            numMissing++;
    }
    if (numMissing)
    {
        if (num1 == num2 + numMissing)
            return DedupKeyIsSuperset;
        return DedupKeyIsDifferent;
    }
    if (num1 == num2)
        return DedupKeyIsSame;
    return DedupKeyIsSubset;
}


DedupInfoExtractor::DedupCompareKind DedupInfoExtractor::compareWith(const DedupInfoExtractor & other)
{
    if ((keepLeft != other.keepLeft) || !getConstantKeep() || !other.getConstantKeep())
        return DedupIsDifferent;
    if (isLocal != other.isLocal)
        return DedupIsDifferent;

    switch (compareKeys(other))
    {
    case DedupKeyIsSame:
        if (compareAllRows == other.compareAllRows)
        {
            if (getConstantKeep() < other.getConstantKeep())
                return DedupDoesAll;
            else
                return DedupDoesNothing;
        }
        else
        {
            //dedup(dedup(x,y),y,all) cannot be reduced to dedup(x,y,all) because it may include
            //records that wouldn't have otherwise got through.  dedup(dedup(x,y,all),y) can be though
            if (other.compareAllRows)
            {
                if (getConstantKeep() >= other.getConstantKeep())
                    return DedupDoesNothing;
            }
        }
        break;
    case DedupKeyIsSubset:
        //optimize dedup(dedup(x,y1,y2,keep(2)),y1,keep(2)) to dedup(x,y1,keep(2)) if keep is same
        if (compareAllRows == other.compareAllRows && (getConstantKeep() == other.getConstantKeep()))
            return DedupDoesAll;
        //optimize dedup(dedup(x,y1,y2,keep(2),all),y1,keep(1)) to dedup(x,y1,keep(1))
        if (compareAllRows && other.compareAllRows && (getConstantKeep() <= other.getConstantKeep()))
            return DedupDoesAll;
        break;
    case DedupKeyIsSuperset:
        if (compareAllRows == other.compareAllRows && (getConstantKeep() == other.getConstantKeep()))
            return DedupDoesNothing;
        if (compareAllRows && other.compareAllRows && (getConstantKeep() >= other.getConstantKeep()))
            return DedupDoesNothing;
        break;
    }
    return DedupIsDifferent;
}

IHqlExpression * replaceChild(IHqlExpression * expr, unsigned childIndex, IHqlExpression * newChild)
{
    IHqlExpression * oldChild = expr->queryChild(childIndex);
    if (oldChild == newChild)
        return LINK(expr);
    HqlExprArray args;
    unwindChildren(args, expr);
    args.replace(*LINK(newChild), childIndex);
    return expr->clone(args);
}


IHqlExpression * createIf(IHqlExpression * cond, IHqlExpression * left, IHqlExpression * right)
{
    if (left->isDataset() || right->isDataset())
        return createDataset(no_if, cond, createComma(left, right));

    if (left->isDatarow() || right->isDatarow())
        return createRow(no_if, cond, createComma(left, right));

    ITypeInfo * type = ::getPromotedECLType(left->queryType(), right->queryType());
    return createValue(no_if, type, cond, left, right);
}

extern HQL_API unsigned numRealChildren(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    //Assumes all attributes occur at the end of the operand lists
    while (max && expr->queryChild(max-1)->isAttribute())
        max--;
    return max;
}


//---------------------------------------------------------------------------

static IHqlExpression * getExpandSelectExprTest(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_select);

    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * field = expr->queryChild(1);
    if (field->queryRecord() || !queryNewColumnProvider(ds))
        return NULL;

    TableProjectMapper mapper(ds);
    return mapper.expandFields(expr, ds, ds->queryChild(0)->queryNormalizedSelector());
}

IHqlExpression * getExpandSelectExpr(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_select);

    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * field = expr->queryChild(1);
    if (field->queryRecord())
        return NULL;

    IHqlExpression * mappingExpr = queryNewColumnProvider(ds);
    if (mappingExpr)
    {
        IHqlExpression * ret = NULL;
        switch (mappingExpr->getOperator())
        {
        case no_record:
            {
                IHqlSimpleScope * scope = mappingExpr->querySimpleScope();
                OwnedHqlExpr matched = scope->lookupSymbol(field->queryName());
                assertex(matched == field);
                ret = LINK(queryRealChild(field, 0));
                break;
            }
        case no_newtransform:
            {
                ForEachChild(idx, mappingExpr)
                {
                    IHqlExpression * cur = mappingExpr->queryChild(idx);
                    IHqlExpression * tgt = cur->queryChild(0);
                    if (tgt->getOperator() == no_select && tgt->queryChild(1) == field)
                    {
                        IHqlExpression * src = cur->queryChild(1);
                        ret = ensureExprType(src, tgt->queryType());
                        break;
                    }
                }
                assertex(ret);
                break;
            }
        case no_transform:
            {
                //probably just as efficient..., and not used.
                ret = getExpandSelectExprTest(expr);
                break;
            }
        }
        //OwnedHqlExpr test = getExpandSelectExprTest(expr);
        //assertex(ret==test);
        return ret;
    }
    return NULL;
}


//---------------------------------------------------------------------------

IHqlExpression * replaceChildDataset(IHqlExpression * expr, IHqlExpression * newChild, unsigned whichChild)
{
    HqlMapTransformer mapper;
    IHqlExpression * oldChild = expr->queryChild(whichChild);
    mapper.setMapping(oldChild, newChild);
    mapper.setSelectorMapping(oldChild, newChild);
    return mapper.transformRoot(expr);
}

IHqlExpression * insertChildDataset(IHqlExpression * expr, IHqlExpression * newChild, unsigned whichChild)
{
    assertex(expr->queryChild(whichChild) == newChild->queryChild(0));
    //No need to map because children are still valid...
    HqlExprArray args;
    unwindChildren(args, expr);
    args.replace(*LINK(newChild), whichChild);
    return expr->clone(args);
}

IHqlExpression * swapDatasets(IHqlExpression * parent)
{
    IHqlExpression * child = parent->queryChild(0);
    OwnedHqlExpr newChild = replaceChildDataset(parent, child->queryChild(0), 0);       // any refs to child must be mapped.
    return insertChildDataset(child, newChild, 0);
}


//---------------------------------------------------------------------------

interface IHintVisitor
{
    virtual IHqlExpression * visit(IHqlExpression * hint) = 0;
};

class SearchHintVisitor : implements IHintVisitor
{
public:
    SearchHintVisitor(_ATOM _name) : name(_name) {}

    virtual IHqlExpression * visit(IHqlExpression * hint) 
    {
        return hint->queryProperty(name);
    }

    _ATOM name;
};


class GatherHintVisitor : implements IHintVisitor
{
public:
    GatherHintVisitor(HqlExprCopyArray & _target) : target(_target) {}

    virtual IHqlExpression * visit(IHqlExpression * hint) 
    {
        unwindChildren(target, hint);
        return NULL;
    }

    HqlExprCopyArray & target;
};

static IHqlExpression * walkHints(IHqlExpression * expr, IHintVisitor & visitor)
{
    //First look for any hint annotations.
    loop
    {
        annotate_kind kind = expr->getAnnotationKind();
        if (kind == annotate_meta)
        {
            unsigned i=0;
            IHqlExpression * cur;
            while ((cur = expr->queryAnnotationParameter(i++)) != NULL)
            {
                if (cur->queryName() == hintAtom && cur->isAttribute())
                {
                    IHqlExpression * ret = visitor.visit(cur);
                    if (ret)
                        return ret;
                }
            }
        }
        if (kind == annotate_none)
            break;
        expr = expr->queryBody(true);
    }

    //Then look for any hint attributes.
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if ((cur->queryName() == hintAtom) && cur->isAttribute())
        {
            IHqlExpression * match = visitor.visit(cur);
            if (match)
                return match;
        }
    }
    return NULL;
}

IHqlExpression * queryHint(IHqlExpression * expr, _ATOM name)
{
    SearchHintVisitor visitor(name);
    return walkHints(expr, visitor);
}

void gatherHints(HqlExprCopyArray & target, IHqlExpression * expr)
{
    GatherHintVisitor visitor(target);
    walkHints(expr, visitor);
}

IHqlExpression * queryHintChild(IHqlExpression * expr, _ATOM name, unsigned idx)
{
    IHqlExpression * match = queryHint(expr, name);
    if (match)
        return match->queryChild(idx);
    return NULL;
}


//---------------------------------------------------------------------------

IHqlExpression * createCompare(node_operator op, IHqlExpression * l, IHqlExpression * r)
{
    if ((l->getOperator() == no_constant) && (r->getOperator() != no_constant))
        return createCompare(getReverseOp(op), r, l);

    ITypeInfo * t1 = l->queryType();
    ITypeInfo * t2 = r->queryType();

    //Check for comparisons that are always true/false....
    IValue * value = r->queryValue();
    if (value)
    {
        //Sometimes comparing an unsigned field with a constant can cause both parameters to be promoted
        //to a larger type, because constants default to signed if small enough.
        if (t1->getTypeCode() == type_int)
        {
            if (!t1->isSigned() && t2->isSigned() && t1->getSize() >= t2->getSize())
            {
                if (value->getIntValue() >= 0)
                    return createValue(op, makeBoolType(), LINK(l), ensureExprType(r, t1));
            }

            if ((queryUnqualifiedType(t1) != queryUnqualifiedType(t2)) && preservesValue(t1, r))
            {
                OwnedHqlExpr cast = ensureExprType(r, t1);
                if (r != cast)
                    return createCompare(op, l, cast);
            }
        }
    }

    Owned<ITypeInfo> compareType = getPromotedECLCompareType(t1, t2);
    return createValue(op, makeBoolType(), ensureExprType(l, compareType), ensureExprType(r, compareType));
}

IHqlExpression * flattenListOwn(IHqlExpression * list)
{
    HqlExprArray args;
    ITypeInfo * type = list->getType();
    flattenListOwn(args, list);
    return createValue(no_comma, type, args);
}


void flattenListOwn(HqlExprArray & out, IHqlExpression * list)
{
    list->unwindList(out, no_comma);
    releaseList(list);
}

void releaseList(IHqlExpression * list)
{
    //normally lists are (((((a,b),c),d),e),f)
    //so release rhs, and loop through lhs to reduce stack usage
    while (list->getOperator() == no_comma)
    {
        IHqlExpression * next = LINK(list->queryChild(0));
        list->Release();
        list = next;
    }
    list->Release();
}


void expandRowSelectors(HqlExprArray & target, HqlExprArray const & source)
{
    ForEachItemIn(i, source)
    {
        IHqlExpression & cur = source.item(i);
        if (cur.isDatarow())
        {
            RecordSelectIterator iter(cur.queryRecord(), &cur);
            ForEach(iter)
                target.append(*iter.get());
        }
        else
            target.append(OLINK(cur));
    }
}
        
//---------------------------------------------------------------------------

unsigned getFirstActivityArgument(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_mapto:
    case no_if:
    case no_case:
    case no_fetch:
    case no_libraryselect:
        return 1;
    }
    return 0;
}

unsigned getNumActivityArguments(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_compound_diskread:
    case no_compound_indexread:
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
    case no_compound_inline:
    case no_keyindex:
    case no_newkeyindex:
    case no_table:
    case no_preload:
    case no_countfile:
    case no_allnodes:
    case no_thisnode:
    case no_keydiff:
        return 0;
    case no_setresult:
        if (expr->queryChild(0)->isAction())
            return 1;
        return 0;
    case no_compound_selectnew:
    case no_libraryselect:
        return 1;
    case no_libraryscopeinstance:
        {
            //It would be very nice to be able to cache this, but because the arguments are associated with the
            //no_funcdef instead of the no_libraryscope it is a bit tricky.  It could be optimized onto the 
            //no_libraryscopeinstance I guess.
            IHqlExpression * libraryFuncDef = expr->queryDefinition();
            IHqlExpression * library = libraryFuncDef->queryChild(0);
            if (library->hasProperty(_noStreaming_Atom))
                return 0;
            IHqlExpression * libraryFormals = libraryFuncDef->queryChild(1);
            unsigned numStreaming = 0;
            ForEachChild(i, libraryFormals)
            {
                if (libraryFormals->queryChild(i)->isDataset())
                    numStreaming++;
            }
            return numStreaming;
        }
    case no_select:
        if (isNewSelector(expr))
            return 1;
        return 0;
    case no_datasetfromrow:
        {
            IHqlExpression * row = expr->queryChild(0);
            //Is this special casing really the best way to handle this?  I'm not completely convinced.
            loop
            {
                switch (row->getOperator())
                {
                case no_selectnth:
                case no_split:
                case no_spill:
                    return 1;
                case no_alias:
                case no_alias_scope:
                case no_nofold:
                case no_nohoist:
                case no_section:
                case no_sectioninput:
                case no_globalscope:
                case no_thisnode:
                    break;
                default:
                    return 0;
                }
                row = row->queryChild(0);
            }
        }
    case no_fetch:
    case no_mapto:
    case no_evaluate:
    case no_extractresult:
    case no_outputscalar:
    case no_keyeddistribute:
    case no_normalize:
    case no_process:
    case no_mergejoin:
    case no_nwayjoin:
    case no_nwaymerge:
    case no_related:        // not an activity
        return 1;
    case no_denormalize:
    case no_denormalizegroup:
    case no_join:
    case no_joincount:
        if (isKeyedJoin(expr) && !expr->hasProperty(_complexKeyed_Atom))
            return 1;
        return 2;
    case no_combine:
    case no_combinegroup:
    case no_executewhen:
        return 2;
    case no_if:
        if (queryRealChild(expr, 2))
            return 2;
        return 1;
    case no_sequential:
    case no_parallel:
    case no_actionlist:
    case no_comma:
    case no_compound:
    case no_addfiles:
    case no_map:
        return expr->numChildren();
    case no_case:
        return expr->numChildren()-1;
    case no_forcelocal:
        return 0;
    default:
        return getNumChildTables(expr);
    }
}


bool isDistributedSourceActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_table:
    case no_keyindex:
    case no_newkeyindex:
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
        return true;
    case no_getgraphresult:
        return expr->hasProperty(_distributed_Atom);
    case no_workunit_dataset:
    case no_getgraphloopresult:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_datasetfromrow:
    case no_null:
    case no_all:
    case no_select:
    case no_soapcall:
    case no_newsoapcall:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_rows:
        return false;
    default:
        UNIMPLEMENTED;
    }
}

bool isSourceActivity(IHqlExpression * expr, bool ignoreCompound)
{
    switch (expr->getOperator())
    {
    case no_table:
    case no_keyindex:
    case no_newkeyindex:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
//  case no_all:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_rows:
    case no_allnodes:
    case no_thisnode:
        return true;
    case no_null:
        return expr->isDataset();
    case no_select:
        if (isNewSelector(expr))
            return false;
        return expr->isDataset();
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
    case no_compound_inline:
        return !ignoreCompound;
    case no_datasetfromrow:
        return (getNumActivityArguments(expr) == 0);
    }
    return false;
}

bool isSinkActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_parallel:
    case no_actionlist:
    case no_sequential:
    case no_apply:
    case no_output:
    case no_buildindex:
    case no_distribution:
    case no_keydiff:
    case no_keypatch:
    case no_returnresult:
    case no_extractresult:
    case no_setresult:
    case no_setgraphresult:
    case no_setgraphloopresult:
    case no_updatestate:
    case no_definesideeffect:
    //case no_callsideeffect:       //??
        return true;
    case no_soapcall:
    case no_newsoapcall:
    case no_if:
    case no_null:
        return expr->isAction();
    }
    return false;
}

bool isDistributedActivity(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_table:
    case no_keyindex:
    case no_newkeyindex:
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
        return true;
    case no_join:
    case no_joincount:
    case no_denormalize:
    case no_denormalizegroup:
        return isKeyedJoin(expr);
    case no_fetch:
    case no_compound_fetch:
        return true;
    }
    return false;
}

unsigned getFieldCount(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            unsigned count = 0;
            ForEachChild(i, expr)
                count += getFieldCount(expr->queryChild(i));
            return count;
        }
    case no_ifblock:
        return getFieldCount(expr->queryChild(1));
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            if (type->getTypeCode() == type_row)
                return getFieldCount(expr->queryRecord());
            return 1;
        }
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        return 0;
    default:
        //UNIMPLEMENTED;
        return 0;
    }
}


IHqlExpression * queryChildActivity(IHqlExpression * expr, unsigned index)
{
    unsigned firstActivityIndex = 0;
    switch (expr->getOperator())
    {
    case no_compound_selectnew:
        if (index == 0)
            return queryRoot(expr)->queryChild(0);
        return NULL;
    case no_mapto:
    case no_if:
    case no_case:
    case no_fetch:
        firstActivityIndex = 1;
        break;
    }

    return queryRealChild(expr, firstActivityIndex + index);
}


unsigned getFlatFieldCount(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            unsigned count = 0;
            ForEachChild(i, expr)
                count += getFlatFieldCount(expr->queryChild(i));
            return count;
        }
    case no_ifblock:
        return getFlatFieldCount(expr->queryChild(1));
    case no_field:
        return 1;
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        return 0;
    default:
        UNIMPLEMENTED;
    }
}


unsigned isEmptyRecord(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (!isEmptyRecord(cur))
                return false;
            break;
        case no_ifblock:
            if (!isEmptyRecord(cur->queryChild(1)))
                return false;
            break;
        case no_field:
            return false;
        }
    }
    return true;
}


bool isTrivialSelectN(IHqlExpression * expr)
{
    IHqlExpression * index = expr->queryChild(1);
    if (index->queryValue() && (index->queryValue()->getIntValue() == 1))
        return hasSingleRow(expr->queryChild(0));
    return false;
}

IHqlExpression * queryPropertyChild(IHqlExpression * expr, _ATOM name, unsigned idx)
{
    IHqlExpression * match = expr->queryProperty(name);
    if (match)
        return match->queryChild(idx);
    return NULL;
}

int getResultSequenceValue(IHqlExpression * set)
{
    switch (set->getOperator())
    {
    case no_setresult:
    case no_ensureresult:
    case no_extractresult:
    case no_output:
        return (int)getIntValue(queryPropertyChild(set, sequenceAtom, 0), 0);
    }
    return 0;
}


IHqlExpression * querySequence(IHqlExpression * expr)
{
    IHqlExpression * seq = expr->queryProperty(sequenceAtom);
    if (seq)
        return seq->queryChild(0);
    if (expr->queryValue())
        return expr;
    return NULL;
}

IHqlExpression * queryResultName(IHqlExpression * expr)
{
    IHqlExpression * name = expr->queryProperty(namedAtom);
    if (name)
        return name->queryChild(0);
    return NULL;
}

//---------------------------------------------------------------------------

IHqlExpression * queryConvertChoosenNSort(IHqlExpression * expr, unsigned __int64 topNlimit)
{
    OwnedHqlExpr first = foldHqlExpression(queryRealChild(expr, 2));
    IHqlExpression * child = expr->queryChild(0);
    if ((child->getOperator() != no_sort) || isGroupedActivity(child))
        return NULL;
    IHqlExpression * cosort = queryRealChild(child, 2);
    if (cosort)
        return NULL;
    
    //grouped sort->choosen.  Don't convert unless choosen preserves grouping
    if (isGrouped(child) && !expr->hasProperty(groupedAtom))
        return NULL;

    OwnedHqlExpr count = foldHqlExpression(expr->queryChild(1));
    bool clone = false;
    if (count->queryValue())
    {
        unsigned __int64 limit = count->queryValue()->getIntValue();
        if (first)
        {
            if (!first->queryValue())
                return NULL;
            limit += (first->queryValue()->getIntValue() - 1);
            count.setown(createConstant((__int64)limit));
            clone = true;
        }
        if (limit > topNlimit)
            return NULL;
    }
    else 
    {
        if (!expr->hasProperty(fewAtom) || first)
            return NULL;
    }

    //choosen(sort(x,a,local),n) -> do the topn local, but need to reapply the global choosen
    if (expr->hasProperty(localAtom))
    {
        if (!child->hasProperty(localAtom))
            return NULL;
    }
    else
    {
        if (child->hasProperty(localAtom))
            clone = true;
    }
    HqlExprArray args;
    unwindChildren(args, child);
    args.add(*LINK(count), 2);
    IHqlExpression * top = createDataset(no_topn, args);
    if (!clone)
        return top;
    args.kill();
    unwindChildren(args, expr);
    args.replace(*top, 0);
    return expr->clone(args);
}

//---------------------------------------------------------------------------

void DependenciesUsed::clear()
{
    tablesRead.kill();
    tablesWritten.kill();
    resultsRead.kill();
    resultsWritten.kill();
    allRead = false;
    allWritten = false;
}

bool DependenciesUsed::canSwapOrder(const DependenciesUsed & other) const
{
    //Dependant on output from the previous
    if (isDependantOn(other) || other.isDependantOn(*this))
        return false;
    return true;
}

bool DependenciesUsed::isDependantOn(const DependenciesUsed & other) const
{
    //Dependant on output from the previous
    if (allRead && (other.allWritten || other.tablesWritten.ordinality()))
        return true;
    if (other.allWritten && tablesRead.ordinality())
        return true;
    return isExplicitlyDependantOn(other);
}


bool DependenciesUsed::isExplicitlyDependantOn(const DependenciesUsed & other) const
{
    ForEachItemIn(idx1, tablesRead)
    {
        if (other.tablesWritten.find(tablesRead.item(idx1)) != NotFound)
            return true;
    }
    ForEachItemIn(idx2, resultsRead)
    {
        if (other.resultsWritten.find(resultsRead.item(idx2)) != NotFound)
            return true;
    }
    return false;
}


void DependenciesUsed::addFilenameRead(IHqlExpression * expr)
{
    OwnedHqlExpr normalized = getNormalizedFilename(expr);
    if (tablesRead.find(*normalized) == NotFound)
    {
        appendUniqueExpr(tablesRead, LINK(normalized));
        if (!normalized->queryValue())
            allRead = true;
    }
}

void DependenciesUsed::addFilenameWrite(IHqlExpression * expr)
{
    OwnedHqlExpr normalized = getNormalizedFilename(expr);
    if (appendUniqueExpr(tablesWritten, LINK(normalized)))
        if (tablesRead.contains(*normalized))
            noteInconsistency(normalized);
    if (!normalized->queryValue())
        allWritten = true;
}

void DependenciesUsed::addResultRead(IHqlExpression * seq, IHqlExpression * name, bool isGraphResult)
{
    if (!isGraphResult)
        if (!seq || !seq->queryValue())
            return;         //Can be called in parser when no sequence has been allocated
    OwnedHqlExpr result = createAttribute(resultAtom, LINK(seq), LINK(name));
    if (resultsWritten.find(*result) == NotFound)
        appendUniqueExpr(resultsRead, LINK(result));
}

void DependenciesUsed::addResultWrite(IHqlExpression * seq, IHqlExpression * name, bool isGraphResult)
{
    if (!isGraphResult)
        if (!seq || !seq->queryValue())
            return;         //Can be called in parser when no sequence has been allocated
    OwnedHqlExpr result = createAttribute(resultAtom, LINK(seq), LINK(name));
    if (appendUniqueExpr(resultsWritten, LINK(result)))
        if (resultsRead.contains(*result))
            noteInconsistency(result);
}


void DependenciesUsed::addRefDependency(IHqlExpression * expr)
{
    IHqlExpression * filename = queryTableFilename(expr);
    if (filename)
        addFilenameRead(filename);
}

IHqlExpression * DependenciesUsed::getNormalizedFilename(IHqlExpression * filename)
{
    if (normalize)
        return ::getNormalizedFilename(filename);
    return LINK(filename);
}

bool DependenciesUsed::isSubsetOf(const DependenciesUsed & other) const
{
    ForEachItemIn(idx1, tablesRead)
        if (!other.tablesRead.contains(tablesRead.item(idx1)))
            return false;

    ForEachItemIn(idx2, resultsRead)
        if (!other.resultsRead.contains(resultsRead.item(idx2)))
            return false;

    return true;
}

void DependenciesUsed::mergeIn(const DependenciesUsed & other)
{
    appendArray(tablesRead, other.tablesRead);
    appendArray(tablesWritten, other.tablesWritten);
    appendArray(resultsRead, other.resultsRead);
    appendArray(resultsWritten, other.resultsWritten);
    if (other.allRead)
        allRead = true;
    if (other.allWritten)
        allWritten = true;
}

void DependenciesUsed::extractDependencies(IHqlExpression * expr, unsigned flags)
{
    switch (expr->getOperator())
    {
    case no_buildindex:
    case no_output:
        {
            IHqlExpression * out = queryRealChild(expr, 1);
            if (out)
            {
                if (flags & GatherFileWrite)
                {
                    switch (out->getOperator())
                    {
                    case no_pipe:
                        allWritten = true;
                        break;
                    default:
                        addFilenameWrite(out);
                        break;
                    }
                }
            }
            else
            {
                if (flags & GatherResultWrite)
                    addResultWrite(querySequence(expr), queryResultName(expr), false);
            }
        }
        break;
    case no_newkeyindex:
    case no_keyindex:
        if (flags & GatherFileRead)
            addRefDependency(expr);
        break;
    case no_keydiff:
        if (flags & GatherFileRead)
        {
            addRefDependency(expr->queryChild(0));
            addRefDependency(expr->queryChild(1));
        }
        if (flags & GatherFileWrite)
            addFilenameWrite(expr->queryChild(2));
        break;
    case no_keypatch:
        if (flags & GatherFileRead)
        {
            addRefDependency(expr->queryChild(0));
            addFilenameRead(expr->queryChild(1));
        }
        if (flags & GatherFileWrite)
            addFilenameWrite(expr->queryChild(2));
        break;
    case no_table:
        if (flags & GatherFileRead)
        {
            IHqlExpression * in = expr->queryChild(0);
            IHqlExpression * mode = expr->queryChild(2);
            if (mode->getOperator() == no_pipe)
                allRead = true;
            addFilenameRead(in);
        }
        break;
    case no_workunit_dataset:
        if (flags & GatherResultRead)
        {
            IHqlExpression * sequence = queryPropertyChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryPropertyChild(expr, nameAtom, 0);
            addResultRead(sequence, name, false);
        }
        break;
    case no_getgraphresult:
        if (flags & GatherGraphResultRead)
            addResultRead(expr->queryChild(1), expr->queryChild(2), true);
        break;
    case no_setgraphresult:
        if (flags & GatherGraphResultWrite)
            addResultWrite(expr->queryChild(1), expr->queryChild(2), true);
        break;
    case no_getresult:
        if (flags & GatherResultRead)
        {
            IHqlExpression * sequence = queryPropertyChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
            addResultRead(sequence, name, false);
        }
        break;
    case no_ensureresult:
    case no_setresult:
    case no_extractresult:
        if (flags & GatherResultWrite)
        {
            IHqlExpression * sequence = queryPropertyChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryPropertyChild(expr, namedAtom, 0);
            addResultWrite(sequence, name, false);
        }
        break;
    case no_definesideeffect:
        if (flags & GatherResultWrite)
        {
            addResultWrite(expr->queryProperty(_uid_Atom), NULL, false);
        }
        break;
    case no_callsideeffect:
        if (flags & GatherResultRead)
        {
            addResultRead(expr->queryProperty(_uid_Atom), NULL, false);
        }
        break;
    }
}

void DependenciesUsed::noteInconsistency(IHqlExpression * expr)
{
    if (!inconsistent)
        inconsistent.set(expr);
}


void DependenciesUsed::removeInternalReads()
{
    ForEachItemInRev(idx1, tablesRead)
    {
        IHqlExpression & cur = tablesRead.item(idx1);
        if (tablesWritten.contains(cur))
            tablesRead.remove(idx1);
    }
}

void checkDependencyConsistency(IHqlExpression * expr)
{
    DependenciesUsed depends(true);
    gatherDependencies(expr, depends, GatherAll);
    if (depends.inconsistent)
    {
        StringBuffer s;
        if (depends.inconsistent->queryName() == resultAtom)
        {
            getStoredDescription(s, depends.inconsistent->queryChild(0), depends.inconsistent->queryChild(1), true);
            throw MakeStringException(ERR_RECURSIVE_DEPENDENCY, "Result '%s' used before it is written", s.str());
        }
        else
        {
            depends.inconsistent->toString(s);

//  DBGLOG("Filename %s used before it is written", s.str());
            throw MakeStringException(ERR_RECURSIVE_DEPENDENCY, "Filename %s used before it is written", s.str());
        }
    }
}

void checkDependencyConsistency(const HqlExprArray & exprs)
{
    DependenciesUsed depends(true);
    DependencyGatherer gatherer(depends, GatherAll);
    ForEachItemIn(i, exprs)
        gatherer.gatherDependencies(&exprs.item(i));
    if (depends.inconsistent)
    {
        StringBuffer s;
        if (depends.inconsistent->queryName() == resultAtom)
        {
            getStoredDescription(s, depends.inconsistent->queryChild(0), depends.inconsistent->queryChild(1), true);
            throw MakeStringException(ERR_RECURSIVE_DEPENDENCY, "Result '%s' used before it is written", s.str());
        }
        else
        {
            depends.inconsistent->toString(s);

//  DBGLOG("Filename %s used before it is written", s.str());
            throw MakeStringException(ERR_RECURSIVE_DEPENDENCY, "Filename %s used before it is written", s.str());
        }
    }
}


//---------------------------------------------------------------------------

void DependencyGatherer::doGatherDependencies(IHqlExpression * expr)
{
    if (expr->queryTransformExtra())
        return;
    expr->setTransformExtraUnlinked(expr);

    used.extractDependencies(expr, flags);

    unsigned first = 0;
    unsigned max = expr->numChildren();
    switch (expr->getOperator())
    {
    case no_field:      // by now there should be no default values as children of fields.
    case no_attr:
    case no_attr_link:
        return;
    case no_select:
        if (!expr->hasProperty(newAtom))
            return;
        max = 1;        // by now there should be no default values as children of fields.
        break;
    case no_keyindex:
        first = 2;
        break;
    case no_newkeyindex:
        first = 3;
        break;
    case no_executewhen:
        if (expr->hasProperty(beforeAtom))
        {
            for (unsigned i=max; i-- != 0; )
                doGatherDependencies(expr->queryChild(i));
            return;
        }
        break;
    }
    for (unsigned i = first; i < max; i++)
        doGatherDependencies(expr->queryChild(i));
}


void DependencyGatherer::gatherDependencies(IHqlExpression * expr)
{
    TransformMutexBlock lock;
    doGatherDependencies(expr);
}

extern HQL_API void gatherDependencies(IHqlExpression * expr, DependenciesUsed & used, unsigned flags)
{
    DependencyGatherer gatherer(used, flags);
    gatherer.gatherDependencies(expr);
}

extern HQL_API bool introducesNewDependencies(IHqlExpression * oldExpr, IHqlExpression * newExpr)
{
    DependenciesUsed oldDepends(true);
    DependenciesUsed newDepends(true);
    gatherDependencies(newExpr, newDepends, GatherAllRead);
    gatherDependencies(oldExpr, oldDepends, GatherAllRead);
    return !newDepends.isSubsetOf(oldDepends);
}

//---------------------------------------------------------------------------


RecordSelectIterator::RecordSelectIterator(IHqlExpression * record, IHqlExpression * selector)
{
    rootRecord.set(record);
    rootSelector.set(selector);
    nestingDepth = 0;
    ifblockDepth = 0;
}

bool RecordSelectIterator::doNext()
{
    while (indices.ordinality())
    {
        loop
        {
            unsigned next = indices.tos();
            IHqlExpression & curRecord = records.tos();
            unsigned max = curRecord.numChildren();

            if (next >= max)
                break;

            indices.pop();
            indices.append(next+1);

            IHqlExpression * cur = curRecord.queryChild(next);
            switch (cur->getOperator())
            {
            case no_record:
                beginRecord(cur);
                break;
            case no_field:
                switch (cur->queryType()->getTypeCode())
                {
                case type_row:
                    beginRecord(cur->queryRecord());
                    selector.setown(createSelectExpr(LINK(selector), LINK(cur)));
                    nestingDepth++;
                    break;
                default:
                    curSelector.setown(createSelectExpr(LINK(selector), LINK(cur)));
                    return true;
                }
                break;
            case no_ifblock:
                beginRecord(cur->queryChild(1));
                ifblockDepth++;
                break;
            case no_attr:
            case no_attr_link:
            case no_attr_expr:
                break;
            default:
                UNIMPLEMENTED;
            }
        }

        indices.pop();
        records.pop();
        selector.setown(&savedSelector.popGet());
        if (records.ordinality())
        {
            switch (records.tos().queryChild(indices.tos()-1)->getOperator())
            {
            case no_ifblock:
                ifblockDepth--;
                break;
            case no_field:
                nestingDepth--;
                break;
            }
        }

    }

    curSelector.clear();
    return false;
}

void RecordSelectIterator::beginRecord(IHqlExpression * record)
{
    savedSelector.append(*LINK(selector));
    records.append(*record);
    indices.append(0);
}

bool RecordSelectIterator::first()
{
    savedSelector.kill();
    records.kill();
    indices.kill();
    selector.set(rootSelector);
    beginRecord(rootRecord);
    ifblockDepth = 0;
    nestingDepth = 0;
    return doNext();
}

bool RecordSelectIterator::next()
{
    return doNext();
}

bool RecordSelectIterator::isValid()
{
    return (curSelector != NULL);
}

bool RecordSelectIterator::isInsideIfBlock()
{
    return ifblockDepth > 0;
}

IHqlExpression * RecordSelectIterator::get()
{
    return LINK(curSelector);
}

IHqlExpression * RecordSelectIterator::query()
{
    return curSelector;
}

//---------------------------------------------------------------------------

unsigned countTotalFields(IHqlExpression * record, bool includeVirtual)
{
    unsigned count = 0;
    ForEachChild(i, record)
    {
        IHqlExpression * expr = record->queryChild(i);
        switch (expr->getOperator())
        {
        case no_record:
            count += countTotalFields(expr, includeVirtual);
            break;
        case no_ifblock:
            count += countTotalFields(expr->queryChild(1), includeVirtual);
            break;
        case no_field:
            switch (expr->queryType()->getTypeCode())
            {
            case type_record:
                throwUnexpected();
            case type_row:
                count += countTotalFields(expr->queryRecord(), includeVirtual);
                break;
            default:
                if (includeVirtual || !expr->hasProperty(virtualAtom))
                    count++;
                break;
            }
            break;
        }
    }
    return count;
}

bool transformContainsSkip(IHqlExpression * transform)
{
    return containsSkip(transform);
}

bool transformListContainsSkip(IHqlExpression * transforms)
{
    ForEachChild(i, transforms)
    {
        if (transformContainsSkip(transforms->queryChild(i)))
            return true;
    }
    return false;
}



IHqlExpression * queryNextRecordField(IHqlExpression * record, unsigned & idx)
{
    loop
    {
        IHqlExpression * cur = record->queryChild(idx++);
        if (!cur || cur->getOperator() == no_field)
            return cur;
    }
}


IHqlExpression * ensureTransformType(IHqlExpression * transform, node_operator op)
{
    if (transform->getOperator() == op)
        return LINK(transform);
    if (transform->getOperator() == no_call)
        return LINK(transform); // This needs handling some other way!

    HqlExprArray args;
    unwindChildren(args, transform);
    return createValue(op, transform->getType(), args);
}


//---------------------------------------------------------------------------

//NB: This needs to be kept in sync with the capabilities of the code generator.
extern HQL_API IHqlExpression * queryInvalidCsvRecordField(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_ifblock:
        return queryInvalidCsvRecordField(expr->queryChild(1));
    case no_record:
        {
            ForEachChild(i, expr)
            {
                IHqlExpression * invalid = queryInvalidCsvRecordField(expr->queryChild(i));
                if (invalid)
                    return invalid;
            }
            return NULL;
        }
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            switch (type->getTypeCode())
            {
            case type_row:
                return queryInvalidCsvRecordField(expr->queryRecord());
            case type_table:
            case type_groupedtable:
            case type_set:
                return expr;
            default:
                return NULL;
            }
        }
    }
    return NULL;
}

bool isValidCsvRecord(IHqlExpression * expr)
{
    return queryInvalidCsvRecordField(expr) == NULL;
}


bool isValidXmlRecord(IHqlExpression * expr)
{
    return true;
}

bool getBoolValue(IHqlExpression * expr, bool dft)
{
    if (expr)
    {
        if (expr->getOperator() == no_translated)
            expr = expr->queryChild(0);
        IValue * value = expr->queryValue();
        if (value)
            return value->getBoolValue();
    }
    return dft;
}

__int64 getIntValue(IHqlExpression * expr, __int64 dft)
{
    if (expr)
    {
        if (expr->getOperator() == no_translated)
            expr = expr->queryChild(0);
        IValue * value = expr->queryValue();
        if (value)
            return value->getIntValue();
    }
    return dft;
}

StringBuffer & getStringValue(StringBuffer & out, IHqlExpression * expr, const char * dft)
{
    if (expr)
    {
        if (expr->getOperator() == no_translated)
            expr = expr->queryChild(0);
        IValue * value = expr->queryValue();
        if (value)
        {
            value->getStringValue(out);
            return out;
        }
    }
    if (dft)
        out.append(dft);
    return out;
}

bool matchesConstantValue(IHqlExpression * expr, __int64 test)
{
    if (!expr) return false;
    if (expr->getOperator() == no_translated)
        expr = expr->queryChild(0);
    IValue * value = expr->queryValue();
    return value && value->queryType()->isInteger() && (value->getIntValue() == test);
}

bool matchesBoolean(IHqlExpression * expr, bool test)
{
    if (!expr) return false;
    if (expr->getOperator() == no_translated)
        expr = expr->queryChild(0);
    IValue * value = expr->queryValue();
    return value && value->queryType()->getTypeCode() == type_boolean && (value->getBoolValue() == test);
}

bool matchesConstantString(IHqlExpression * expr, const char * text, bool ignoreCase)
{
    if (!expr) return false;
    if (expr->getOperator() == no_translated)
        expr = expr->queryChild(0);
    IValue * value = expr->queryValue();
    if (!value || !isStringType(value->queryType()))
        return false;
    //Would be more efficient to compare directly, but would need to handle all the variants
    StringBuffer temp;
    value->getStringValue(temp);
    if (ignoreCase)
        return stricmp(temp.str(), text) == 0;
    return strcmp(temp.str(), text) == 0;
}

bool isEmptyList(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_null:
        return true;
    case no_list:
    case no_datasetlist:
        return (expr->numChildren() == 0);
    default:
        return false;
    }
}


bool recordContainsNestedRecord(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (recordContainsNestedRecord(cur))
                return true;
            break;
        case no_ifblock:
            if (recordContainsNestedRecord(cur->queryChild(1)))
                return true;
            break;
        case no_field:
            if (cur->queryType()->getTypeCode() == type_row)
                return true;
            break;
        }
    }
    return false;
}


//Is this a select of the form a.b.c where only a is active, and b,c are datasets.
//If a is a row e.g., ds[1] and b is a record field then not true.
IHqlExpression * queryNextMultiLevelDataset(IHqlExpression * expr, bool followActiveSelectors)
{
    while (expr->isDatarow())
    {
        switch (expr->getOperator())
        {
        case no_select:
        case no_selectnth:
            expr = expr->queryChild(0);
            break;
        default:
            return NULL;
        }
    }

    IHqlExpression * root = queryRoot(expr);
    if (!root || (root->getOperator() != no_select) || (!followActiveSelectors && !root->hasProperty(newAtom)))
        return NULL;
    IHqlExpression * ds = root->queryChild(0);
    loop
    {
        if (ds->isDataset())
            return ds;
        if (ds->getOperator() != no_select)
            return NULL;
        ds = ds->queryChild(0);
    }
}

bool isMultiLevelDatasetSelector(IHqlExpression * expr, bool followActiveSelectors)
{
    return queryNextMultiLevelDataset(expr, followActiveSelectors) != NULL;
}

IHqlExpression * getInverse(IHqlExpression * op)
{
    node_operator opKind = op->getOperator();
    if (opKind == no_not)
        return LINK(op->queryChild(0));
    else if (opKind == no_constant)
        return createConstant(!op->queryValue()->getBoolValue());
    else if (opKind == no_alias_scope)
    {
        HqlExprArray args;
        args.append(*getInverse(op->queryChild(0)));
        unwindChildren(args, op, 1);
        return op->clone(args);
    }

    node_operator inv = getInverseOp(opKind);
    if (inv)
    {
        IHqlExpression * value = createOpenValue(inv, op->getType());
        ForEachChild(i, op)
            value->addOperand(LINK(op->queryChild(i)));
        return value->closeExpr();
    }
    switch (opKind)
    {
    case no_if:
        return createBoolExpr(no_if, LINK(op->queryChild(0)), getInverse(op->queryChild(1)), getInverse(op->queryChild(2)));
    }

    return createValue(no_not, makeBoolType(), LINK(op));
}

IHqlExpression * getNormalizedCondition(IHqlExpression * expr)
{
    if (expr->getOperator() == no_not)
        return getInverse(expr->queryChild(0)->queryBody());
    return LINK(expr->queryBody());
}

bool areInverseExprs(IHqlExpression * left, IHqlExpression* right)
{
    if (left->getOperator() == no_not)
        return left->queryChild(0)->queryBody() == right->queryBody();
    if (right->getOperator() == no_not)
        return right->queryChild(0)->queryBody() == left->queryBody();

    node_operator leftOp = left->getOperator();
    node_operator rightOp = right->getOperator();
    if (leftOp != rightOp)
    {
        if (getInverseOp(leftOp) != rightOp)
            return false;
    }

    OwnedHqlExpr inverseLeft = getInverse(left->queryBody());
    return inverseLeft->queryBody() == right->queryBody();
}


IHqlExpression * getNegative(IHqlExpression * expr)
{
    IValue * value = expr->queryValue();
    if (value && isNumericType(value->queryType()))
        return createConstant(negateValue(value));

    if (expr->getOperator() == no_negate)
        return LINK(expr->queryChild(0));

    return createValue(no_negate, expr->getType(), LINK(expr));
}

bool isCompoundSource(IHqlExpression * expr)
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
    case no_compound_inline:
        return true;
    }
    return false;
}

static void convertRecordToAssigns(HqlExprArray & assigns, IHqlExpression * record, IHqlExpression * targetSelector, bool canOmit)
{
    ForEachChild(idx, record)
    {
        IHqlExpression * cur = record->queryChild(idx);

        switch (cur->getOperator())
        {
        case no_record:
            convertRecordToAssigns(assigns, cur, targetSelector, canOmit);
            break;
        case no_ifblock:
            convertRecordToAssigns(assigns, cur->queryChild(1), targetSelector, canOmit);
            break;
        case no_field:
            {
                IHqlExpression * fieldRecord = cur->queryRecord();
                IHqlExpression * value = queryRealChild(cur, 0);
                OwnedHqlExpr newTargetSelector = createSelectExpr(LINK(targetSelector), LINK(cur));

                if (fieldRecord && !cur->isDataset() && !value)
                {
                    //convertRecordToAssigns(assigns, cur->queryRecord(), newTargetSelector, canOmit);
                    IHqlExpression * transform = convertRecordToTransform(fieldRecord, canOmit);
                    IHqlExpression * newValue = createRow(no_createrow, transform);
                    assigns.append(*createAssign(LINK(newTargetSelector), newValue));
                }
                else
                {
                    assertex(value || canOmit);
                    if (value)
                        assigns.append(*createAssign(LINK(newTargetSelector), LINK(value)));
                }
                break;
            }
        }
    }
}

IHqlExpression * convertRecordToTransform(IHqlExpression * record, bool canOmit)
{
    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(record);
    convertRecordToAssigns(assigns, record, self, canOmit);
    return createValue(no_transform, makeTransformType(record->getType()), assigns);
}


IHqlExpression * createTranformForField(IHqlExpression * field, IHqlExpression * value)
{
    OwnedHqlExpr record = createRecord(field);
    OwnedHqlExpr self = getSelf(record);
    OwnedHqlExpr target = createSelectExpr(LINK(self), LINK(field));
    OwnedHqlExpr assign = createAssign(LINK(target), LINK(value));
    return createValue(no_transform, makeTransformType(record->getType()), assign.getClear());
}


inline bool isScheduleAction(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_when:
    case no_priority:
        return true;
    }
    return false;
}

bool workflowContainsSchedule(IHqlExpression * colonExpr)
{
    HqlExprArray actions;
    colonExpr->queryChild(1)->unwindList(actions, no_comma);
    ForEachItemIn(i, actions)
        if (isScheduleAction(&actions.item(i)))
            return true;
    return false;
}

bool workflowContainsNonSchedule(IHqlExpression * colonExpr)
{
    HqlExprArray actions;
    colonExpr->queryChild(1)->unwindList(actions, no_comma);
    ForEachItemIn(i, actions)
        if (!isScheduleAction(&actions.item(i)))
            return true;
    return false;
}

bool isUngroup(IHqlExpression * expr)
{
    return (expr->getOperator() == no_group) && !queryRealChild(expr, 1);
}


void unwindFilterConditions(HqlExprArray & conds, IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    for (unsigned idx=1; idx < max; idx++)
    {
        IHqlExpression * cur = queryRealChild(expr, idx);
        if (cur)
            cur->unwindList(conds, no_and);
    }
}

unsigned getBestLengthEstimate(IHqlExpression * expr)
{
    ITypeInfo * exprType = expr->queryType();
    unsigned len = exprType->getStringLen();
    if (len != UNKNOWN_LENGTH)
        return len;

    switch (expr->getOperator())
    {
    case no_cast:
    case no_implicitcast:
        if ((isStringType(exprType) || isUnicodeType(exprType)))
        {
            IHqlExpression * uncast = expr->queryChild(0);
            ITypeInfo * uncastType = uncast->queryType();

            if ((uncastType->getSize() != UNKNOWN_LENGTH) && (isStringType(uncastType) || isUnicodeType(uncastType)))
                return uncastType->getStringLen();
        }
        break;
    }

    return len;
}

//---------------------------------------------------------------------------

SubStringHelper::SubStringHelper(IHqlExpression * expr)
{
    init(expr->queryChild(0), expr->queryChild(1));
}

SubStringHelper::SubStringHelper(IHqlExpression * _src, IHqlExpression * range)
{
    init(_src, range);
}

void SubStringHelper::init(IHqlExpression * _src, IHqlExpression * range)
{
    special = false;
    infiniteString = false;
    from = NULL;
    to = NULL;
    src = _src;
    srcType = src->queryType();
    unsigned strSize = getBestLengthEstimate(src);

    switch (range->getOperator())
    {
    case no_range:
        from = range->queryChild(0);
        to = range->queryChild(1);
        break;
    case no_rangeto:
        to = range->queryChild(0);
        break;
    case no_rangefrom:
    case no_rangecommon:
        from = range->queryChild(0);
        break;
    default:
        from = range;
        to = range;
        break;
    }

    if (from)
    {
        IValue * startValue = from->queryValue();
        if (startValue)
        {
            fixedStart = (unsigned)startValue->getIntValue();
            if ((int)fixedStart <= 0)
                fixedStart = 1;
        }
        else
            fixedStart = UNKNOWN_LENGTH;
    }
    else
        fixedStart = 1;

    if (to)
    {
        IValue * endValue = to->queryValue();
        if (endValue)
        {
            fixedEnd = (unsigned)endValue->getIntValue();
            if ((int)fixedEnd <= 0)
                fixedEnd = 1;
            if (knownStart() && fixedEnd < fixedStart)
                fixedEnd = fixedStart-1;
        }
        else
            fixedEnd = UNKNOWN_LENGTH;
    }
    else
        fixedEnd = strSize;


    bool isStringOrData = false;
    switch (srcType->getTypeCode())
    {
    case type_string:
    case type_data:
    case type_unicode:
        isStringOrData = true;
        break;
    }

    bool isUnicode = srcType->getTypeCode() == type_unicode;
    if (isStringOrData)
    {
        if (srcType->getSize() == UNKNOWN_LENGTH)
        {
            if ((src->getOperator() == no_cast) || (src->getOperator() == no_implicitcast))
            {
                ITypeInfo * childType = src->queryChild(0)->queryType();
                type_t childTC = childType->getTypeCode();
                type_t srcTC = srcType->getTypeCode();
                switch (childType->getTypeCode())
                {
                case type_string:
                case type_data:
                    if ((srcTC == type_data) || (childTC == type_data) || (srcType->queryCharset() == childType->queryCharset()))
                    {
                        src = src->queryChild(0);
                        srcType = childType;
                    }
                    break;
                case type_unicode:
                    if(isUnicode && (srcType->queryLocale() == childType->queryLocale()))
                    {
                        src = src->queryChild(0);
                        srcType = childType;
                    }
                    break;
                }
            }
        }

        if (srcType->getSize() != UNKNOWN_LENGTH)
            if (knownStart() && knownEnd())
                special = true;
    }
    
    if (isStringOrData && srcType->getSize() == INFINITE_LENGTH)
        infiniteString = true;
}


bool hasMaxLength(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            //inherited maxlength?
            if (hasMaxLength(cur))
                return true;
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            if (cur->queryName() == maxLengthAtom)
                return true;
            break;
        }
    }
    return false;
}


void unwindFields(HqlExprArray & fields, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            unwindFields(fields, cur);
            break;
        case no_ifblock:
            unwindFields(fields, cur->queryChild(1));
            break;
        case no_field:
            fields.append(*LINK(cur));
            break;
        }
    }
}


void unwindFields(HqlExprCopyArray & fields, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            unwindFields(fields, cur);
            break;
        case no_ifblock:
            unwindFields(fields, cur->queryChild(1));
            break;
        case no_field:
            fields.append(*LINK(cur));
            break;
        }
    }
}


unsigned numAttributes(const HqlExprArray & args)
{
    unsigned cnt = 0;
    ForEachItemIn(i, args)
        if (args.item(i).isAttribute())
            cnt++;
    return cnt;
}

unsigned numAttributes(const IHqlExpression * expr)
{
    unsigned cnt = 0;
    ForEachChild(i, expr)
        if (expr->queryChild(i)->isAttribute())
            cnt++;
    return cnt;
}


IHqlExpression * createGetResultFromSetResult(IHqlExpression * setResult, ITypeInfo * type)
{
    IHqlExpression * seqAttr = setResult->queryProperty(sequenceAtom);
    IHqlExpression * aliasAttr = setResult->queryProperty(namedAtom);
    Linked<ITypeInfo> valueType = type;
    if (!valueType)
    {
        IHqlExpression * value;
        if (setResult->getOperator() == no_extractresult)
            value = setResult->queryChild(1);
        else
            value = setResult->queryChild(0);
        valueType.setown(value->getType());
    }

    switch (valueType->getTypeCode())
    {
    case type_table:
        return createDataset(no_getresult, LINK(queryOriginalRecord(valueType)), createComma(LINK(seqAttr), LINK(aliasAttr)));
    case type_groupedtable:
        return createDataset(no_getresult, LINK(queryOriginalRecord(valueType)), createComma(LINK(seqAttr), createAttribute(groupedAtom), LINK(aliasAttr)));
    case type_row:
    case type_record:
         return createRow(no_getresult, LINK(queryOriginalRecord(valueType)), createComma(LINK(seqAttr), LINK(aliasAttr)));
    }

    return createValue(no_getresult, valueType.getLink(), LINK(seqAttr), LINK(aliasAttr));
}

_ATOM queryCsvEncoding(IHqlExpression * mode)
{
    if (mode)
    {
        if (mode->queryProperty(asciiAtom))
            return asciiAtom;
        if (mode->queryProperty(ebcdicAtom))
            return ebcdicAtom;
        if (mode->queryProperty(unicodeAtom))
            return unicodeAtom;
    }
    return NULL;
}

_ATOM queryCsvTableEncoding(IHqlExpression * tableExpr)
{
    if (!tableExpr)
        return NULL;
    IHqlExpression * mode = tableExpr->queryChild(2);
    assertex(mode->getOperator() == no_csv);
    return queryCsvEncoding(mode);
}


IHqlExpression * createTrimExpr(IHqlExpression * value, IHqlExpression * flags)
{
    LinkedHqlExpr expr = value;
    ITypeInfo * exprType = expr->queryType()->queryPromotedType();
    if (!isSimpleStringType(exprType) && !isUnicodeType(exprType))
    {
        unsigned strLen = isStringType(exprType) ? exprType->getStringLen() : UNKNOWN_LENGTH;
        Owned<ITypeInfo> type = makeStringType(strLen, NULL, NULL);
        expr.setown(ensureExprType(expr, type));
    }

    ITypeInfo * srcType = expr->queryType();
    ITypeInfo * tgtType;
    if (srcType->getTypeCode() == type_utf8)
        tgtType = makeUtf8Type(UNKNOWN_LENGTH, srcType->queryLocale());
    else if (isUnicodeType(srcType))
        tgtType = makeUnicodeType(UNKNOWN_LENGTH, srcType->queryLocale());
    else
        tgtType = makeStringType(UNKNOWN_LENGTH, getCharset(asciiAtom), getCollation(asciiAtom));

    HqlExprArray args;
    args.append(*LINK(expr));
    if (flags)
        flags->unwindList(args, no_comma);
    return createValue(no_trim, tgtType, args);
}

bool isLengthPreservingCast(IHqlExpression * expr)
{
    if (!isCast(expr))
        return false;

    ITypeInfo * type = expr->queryType();
    ITypeInfo * baseType = expr->queryChild(0)->queryType();
    if ((isStringType(type) || isUnicodeType(type)) &&
        (isStringType(baseType) || isUnicodeType(baseType)))
    {
        if (type->getStringLen() == baseType->getStringLen())
            return true;
    }
    return false;
}


static void createDefaultTransformAssigns(HqlExprArray & args, IHqlExpression * self, IHqlExpression * right, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            createDefaultTransformAssigns(args, self, right, cur);
            break;
        case no_ifblock:
            createDefaultTransformAssigns(args, self, right, cur->queryChild(1));
            break;
        case no_field:
            {
                IHqlExpression * target = createSelectExpr(LINK(self), LINK(cur));
                IHqlExpression * value;
                if (right)
                    value = createSelectExpr(LINK(right), LINK(cur));
                else
                    value = createNullExpr(cur);
                args.append(*createAssign(target, value));
                break;
            }
        }
    }
}

IHqlExpression * createTransformFromRow(IHqlExpression * expr)
{
    IHqlExpression * record = expr->queryRecord();
    OwnedHqlExpr self = createSelector(no_self, record, NULL);
    HqlExprArray args;
    createDefaultTransformAssigns(args, self, expr, record);
    return createValue(no_transform, makeTransformType(record->getType()), args);
}

IHqlExpression * createNullTransform(IHqlExpression * record)
{
    OwnedHqlExpr self = createSelector(no_self, record, NULL);
    HqlExprArray args;
    createDefaultTransformAssigns(args, self, NULL, record);
    return createValue(no_transform, makeTransformType(record->getType()), args);
}


static bool foundDifference()
{
    return false;
}

bool debugFindFirstDifference(IHqlExpression * left, IHqlExpression * right)
{
    if (left == right)
        return true;
    if (!left || !right)
        return foundDifference();
    if (left->queryBody() == right->queryBody())
    {
        if ((left->getOperator() == no_call) && (right->getOperator() == no_call))
        {
            IHqlExpression * leftDefinition = left->queryDefinition();
            IHqlExpression * rightDefinition = right->queryDefinition();
            if ((left != leftDefinition) || (right != rightDefinition))
            {
                if (debugFindFirstDifference(left->queryDefinition(), right->queryDefinition()))
                    return false;
            }
            return foundDifference();       // break point here.
        }

        return foundDifference();       // break point here.
    }
    if (left->getOperator() != right->getOperator())
        return foundDifference();
    if (left->queryName() != right->queryName())
        return foundDifference();
    ForEachChild(i, left)
    {
        if (!debugFindFirstDifference(left->queryChild(i), right->queryChild(i)))
            return false;
    }
    if (left->getOperator() != no_record)
    {
        IHqlExpression * leftRecord = queryOriginalRecord(left);
        IHqlExpression * rightRecord = queryOriginalRecord(right);
        if (leftRecord || rightRecord)
        {
            if (!debugFindFirstDifference(leftRecord, rightRecord))
                return false;
        }
    }
    if (left->queryType() != right->queryType())
    {
        IHqlExpression * lTypeExpr = queryExpression(left->queryType());
        IHqlExpression * rTypeExpr = queryExpression(right->queryType());
        if (((left != lTypeExpr) || (right != rTypeExpr)) &&
            (lTypeExpr && rTypeExpr && lTypeExpr != rTypeExpr))
            return debugFindFirstDifference(lTypeExpr, rTypeExpr);
        return foundDifference();
    }

    return foundDifference();//something else
}

//------------------------------------------------------------------------------------------------

static IHqlExpression * expandConditions(IHqlExpression * expr, DependenciesUsed & dependencies, HqlExprArray & conds, HqlExprArray & values)
{
    if (expr->getOperator() != no_if)
        return expr;

    IHqlExpression * cond = expr->queryChild(0);
    //don't combine if conditions it it means that extra dependencies will be needed to test the if condition
    //since it may cause unnecessary code to be executed.
    if (conds.ordinality() == 0)
        gatherDependencies(cond, dependencies, GatherAllResultRead);
    else
    {
        DependenciesUsed condDependencies(true);
        gatherDependencies(cond, condDependencies, GatherAllResultRead);
        if (!condDependencies.isSubsetOf(dependencies))
            return expr;
    }
    conds.append(*LINK(cond));
    values.append(*LINK(expr->queryChild(1)));
    return expandConditions(expr->queryChild(2), dependencies, conds, values);
}

IHqlExpression * combineIfsToMap(IHqlExpression * expr)
{
    if (expr->isAction())
        return NULL;

    HqlExprArray conds, values;
    DependenciesUsed dependencies(true);
    IHqlExpression * falseExpr = expandConditions(expr, dependencies, conds, values);
    if (conds.ordinality() <= 1)
        return NULL;

    HqlExprArray args;
    ForEachItemIn(i, conds)
    {
        IHqlExpression & curValue = values.item(i);
        args.append(*createValue(no_mapto, curValue.getType(), LINK(&conds.item(i)), LINK(&curValue)));
    }
    args.append(*LINK(falseExpr));
    return createWrapper(no_map, expr->queryType(), args);
}

bool castPreservesValueAndOrder(IHqlExpression * expr)
{
    assertex(isCast(expr));
    IHqlExpression * uncast = expr->queryChild(0);
    ITypeInfo * castType = expr->queryType();
    ITypeInfo * uncastType = uncast->queryType();
    if (castLosesInformation(castType, uncastType))
        return false;
    if (!preservesOrder(castType, uncastType))
        return false;
    return true;
}

//------------------------------------------------------------------------------------------------


class RecordMetaCreator
{
public:
    RecordMetaCreator(IMaxSizeCallback * _callback) { callback = _callback; }

    IDefRecordElement * createRecord(IHqlExpression * record, IHqlExpression * self);

protected:
    IDefRecordElement * createField(IHqlExpression * cur, IHqlExpression * self);
    IDefRecordElement * createIfBlock(IHqlExpression * cur, IHqlExpression * self);
    bool createRecord(IDefRecordBuilder * builder, IHqlExpression * record, IHqlExpression * self);

protected:
    HqlExprArray selects;
    IDefRecordElementArray created;
    IMaxSizeCallback * callback;
};


IDefRecordElement * RecordMetaCreator::createField(IHqlExpression * cur, IHqlExpression * self)
{
    IHqlExpression * fieldRecord = cur->queryRecord();
    Owned<IDefRecordElement> childDefRecord;
    ITypeInfo * type = cur->queryType();
    Linked<ITypeInfo> defType = type;
    switch (type->getTypeCode())
    {
    case type_row:
        //Backward compatibility!
        defType.setown(makeRecordType());
        childDefRecord.setown(createRecord(fieldRecord, self));
        break;
    case type_table:
        defType.setown(makeTableType(makeRowType(makeRecordType()), NULL, NULL, NULL));
        childDefRecord.setown(::createMetaRecord(fieldRecord, callback));
        break;
    case type_groupedtable:
        {
            ITypeInfo * tableType = makeTableType(makeRowType(makeRecordType()), NULL, NULL, NULL);
            defType.setown(makeGroupedTableType(tableType, createAttribute(groupedAtom), NULL));
            childDefRecord.setown(::createMetaRecord(fieldRecord, callback));
            break;
        }
    case type_alien:
    case type_any:
    case type_bitfield:
        //MORE: Not Allowed....
        return NULL;
    }
    size32_t maxSize = 0;
    Owned<IDefRecordElement> elem = createDEfield(cur->queryName(), defType, childDefRecord, maxSize);
    if (cur->hasProperty(blobAtom))
    {
        Owned<ITypeInfo> blobType = makeBlobType();
        return createDEfield(cur->queryName(), blobType, elem, 0);
    }
    return elem.getClear();
}


IDefRecordElement * RecordMetaCreator::createIfBlock(IHqlExpression * cur, IHqlExpression * self)
{
    IHqlExpression * cond = cur->queryChild(0);
    IHqlExpression * select;
    LinkedHqlExpr value;
    switch (cond->getOperator())
    {
    case no_select:
        //ifblock(self.booleanField)
        select = cond;
        value.setown(createConstant(true));
        break;
    case no_eq:
        select = cond->queryChild(0);
        value.set(cond->queryChild(1));
        if (select->getOperator() != no_select)
            return NULL;
        if (value->getOperator() != no_constant)
            return NULL;
        break;
    default:
        return NULL;
    }

    unsigned match = selects.find(*select);
    if (match == NotFound)
        return NULL;
    IDefRecordElement & condField = created.item(match);
    Owned<IDefRecordElement> record = createRecord(cur->queryChild(1), self);
    if (!record)
        return NULL;
    return createDEifblock(&condField, value->queryValue(), record);
}


bool RecordMetaCreator::createRecord(IDefRecordBuilder * builder, IHqlExpression * record, IHqlExpression * self)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (!createRecord(builder, cur, self))
                return false;
            break;
        case no_field:
            {
                OwnedHqlExpr selector = createSelectExpr(LINK(self), LINK(cur));
                IDefRecordElement * next = createField(cur, selector);
                if (!next)
                    return false;
                builder->addChildOwn(next);
                selects.append(*LINK(selector));
                created.append(*LINK(next));
                break;
            }
        case no_ifblock:
            {
                IDefRecordElement * next = createIfBlock(cur, self);
                if (!next)
                    return false;
                builder->addChildOwn(next);
                break;
            }
        default:
            break;
        }
    }
    return true;
}

IDefRecordElement * RecordMetaCreator::createRecord(IHqlExpression * record, IHqlExpression * self)
{
    size32_t maxSize = callback ? callback->getMaxSize(record) : 4096;
    Owned<IDefRecordBuilder> builder = createDErecord(maxSize);
    if (!createRecord(builder, record, self))
        return NULL;
    return builder->close();
}

IDefRecordElement * createMetaRecord(IHqlExpression * record, IMaxSizeCallback * callback)
{
    RecordMetaCreator creator(callback);
    return creator.createRecord(record, querySelfReference());
}


static bool doContainsExpression(IHqlExpression * expr, IHqlExpression * search)
{
    loop
    {
        if (expr->queryTransformExtra())
            return false;
        if (expr == search)
            return true;
        expr->setTransformExtraUnlinked(expr);
        IHqlExpression * body = expr->queryBody(true);
        if (body == expr)
            break;
        expr = body;
    }
    ForEachChild(i, expr)
    {
        if (doContainsExpression(expr->queryChild(i), search))
            return true;
    }
    return false;
}

bool containsExpression(IHqlExpression * expr, IHqlExpression * search)
{
    TransformMutexBlock lock;
    return doContainsExpression(expr, search);
}

static bool doContainsOperator(IHqlExpression * expr, node_operator search)
{
    if (expr->queryTransformExtra())
        return false;
    if (expr->getOperator() == search)
        return true;
    expr->setTransformExtraUnlinked(expr);
    ForEachChild(i, expr)
    {
        if (doContainsOperator(expr->queryChild(i), search))
            return true;
    }
    return false;
}

bool containsOperator(IHqlExpression * expr, node_operator search)
{
    TransformMutexBlock lock;
    return doContainsOperator(expr, search);
}

static HqlTransformerInfo annotationRemoverInfo("AnnotationRemover");
class AnnotationRemover : public NewHqlTransformer
{
public:
    AnnotationRemover(IHqlExpression * _searchExpr)
        : NewHqlTransformer(annotationRemoverInfo), searchExpr(_searchExpr)
    {
    }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        if (expr->queryBody() == searchExpr)
            return LINK(searchExpr);
        return NewHqlTransformer::createTransformed(expr);
    }

protected:
    IHqlExpression * searchExpr;
};


IHqlExpression * removeAnnotations(IHqlExpression * expr, IHqlExpression * search)
{
    AnnotationRemover remover(search);
    return remover.transformRoot(expr);
}

bool containsIfBlock(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (containsIfBlock(cur))
                return true;
            break;
        case no_field:
            if (cur->isDatarow() && containsIfBlock(cur->queryRecord()))
                return true;
            break;
        case no_ifblock:
            return true;
        }
    }
    return false;
}


IHqlExpression * getFailCode(IHqlExpression * failExpr)
{
    IHqlExpression * arg0 = failExpr->queryChild(0);
    if (arg0 && arg0->queryType()->isInteger())
        return LINK(arg0);
    return createConstant(0);
}

IHqlExpression * getFailMessage(IHqlExpression * failExpr, bool nullIfOmitted)
{
    IHqlExpression * arg0 = failExpr->queryChild(0);
    if (arg0)
    {
        if (!arg0->queryType()->isInteger())
            return LINK(arg0);

        IHqlExpression * arg1 = failExpr->queryChild(1);
        if (arg1)
            return LINK(arg1);
    }
    if (nullIfOmitted)
        return NULL;
    return createConstant("");
}

int compareAtoms(IInterface * * pleft, IInterface * * pright)
{
    IAtom * left = static_cast<IAtom *>(*pleft);
    IAtom * right = static_cast<IAtom *>(*pright);

    return stricmp(left->str(), right->str());
}


int compareSymbolsByName(IInterface * * pleft, IInterface * * pright)
{
    IHqlExpression * left = static_cast<IHqlExpression *>(*pleft);
    IHqlExpression * right = static_cast<IHqlExpression *>(*pright);

    return stricmp(left->queryName()->str(), right->queryName()->str());
}

class ModuleExpander
{
public:
    ModuleExpander(HqlLookupContext & _ctx, bool _expandCallsWhenBound, node_operator _outputOp) 
        : ctx(_ctx), expandCallsWhenBound(_expandCallsWhenBound), outputOp(_outputOp) {}

    IHqlExpression * createExpanded(IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, const char * prefix);

protected:
    HqlLookupContext ctx;
    bool expandCallsWhenBound;
    node_operator outputOp;
};

IHqlExpression * ModuleExpander::createExpanded(IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, const char * prefix)
{
    IHqlScope * scope = scopeExpr->queryScope();

    HqlExprArray symbols;
    ensureSymbolsDefined(ifaceExpr, ctx);
    ifaceExpr->queryScope()->getSymbols(symbols);

    symbols.sort(compareSymbolsByName);         // should really be in definition order, but that isn't currently preserved

    HqlExprArray outputs;
    StringBuffer lowername;
    ForEachItemIn(i, symbols)
    {
        IHqlExpression & cur = symbols.item(i);
        _ATOM name = cur.queryName();
        OwnedHqlExpr resolved = scope->lookupSymbol(name, LSFpublic, ctx);
        LinkedHqlExpr value = resolved;
        if (value && value->isFunction())
        {
            bool allArgumentsHaveDefaults = false;
            if (value->isFunctionDefinition())
            {
                //Allow functions that supply default values for each of the parameters to be expanded
                IHqlExpression * formals = value->queryChild(1);
                IHqlExpression * defaults = value->queryChild(2);
                unsigned numFormals = formals->numChildren();
                if (numFormals == 0)
                    allArgumentsHaveDefaults = true;
                else if (defaults && (defaults->numChildren() == numFormals))
                {
                    allArgumentsHaveDefaults = true;
                    ForEachChild(i, defaults)
                    {
                        IHqlExpression * param = defaults->queryChild(i);
                        if (param->getOperator() == no_omitted)
                            allArgumentsHaveDefaults = false;
                    }
                }
            }

            if (allArgumentsHaveDefaults)
            {
                HqlExprArray args;
                value.setown(createBoundFunction(NULL, value, args, NULL, expandCallsWhenBound));
            }
            else
                value.clear();
        }
        if (value && isExported(resolved))
        {
            lowername.clear().append(prefix).append(name).toLowerCase();

            node_operator op = no_none;
            if (outputOp == no_output)
            {
                if (value->isDataset())
                {
                    value.setown(createDataset(no_selectfields, LINK(value), createValue(no_null)));
                    op = no_output;
                }
                else if (value->isDatarow())
                    op = no_output;
                else if (value->isList() || value->queryType()->isScalar())
                    op = no_outputscalar;
            }
            else
            {
                assertex(outputOp == no_evaluate_stmt);
                if (value->isList() || value->queryType()->isScalar())
                    op = no_evaluate_stmt;
            }

            switch (value->getOperator())
            {
            case no_typedef:
                op = no_none;
                break;
            }

            if (op != no_none)
                outputs.append(*createValue(op, makeVoidType(), LINK(value), createAttribute(namedAtom, createConstant(lowername))));
            else if (value->isAction())
                outputs.append(*LINK(value));
            else if (value->isScope())
            {
                lowername.append(".");
                OwnedHqlExpr child = createExpanded(value, value, lowername.str());
                if (child->getOperator() != no_null)
                    outputs.append(*child.getClear());
            }
        }
    }

    return createActionList(outputs);
}


IHqlExpression * createEvaluateOutputModule(HqlLookupContext & ctx, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, bool expandCallsWhenBound, node_operator outputOp)
{
    ModuleExpander expander(ctx, expandCallsWhenBound, outputOp);
    return expander.createExpanded(scopeExpr, ifaceExpr, NULL);
}


extern HQL_API IHqlExpression * createStoredModule(IHqlExpression * scopeExpr)
{
    IHqlScope * scope = scopeExpr->queryScope();

    HqlExprArray symbols;
    scope->getSymbols(symbols);

    symbols.sort(compareSymbolsByName);         // should really be in definition order, but that isn't currently preserved

    Owned<IHqlScope> newScope = createVirtualScope();
    IHqlExpression * newScopeExpr = queryExpression(newScope);
    newScopeExpr->addOperand(LINK(scopeExpr));

    HqlExprArray noParameters;
    StringBuffer lowername;
    _ATOM moduleName = NULL;
    ForEachItemIn(i, symbols)
    {
        IHqlExpression & cur = symbols.item(i);
        if (isExported(&cur) && !cur.isFunction())
        {
            LinkedHqlExpr value = &cur;
            if (value->isDataset() || value->isDatarow() || value->isList() || value->queryType()->isScalar())
            {
                if (value->getOperator() == no_purevirtual)
                    value.setown(createNullExpr(value));

                _ATOM name = symbols.item(i).queryName();
                lowername.clear().append(name).toLowerCase();
                OwnedHqlExpr failure = createValue(no_stored, makeVoidType(), createConstant(lowername));

                HqlExprArray meta;
                value.setown(attachWorkflowOwn(meta, value.getClear(), failure, NULL));
                newScope->defineSymbol(name, moduleName, value.getClear(), 
                                       true, false, cur.getSymbolFlags());
            }
        }
    }

    return queryExpression(newScope.getClear())->closeExpr();
}


extern HQL_API IHqlExpression * convertScalarAggregateToDataset(IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * arg = queryRealChild(expr, 1);

    node_operator newop;
    switch (expr->getOperator())
    {
    case no_ave:   newop = no_avegroup; break;
    case no_count: newop = no_countgroup; break;
    case no_min:   newop = no_mingroup; break;
    case no_max:   newop = no_maxgroup; break;
    case no_sum:   newop = no_sumgroup; break;
    case no_exists:newop = no_existsgroup; break;
    case no_notexists:  newop = no_notexistsgroup; break;
    case no_variance:   newop = no_vargroup; break;
    case no_covariance: newop = no_covargroup; break;
    case no_correlation:newop = no_corrgroup; break;
    default:
        return NULL;
    }

    //more: InheritMaxlength
    OwnedHqlExpr field;
    if ((newop == no_mingroup || newop == no_maxgroup) && (arg->getOperator() == no_select))
        field.set(arg->queryChild(1));                  // inherit maxlength etc...
    else
        field.setown(createField(valueAtom, expr->getType(), NULL));

    IHqlExpression * aggregateRecord = createRecord(field);
    IHqlExpression * keyedAttr = expr->queryProperty(keyedAtom);
    IHqlExpression * prefetchAttr = expr->queryProperty(prefetchAtom);

    HqlExprArray valueArgs;
    unwindChildren(valueArgs, expr, 1);
    IHqlExpression * newValue = createValue(newop, expr->getType(), valueArgs);
    IHqlExpression * assign = createAssign(createSelectExpr(getSelf(aggregateRecord), LINK(field)), newValue);
    IHqlExpression * transform = createValue(no_newtransform, makeTransformType(aggregateRecord->getType()), assign);

    //remove grouping if dataset happens to be grouped...
    dataset->Link();
    if (dataset->queryType()->getTypeCode() == type_groupedtable)
        dataset = createDataset(no_group, dataset, NULL);

    IHqlExpression * project = createDataset(no_newusertable, dataset, createComma(aggregateRecord, transform, LINK(keyedAttr), LINK(prefetchAttr)));
    return createRow(no_selectnth, project, createConstantOne());
}


//---------------------------------------------------------------------------

void HqlExprHashTable::onAdd(void *et) 
{  
    ((IHqlExpression*)et)->Link();
}

void HqlExprHashTable::onRemove(void *et) 
{
    ((IHqlExpression*)et)->Release();
}

unsigned HqlExprHashTable::getHashFromElement(const void *et) const
{
    return ((IHqlExpression*)et)->getHash();
}

unsigned HqlExprHashTable::getHashFromFindParam(const void *fp) const
{
    return ((IHqlExpression*)fp)->getHash();
}

const void * HqlExprHashTable::getFindParam(const void *et) const
{
    return et;
}

bool HqlExprHashTable::matchesFindParam(const void *et, const void *key, unsigned fphash) const
{
    return et == key;
}

//---------------------------------------------------------------------------

bool BitfieldPacker::checkSpaceAvailable(unsigned & thisBitOffset, unsigned & thisBits, ITypeInfo * type)
{
    bool fitted = true;
    thisBits = type->getBitSize();
    if (thisBits > bitsRemaining)
    {
        ITypeInfo * storeType = type->queryChildType();
        unsigned unitSize = storeType->getSize();
        bitsRemaining = unitSize * 8;
        nextBitOffset = 0;
        fitted = false;
    }

    thisBitOffset = nextBitOffset;
    nextBitOffset += thisBits;
    bitsRemaining -= thisBits;
    return fitted;
}


void reorderAttributesToEnd(HqlExprArray & target, const HqlExprArray & source)
{
    ForEachItemIn(i1, source)
    {
        IHqlExpression & cur = source.item(i1);
        if (!cur.isAttribute())
            target.append(OLINK(cur));
    }
    ForEachItemIn(i2, source)
    {
        IHqlExpression & cur = source.item(i2);
        if (cur.isAttribute())
            target.append(OLINK(cur));
    }
}


bool hasActiveTopDataset(IHqlExpression * expr)
{
    switch (getChildDatasetType(expr))
    {
    case childdataset_merge:
    case childdataset_dataset:
    case childdataset_datasetleft:
    case childdataset_top_left_right:
        return true;
    }
    return false;
}

//-------------------------------------------------------------------------------------------------------

void getStoredDescription(StringBuffer & text, IHqlExpression * sequence, IHqlExpression * name, bool includeInternalName)
{
    if (sequence)
    {
        switch (sequence->queryValue()->getIntValue())
        {
        case ResultSequencePersist: 
            text.append("PERSIST(");
            name->toString(text);
            text.append(")");
            break;
        case ResultSequenceStored: 
            text.append("STORED(");
            name->toString(text);
            text.append(")");
            break;
        case ResultSequenceInternal:
            text.append("Internal");
            if (includeInternalName)
                name->toString(text.append("(")).append(")");
            break;
        default:
            if (name)
                name->toString(text);
            else
                text.append("Result #").append(sequence->queryValue()->getIntValue()+1);
            break;
        }
    }
}


IHqlExpression * appendOwnedOperandsF(IHqlExpression * expr, ...)
{
    HqlExprArray children;
    unwindChildren(children, expr);

    va_list args;
    va_start(args, expr);
    for (;;)
    {
        IHqlExpression *parm = va_arg(args, IHqlExpression *);
        if (!parm)
            break;
        children.append(*parm);
    }
    va_end(args);
    return expr->clone(children);
}


IHqlExpression * inheritAttribute(IHqlExpression * expr, IHqlExpression * donor, _ATOM name)
{
    return appendOwnedOperand(expr, LINK(donor->queryProperty(name)));
}

IHqlExpression * appendOwnedOperand(IHqlExpression * expr, IHqlExpression * ownedOperand)
{
    if (!ownedOperand)
        return LINK(expr);
    HqlExprArray args;
    unwindChildren(args, expr);
    args.append(*ownedOperand);
    return expr->clone(args);
}

IHqlExpression * removeOperand(IHqlExpression * expr, IHqlExpression * operand)
{
    HqlExprArray args;
    unwindChildren(args, expr);
    args.zap(*operand);
    return expr->clone(args);
}

IHqlExpression * removeChildOp(IHqlExpression * expr, node_operator op)
{
    HqlExprArray args;
    unwindChildren(args, expr);
    ForEachItemInRev(i, args)
        if (args.item(i).getOperator() == op)
            args.remove(i);
    return expr->clone(args);
}

IHqlExpression * removeProperty(IHqlExpression * expr, _ATOM attr)
{
    HqlExprArray args;
    unwindChildren(args, expr);
    if (removeProperty(args, attr))
        return expr->clone(args);
    return LINK(expr);
}

IHqlExpression * replaceOwnedProperty(IHqlExpression * expr, IHqlExpression * ownedOperand)
{
    HqlExprArray args;
    unwindChildren(args, expr);
    removeProperty(args, ownedOperand->queryName());
    args.append(*ownedOperand);
    return expr->clone(args);
}


IHqlExpression * appendLocalAttribute(IHqlExpression * expr)
{
    return appendOwnedOperand(expr, createLocalAttribute());
}

IHqlExpression * removeLocalAttribute(IHqlExpression * expr)
{
    return removeProperty(expr, localAtom);
}

//-------------------------------------------------------------------------------------------------------

class HQL_API SplitDatasetAttributeTransformer : public NewHqlTransformer
{
public:
    SplitDatasetAttributeTransformer();

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    bool split(SharedHqlExpr & dataset, SharedHqlExpr & attribute, IHqlExpression * expr);

protected:
    void doAnalyseSelect(IHqlExpression * expr);
    IHqlExpression * doTransformSelect(IHqlExpression * expr);

protected:
    OwnedHqlExpr selSeq;
    OwnedHqlExpr rowsId;
    HqlExprCopyArray datasets;
    HqlExprArray newDatasets;
    HqlExprArray selectors;
};


static HqlTransformerInfo hqlSplitDatasetAttributeInfo("SplitDatasetAttributeTransformer");
SplitDatasetAttributeTransformer::SplitDatasetAttributeTransformer(): NewHqlTransformer(hqlSplitDatasetAttributeInfo)
{ 
}

void SplitDatasetAttributeTransformer::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr))
        return;
    switch (expr->getOperator())
    {
    case no_select:
        doAnalyseSelect(expr);
        return;
    case NO_AGGREGATE:
    case no_createset:
    case no_countfile:
    case no_colon:
    case no_globalscope:
    case no_nothor:
        //Ugly, should really be normalized to no_select(no_aggregate[1], x)
        return;
    case no_if:
    case no_mapto:
        analyseExpr(expr->queryChild(0));
        return;
    case no_add:
    case no_sub:
    case no_mul:
    case no_div:
    case no_cast:
    case no_implicitcast:
    case no_concat:
    case no_eq: case no_ne: case no_gt: case no_ge: case no_lt: case no_le:
    case no_case:
    case no_map:
    case no_externalcall:
    case no_call:
        break;
//  default:
//      return;
    }

    ITypeInfo * type = expr->queryType();
    if (type && type->getTypeCode() == type_void)
        return;

    NewHqlTransformer::analyseExpr(expr);
}



void SplitDatasetAttributeTransformer::doAnalyseSelect(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    if (expr->hasProperty(newAtom))
    {
        if (!datasets.contains(*ds))
        {
            IHqlExpression * lhs = LINK(ds);
            if (lhs->isDataset()) lhs = createRow(no_activerow, lhs);
            datasets.append(*ds);
            newDatasets.append(*createDatasetFromRow(lhs));
        }
        return;
    }

    //ds is a no_select, so will be handled correctly.  
    NewHqlTransformer::analyseExpr(expr);
}



IHqlExpression * SplitDatasetAttributeTransformer::createTransformed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select:
        if (expr->hasProperty(newAtom))
            return doTransformSelect(expr);
        break;
    case no_colon:
    case no_globalscope:
    case no_nothor:
        return LINK(expr);
//  case NO_AGGREGATE:
//  case no_createset:
        //See comment in analyse above
//      return LINK(expr);
    }

    return NewHqlTransformer::createTransformed(expr);
}


IHqlExpression * SplitDatasetAttributeTransformer::doTransformSelect(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * field = expr->queryChild(1);
    unsigned match = datasets.find(*ds);
    if (match == NotFound)
        return NewHqlTransformer::createTransformed(expr);
    return createSelectExpr(transform(&selectors.item(match)), LINK(field));    // note, remove new attributes
}




bool SplitDatasetAttributeTransformer::split(SharedHqlExpr & dataset, SharedHqlExpr & attribute, IHqlExpression * expr)
{
    analyseExpr(expr);

    //First remove trivial datasets...
    {
        unsigned num = datasets.ordinality();
        for (unsigned i=0; i< num;)
        {
            IHqlExpression * cur = &datasets.item(i);
            while ((cur->getOperator() == no_selectnth) || (cur->getOperator() == no_preservemeta))
                cur = cur->queryChild(0);

            bool remove = false;
            switch (cur->getOperator())
            {
            case no_getgraphresult:
                remove = !expr->hasProperty(_distributed_Atom);
                break;
            case no_workunit_dataset:
            case no_getgraphloopresult:
            case no_left:
            case no_right:
            case no_colon:
            case no_globalscope:
            case no_nothor:
                remove = true;
                break;
            }

            if (remove)
            {
                datasets.remove(i);
                newDatasets.remove(i);
                num--;
            }
            else
                i++;
        }
    }

    const unsigned maxDatasets = 1;
    datasets.trunc(maxDatasets);
    newDatasets.trunc(maxDatasets);
    switch (datasets.ordinality())
    {
    case 0:
        return false;
    case 1:
        selectors.append(*LINK(&newDatasets.item(0)));
        break;
    case 2:
        selSeq.setown(createSelectorSequence());
        selectors.append(*createSelector(no_left, &datasets.item(0), selSeq));
        selectors.append(*createSelector(no_right, &datasets.item(1), selSeq));
        break;
    default:
        {
            selSeq.setown(createSelectorSequence());
            rowsId.setown(createUniqueRowsId());

            ForEachItemIn(i, newDatasets)
            {
                IHqlExpression & cur = newDatasets.item(i);
                OwnedHqlExpr rows = createDataset(no_rows, LINK(&cur), LINK(rowsId)); 
                selectors.append(*createRow(no_selectnth, LINK(rows), createComma(getSizetConstant(i+1), createAttribute(noBoundCheckAtom))));
            }
            break;
        }
    }

    OwnedHqlExpr value = transform(expr);
    switch (datasets.ordinality())
    {
    case 1:
        dataset.set(&newDatasets.item(0));
        attribute.set(value);
        break;
    case 2:
        {
            OwnedHqlExpr field = createField(unnamedAtom, value->getType(), NULL);
            OwnedHqlExpr transform = createTranformForField(field, value);
            OwnedHqlExpr combine = createDatasetF(no_combine, LINK(&newDatasets.item(0)), LINK(&newDatasets.item(1)), LINK(transform), LINK(selSeq), NULL);
            OwnedHqlExpr first = createRowF(no_selectnth, LINK(combine), getSizetConstant(1), createAttribute(noBoundCheckAtom), NULL);
            dataset.setown(createDatasetFromRow(first.getClear()));
            attribute.setown(createSelectExpr(LINK(dataset->queryNormalizedSelector()), LINK(field)));
            break;
        }
    default:
        return false;
    }
    return true;
}

static bool splitDatasetAttribute(SharedHqlExpr & dataset, SharedHqlExpr & attribute, IHqlExpression * expr)
{
#if 0
    //The following code works.  However I think it has the side-effect of modifying expressions so that they are no longer
    //csed with other expressions (similar to including too many items in the case statement below).  So currently disabled.
    SplitDatasetAttributeTransformer transformer;
    return transformer.split(dataset, attribute, queryNonAliased(expr));
#else
    IHqlExpression * left = expr->queryChild(0);
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_evaluate:
    case no_field:
        throwUnexpectedOp(op);
    case no_constant:
        return false;
    case no_select:
        break;
        //Play it safe, by only containing a subset of the expressions here
        //The list of expressions to include here seems to be a bit of a black art.  For instance including and/or/not makes many queries worse.
    case no_add:
    case no_sub:
    case no_mul:
    case no_div:
    case no_cast:
    case no_implicitcast:
    case no_concat:
    case no_eq: case no_ne: case no_gt: case no_ge: case no_lt: case no_le:
    case no_if:
    case no_mapto:
    case no_case:
    case no_map:
    case no_externalcall:
    case no_call:
//  case no_not:
//  case no_and:
//  case no_or:
//  case no_substring:
//  case no_charlen:
        {
            HqlExprArray args;
            bool mapped = false;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                OwnedHqlExpr ds, attr;
                if (splitDatasetAttribute(ds, attr, cur) && (!dataset || ds.get() == dataset.get()))
                {
                    args.append(*attr.getClear());
                    dataset.set(ds);
                    mapped = true;
                }
                else
                    args.append(*LINK(cur));
            }
            if (!mapped)
                return false;
            attribute.setown(expr->clone(args));
            return true;
        }
    default:
        return false;
    }

#ifdef _SR6_
    if (isInlineTrivialDataset(left))
    {
        attribute.set(expr);
        return true;
    }
#endif

    node_operator leftOp = left->getOperator();
    if ((leftOp !=no_select) || expr->hasProperty(newAtom))
    {
        IHqlExpression * lhs = LINK(left);
        IHqlExpression * field = expr->queryChild(1);
        if (lhs->isDataset()) lhs = createRow(no_activerow, lhs);
        dataset.setown(createDatasetFromRow(lhs));
        attribute.setown(createSelectExpr(LINK(dataset), LINK(field))); // remove new attributes
        return true;
    }

    if (!splitDatasetAttribute(dataset, attribute, left))
        return false;

    HqlExprArray args;
    args.append(*attribute.getClear());
    unwindChildren(args, expr, 1);

    attribute.setown(expr->clone(args));
    return true;
#endif
}


static bool splitSetResultValue(SharedHqlExpr & dataset, SharedHqlExpr & attribute, IHqlExpression * value)
{
    if (value->isDataset())
        return false;

    if (splitDatasetAttribute(dataset, attribute, value))
        return true;

    if (value->isDatarow())
    {
        dataset.setown(createDatasetFromRow(LINK(value)));
        attribute.setown(ensureActiveRow(dataset));
        return true;
    }
    return false;
}



IHqlExpression * createSetResult(HqlExprArray & args)
{
    HqlExprAttr dataset, attribute;
    IHqlExpression * value = &args.item(0);
    assertex(value->getOperator() != no_param);
    if (splitSetResultValue(dataset, attribute, value))
    {
        args.replace(*dataset.getClear(), 0);
        args.add(*attribute.getClear(), 1);
        return createValue(no_extractresult, makeVoidType(), args);
    }
    if (value->isDataset())
        return createValue(no_output, makeVoidType(), args);
    return createValue(no_setresult, makeVoidType(), args);
}


IHqlExpression * convertSetResultToExtract(IHqlExpression * setResult)
{
    HqlExprAttr dataset, attribute;
    if (splitSetResultValue(dataset, attribute, setResult->queryChild(0)))
    {
        HqlExprArray args;
        args.append(*dataset.getClear());
        args.append(*attribute.getClear());
        unwindChildren(args, setResult, 1);
        return createValue(no_extractresult, makeVoidType(), args);
    }
    return NULL;
}

IHqlExpression * removeDatasetWrapper(IHqlExpression * ds)
{
    node_operator dsOp = ds->getOperator();
    switch (dsOp)
    {
    case no_datasetfromrow:
        return LINK(ds->queryChild(0));
    case no_inlinetable:
        {
            IHqlExpression * values = ds->queryChild(0);
            assertex(values->numChildren() == 1);
            return createRow(no_createrow, LINK(values->queryChild(0)));
        }
    }
    if (hasSingleRow(ds))
        return createRow(no_selectnth, LINK(ds), createConstantOne());
    throwUnexpectedOp(dsOp);
}

//-------------------------------------------------------------------------------------------------------

bool containsVirtualFields(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (cur->hasProperty(virtualAtom))
                return true;
            //does not walk into nested records
            break;
        case no_ifblock:
            if (containsVirtualFields(cur->queryChild(1)))
                return true;
            break;
        case no_record:
            if (containsVirtualFields(cur))
                return true;
            break;
        }
    }
    return false;
}

IHqlExpression * removeVirtualFields(IHqlExpression * record)
{
    HqlExprArray args;
    args.ensure(record->numChildren());
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (!cur->hasProperty(virtualAtom))
                args.append(*LINK(cur));
            //does not walk into nested records
            break;
        case no_ifblock:
            {
                HqlExprArray ifargs;
                ifargs.append(*LINK(cur->queryChild(0)));
                ifargs.append(*removeVirtualFields(cur->queryChild(1)));
                args.append(*cur->clone(ifargs));
                break;
            }
        case no_record:
            args.append(*removeVirtualFields(cur));
            break;
        default:
            args.append(*LINK(cur));
            break;
        }
    }
    return record->clone(args);
}


#if 0
void VirtualReplacer::createProjectAssignments(HqlExprArray & assigns, IHqlExpression * expr, IHqlExpression * tgtSelector, IHqlExpression * srcSelector, IHqlExpression * dataset)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                createProjectAssignments(assigns, expr->queryChild(i), tgtSelector, srcSelector, dataset);
            break;
        }
    case no_ifblock:
        createProjectAssignments(assigns, expr->queryChild(1), tgtSelector, srcSelector, dataset);
        break;
    case no_field:
        {
            OwnedHqlExpr target = createSelectExpr(LINK(tgtSelector), LINK(expr));
            IHqlExpression * newValue;
            if (expr->hasProperty(virtualAtom))
                newValue = getVirtualReplacement(expr, expr->queryProperty(virtualAtom)->queryChild(0), dataset);
            else
                newValue = createSelectExpr(LINK(srcSelector), LINK(expr));
            assigns.append(*createAssign(target.getClear(), newValue));
            break;
        }
    }
}
#endif

void unwindTransform(HqlExprCopyArray & exprs, IHqlExpression * transform)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            unwindTransform(exprs, cur);
            break;
        default:
            exprs.append(*cur);
            break;
        }
    }
}

bool isConstantTransform(IHqlExpression * transform)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            if (!isConstantTransform(cur))
                return false;
            break;
        case no_assign:
            {
                IHqlExpression * rhs = cur->queryChild(1);
                if (!rhs->isConstant())
                {
                    switch (rhs->getOperator())
                    {
                    case no_null:
                    case no_all:
                        break;
                    default:
                        return false;
                    }
                }
                break;
            }
        case no_attr:
        case no_attr_expr:
            break;
        default:
            return false;
        }
    }
    return true;
}


//would be sensible to extend this to some simple expressions
static bool isSimpleValue(IHqlExpression * expr)
{
    loop
    {
        if (expr->isConstant())
            return true;

        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_null:
        case no_all:
            return true;
        case no_select:
            return !isNewSelector(expr);
        case no_cast:
        case no_implicitcast:
            break;
        default:
            //Do not include access to stored variables
            return false;
        }
        expr = expr->queryChild(0);
    }
}

IHqlExpression * queryUncastExpr(IHqlExpression * expr)
{
    loop
    {
        if (!isCast(expr))
            return expr;
        expr = expr->queryChild(0);
    }
}

bool isSimpleTransformToMergeWith(IHqlExpression * expr, int & varSizeCount)
{
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            {
                if (!isSimpleTransformToMergeWith(cur, varSizeCount))
                    return false;
                break;
            }
        case no_assign:
            {
                IHqlExpression * rhs = cur->queryChild(1);
                if (!isSimpleValue(rhs))
                    return false;

                //Want to take note of whether it reduces the number of variable size fields, if it makes many variable sized into fixed size then it won't be good to remove
                ITypeInfo * srcType = queryUncastExpr(rhs)->queryType();
                ITypeInfo * tgtType = cur->queryChild(0)->queryType();
                if (tgtType->getSize() == UNKNOWN_LENGTH)
                    varSizeCount--;
                if (srcType->getSize() == UNKNOWN_LENGTH)
                    varSizeCount++;
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        default:
            return false;
        }
    }
    return true;
}

bool isSimpleTransformToMergeWith(IHqlExpression * expr)
{
    int varSizeCount = 0;
    return isSimpleTransformToMergeWith(expr, varSizeCount) && varSizeCount < 3;
}



bool isConstantDataset(IHqlExpression * expr)
{
    assertex(expr->getOperator() == no_inlinetable);
    IHqlExpression * values = expr->queryChild(0);
    ForEachChild(i, values)
    {
        if (!isConstantTransform(values->queryChild(i)))
            return false;
    }
    return true;
}

inline bool iseol(char c) { return c == '\r' || c == '\n'; }

#define MATCHOPTION(len, text) (((end - start) == len) && (memicmp(buffer+start, text, len) == 0))

IHqlExpression * extractCppBodyAttrs(unsigned len, const char * buffer)
{
    OwnedHqlExpr attrs;
    unsigned prev = '\n';
    for (unsigned i=0; i < len; i++)
    {
        char next = buffer[i];
        switch (next)
        {
        case ' ': case '\t':
            // allow whitespace in front of #option
            break;
        case '#':
            if (prev == '\n')
            {
                if ((i + 1 + 6 < len) && memicmp(buffer+i+1, "option", 6) == 0)
                {
                    unsigned start = i+1+6;
                    while (start < len && isspace((byte)buffer[start]))
                        start++;
                    unsigned end = start;
                    while (end < len && !iseol((byte)buffer[end]))
                        end++;
                    if (MATCHOPTION(4, "pure"))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(pureAtom)));
                    else if (MATCHOPTION(4, "once"))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(onceAtom)));
                    else if (MATCHOPTION(6, "action"))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(actionAtom)));
                }
            }
            //fallthrough
        default:
            prev = next;
            break;
        }
    }
    return attrs.getClear();
}

unsigned cleanupEmbeddedCpp(unsigned len, char * buffer)
{
    unsigned delta = 0;
    unsigned prev = '\n';
    for (unsigned i=0; i < len; i++)
    {
        char next = buffer[i];
        unsigned skip = 0;
        switch (next)
        {
        case '\r':
            skip = 1;
            prev = next;
            break;
        case ' ': case '\t':
            break;
        case '#':
            if (prev == '\n')
            {
                if ((i + 1 + 6 < len) && memicmp(buffer+i+1, "option", 6) == 0)
                {
                    //skip to newline after #option
                    unsigned end = i + 1 + 6;
                    while (end < len && !iseol(buffer[end]))
                        end++;
                    skip = end - i;
                }
            }
            //fallthrough
        default:
            prev = next;
            break;
        }
        if (skip != 0)
        {
            delta += skip;
            i += (skip - 1);
        }
        else if (delta)
            buffer[i-delta] = next;
    }
    return len-delta;
}

bool isNullList(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_null:
        return true;
    case no_list:
    case no_datasetlist:
    case no_sortlist:
        return expr->numChildren() == 0;
    }
    return false;
}



//--------------------------------------------------------------------------------------

class TempTableTransformer
{
public:
    TempTableTransformer(IErrorReceiver * _errors, ECLlocation & _location) : errors(_errors), defaultLocation(_location) {}

    IHqlExpression * createTempTableTransform(IHqlExpression * curRow, IHqlExpression * record);

protected:
    void createTempTableAssign(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * curRow, IHqlExpression * expr, unsigned & col, IHqlExpression * selector, HqlMapTransformer & mapper, bool included);
    IHqlExpression * createTempTableTransform(IHqlExpression * self, IHqlExpression * curRow, IHqlExpression * expr, unsigned & col, IHqlExpression * selector, HqlMapTransformer & mapper, bool included);

    void reportWarning(IHqlExpression * location, int code,const char *format, ...) __attribute__((format(printf, 4, 5)));
    void reportError(IHqlExpression * location, int code,const char *format, ...) __attribute__((format(printf, 4, 5)));

protected:
    IErrorReceiver * errors;
    ECLlocation & defaultLocation;
};


IHqlExpression * TempTableTransformer::createTempTableTransform(IHqlExpression * self, IHqlExpression * curRow, IHqlExpression * expr, unsigned & col, IHqlExpression * selector, HqlMapTransformer & mapper, bool included)
{
    HqlExprArray assigns;
    createTempTableAssign(assigns, self, curRow, expr, col, selector, mapper, included);
    return createValue(no_transform, makeTransformType(createRecordType(expr)), assigns);
}


IHqlExpression * TempTableTransformer::createTempTableTransform(IHqlExpression * curRow, IHqlExpression * record)
{
    OwnedHqlExpr self = getSelf(record);
    HqlMapTransformer mapping;
    unsigned col = 0;
    OwnedHqlExpr ret = createTempTableTransform(self, curRow, record, col, self, mapping, true);
    if (queryRealChild(curRow, col))
    {
        StringBuffer s;
        getExprECL(curRow->queryChild(col), s);
        ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_TooManyInitializers, s.str());
    }
    return ret.getClear();
}


//NB: Skating on thin ice - can't call transform() inside here because the mapper has the transform mutex.
//So don't make it a member function...
void TempTableTransformer::createTempTableAssign(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * curRow, IHqlExpression * expr, unsigned & col, IHqlExpression * selector, HqlMapTransformer & mapper, bool included)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            OwnedHqlExpr target = createSelectExpr(LINK(selector), LINK(expr));
            OwnedHqlExpr castValue;
            IHqlExpression * record = expr->queryRecord();
            if (record)
            {
                if (included)
                {
                    LinkedHqlExpr src = queryRealChild(curRow, col);
                    if (expr->isDataset())
                    {
                        if (src)
                            col++;
                        else
                        {
                            src.set(expr->queryChild(0));
                            if (!src || src->isAttribute())
                            {
                                ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_NoDefaultProvided, expr->queryName()->str());
                                return;
                            }
                        }
                        src.setown(replaceSelfRefSelector(src, self));

                        if (src->getOperator() == no_list)
                        {
                            HqlExprArray children;
                            children.append(*LINK(src));
                            children.append(*LINK(record));
                            OwnedHqlExpr tempTable = createValue(no_temptable, children);
//                          castValue.setown(transform(tempTable));
                            castValue.set(tempTable);
                        }
                        else if (src->getOperator() == no_recordlist)
                        {
                            HqlExprArray transforms;
                            ForEachChild(idx, src)
                                transforms.append(*createTempTableTransform(src->queryChild(idx), record));

                            HqlExprArray children;
                            children.append(*createValue(no_transformlist, transforms));
                            children.append(*LINK(record));
                            castValue.setown(createDataset(no_inlinetable, children));
                        }
                        else if (src->isDataset())
                        {
                            if (!recordTypesMatch(src, target))
                            {
                                ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_IncompatibleTypesForField, expr->queryName()->str());
                                return;
                            }
                            if (isGrouped(src))
                                castValue.setown(createDataset(no_group, LINK(src)));
                            else
                                castValue.set(src);

                        }
                        else
                        {
                            ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_IncompatibleTypesForField, expr->queryName()->str());
                            return;
                        }
                    }
                    else
                    {
                        if (src && src->isDatarow())
                        {
                            if (!recordTypesMatch(src, target))
                            {
                                ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_IncompatibleTypesForField, expr->queryName()->str());
                                return;
                            }
                            castValue.set(src);
                            col++;
                        }
                        else
                        {
                            //structured initialisers for nested records...
                            OwnedHqlExpr transform;
                            if (src && src->getOperator() == no_rowvalue)
                            {
                                col++;
                                transform.setown(createTempTableTransform(src, record));
                            }
                            else
                            {
                                OwnedHqlExpr localSelf = getSelf(record);
                                HqlMapTransformer localMapping;
                                transform.setown(createTempTableTransform(self, curRow, record, col, localSelf, localMapping, true));
                            }
                            castValue.setown(createRow(no_createrow, LINK(transform)));
                        }
                    }
                }
                else
                {
                    if (expr->isDataset())
                        castValue.setown(createDataset(no_null, LINK(record)));
                    else
                        castValue.setown(createRow(no_null, LINK(record)));
                }
            }
            else
            {
                ITypeInfo * type = expr->queryType()->queryPromotedType();
                if (included)
                {
                    LinkedHqlExpr src = queryRealChild(curRow, col++);
                    if (!src)
                    {
                        IHqlExpression * defaultValue = expr->queryChild(0);
                        src.setown(replaceSelfRefSelector(defaultValue, self));
                        if (src)
                            src.setown(mapper.transformRoot(src));
                    }

                    if (!src || src->isAttribute())
                    {
                        if (expr->hasProperty(virtualAtom))
                            ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_VirtualFieldInTempTable, expr->queryName()->str());
                        else
                            ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_NoDefaultProvided, expr->queryName()->str());
                        return;
                    }
                    if (src->getOperator() == no_recordlist)
                    {
                        ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_IncompatiableInitailiser, expr->queryName()->str());
                        return;
                    }
                    else if (type->isScalar() != src->queryType()->isScalar())
                    {
//                  if (!type->assignableFrom(src->queryType()))            // stricter would be better, but might cause problems.
                        ERRORAT1(curRow->queryProperty(_location_Atom), HQLERR_IncompatibleTypesForField, expr->queryName()->str());
                        return;
                    }

                    castValue.setown(ensureExprType(src, type));
                }
                else
                    castValue.setown(createNullExpr(type));
            }
            assigns.append(*createAssign(LINK(target), LINK(castValue)));
            mapper.setMapping(target, castValue);
            break;
        }

    case no_ifblock:
        {
            OwnedHqlExpr cond = replaceSelfRefSelector(expr->queryChild(0), selector);
            OwnedHqlExpr mapped = mapper.transformRoot(cond);
            mapped.setown(foldHqlExpression(mapped, NULL, HFOfoldimpure|HFOforcefold));
            IValue * mappedValue = mapped->queryValue();

            if (included)
            {
                if (!mappedValue)
                    reportWarning(NULL, HQLWRN_CouldNotConstantFoldIf, HQLWRN_CouldNotConstantFoldIf_Text);
                else if (!mappedValue->getBoolValue())
                    included = false;
            }
            createTempTableAssign(assigns, self, curRow, expr->queryChild(1), col, selector, mapper, included);
            break;
        }
    case no_record:
        {
            ForEachChild(idx, expr)
                createTempTableAssign(assigns, self, curRow, expr->queryChild(idx), col, selector, mapper, included);
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    }
}


void TempTableTransformer::reportError(IHqlExpression * location, int code,const char *format, ...)
{
    if (!errors) return;

    ECLlocation * where = &defaultLocation;
    ECLlocation thisLocation;
    if (location)
    {
        thisLocation.extractLocationAttr(location);
        where = &thisLocation;
    }

    StringBuffer errorMsg;
    va_list args;
    va_start(args, format);
    errorMsg.valist_appendf(format, args);
    va_end(args);
    errors->reportError(code, errorMsg.str(), where->sourcePath->str(), where->lineno, where->column, where->position);
}

void TempTableTransformer::reportWarning(IHqlExpression * location, int code,const char *format, ...)
{
    if (!errors) return;

    ECLlocation * where = &defaultLocation;
    ECLlocation thisLocation;
    if (location)
    {
        thisLocation.extractLocationAttr(location);
        where = &thisLocation;
    }

    StringBuffer errorMsg;
    va_list args;
    va_start(args, format);
    errorMsg.valist_appendf(format, args);
    va_end(args);
    errors->reportWarning(code, errorMsg.str(), where->sourcePath->str(), where->lineno, where->column, where->position);
}

IHqlExpression * convertTempRowToCreateRow(IErrorReceiver * errors, ECLlocation & location, IHqlExpression * expr)
{
    IHqlExpression * oldValues = expr->queryChild(0);
    IHqlExpression * record = expr->queryChild(1);
    OwnedHqlExpr values = normalizeListCasts(oldValues);

    TempTableTransformer transformer(errors, location);
    OwnedHqlExpr newTransform = transformer.createTempTableTransform(oldValues, record);
    HqlExprArray children;
    children.append(*LINK(newTransform));
    OwnedHqlExpr ret = createRow(no_createrow, children);
    return expr->cloneAllAnnotations(ret);
}

IHqlExpression * convertTempTableToInlineTable(IErrorReceiver * errors, ECLlocation & location, IHqlExpression * expr)
{
    IHqlExpression * oldValues = expr->queryChild(0);
    IHqlExpression * record = expr->queryChild(1);
    OwnedHqlExpr values = normalizeListCasts(oldValues);
    node_operator valueOp = values->getOperator();

    if ((valueOp == no_list) && (values->numChildren() == 0))
        return createDataset(no_null, LINK(record));

    if ((valueOp != no_recordlist) && (valueOp != no_list))
        return LINK(expr);

    TempTableTransformer transformer(errors, location);
    HqlExprArray transforms;
    ForEachChild(idx, values)
    {
        LinkedHqlExpr cur = values->queryChild(idx);
        if (valueOp == no_list)
            cur.setown(createValue(no_rowvalue, makeNullType(), LINK(cur)));
        if (cur->getOperator() == no_record)
        {
            HqlExprArray row;
            ForEachChild(idx, cur)
            {
                IHqlExpression * field = cur->queryChild(idx);
                if (field->getOperator() == no_field)
                    row.append(*LINK(field->queryChild(0)));
            }
            cur.setown(createValue(no_rowvalue, makeNullType(), row));
        }
        transforms.append(*transformer.createTempTableTransform(cur, record));
    }

    HqlExprArray children;
    children.append(*createValue(no_transformlist, makeNullType(), transforms));
    children.append(*LINK(record));
    OwnedHqlExpr ret = createDataset(no_inlinetable, children);
    return expr->cloneAllAnnotations(ret);
}


bool areTypesComparable(ITypeInfo * leftType, ITypeInfo * rightType)
{
    if (leftType == rightType)
        return true;
    if (!leftType || !rightType)
        return false;
    type_t ltc = leftType->getTypeCode();
    type_t rtc = rightType->getTypeCode();
    if (ltc != rtc)
        return false;
    switch (ltc)
    {
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return haveCommonLocale(leftType, rightType);
    case type_data:
    case type_decimal:
        return true;
    case type_qstring:
    case type_varstring:
    case type_string:
        return (leftType->queryCharset() == rightType->queryCharset()) &&
               (leftType->queryCollation() == rightType->queryCollation());
    case type_set:
    case type_array:
        return areTypesComparable(leftType->queryChildType(), rightType->queryChildType());
    case type_row:
    case type_table:
    case type_groupedtable:
        return recordTypesMatch(leftType, rightType);
    }
    return false;
}

bool arraysMatch(const HqlExprArray & left, const HqlExprArray & right)
{
    unsigned numLeft = left.ordinality();
    unsigned numRight = right.ordinality();
    if (numLeft != numRight)
        return false;
    for (unsigned i=0; i < numLeft; i++)
    {
        if (&left.item(i) != &right.item(i))
            return false;
    }
    return true;
}

bool isBlankString(IHqlExpression * expr)
{
    if (expr->getOperator() != no_constant)
        return false;
    IValue * value = expr->queryValue();
    if (value->getTypeCode() != type_string)
        return false;
    unsigned size = value->getSize();
    return rtlCompareStrBlank(size, (const char *)value->queryValue()) == 0;
}


bool isNullString(IHqlExpression * expr)
{
    ITypeInfo * exprType = expr->queryType();
    switch (exprType->getTypeCode())
    {
    case type_data:
    case type_string:
    case type_qstring:
        return exprType->getSize() == 0;
    }
    return false;
}

const char * queryChildNodeTraceText(StringBuffer & s, IHqlExpression * expr)
{
    s.clear().append(getOpString(expr->getOperator()));
    if (expr->queryName())
        s.append("[").append(expr->queryName()).append("]");
//  s.appendf(" {%lx}", (unsigned)expr);
    return s.str();
}


extern HQL_API bool areConstant(const HqlExprArray & args)
{
    ForEachItemIn(i, args)
    {
        if (!args.item(i).isConstant())
            return false;
    }
    return true;
}


//===========================================================================

void appendArray(HqlExprCopyArray & tgt, const HqlExprCopyArray & src)
{
    ForEachItemIn(idx, src)
        tgt.append(src.item(idx));
}


void appendArray(HqlExprCopyArray & tgt, const HqlExprArray & src)
{
    ForEachItemIn(idx, src)
        tgt.append(src.item(idx));
}


void replaceArray(HqlExprArray & tgt, const HqlExprArray & src)
{
    tgt.kill();
    appendArray(tgt, src);
}


//--------------------------------------------------------------

static void gatherSortOrder(HqlExprArray & sorts, IHqlExpression * ds, IHqlExpression * record, unsigned maxfield = NotFound)
{
    unsigned max = record->numChildren();
    if (max > maxfield) max = maxfield;
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * cur = record->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_record:
            gatherSortOrder(sorts, ds, cur);
            break;
        case no_ifblock:
            gatherSortOrder(sorts, ds, cur->queryChild(1));
            break;
        case no_field:
            sorts.append(*createSelectExpr(LINK(ds), LINK(cur)));
            break;
        }
    }
}

void gatherIndexBuildSortOrder(HqlExprArray & sorts, IHqlExpression * expr, bool sortIndexPayload)
{ 
    // If any field types collate differently before and after translation to their hozed
    // format, then we need to do the translation here, otherwise this
    // sort may not be in the correct order.  (ebcdic->ascii?  integers are ok; unicode isn't!)
    // First build the sort order we need....
    LinkedHqlExpr dataset = expr->queryChild(0);
    IHqlExpression * normalizedDs = dataset->queryNormalizedSelector();
    IHqlExpression * buildRecord = dataset->queryRecord();
    unsigned payloadCount = numPayloadFields(expr);

    //Option to not sort by fields that aren't part of the sorted key.
    unsigned indexFirstPayload = firstPayloadField(buildRecord, payloadCount);
    unsigned max;
    bool sortPayload = sortIndexPayload ? !expr->hasProperty(sort_KeyedAtom) : expr->hasProperty(sort_AllAtom);
    if (sortPayload)
    {
        max = buildRecord->numChildren();
        //If the last field is an implicit fpos, then they will all have the same value, so no point sorting.
        if (queryLastField(buildRecord)->hasProperty(_implicitFpos_Atom))
            max--;
    }
    else
        max = indexFirstPayload;

    gatherSortOrder(sorts, normalizedDs, buildRecord, max);
    ForEachItemIn(i0, sorts)
    {
        IHqlExpression & cur = sorts.item(i0);
        if (cur.isDataset())
        {
            sorts.replace(*createValue(no_typetransfer, makeDataType(UNKNOWN_LENGTH), LINK(&cur)), i0);
        }
        else if ((i0 < indexFirstPayload) && isUnicodeType(cur.queryType()))
        {
            sorts.replace(*createValue(no_typetransfer, makeDataType(cur.queryType()->getSize()), LINK(&cur)), i0);
        }
    }
}

//------------------------- Library processing -------------------------------------


int compareLibraryParameterOrder(IInterface * * pleft, IInterface * * pright)
{
    IHqlExpression * left = static_cast<IHqlExpression *>(*pleft);
    IHqlExpression * right = static_cast<IHqlExpression *>(*pright);

    //datasets come first - even if not streamed
    if (left->isDataset())
    {
        if (!right->isDataset())
            return -1;
    }
    else
    {
        if (right->isDataset())
            return +1;
    }

    //Then fixed size fields - to minimize the code generated to access them
    if (left->queryType()->getSize() == UNKNOWN_LENGTH)
    {
        if (right->queryType()->getSize() != UNKNOWN_LENGTH)
            return +1;
    }
    else
    {
        if (right->queryType()->getSize() == UNKNOWN_LENGTH)
            return -1;
    }

    //then by name
    return stricmp(left->queryName()->str(), right->queryName()->str());
}



LibraryInputMapper::LibraryInputMapper(IHqlExpression * _libraryInterface)
: libraryInterface(_libraryInterface)
{
    assertex(libraryInterface->getOperator() == no_funcdef);
    scopeExpr.set(libraryInterface->queryChild(0));
    streamingAllowed = !scopeExpr->hasProperty(_noStreaming_Atom);  // ?? is this in the correct place, probably, just nasty

    expandParameters();
}


void LibraryInputMapper::expandParameters()
{
    IHqlExpression * formals = libraryInterface->queryChild(1);
    unsigned nextParameter = formals->numChildren()+1;
    ForEachChild(i, formals)
        expandParameter(formals->queryChild(i), nextParameter);
    realParameters.sort(compareLibraryParameterOrder);

    //Count number of datasets (always at the front), so can use to adjust library counts when streaming
    numDatasets = 0; 
    while ((numDatasets < realParameters.ordinality()) && realParameters.item(numDatasets).isDataset())
        numDatasets++;
}


void LibraryInputMapper::expandParameter(IHqlExpression * expr, unsigned & nextParameter)
{
    if (expr->isScope())
    {
        IHqlScope * scope = expr->queryScope();
        HqlExprArray symbols;
        scope->getSymbols(symbols);

        ForEachItemIn(i, symbols)
        {
            IHqlExpression & cur = symbols.item(i);
            _ATOM nestedName = createMangledName(expr, &cur);

            //default values are handled elsewhere - lost from the mapped values here.
            HqlExprArray attrs;
            OwnedHqlExpr renamed = createParameter(nestedName, nextParameter++, cur.getType(), attrs);
            expandParameter(renamed, nextParameter);
        }
    }
    else
        realParameters.append(*LINK(expr));
}


unsigned LibraryInputMapper::findParameter(_ATOM search)
{
    ForEachItemIn(i, realParameters)
        if (realParameters.item(i).queryName() == search)
            return i;
    return NotFound;
}

IHqlExpression * LibraryInputMapper::resolveParameter(_ATOM search)
{
    unsigned match = findParameter(search);
    assertex(match != NotFound);
    return &realParameters.item(match);
}


void LibraryInputMapper::mapRealToLogical(HqlExprArray & inputExprs, HqlExprArray & logicalParams, IHqlExpression * libraryId, bool canStream, bool distributed)
{
    //Create a list of expressions representing each of the inputs...
    ForEachItemIn(i1, realParameters)
    {
        IHqlExpression * cur = &realParameters.item(i1);
        IHqlExpression * result = NULL;
        unsigned inputIndex = i1;
        if (canStream && streamingAllowed)
        {
            if (cur->isDataset())
            {
                HqlExprArray args;
                args.append(*LINK(cur->queryRecord()));
                args.append(*LINK(libraryId));
                args.append(*getSizetConstant(inputIndex));
                args.append(*createAttribute(_streaming_Atom));
                if (isGrouped(cur))
                    args.append(*createAttribute(groupedAtom));
                if (distributed)
                    args.append(*createAttribute(_distributed_Atom));
                result = createDataset(no_getgraphresult, args);
            }
            else
                inputIndex -= numDatasets;
        }

        if (!result)
        {
            IHqlExpression * seq = getSizetConstant(inputIndex);
            if (cur->isDataset())
            {
                IHqlExpression * groupAttr = isGrouped(cur) ? createAttribute(groupedAtom) : NULL;
                result = createDataset(no_libraryinput, LINK(cur->queryRecord()), createComma(seq, groupAttr));
            }
            else if (cur->isDatarow())
                result = createDataset(no_libraryinput, LINK(cur->queryRecord()), seq);     // should this be a row?
            else
                result = createValue(no_libraryinput, cur->getType(), seq);
        }
        
        inputExprs.append(*createSymbol(cur->queryName(), result, ob_private));
    }

    IHqlExpression * formals = libraryInterface->queryChild(1);
    ForEachChild(i, formals)
        logicalParams.append(*mapRealToLogical(inputExprs, formals->queryChild(i), libraryId));
}

IHqlExpression * LibraryInputMapper::mapRealToLogical(const HqlExprArray & inputExprs, IHqlExpression * expr, IHqlExpression * libraryId)
{
    if (expr->isScope())
    {
        IHqlScope * scope = expr->queryScope();
        HqlExprArray symbols;
        scope->getSymbols(symbols);

        Owned<IHqlScope> newScope = createVirtualScope();
        ForEachItemIn(i, symbols)
        {
            IHqlExpression & cur = symbols.item(i);
            IHqlExpression * param = resolveParameter(createMangledName(expr, &cur));
            OwnedHqlExpr mapped = mapRealToLogical(inputExprs, param, libraryId);

            OwnedHqlExpr named = createSymbol(cur.queryName(), LINK(mapped), ob_private);
            newScope->defineSymbol(named.getClear());
        }
        return queryExpression(closeScope(newScope.getClear()));
    }
    else
    {
        unsigned inputIndex = realParameters.find(*expr);
        return LINK(&inputExprs.item(inputIndex));
    }
}


void LibraryInputMapper::mapLogicalToReal(HqlExprArray & mapped, HqlExprArray & params)
{
    IHqlExpression * placeholder = queryActiveTableSelector();
    ForEachItemIn(i1, realParameters)
        mapped.append(*LINK(placeholder));

    IHqlExpression * formals = libraryInterface->queryChild(1);
    ForEachChild(i, formals)
        mapLogicalToReal(mapped, formals->queryChild(i), &params.item(i));
}


void LibraryInputMapper::mapLogicalToReal(HqlExprArray & mapped, IHqlExpression * expr, IHqlExpression * value)
{
    if (expr->isScope())
    {
        IHqlScope * scope = expr->queryScope();
        IHqlScope * valueScope = value->queryScope();
        HqlExprArray symbols;
        scope->getSymbols(symbols);

        HqlDummyLookupContext lookupCtx(NULL);
        ForEachItemIn(i, symbols)
        {
            IHqlExpression & cur = symbols.item(i);
            IHqlExpression * param = resolveParameter(createMangledName(expr, &cur));
            
            OwnedHqlExpr childValue = valueScope->lookupSymbol(cur.queryName(), LSFpublic, lookupCtx);
            mapLogicalToReal(mapped, param, childValue);
        }
    }
    else
    {
        //Replace the real parameter at the appropriate position
        unsigned match = realParameters.find(*expr);
        assertex(match != NotFound);
        mapped.replace(*LINK(value), match);
    }
}

static byte key[32] = { 
    0xf7, 0xe8, 0x79, 0x40, 0x44, 0x16, 0x66, 0x18, 0x52, 0xb8, 0x18, 0x6e, 0x77, 0xd1, 0x68, 0xd3,
    0x87, 0x47, 0x01, 0xe6, 0x66, 0x62, 0x2f, 0xbe, 0xc1, 0xd5, 0x9f, 0x4a, 0x53, 0x27, 0xae, 0xa1,
};

extern HQL_API void encryptEclAttribute(IStringVal & out, size32_t len, const void * in)
{
    MemoryBuffer encrypted;
    aesEncrypt(key, sizeof(key), in, len, encrypted);

    StringBuffer base64;
    JBASE64_Encode(encrypted.toByteArray(), encrypted.length(), base64, false);
    StringBuffer text;
    text.append("ENC").append('R').append('Y').append("PTE").append("D(").newline();
    const char * base64Text = base64.str();
    unsigned max = base64.length();
    const unsigned chunk = 60;
    unsigned i;
    for (i = 0; i + chunk < max; i += chunk)
    {
        text.append('\t').append("'").append(chunk, base64Text+i).append("',").newline();
    }
    text.append('\t').append("'").append(max-i, base64Text+i).append("'").newline().append(");").newline();
    out.set(text.str());
}


void decryptEclAttribute(MemoryBuffer & out, const char * in)
{
    StringBuffer decoded;
    JBASE64_Decode(in, decoded);
    aesDecrypt(key, sizeof(key), decoded.str(), decoded.length(), out);
}

//---------------------------------------------------------------------------

class HQL_API GraphIdCollector : public NewHqlTransformer
{
public:
    GraphIdCollector(HqlExprCopyArray & _graphs, bool _externalIds);

    virtual void analyseExpr(IHqlExpression * expr);

protected:
    HqlExprCopyArray & graphs;
    bool externalIds;
};


static HqlTransformerInfo hqlGraphIdCollectorInfo("GraphIdCollector");
GraphIdCollector::GraphIdCollector(HqlExprCopyArray & _graphs, bool _externalIds)
: NewHqlTransformer(hqlGraphIdCollectorInfo), graphs(_graphs), externalIds(_externalIds)
{ 
}

void GraphIdCollector::analyseExpr(IHqlExpression * expr)
{
    if (alreadyVisited(expr))
        return;

    switch (expr->getOperator())
    {
    case no_loopcounter:
        if (!externalIds)
            graphs.append(*expr->queryChild(0));
        return;
    case no_getgraphresult:
        if (externalIds)
        {
            IHqlExpression * id = queryPropertyChild(expr, externalAtom, 0);
            if (id)
                graphs.append(*id);
        }
        else
            graphs.append(*expr->queryChild(1));
        return;
    }

    NewHqlTransformer::analyseExpr(expr);
}




void gatherGraphReferences(HqlExprCopyArray & graphs, IHqlExpression * value, bool externalIds)
{
    GraphIdCollector collector(graphs, externalIds);
    collector.analyse(value, 0);
}

//---------------------------------------------------------------------------

_ATOM getWarningAction(unsigned errorCode, const HqlExprArray & overrides, unsigned first)
{
    //warnings are assumed to be infrequent, so don't worry about efficiency here.
    const unsigned max = overrides.ordinality();
    for (unsigned i=first; i < max; i++)
    {
        IHqlExpression & cur = overrides.item(i);
        if (matchesConstantValue(cur.queryChild(0), errorCode))
            return cur.queryChild(1)->queryName();
    }
    return defaultAtom;
}

WarningProcessor::WarningProcessor() 
{ 
    firstLocalOnWarning = 0; 
    activeSymbol = NULL; 
//  addGlobalOnWarning(WRN_RECORDDEFAULTMAXLENGTH, errorAtom);
}


void WarningProcessor::addWarning(IECLError * warning)
{
    //warnings are assumed to be infrequent, so don't worry about efficiency here.
    _ATOM action = getWarningAction(warning->errorCode(), localOnWarnings, firstLocalOnWarning);

    if (action == defaultAtom)
        appendUnique(possibleWarnings, warning);
    else if (action == warningAtom)
        appendUnique(warnings, warning);
    else if (action == errorAtom)
    {
        allErrors.append(*changeErrorType(true, warning));
    }
    else
    {
        assertex(action == ignoreAtom);
    }
}

void WarningProcessor::addGlobalOnWarning(unsigned code, _ATOM action)
{
    globalOnWarnings.append(*createAttribute(onWarningAtom, getSizetConstant(code), createAttribute(action)));
}

void WarningProcessor::addGlobalOnWarning(IHqlExpression * setMetaExpr)
{
    globalOnWarnings.append(*createAttribute(onWarningAtom, LINK(setMetaExpr->queryChild(1)), LINK(setMetaExpr->queryChild(2))));
}

void WarningProcessor::processMetaAnnotation(IHqlExpression * expr)
{
    gatherMetaProperties(localOnWarnings, onWarningAtom, expr);
}

void WarningProcessor::processWarningAnnotation(IHqlExpression * expr)
{
    //would be cleaner if each annotation defined an interface, and this was a dynamic cast
    //but not sufficiently complicated to warrent it.
    IECLError * error = static_cast<CHqlWarningAnnotation *>(expr)->queryWarning();
    addWarning(error);
}

void WarningProcessor::pushSymbol(OnWarningState & saved, IHqlExpression * _symbol)
{
    saveState(saved);
    setSymbol(_symbol);
}

void WarningProcessor::saveState(OnWarningState & saved)
{
    saved.firstOnWarning = firstLocalOnWarning;
    saved.onWarningMax = localOnWarnings.ordinality();
    saved.symbol = activeSymbol;
}

void WarningProcessor::setSymbol(IHqlExpression * _symbol)
{
    firstLocalOnWarning = localOnWarnings.ordinality();
    activeSymbol = _symbol;
}

void WarningProcessor::restoreState(const OnWarningState & saved)
{
    while (localOnWarnings.ordinality() > saved.onWarningMax)
        localOnWarnings.pop();
    firstLocalOnWarning = saved.firstOnWarning;
    activeSymbol = saved.symbol;
}

void WarningProcessor::report(IErrorReceiver & errors)
{
    applyGlobalOnWarning();
    combineSandboxWarnings();
    if (allErrors.ordinality())
        reportErrors(errors, allErrors);
    else
        reportErrors(errors, warnings);
}

void WarningProcessor::report(IErrorReceiver * errors, IErrorReceiver * warnings, IECLError * warning)
{
    _ATOM action = getWarningAction(warning->errorCode(), localOnWarnings, firstLocalOnWarning);

    if (action == defaultAtom)
        action = getWarningAction(warning->errorCode(), globalOnWarnings, 0);

    if (action == defaultAtom)
        action = warning->isError() ? errorAtom : warningAtom;

    if ((action == warningAtom) || (action == defaultAtom))
    {
        if (warnings)
            warnings->report(warning);
    }
    else if (action == errorAtom)
    {
        Owned<IECLError> error = changeErrorType(true, warning);
        if (errors)
            errors->report(error);
        else
            throw error.getClear();
    }
}


void WarningProcessor::combineSandboxWarnings()
{
    StringBuffer s;
    ForEachItemInRev(i, warnings)
    {
        IECLError & cur = warnings.item(i);
        if (cur.errorCode() == WRN_DEFINITION_SANDBOXED)
        {
            if (s.length())
                s.append(", ");
            s.append(cur.getFilename());
            warnings.remove(i);
        }
    }
    if (s.length())
    {
        s.insert(0, "The following definitions are sandboxed: ");
        warnings.append(* createECLWarning(WRN_DEFINITION_SANDBOXED, s.str(), NULL, 0, 0, 0));
    }
}

void WarningProcessor::applyGlobalOnWarning()
{
    ForEachItemIn(i, possibleWarnings)
    {
        IECLError & cur = possibleWarnings.item(i);
        _ATOM action = getWarningAction(cur.errorCode(), globalOnWarnings, 0);
        if (action == defaultAtom || action == warningAtom)
        {
            if (cur.isError())
                appendUnique(allErrors, &cur);
            else
                appendUnique(warnings, &cur);
        }
        else if (action == errorAtom)
            allErrors.append(*changeErrorType(true, &cur));
    }
}

bool isActiveRow(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select:
        return !isNewSelector(expr);
    default:
        return isAlwaysActiveRow(expr);
    }
}

StringBuffer & convertToValidLabel(StringBuffer &out, const char * in, unsigned inlen)
{
    for (unsigned o = 0; o < inlen; o++)
    {
        unsigned char c = in[o];
        if (isalnum(c))
            out.append(c);
        else
            out.append('_');
    }
    return out;
}

bool arraysSame(CIArray & left, CIArray & right)
{
    if (left.ordinality() != right.ordinality())
        return false;
    return memcmp(left.getArray(), right.getArray(), left.ordinality() * sizeof(CInterface*)) == 0;
}

bool arraysSame(Array & left, Array & right)
{
    if (left.ordinality() != right.ordinality())
        return false;
    return memcmp(left.getArray(), right.getArray(), left.ordinality() * sizeof(CInterface*)) == 0;
}

bool isFailAction(IHqlExpression * expr)
{
    return expr && (expr->getOperator() == no_fail);
}

bool isFailureGuard(IHqlExpression * expr)
{
    if (expr->getOperator() == no_if)
        return isFailAction(expr->queryChild(1)) || isFailAction(expr->queryChild(2));
    return false;
}


extern HQL_API bool isKeyedDataset(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_keyedlimit:
        return true;
    case no_filter:
        return filterIsKeyed(expr);
    case no_hqlproject:
    case no_newusertable:
    case no_aggregate:
    case no_newaggregate:
        return expr->hasProperty(keyedAtom);
    }
    return false;
}

extern HQL_API bool isSteppedDataset(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_stepped:
    case no_mergejoin:
    case no_nwayjoin:
        return true;
    }
    return false;
}

extern HQL_API IHqlExpression * queryFieldFromExpr(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_field:
            return expr;
        case no_indirect:
            expr = expr->queryChild(0);
            break;
        case no_select:
            expr = expr->queryChild(1);
            break;
        default:
            return expr;
        }
    }
}


extern HQL_API IHqlExpression * queryFieldFromSelect(IHqlExpression * expr)
{
    IHqlExpression * ret = queryFieldFromExpr(expr);
    assertex(ret->getOperator() == no_field);
    return ret;
}


extern HQL_API bool isValidFieldReference(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select:
    case no_field:
    case no_indirect:
        return true;
    case no_param:
        return expr->hasProperty(fieldAtom);
    }
    return false;
}


extern HQL_API bool isFieldSelectedFromRecord(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select:
    case no_indirect:
        expr = expr->queryChild(0);
        break;
    default:
        return false;
    }

    loop
    {
        switch (expr->getOperator())
        {
        case no_record:
            return true;
        case no_select:
        case no_indirect:
            expr = expr->queryChild(0);
            break;
        default:
            return false;
        }
    }
}

void createClearAssigns(HqlExprArray & assigns, IHqlExpression * record, IHqlExpression * targetSelector)
{
    ForEachChild(idx, record)
    {
        IHqlExpression * field = record->queryChild(idx);

        switch (field->getOperator())
        {
        case no_ifblock:
            createClearAssigns(assigns, field->queryChild(1), targetSelector);
            break;
        case no_record:
            createClearAssigns(assigns, field, targetSelector);
            break;
        case no_field:
            {
                OwnedHqlExpr newTargetSelector = createSelectExpr(LINK(targetSelector), LINK(field));
                IHqlExpression * value = createNullExpr(newTargetSelector);
                assigns.append(*createAssign(LINK(newTargetSelector), value));
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        }
    }
}

IHqlExpression * createClearTransform(IHqlExpression * record)
{
    HqlExprArray assigns;
    OwnedHqlExpr self = getSelf(record);
    createClearAssigns(assigns, record, self);
    return createValue(no_transform, makeTransformType(record->getType()), assigns);
}

IHqlExpression * createDefaultAssertMessage(IHqlExpression * cond)
{
    if (cond->getOperator() == no_assertconstant)
    {
        OwnedHqlExpr msg = createDefaultAssertMessage(cond->queryChild(0));
        return createWrapper(no_assertconstant, msg.getClear());
    }

    StringBuffer suffix;
    getExprECL(cond, suffix, true);

    node_operator op = cond->getOperator();
    StringBuffer temp;
    switch (op)
    {
    case no_eq:
    case no_ne:
    case no_gt:
    case no_ge:
    case no_lt:
    case no_le:
        break;
    default:
        return createConstant(temp.append("Assert failed: ").append(suffix));
    }

    IHqlExpression * lhs = cond->queryChild(0);
    IHqlExpression * rhs = cond->queryChild(1);
    if (!lhs->queryType()->isScalar() || !rhs->queryType()->isScalar())
        return createConstant(temp.append("Assert failed: ").append(suffix));

    StringBuffer prefix;
    prefix.append("Assert (");
    suffix.insert(0, ") failed [");
    suffix.append("]");

    StringBuffer cmpText;
    cmpText.append(" ").append(getOpString(op)).append(" ");

    OwnedITypeInfo unknownStringType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
    OwnedITypeInfo unknownVarStringType = makeVarStringType(UNKNOWN_LENGTH, NULL, NULL);
    HqlExprArray args;
    args.append(*createConstant(prefix));
    args.append(*ensureExprType(lhs, unknownStringType));
    args.append(*createConstant(cmpText));
    args.append(*ensureExprType(rhs, unknownStringType));
    args.append(*createConstant(suffix));
    return createBalanced(no_concat, unknownVarStringType, args);
}

//-------------------------------------------------------------------------------------------------------------------

static char const gccMangledIntegers[2][8] = {
    { 'h', 't', 'j', 'j', 'y', 'y', 'y', 'y' },
    { 'c', 's', 'i', 'i', 'x', 'x', 'x', 'x' }
};

// gcc see http://www.codesourcery.com/public/cxx-abi/abi.html#mangling
class GccCppNameMangler
{
public:
    bool mangleFunctionName(StringBuffer & mangled, IHqlExpression * funcdef)
    {
        IHqlExpression *body = funcdef->queryChild(0);
        IHqlExpression *formals = funcdef->queryChild(1);
        ITypeInfo * retType = funcdef->queryType()->queryChildType();

        enum { ServiceApi, RtlApi, BcdApi, CApi, LocalApi } api = ServiceApi;
        if (body->hasProperty(eclrtlAtom))
            api = RtlApi;
        else if (body->hasProperty(bcdAtom))
            api = BcdApi;
        else if (body->hasProperty(cAtom))
            api = CApi;
        else if (body->hasProperty(localAtom))
            api = LocalApi;

        StringBuffer entrypoint;
        getProperty(body, entrypointAtom, entrypoint);
        if (entrypoint.length() == 0)
            return false;

        if ((api == ServiceApi) || api == CApi)
        {
            mangled.append(entrypoint); // extern "C"
            return true;
        }

        if (body->hasProperty(oldSetFormatAtom))
            return false;

        mangled.append("_Z").append(entrypoint.length()).append(entrypoint);

        StringBuffer mangledReturn;
        StringBuffer mangledReturnParameters;
        mangleFunctionReturnType(mangledReturn, mangledReturnParameters, retType);

        if (body->hasProperty(contextAtom))
            mangled.append("P12ICodeContext");
        else if (body->hasProperty(globalContextAtom) )
            mangled.append("P18IGlobalCodeContext");
        else if (body->hasProperty(userMatchFunctionAtom))
            mangled.append("P12IMatchWalker");

        mangled.append(mangledReturnParameters);

        ForEachChild(i, formals)
        {
            IHqlExpression * param = formals->queryChild(i);
            ITypeInfo *paramType = param->queryType();

            bool isOut = param->hasProperty(outAtom);
            bool isConst = param->hasProperty(constAtom);

            if (isOut)
                mangled.append("R");
            if (!mangleSimpleType(mangled, paramType, isConst))
                return false;
        }
        return true;
    }

protected:
    bool mangleSimpleType(StringBuffer & result, ITypeInfo * type, bool hasConst)
    {
        if (!type)
            return false;

        switch (type->getTypeCode())
        {
        case type_boolean:
            result.append("b");
            return true;
        case type_int:
        case type_swapint:
            result.append(gccMangledIntegers[type->isSigned() ? 1 : 0][type->getSize()-1]);
            return true;
        case type_real:
            result.append(type->getSize() == 4 ? "f" : "d");
            return true;
        case type_decimal:
            //Should really define this properly  (size, precision, ptr)
            return false;
        case type_string:
        case type_qstring:
        case type_utf8:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("j");
            result.append(hasConst ? "PKc" : "Pc");
            return true;
        case type_varstring:
            result.append(hasConst ? "PKc" : "Pc");
            return true;
        case type_data:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("j");
            result.append(hasConst ? "PKv" : "Pv");
            return true;
        case type_unicode:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("j");
            result.append(hasConst ? "PKt" : "Pt");
            return true;
        case type_varunicode:
            result.append(hasConst ? "PKt" : "Pt");
            return true;
        case type_char:
            result.append("c");
            return true;
        case type_enumerated:
            return mangleSimpleType(result, type->queryChildType(), hasConst);
        case type_pointer:
            result.append("P");
            return mangleSimpleType(result, type->queryChildType(), hasConst);
        case type_array:
            result.append("A").append(type->getSize()).append("_");;
            return mangleSimpleType(result, type->queryChildType(), hasConst);
        case type_table:
        case type_groupedtable:
            result.append("j"); // size32_t
            result.append(hasConst ? "PKv" : "Pv"); // [const] void *
            return true;
        case type_set:
            result.append("b"); // bool
            result.append("j"); // unsigned
            result.append(hasConst ? "PKv" : "Pv"); // *
            return true;
        case type_row:
            result.append("Ph");
            return true;
        case type_void:
            result.append("v");
            return true;
        case type_scope:
        case type_transform:
        case type_function:
        case type_any:
        case type_packedint:
        case type_alien:
        case type_class:
        case type_date:
            //may possibly have some support in the future, but not yet...
            return false;
        }
        throwUnexpected();
    }


    bool mangleFunctionReturnType(StringBuffer & returnType, StringBuffer & params, ITypeInfo * retType)
    {
        type_t tc = retType->getTypeCode();
        switch (tc)
        {
        case type_varstring:
            if (retType->getSize() == UNKNOWN_LENGTH)
                returnType.append("Pc");    // char *
            else
                params.append("Pc");        // char *
            break;
        case type_varunicode:
            if (retType->getSize() == UNKNOWN_LENGTH)
                returnType.append("Pt");    // ushort *
            else
                params.append("Pt");        // ushort *
            break;
        case type_qstring:
        case type_string:
        case type_utf8:
            if (retType->getSize() == UNKNOWN_LENGTH)
            {
                params.append("Rj");    // size32_t &
                params.append("RPc");   // char * &
            }
            else
                params.append("Pc");    // char *
            break;
        case type_data:
            if (retType->getSize() == UNKNOWN_LENGTH)
            {
                params.append("Rj");    // size32_t &
                params.append("RPv");   // void * &
            }
            else
                params.append("Pv");    // void *
            break;
        case type_unicode:
            if (retType->getSize() == UNKNOWN_LENGTH)
            {
                params.append("Rj");    // size32_t &
                params.append("RPt");   // UChar * &
            }
            else
                params.append("Pt");    // UChar *
            break;
        case type_table:
        case type_groupedtable:
            params.append("Rj");    // size32_t &
            params.append("RPv");   // void * &
            break;
        case type_set:
            params.append("Rb");    // bool &
            params.append("Rj");    // size32_t &
            params.append("RPv");   // void * &
            break;
        case type_row:
            params.append("Ph");        // byte *
            break;
        }
        return true;
    }
};

//-------------------------------------------------------------------------------------------------------------------

//See http://www.kegel.com/mangle.html for details
//See http://www.agner.org/optimize/calling_conventions.pdf for details

static const char * const vs6MangledIntegers[2][8] = {
    { "E", "G", "I", "I", "_K", "_K", "_K", "_K" },
    { "D", "F", "H", "H", "_J", "_J", "_J", "_J" }
};

class Vs6CppNameMangler
{
public:
    Vs6CppNameMangler()
    {
#ifdef __64BIT__
        pointerBaseCode.set("E");
#endif
    }

    bool mangle(StringBuffer & mangled, IHqlExpression * funcdef)
    {
        IHqlExpression *body = funcdef->queryChild(0);
        IHqlExpression *formals = funcdef->queryChild(1);

        enum { ServiceApi, RtlApi, BcdApi, CApi, LocalApi } api = ServiceApi;
        if (body->hasProperty(eclrtlAtom))
            api = RtlApi;
        else if (body->hasProperty(bcdAtom))
            api = BcdApi;
        else if (body->hasProperty(cAtom))
            api = CApi;
        else if (body->hasProperty(localAtom))
            api = LocalApi;

        StringBuffer entrypoint;
        getProperty(body, entrypointAtom, entrypoint);
        if (entrypoint.length() == 0)
            return false;

        if ((api == ServiceApi) || api == CApi)
        {
            mangled.append(entrypoint); // extern "C"
            return true;
        }

        if (body->hasProperty(oldSetFormatAtom))
            return false;

        mangled.append("?").append(entrypoint).append("@@").append("Y");
        switch (api)
        {
        case CApi:
            mangled.append("A");    // _cdecl
            break;
        case BcdApi:
            mangled.append("T");    //  __fastcall"
            break;
        default:
            mangled.append("A");    // _cdecl
            break;
        //  mangled.append("G");    // __stdcall
        }

        StringBuffer mangledReturn;
        StringBuffer mangledReturnParameters;
        ITypeInfo * retType = funcdef->queryType()->queryChildType();
        mangleFunctionReturnType(mangledReturn, mangledReturnParameters, retType);

        mangled.append(mangledReturn);

        if (body->hasProperty(contextAtom))
            mangled.append("PVICodeContext@@");
        else if (body->hasProperty(globalContextAtom) )
            mangled.append("PVIGlobalCodeContext@@");
        else if (body->hasProperty(userMatchFunctionAtom))
            mangled.append("PVIMatchWalker@@");

        if (mangledReturnParameters.length())
            mangled.append(mangledReturnParameters);

        ForEachChild(i, formals)
        {
            IHqlExpression * param = formals->queryChild(i);
            ITypeInfo *paramType = param->queryType();

            bool isOut = param->hasProperty(outAtom);
            bool isConst = param->hasProperty(constAtom);

            if (isOut)
                appendRef(mangled, false);
            if (!mangleSimpleType(mangled, paramType, isConst))
                return false;
        }

        mangled.append("@Z");
        return true;
    }

protected:
    bool mangleSimpleType(StringBuffer & result, ITypeInfo * type, bool hasConst)
    {
        if (!type)
            return false;

        switch (type->getTypeCode())
        {
        case type_boolean:
            result.append("_N");
            return true;
        case type_int:
        case type_swapint:
            result.append(vs6MangledIntegers[type->isSigned() ? 1 : 0][type->getSize()-1]);
            return true;
        case type_real:
            result.append(type->getSize() == 4 ? "M" : "N");
            return true;
        case type_decimal:
            //Should really define this properly  (size, precision, ptr)
            return false;
        case type_string:
        case type_qstring:
        case type_utf8:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("I");
            appendPtr(result, hasConst).append("D");
            return true;
        case type_varstring:
            appendPtr(result, hasConst).append("D");
            return true;
        case type_data:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("I");
            appendPtr(result, hasConst).append("X");
            return true;
        case type_unicode:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("I");
            appendPtr(result, hasConst).append("G");
            return true;
        case type_varunicode:
            appendPtr(result, hasConst).append("G");
            return true;
        case type_char:
            result.append("D");
            return true;
        case type_enumerated:
            return mangleSimpleType(result, type->queryChildType(), hasConst);
        case type_pointer:
            result.append("PEB");
            return mangleSimpleType(result, type->queryChildType(), hasConst);
        case type_array:
            return false;       // QEA???
        case type_table:
        case type_groupedtable:
            result.append("I"); // size32_t
            appendPtr(result, hasConst).append("X");
            return true;
        case type_set:
            result.append("_N");    // bool
            result.append("I"); // unsigned
            appendPtr(result, hasConst).append("X");
            return true;
        case type_row:
            appendPtr(result, hasConst).append("E");
            return true;
        case type_void:
            result.append("X");
            return true;
        case type_scope:
        case type_transform:
        case type_function:
        case type_any:
        case type_packedint:
        case type_alien:
        case type_class:
        case type_date:
            //may possibly have some support in the future, but not yet...
            return false;
        }
        throwUnexpected();
    }


    bool mangleFunctionReturnType(StringBuffer & returnType, StringBuffer & params, ITypeInfo * retType)
    {
        type_t tc = retType->getTypeCode();
        bool hasConst = false;
        switch (tc)
        {
        case type_varstring:
            if (retType->getSize() == UNKNOWN_LENGTH)
            {
                appendPtr(returnType, hasConst).append("D");    // char *
            }
            else
            {
                returnType.append("X");
                appendPtr(params, hasConst).append("D");        // char *
            }
            break;
        case type_varunicode:
            if (retType->getSize() == UNKNOWN_LENGTH)
            {
                appendPtr(returnType, hasConst).append("G");    // char *
            }
            else
            {
                returnType.append("X");
                appendPtr(params, hasConst).append("G");        // char *
            }
            break;
        case type_qstring:
        case type_string:
        case type_utf8:
            returnType.append("X");
            appendString(params, retType, "D");
            break;
        case type_data:
            returnType.append("X");
            appendString(params, retType, "X");
            break;
        case type_unicode:
            returnType.append("X");
            appendString(params, retType, "G");
            break;
        case type_table:
        case type_groupedtable:
            returnType.append("X");
            appendRef(params, false).append("I");   // size32_t &
            appendRef(params, false);
            appendPtr(params, false).append("X");   // void * &
            break;
        case type_set:
            returnType.append("X");
            appendRef(params, false).append("_N");  // bool &
            appendRef(params, false).append("I");   // size32_t &
            appendRef(params, false);
            appendPtr(params, false).append("X");   // void * &
            break;
        case type_row:
            returnType.append("X");
            appendPtr(params, false).append("E");   // byte *
            break;
        default:
            return mangleSimpleType(returnType, retType, false);
        }
        return true;
    }

    StringBuffer & appendPtr(StringBuffer & s, bool hasConst) { return s.append("P").append(pointerBaseCode).append(hasConst ? "B" : "A"); }
    StringBuffer & appendRef(StringBuffer & s, bool hasConst) { return s.append("A").append(pointerBaseCode).append(hasConst ? "B" : "A"); }

    StringBuffer & appendString(StringBuffer & params, ITypeInfo * type, const char * suffix)
    {
        if (type->getSize() == UNKNOWN_LENGTH)
        {
            appendRef(params, false).append("I");   // size32_t &
            appendRef(params, false);
            appendPtr(params, false).append(suffix);    // X * &
        }
        else
            appendPtr(params, false).append(suffix);    // X *
        return params;
    }

protected:
    StringAttr pointerBaseCode;
};

//-------------------------------------------------------------------------------------------------------------------


//This code is provisional, and needs a lot more testing.  However it seems to work on my limited examples.
bool createMangledFunctionName(StringBuffer & mangled, IHqlExpression * funcdef, CompilerType compiler)
{
    switch (compiler)
    {
    case GccCppCompiler:
        {
            GccCppNameMangler mangler;
            return mangler.mangleFunctionName(mangled, funcdef);
        }
    case Vs6CppCompiler:
        {
            Vs6CppNameMangler mangler;
            return mangler.mangle(mangled, funcdef);
        }
    }
    return false;
}

bool createMangledFunctionName(StringBuffer & mangled, IHqlExpression * funcdef)
{
    return createMangledFunctionName(mangled, funcdef, DEFAULT_COMPILER);
}

//-------------------------------------------------------------------------------------------------------------------

static void trimSlash(StringBuffer & name)
{
    unsigned len = name.length();
    if (len && name.charAt(len-1) == '/')
        name.setLength(len-1);
}

void extractXmlName(StringBuffer & name, StringBuffer * itemName, StringBuffer * valueName, IHqlExpression * field, const char * defaultItemName, bool reading)
{
    IHqlExpression * xpathAttr = field->queryProperty(xpathAtom);
    if (xpathAttr)
    {
        StringBuffer tagName;
        IHqlExpression * xpath = xpathAttr->queryChild(0);
        xpath->queryValue()->getStringValue(tagName);

        unsigned lenContents = strlen(XPATH_CONTENTS_TEXT);
        unsigned lenTagName = tagName.length();
        if ((lenTagName >= lenContents) && (memcmp(tagName.str() + (lenTagName - lenContents), XPATH_CONTENTS_TEXT, lenContents) == 0))
            tagName.setLength(lenTagName - lenContents);

        //Only take the xpath if it isn't an attribute, sub element, or a filtered element.
        //we should probably think about handling attributes as a special case.
        //would probably mean two passes.
        if (!tagName.length())
            return;

        const char * text = tagName.str();
        if (reading || !strchr(text, '['))
        {
            const char * sep = strchr(text, '/');
            if (valueName && sep)
            {
                const char * sep2 = strchr(sep+1, '/');
                if (sep2)
                {
                    valueName->append(sep2+1);
                    itemName->append(sep2-(sep+1), (sep+1));
                    name.append(sep-text, text);
                    trimSlash(name);
                    return;
                }
            }

            trimSlash(tagName);
            const char * text = tagName.str();
            if (reading || !strchr(text+1, '@'))
            {
                if (itemName)
                {
                    const char * sep = strrchr(text, '/');
                    if (sep)
                    {
                        name.append(sep-text, text);
                        itemName->append(strlen(sep+1), sep+1);
                    }
                    else
                        itemName->append(tagName);
                    return;
                }
                else
                {
                    name.append(tagName);
                }
            }
        }
    }
    else
    {
        IHqlExpression * namedAttr = field->queryProperty(namedAtom);
        if (namedAttr)
            namedAttr->queryChild(0)->queryValue()->getStringValue(name);
    }


    bool useDefaultName = (name.length() == 0);
    if (useDefaultName)
    {
        StringBuffer tagName;
        tagName.append(field->queryName()).toLowerCase();
        name.append(tagName);
    }

    if (itemName && itemName->length() == 0)
    {
        if (useDefaultName)
            itemName->append(defaultItemName);
        else
        {
            itemName->append(name);
            name.clear();
        }
    }
}


void extractXmlName(SharedHqlExpr & name, OwnedHqlExpr * itemName, OwnedHqlExpr * valueName, IHqlExpression * field, const char * defaultItemName, bool reading)
{
    StringBuffer nameText, itemNameText, valueNameText;

    extractXmlName(nameText, itemName ? &itemNameText : NULL, valueName ? &valueNameText : NULL, field, defaultItemName, reading);

    if (valueNameText.length())
        valueName->setown(createConstant(constUnknownVarStringType->castFrom(valueNameText.length(), valueNameText.str())));
    if (itemNameText.length())
        itemName->setown(createConstant(constUnknownVarStringType->castFrom(itemNameText.length(), itemNameText.str())));
    if (nameText.length())
        name.setown(createConstant(constUnknownVarStringType->castFrom(nameText.length(), nameText.str())));
}


//-------------------------------------------------------------------------------------------------------------------

/*

* the xml schema is being generated
* there is a dataset with a single element (with xpaths for the element and the row)
* that element has an xpath of ''
then generate a simplified schema

*/


static ITypeInfo * containsSingleSimpleFieldBlankXPath(IHqlExpression * record)
{
    if (record->numChildren() != 1)
        return NULL;

    IHqlExpression * field = record->queryChild(0);
    if (field->getOperator() != no_field)
        return NULL;

    IHqlExpression * xpath = field->queryProperty(xpathAtom);
    if (!xpath)
        return NULL;

    StringBuffer xpathText;
    if (getStringValue(xpathText, xpath->queryChild(0)).length() != 0)
        return NULL;

    ITypeInfo * type = field->queryType();
    if (type->getTypeCode() == type_alien)
        type = queryAlienType(type)->queryLogicalType();
    return type;
}


class EclXmlSchemaBuilder
{
public:
    EclXmlSchemaBuilder(ISchemaBuilder & _builder, bool _useXPath) 
        : builder(_builder), useXPath(_useXPath)
    {
    }

    void build(IHqlExpression * record) const;

protected:
    void extractName(StringBuffer & name, StringBuffer * itemName, StringBuffer * valueName, IHqlExpression * field, const char * defaultItemName) const;

protected:
    ISchemaBuilder & builder;
    bool useXPath;
};



void EclXmlSchemaBuilder::build(IHqlExpression * record) const
{
    StringBuffer name, childName;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);

        switch (cur->getOperator())
        {
        case no_field:
            {
                ITypeInfo * type = cur->queryType();
                switch (cur->queryType()->getTypeCode())
                {
                case type_row:
                    {
                        extractName(name.clear(), NULL, NULL, cur, NULL);
                        builder.beginRecord(name);
                        build(cur->queryRecord());
                        builder.endRecord(name);
                        break;
                    }
                case type_set:
                    {
                        extractName(name.clear(), &childName.clear(), NULL, cur, "Item");
                        builder.addSetField(name, childName, *type);
                        break;
                    }
                case type_table:
                case type_groupedtable:
                    {
                        extractName(name.clear(), &childName.clear(), NULL, cur, "Row");
                        ITypeInfo * singleFieldType = (useXPath && name.length() && childName.length()) ? containsSingleSimpleFieldBlankXPath(cur->queryRecord()) : NULL;
                        if (!singleFieldType || !builder.addSingleFieldDataset(name, childName, *singleFieldType))
                        {
                            if (builder.beginDataset(name, childName))
                                build(cur->queryRecord());
                            builder.endDataset(name, childName);
                        }
                        break;
                    }
                case type_alien:
                    type = queryAlienType(type)->queryLogicalType();
                default:
                    extractName(name.clear(), NULL, NULL, cur, NULL);
                    builder.addField(name, *type);
                    break;
                }
                break;
            }
        case no_ifblock:
            builder.beginIfBlock();
            build(cur->queryChild(1));
            builder.endIfBlock();
            break;
        case no_record:
            build(cur);
            break;
        }
    }
}

void EclXmlSchemaBuilder::extractName(StringBuffer & name, StringBuffer * itemName, StringBuffer * valueName, IHqlExpression * field, const char * defaultItemName) const
{
    if (useXPath)
    {
        ::extractXmlName(name, itemName, valueName, field, defaultItemName, false);
    }
    else
    {
        name.append(field->queryName()).toLowerCase();
        if (itemName)
            itemName->append(defaultItemName);
    }
}


void getRecordXmlSchema(StringBuffer & result, IHqlExpression * record, bool useXPath)
{
    XmlSchemaBuilder xmlbuilder(false);
    EclXmlSchemaBuilder builder(xmlbuilder, useXPath);
    builder.build(record);
    xmlbuilder.getXml(result);
}

//---------------------------------------------------------------------------

static IHqlExpression * simplifyInExpr(IHqlExpression * expr)
{
    IHqlExpression * ret = querySimplifyInExpr(expr);
    if (ret)
        return ret;
    return LINK(expr);
}

IHqlExpression * querySimplifyInExpr(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_in:
    case no_notin:
        break;
    default:
        return NULL;
    }

    IHqlExpression * lhs = expr->queryChild(0);
    IHqlExpression * rhs = expr->queryChild(1);
    HqlExprArray args;
    OwnedHqlExpr ret;
    switch (rhs->getOperator())
    {
    case no_addsets:
        {
            OwnedHqlExpr newLeft = createBoolExpr(op, LINK(lhs), LINK(rhs->queryChild(0)));
            OwnedHqlExpr newRight = createBoolExpr(op, LINK(lhs), LINK(rhs->queryChild(1)));
            args.append(*simplifyInExpr(newLeft));
            args.append(*simplifyInExpr(newRight));
            ret.setown(createValue((op == no_in) ? no_or : no_and, makeBoolType(), args));
            break;
        }
    case no_if:
        {
            OwnedHqlExpr newLeft = createBoolExpr(op, LINK(lhs), LINK(rhs->queryChild(1)));
            OwnedHqlExpr newRight = createBoolExpr(op, LINK(lhs), LINK(rhs->queryChild(2)));
            args.append(*LINK(rhs->queryChild(0)));
            args.append(*simplifyInExpr(newLeft));
            args.append(*simplifyInExpr(newRight));
            ret.setown(createValue(no_if, makeBoolType(), args));
            break;
        }
    }
    if (ret)
        return expr->cloneAllAnnotations(ret);
    return NULL;
}

bool canSetBeAll(IHqlExpression * expr)
{
    if (!expr)
        return false;
    switch (expr->getOperator())
    {
    case no_createset:
    case no_list:
        return false;
        //more: no_addsets, no_if
    case no_if:
        return canSetBeAll(expr->queryChild(1)) || canSetBeAll(expr->queryChild(2));
    case no_cast:
    case no_implicitcast:
        return canSetBeAll(expr->queryChild(0));
    }
    return true;
}

extern HQL_API bool hasNonNullRecord(ITypeInfo * type)
{
    IHqlExpression * record = queryRecord(type);
    if (!record)
        return false;
    return record->numChildren() != 0;
}

extern HQL_API IHqlExpression * createSizeof(IHqlExpression * expr)
{
    return createValue(no_sizeof, LINK(sizetType), LINK(expr));
}

extern HQL_API bool allParametersHaveDefaults(IHqlExpression * function)
{
    assertex(function->isFunction());
    IHqlExpression * formals = queryFunctionParameters(function);
    IHqlExpression * defaults = queryFunctionDefaults(function);
    ForEachChild(idx, formals)
    {
        IHqlExpression * defvalue = queryDefaultValue(defaults, idx);
        if (!defvalue)
            return false;
    }
    return true;
}

extern HQL_API bool expandMissingDefaultsAsStoreds(HqlExprArray & args, IHqlExpression * function)
{
    assertex(function->isFunction());
    IHqlExpression * formals = queryFunctionParameters(function);
    IHqlExpression * defaults = queryFunctionDefaults(function);
    try
    {
        ForEachChild(idx, formals)
        {
            IHqlExpression *formal = formals->queryChild(idx);
            IHqlExpression * defvalue = queryDefaultValue(defaults, idx);
            if (defvalue)
            {
                args.append(*LINK(defvalue));
            }
            else
            {
                OwnedHqlExpr nullValue = createNullExpr(formal->queryType());
                OwnedHqlExpr storedName = createConstant(formal->queryName()->str());
                OwnedHqlExpr stored = createValue(no_stored, makeVoidType(), storedName.getClear());
                args.append(*createValue(no_colon, formal->getType(), LINK(nullValue), LINK(stored)));
            }
        }
    }
    catch (IException * e)
    {
        e->Release();
        return false;
    }

    return true;
}

//--------------------------------------------------------------------------------------------------------------------

const unsigned maxSensibleInlineElementSize = 10000;
class ConstantRowCreator
{
public:
    ConstantRowCreator(MemoryBuffer & _out) : out(_out) { expectedIndex = 0; }

    bool buildTransformRow(IHqlExpression * transform);

protected:
    bool expandAssignChildren(IHqlExpression * expr);
    bool expandAssignElement(IHqlExpression * expr);

    bool processElement(IHqlExpression * expr, IHqlExpression * parentSelector);
    bool processRecord(IHqlExpression * record, IHqlExpression * parentSelector);

    IHqlExpression * queryMatchingAssign(IHqlExpression * self, IHqlExpression * search);

protected:
    Owned<NestedHqlMapTransformer> mapper;
    MemoryBuffer & out;
    HqlExprCopyArray assigns;
    unsigned expectedIndex;
};

bool ConstantRowCreator::expandAssignChildren(IHqlExpression * expr)
{
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (!expandAssignElement(cur))
            return false;
    }
    return true;
}


bool ConstantRowCreator::expandAssignElement(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_assignall:
    case no_transform:
    case no_newtransform:
        return expandAssignChildren(expr);
    case no_assign:
        assigns.append(*expr);
        return true;
    case no_skip:
        return false;
    case no_alias_scope:
        expandAssignElement(expr->queryChild(0));
        return true;
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        return true;
    default:
        return false;
    }
}


IHqlExpression * ConstantRowCreator::queryMatchingAssign(IHqlExpression * self, IHqlExpression * search)
{
    const unsigned endIndex = expectedIndex;
    unsigned searchIndex = expectedIndex;
    do
    {
        IHqlExpression & candidate = assigns.item(searchIndex);
        IHqlExpression * lhs = candidate.queryChild(0);
        IHqlExpression * candidateField = lhs->queryChild(1);
        searchIndex++;
        if (searchIndex == assigns.ordinality())
            searchIndex = 0;
        if (candidateField == search)
        {
            expectedIndex = searchIndex;
            return &candidate;
        }
    } while (searchIndex != endIndex);
    throwUnexpected();
}


bool ConstantRowCreator::processElement(IHqlExpression * expr, IHqlExpression * parentSelector)
{
    switch (expr->getOperator())
    {
    case no_ifblock:
        {
            OwnedHqlExpr test = replaceSelector(expr->queryChild(0), querySelfReference(), parentSelector);
            OwnedHqlExpr foldedTest = mapper->transformRoot(test);
            foldedTest.setown(foldHqlExpression(foldedTest));           // can only contain references to self, so don't need to worry about other datasets in scope being messed up.

            IValue * foldedValue = foldedTest->queryValue();
            if (!foldedValue)
                return false;

            if (!foldedValue->getBoolValue())
                return true;

            return processRecord(expr->queryChild(1), parentSelector);
        }
        break;
    case no_record:
        return processRecord(expr, parentSelector);
    case no_field:
        {
            IHqlExpression * match = queryMatchingAssign(parentSelector, expr);
            if (!match || (match->getOperator() != no_assign))
                return false;

            ITypeInfo * lhsType = expr->queryType();
            size32_t lenLhs = lhsType->getStringLen();
            size32_t sizeLhs  = lhsType->getSize();
            IHqlExpression * rhs = match->queryChild(1);
            node_operator rhsOp = rhs->getOperator();

            switch (lhsType->getTypeCode())
            {
            case type_packedint:
                if (!rhs->queryValue())
                    return false;
                //MORE: Could handle this...
                return false;
            case type_set:
                if (isNullList(rhs))
                {
                    out.append(false);
                    rtlWriteSize32t(out.reserve(sizeof(size32_t)), 0);
                    return true;
                }
                if (rhsOp == no_all)
                {
                    out.append(true);
                    rtlWriteSize32t(out.reserve(sizeof(size32_t)), 0);
                    return true;
                }
                return false;
            case type_row:
                if (rhsOp == no_null)
                    return createConstantNullRow(out, queryOriginalRecord(lhsType));
                if (rhsOp == no_createrow)
                    return createConstantRow(out, rhs->queryChild(0));
                return false;
            case type_table:
            case type_groupedtable:
                if (!expr->hasProperty(countAtom) && !expr->hasProperty(sizeofAtom))
                {
                    if (rhsOp == no_null)
                    {
                        if (expr->hasProperty(_linkCounted_Atom))
                        {
                            rtlWriteSize32t(out.reserve(sizeof(size32_t)), 0);
                            memset(out.reserve(sizeof(byte * *)), 0, sizeof(byte * *));
                        }
                        else
                            rtlWriteSize32t(out.reserve(sizeof(size32_t)), 0);
                        return true;
                    }
                    //MORE: Could expand if doesn't have linkcounted, but less likely these days.
                }
                return false;
            }

            if ((lenLhs != UNKNOWN_LENGTH) && (lenLhs > maxSensibleInlineElementSize))
                return false;

            OwnedHqlExpr castRhs = ensureExprType(rhs, lhsType);
            IValue * castValue = castRhs->queryValue();
            if (!castValue)
                return false;
            
            IHqlExpression * lhs = match->queryChild(0);
            if (mapper)
                mapper->setMapping(lhs, castRhs);

            ITypeInfo * castValueType = castValue->queryType();
            size32_t lenValue = castValueType->getStringLen();
            assertex(lenLhs == UNKNOWN_LENGTH || lenLhs == lenValue);

            switch (lhsType->getTypeCode())
            {
            case type_boolean:
            case type_int:
            case type_swapint:
            case type_real:
            case type_decimal:
                {
                    void * temp = out.reserve(sizeLhs);
                    castValue->toMem(temp);
                    return true;
                }
            case type_data:
            case type_string:
                {
                    if (lenLhs == UNKNOWN_LENGTH)
                        rtlWriteInt4(out.reserve(sizeof(size32_t)), lenValue);
                    castValue->toMem(out.reserve(lenValue));
                    return true;
                }
            case type_varstring:
                {
                    //Move to else
                    if (sizeLhs == UNKNOWN_LENGTH)
                    {
                        void * target = out.reserve(lenValue+1);
                        castValue->toMem(target);
                    }
                    else
                    {
                        //Disabled for the moment to prevent the size of generated expressions getting too big.
                        if (sizeLhs > 40)
                            return false;
                        void * target = out.reserve(sizeLhs);
                        memset(target, ' ', sizeLhs);   // spaces expand better in the c++
                        castValue->toMem(target);
                    }
                    return true;
                }
            case type_unicode:
            case type_qstring:
                {
                    if (lenLhs == UNKNOWN_LENGTH)
                        rtlWriteInt4(out.reserve(sizeof(size32_t)), lenValue);
                    castValue->toMem(out.reserve(castValueType->getSize()));
                    return true;
                }
            //MORE:
            //type_varunicode
            //type_packedint
            }
            return false;
        }
    default:
        return true;
    }
}


bool ConstantRowCreator::processRecord(IHqlExpression * record, IHqlExpression * parentSelector)
{
    ForEachChild(idx, record)
    {
        if (!processElement(record->queryChild(idx), parentSelector))
            return false;
    }
    return true;
}


bool ConstantRowCreator::buildTransformRow(IHqlExpression * transform)
{
    expectedIndex = 0;
    if (!expandAssignChildren(transform))
        return false;
    //if (recordContainsIfBlock(record))
        mapper.setown(new NestedHqlMapTransformer);

    unsigned savedLength = out.length();
    OwnedHqlExpr self = getSelf(transform);
    if (processRecord(transform->queryRecord(), self))
        return true;
    out.setLength(savedLength);
    return false;
}



bool createConstantRow(MemoryBuffer & target, IHqlExpression * transform)
{
    ConstantRowCreator builder(target);
    return builder.buildTransformRow(transform);
}

IHqlExpression * createConstantRowExpr(IHqlExpression * transform)
{
    MemoryBuffer rowData;
    if (!createConstantRow(rowData, transform))
        return NULL;
    Owned<IValue> value = createDataValue(rowData.toByteArray(), rowData.length());
    return createConstant(value.getClear());
}

bool createConstantNullRow(MemoryBuffer & target, IHqlExpression * record)
{
    //MORE: More efficient to not go via a temporary transform
    OwnedHqlExpr nullTransform = createClearTransform(record);
    return createConstantRow(target, nullTransform);
}

IHqlExpression * createConstantNullRowExpr(IHqlExpression * record)
{
    //MORE: optimize
    OwnedHqlExpr nullTransform = createClearTransform(record);
    return createConstantRowExpr(nullTransform);
}

IHqlExpression * ensureOwned(IHqlExpression * expr)
{
    if (expr->isDataset())
    {
        if (hasLinkCountedModifier(expr))
            return createDataset(no_owned_ds, LINK(expr));
    }
    return LINK(expr);
}


IECLError * annotateExceptionWithLocation(IException * e, IHqlExpression * location)
{
    StringBuffer errorMsg;
    e->errorMessage(errorMsg);
    unsigned code = e->errorCode();
    return createECLError(code, errorMsg.str(), location->querySourcePath()->str(), location->getStartLine(), location->getStartColumn(), 0);
}

StringBuffer & appendLocation(StringBuffer & s, IHqlExpression * location, const char * suffix)
{
    if (location)
    {
        int line = location->getStartLine();
        int column = location->getStartColumn();
        s.append(location->querySourcePath()->str());
        if (line)
        {
            s.append("(").append(location->getStartLine());
            if (column)
                s.append(",").append(location->getStartColumn());
            s.append(")");
        }
        s.append(suffix);
    }
    return s;
}

//---------------------------------------------------------------------------------------------------------------------

static IHqlExpression * transformAttributeToQuery(IHqlExpression * expr, HqlLookupContext & ctx)
{
    if (expr->isMacro())
    {
        if (!queryLegacyEclSemantics())
            return NULL;
        //Only expand macros if legacy semantics enabled
        IHqlExpression * macroBodyExpr;
        if (expr->getOperator() == no_funcdef)
        {
            if (expr->queryChild(1)->numChildren() != 0)
                return NULL;
            macroBodyExpr = expr->queryChild(0);
        }
        else
            macroBodyExpr = expr;

        IFileContents * macroContents = static_cast<IFileContents *>(macroBodyExpr->queryUnknownExtra());
        Owned<IHqlScope> scope = createPrivateScope();
        return parseQuery(scope, macroContents, ctx, NULL, true);
    }

    if (expr->isFunction())
    {
        //If a scope with parameters then assume we are building a library.
        if (expr->isScope())
            return LINK(expr);

        HqlExprArray actuals;
        if (!allParametersHaveDefaults(expr))
        {
            if (!expandMissingDefaultsAsStoreds(actuals, expr))
            {
                //For each parameter that doesn't have a default, create a stored variable of the appropriate type
                //with a null value as the default value, and use that.
                const char * name = expr->queryName()->str();
                StringBuffer msg;
                msg.appendf("Definition %s() does not supply default values for all parameters", name ? name : "");
                ctx.errs->reportError(HQLERR_CannotSubmitFunction, msg.str(), NULL, 1, 0, 0);
                return NULL;
            }
        }

        return createBoundFunction(ctx.errs, expr, actuals, ctx.functionCache, ctx.queryExpandCallsWhenBound());
    }

    if (expr->isScope())
    {
        IHqlScope * scope = expr->queryScope();
        OwnedHqlExpr main = scope->lookupSymbol(createAtom("main"), LSFpublic, ctx);
        if (main)
            return main.getClear();

        StringBuffer msg;
        const char * name = scope->queryFullName();
        msg.appendf("Module %s does not EXPORT an attribute main()", name ? name : "");
        ctx.errs->reportError(HQLERR_CannotSubmitModule, msg.str(), NULL, 1, 0, 0);
        return NULL;
    }

    return LINK(expr);
}

IHqlExpression * convertAttributeToQuery(IHqlExpression * expr, HqlLookupContext & ctx)
{
    OwnedHqlExpr query = LINK(expr);
    loop
    {
        OwnedHqlExpr transformed = transformAttributeToQuery(query, ctx);
        if (!transformed || transformed == query)
            return transformed.getClear();
        query.set(transformed);
    }
}

bool isSetWithUnknownElementSize(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_set:
    case type_array:
        return isUnknownSize(type->queryChildType());
    }
    return false;
}

IHqlExpression * replaceParameters(IHqlExpression * body, IHqlExpression * oldParams, IHqlExpression * newParams)
{
    HqlMapTransformer simpleTransformer;
    ForEachChild(i, oldParams)
    {
        IHqlExpression * from = oldParams->queryChild(i);
        IHqlExpression * to = newParams->queryChild(i);
        simpleTransformer.setMapping(from, to);
    }

    return simpleTransformer.transformRoot(body);
}
