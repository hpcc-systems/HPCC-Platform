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
#include "rtldynfield.hpp"

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
#include "hqlusage.hpp"
#include "hqlthql.hpp"
#include "deffield.hpp"
#include "workunit.hpp"
#include "jencrypt.hpp"
#include "hqlattr.hpp"
#include "hqlerror.hpp"
#include "hqlexpr.ipp"
#include "hqlrepository.hpp"

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
static IHqlExpression * cacheProjectedAttr;
static IHqlExpression * cacheReferenceAttr;
static IHqlExpression * cacheStreamedAttr;
static IHqlExpression * cacheUnadornedAttr;
static IHqlExpression * nlpParsePsuedoTable;
static IHqlExpression * xmlParsePsuedoTable;
static IHqlExpression * cachedQuotedNullExpr;
static IHqlExpression * cachedGlobalSequenceNumber;
static IHqlExpression * cachedLocalSequenceNumber;
static IHqlExpression * cachedStoredSequenceNumber;
static IHqlExpression * cachedOmittedValueExpr;

static void initBoolAttr(IAtom * name, IHqlExpression * x[2])
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
    cacheAlignedAttr = createAttribute(_propAligned_Atom);
    cacheEmbeddedAttr = createAttribute(embeddedAtom);
    cacheInlineAttr = createAttribute(inlineAtom);
    cacheLinkCountedAttr = createAttribute(_linkCounted_Atom);
    cacheProjectedAttr = createAttribute(projectedAtom);
    cacheReferenceAttr = createAttribute(referenceAtom);
    cacheStreamedAttr = createAttribute(streamedAtom);
    cacheUnadornedAttr = createAttribute(_propUnadorned_Atom);
    nlpParsePsuedoTable = createDataset(no_pseudods, createRecord()->closeExpr(), createAttribute(_nlpParse_Atom));
    xmlParsePsuedoTable = createDataset(no_pseudods, createRecord()->closeExpr(), createAttribute(_xmlParse_Atom));
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
    cacheProjectedAttr->Release();
    cacheReferenceAttr->Release();
    cacheStreamedAttr->Release();
    cacheUnadornedAttr->Release();
    xmlParsePsuedoTable->Release();
    nlpParsePsuedoTable->Release();
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

inline IIdAtom * createMangledName(IHqlExpression * module, IHqlExpression * child)
{
    StringBuffer mangledName;
    mangledName.append(module->queryName()).append(".").append(child->queryName());

    return createIdAtom(mangledName.str());
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
        return createExprAttribute(_propSize_Atom, LINK(sizeExpr), LINK(sizeExpr), LINK(sizeExpr));
    }

    CriticalBlock block(*sizetCacheCs);     // reuse the critical section
    IHqlExpression * match = fixedAttrSizeCache[size];
    if (!match)
    {
        OwnedHqlExpr sizeExpr = getSizetConstant(size);
        match = fixedAttrSizeCache[size] = createExprAttribute(_propSize_Atom, LINK(sizeExpr), LINK(sizeExpr), LINK(sizeExpr));
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
extern HQL_API IHqlExpression * queryProjectedAttr()
{
    return cacheProjectedAttr;
}
extern HQL_API IHqlExpression * getLinkCountedAttr()
{
    return LINK(cacheLinkCountedAttr);
}
extern HQL_API IHqlExpression * getProjectedAttr()
{
    return LINK(cacheProjectedAttr);
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

extern HQL_API IHqlExpression * queryNlpParsePseudoTable()
{
    return nlpParsePsuedoTable;
}

extern HQL_API IHqlExpression * queryXmlParsePseudoTable()
{
    return xmlParsePsuedoTable;
}

IHqlExpression * getGlobalSequenceNumber()      { return LINK(cachedGlobalSequenceNumber); }
IHqlExpression * getLocalSequenceNumber()       { return LINK(cachedLocalSequenceNumber); }
IHqlExpression * getStoredSequenceNumber()      { return LINK(cachedStoredSequenceNumber); }
IHqlExpression * getOnceSequenceNumber()        { return createConstant(signedType->castFrom(true, (__int64)ResultSequenceOnce)); }

//Does the record (or a base record) contain an ifblock?  This could be tracking using a flag if it started being called a lot.
bool recordContainsIfBlock(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_ifblock:
            return true;
        case no_record:
            if (recordContainsIfBlock(cur))
                return true;
            break;
        }
    }
    return false;
}

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
    for (;;)
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

IHqlExpression * queryStripCasts(IHqlExpression * expr)
{
    while (isCast(expr))
        expr = expr->queryChild(0);
    return expr;
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

IHqlExpression * queryFirstField(IHqlExpression * record)
{
    unsigned idx = 0;
    return queryNextRecordField(record, idx);
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
                if (cur->hasAttribute(blobAtom))
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
        IHqlExpression * attr = cur->queryAttribute(virtualAtom);
        if (attr)
            return cur;
    }
    return NULL;
}


IHqlExpression * queryLastNonAttribute(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    while (max--)
    {
        IHqlExpression * cur = expr->queryChild(max);
        if (!cur->isAttribute())
            return cur;
    }
    return NULL;
}

extern HQL_API unsigned numNonAttributes(IHqlExpression * expr)
{
    unsigned max = expr->numChildren();
    while (max--)
    {
        IHqlExpression * cur = expr->queryChild(max);
        if (!cur->isAttribute())
            return max+1;
    }
    return 0;
}

void expandRecord(HqlExprArray & selects, IHqlExpression * selector, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                expandRecord(selects, selector, expr->queryChild(i));
            break;
        }
    case no_field:
        {
            OwnedHqlExpr subSelector = createSelectExpr(LINK(selector), LINK(expr));
            if (expr->queryRecord() && !expr->isDataset() && !expr->isDictionary())
                expandRecord(selects, subSelector, expr->queryRecord());
            else
            {
                if (selects.find(*subSelector) == NotFound)
                    selects.append(*subSelector.getClear());
            }
            break;
        }
    case no_ifblock:
        expandRecord(selects, selector, expr->queryChild(1));
        break;
    }
}

//---------------------------------------------------------------------------------------------------------------------

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


//---------------------------------------------------------------------------------------------------------------------

bool isFileOutput(IHqlExpression * expr)
{
    return (expr->getOperator() == no_output) && (queryRealChild(expr, 1) != NULL);
}

bool isWorkunitOutput(IHqlExpression * expr)
{
    return (expr->getOperator() == no_output) && (queryRealChild(expr, 1) == NULL);
}

bool isCommonSubstringRange(IHqlExpression * expr)
{
    if (expr->getOperator() != no_substring)
        return false;
    IHqlExpression * range = expr->queryChild(1);
    return (range->getOperator() == no_rangecommon);
}

IHqlExpression * removeCommonSubstringRange(IHqlExpression * expr)
{
    if (expr->getOperator() != no_substring)
        return LINK(expr);
    IHqlExpression * range = expr->queryChild(1);
    if (range->getOperator() != no_rangecommon)
        return LINK(expr);
    IHqlExpression * value = expr->queryChild(0);
    IHqlExpression * from = range->queryChild(0);
    if (matchesConstantValue(from, 1))
        return LINK(value);
    OwnedHqlExpr newRange = createValue(no_rangefrom, makeNullType(), LINK(from));
    return replaceChild(expr, 1, newRange);
}

void AtmostLimit::extractAtmostArgs(IHqlExpression * atmost)
{
    limit.setown(getSizetConstant(0));
    if (atmost)
    {
        unsigned cur = 0;
        IHqlExpression * arg = atmost->queryChild(0);
        if (arg && arg->isBoolean())
        {
            required.set(arg);
            arg = atmost->queryChild(++cur);
        }
        if (arg && arg->queryType()->getTypeCode() == type_sortlist)
        {
            unwindChildren(optional, arg);
            arg = atmost->queryChild(++cur);
        }
        if (arg)
            limit.set(arg);
    }
}


static bool matchesAtmostCondition(IHqlExpression * cond, HqlExprArray & atConds, unsigned & numMatched)
{
    if (atConds.find(*cond) != NotFound)
    {
        numMatched++;
        return true;
    }
    if (cond->getOperator() != no_assertkeyed)
        return false;
    unsigned savedMatched = numMatched;
    HqlExprArray conds;
    cond->queryChild(0)->unwindList(conds, no_and);
    ForEachItemIn(i, conds)
    {
        if (!matchesAtmostCondition(&conds.item(i), atConds, numMatched))
        {
            numMatched = savedMatched;
            return false;
        }
    }
    return true;
}

static bool doSplitFuzzyCondition(IHqlExpression * condition, IHqlExpression * atmostCond, SharedHqlExpr & fuzzy, SharedHqlExpr & hard)
{
    if (atmostCond)
    {
        //If join condition has evaluated to a constant then allow any atmost condition.
        if (!condition->isConstant())
        {
            HqlExprArray conds, atConds;
            condition->unwindList(conds, no_and);
            atmostCond->unwindList(atConds, no_and);
            unsigned numAtmostMatched = 0;
            ForEachItemIn(i, conds)
            {
                IHqlExpression & cur = conds.item(i);
                if (matchesAtmostCondition(&cur, atConds, numAtmostMatched))
                    extendConditionOwn(hard, no_and, LINK(&cur));
                else
                    extendConditionOwn(fuzzy, no_and, LINK(&cur));
            }
            if (atConds.ordinality() != numAtmostMatched)
            {
                hard.clear();
                fuzzy.clear();
                return false;
            }
        }
    }
    else
        hard.set(condition);
    return true;
}

void splitFuzzyCondition(IHqlExpression * condition, IHqlExpression * atmostCond, SharedHqlExpr & fuzzy, SharedHqlExpr & hard)
{
    if (!doSplitFuzzyCondition(condition, atmostCond, fuzzy, hard))
    {
        //Ugly, but sometimes the condition only matches after it has been constant folded.
        //And this function can be called from the normalizer before the expression tree is folded.
        OwnedHqlExpr foldedCond = foldHqlExpression(condition);
        OwnedHqlExpr foldedAtmost = foldHqlExpression(atmostCond);
        if (!doSplitFuzzyCondition(foldedCond, foldedAtmost, fuzzy, hard))
        {
            StringBuffer s;
            getExprECL(atmostCond, s);
            throwError1(HQLERR_AtmostFailMatchCondition, s.str());
        }
    }
}

//---------------------------------------------------------------------------------------------------------------------



class JoinOrderSpotter
{
public:
    JoinOrderSpotter(IHqlExpression * _leftDs, IHqlExpression * _rightDs, IHqlExpression * seq, JoinSortInfo & _joinOrder) : joinOrder(_joinOrder)
    {
        if (_leftDs)
            left.setown(createSelector(no_left, _leftDs, seq));
        if (_rightDs)
            right.setown(createSelector(no_right, _rightDs, seq));
    }

    IHqlExpression * doFindJoinSortOrders(IHqlExpression * condition, bool allowSlidingMatch, HqlExprCopyArray & matched);
    bool doProcessOptional(IHqlExpression * condition);
    void findImplicitBetween(IHqlExpression * condition, HqlExprArray & slidingMatches, HqlExprCopyArray & matched, HqlExprCopyArray & pending);

protected:
    IHqlExpression * traverseStripSelect(IHqlExpression * expr, node_operator & kind);
    IHqlExpression * cachedTraverseStripSelect(IHqlExpression * expr, node_operator & kind);
    IHqlExpression * doTraverseStripSelect(IHqlExpression * expr, node_operator & kind);
    void unwindSelectorRecord(HqlExprArray & target, IHqlExpression * selector, IHqlExpression * record);

protected:
    OwnedHqlExpr left;
    OwnedHqlExpr right;
    JoinSortInfo & joinOrder;
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
        if (curKind == no_select || expr->hasAttribute(newAtom))
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

IHqlExpression * JoinOrderSpotter::doFindJoinSortOrders(IHqlExpression * condition, bool allowSlidingMatch, HqlExprCopyArray & matched)
{
    IHqlExpression *l = condition->queryChild(0);
    IHqlExpression *r = condition->queryChild(1);

    switch(condition->getOperator())
    {
    case no_and:
        {
            IHqlExpression *lmatch = doFindJoinSortOrders(l, allowSlidingMatch, matched);
            IHqlExpression *rmatch = doFindJoinSortOrders(r, allowSlidingMatch, matched);
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
                joinOrder.leftReq.append(*leftStrip.getClear());
                joinOrder.rightReq.append(*rightStrip.getClear());
                return NULL;
            }
            if ((leftSelectKind == no_right) && (rightSelectKind == no_left))
            {
                joinOrder.leftReq.append(*rightStrip.getClear());
                joinOrder.rightReq.append(*leftStrip.getClear());
                return NULL;
            }
            if (((l == left) && (r == right)) || ((l == right) && (r == left)))
            {
                unwindSelectorRecord(joinOrder.leftReq, queryActiveTableSelector(), left->queryRecord());
                unwindSelectorRecord(joinOrder.rightReq, queryActiveTableSelector(), right->queryRecord());
                return NULL;
            }
        }
        return LINK(condition);
    case no_between:
        if (allowSlidingMatch)
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
                    joinOrder.slidingMatches.append(*createValue(no_between, makeBoolType(), LINK(leftStrip), LINK(lowerStrip), LINK(upperStrip), createExprAttribute(commonAtom, LINK(common))));
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

    case no_assertkeyed:
    {
        OwnedHqlExpr ret = doFindJoinSortOrders(l, allowSlidingMatch, matched);
        if (ret)
            return createValue(no_assertkeyed, condition->getType(), ret.getClear());
        return NULL;
    }
    case no_assertwild:
        return NULL;

    default:
        return LINK(condition);
    }
}

bool JoinOrderSpotter::doProcessOptional(IHqlExpression * condition)
{
    switch(condition->getOperator())
    {
    //MORE We could support no_and by adding a list to both sides, but I can't see it being worth the effort.
    case no_constant:
        //remove silly "and true" conditions
        if (condition->queryValue()->getBoolValue())
            return true;
        return false;
    case no_eq:
        {
            IHqlExpression *l = condition->queryChild(0);
            IHqlExpression *r = condition->queryChild(1);
            node_operator leftSelectKind = no_none;
            node_operator rightSelectKind = no_none;
            OwnedHqlExpr leftStrip = traverseStripSelect(l, leftSelectKind);
            OwnedHqlExpr rightStrip = traverseStripSelect(r, rightSelectKind);

            if ((leftSelectKind == no_left) && (rightSelectKind == no_right))
            {
                joinOrder.leftOpt.append(*leftStrip.getClear());
                joinOrder.rightOpt.append(*rightStrip.getClear());
                return true;
            }
            if ((leftSelectKind == no_right) && (rightSelectKind == no_left))
            {
                joinOrder.leftOpt.append(*rightStrip.getClear());
                joinOrder.rightOpt.append(*leftStrip.getClear());
                return true;
            }
            if (((l == left) && (r == right)) || ((l == right) && (r == left)))
            {
                unwindSelectorRecord(joinOrder.leftOpt, queryActiveTableSelector(), left->queryRecord());
                unwindSelectorRecord(joinOrder.rightOpt, queryActiveTableSelector(), right->queryRecord());
                return true;
            }
        }
        return false;
    default:
        return false;
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

JoinSortInfo::JoinSortInfo(IHqlExpression * expr)
: lhs(expr->queryChild(0)), rhs(queryJoinRhs(expr)), cond(expr->queryChild(2)), seq(querySelSeq(expr)), atmostAttr(expr->queryAttribute(atmostAtom))
{
    init();
}
        
JoinSortInfo::JoinSortInfo(IHqlExpression * _condition, IHqlExpression * _leftDs, IHqlExpression * _rightDs, IHqlExpression * _seq, IHqlExpression * _atmost)
: lhs(_leftDs), rhs(_rightDs), cond(_condition), seq(_seq), atmostAttr(_atmost)
{
    init();
}

void JoinSortInfo::init()
{
    conditionAllEqualities = false;
    hasRightNonEquality = false;
    if (lhs)
        left.setown(createSelector(no_left, lhs, seq));
    if (rhs)
        right.setown(createSelector(no_right, rhs, seq));
}

void JoinSortInfo::doFindJoinSortOrders(IHqlExpression * condition, bool allowSlidingMatch)
{
    JoinOrderSpotter spotter(lhs, rhs, seq, *this);
    HqlExprCopyArray matched;
    if (allowSlidingMatch)
    {
        //First spot any implicit betweens using x >= a and x <= b.  Do it first so that the second pass doesn't
        //reorder the join condition (this still reorders it slightly by moving the implicit betweens before explicit)
        HqlExprCopyArray pending;
        spotter.findImplicitBetween(condition, slidingMatches, matched, pending);
    }
    
    extraMatch.setown(spotter.doFindJoinSortOrders(condition, allowSlidingMatch, matched));
    conditionAllEqualities = (extraMatch == NULL);
    if (extraMatch)
    {
        if (extraMatch->usesSelector(right))
            hasRightNonEquality = true;
    }

    //Support for legacy syntax where x[n..*] was present in join and atmost condition
    //Ensure they are tagged as optional join fields.
    ForEachItemInRev(i, leftReq)
    {
        IHqlExpression & left = leftReq.item(i);
        IHqlExpression & right = rightReq.item(i);
        if (isCommonSubstringRange(&left))
        {
            if (isCommonSubstringRange(&right))
            {
                if (leftOpt.ordinality())
                    throwError(HQLERR_AtmostLegacyMismatch);
                leftOpt.append(OLINK(left));
                leftReq.remove(i);
                rightOpt.append(OLINK(right));
                rightReq.remove(i);
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

    if (!hasOptionalEqualities() && allowSlidingMatch)
    {
        ForEachItemIn(i, slidingMatches)
        {
            IHqlExpression & cur = slidingMatches.item(i);
            leftReq.append(*LINK(cur.queryChild(0)));
            rightReq.append(*LINK(cur.queryChild(3)->queryChild(0)));
        }
    }

}

void JoinSortInfo::findJoinSortOrders(bool allowSlidingMatch)
{
    atmost.extractAtmostArgs(atmostAttr);
    if (atmost.optional.ordinality())
    {
        JoinOrderSpotter spotter(lhs, rhs, seq, *this);
        ForEachItemIn(i, atmost.optional)
        {
            if (!spotter.doProcessOptional(&atmost.optional.item(i)))
                throwError(HQLERR_AtmostCannotImplementOpt);
        }
    }

    OwnedHqlExpr fuzzy, hard;
    splitFuzzyCondition(cond, atmost.required, fuzzy, hard);
    if (hard)
        doFindJoinSortOrders(hard, allowSlidingMatch);

    extraMatch.setown(extendConditionOwn(no_and, extraMatch.getClear(), fuzzy.getClear()));
}

IHqlExpression * JoinSortInfo::getContiguousJoinCondition(unsigned numRhsFields)
{
    //Ensure that numRhsFields from RIGHT are joined, and if so return the join condition
    IHqlExpression * rightRecord = rhs->queryRecord();
    HqlExprCopyArray leftMatches, rightMatches;
    RecordSelectIterator iter(rightRecord, queryActiveTableSelector());
    unsigned numMatched = 0;
    ForEach(iter)
    {
        unsigned match = rightReq.find(*iter.query());
        if (match == NotFound)
            return NULL;
        leftMatches.append(leftReq.item(match));
        rightMatches.append(rightReq.item(match));
        if (++numMatched == numRhsFields)
        {
            HqlExprAttr cond;
            ForEachItemIn(i, leftMatches)
            {
                OwnedHqlExpr eq = createBoolExpr(no_eq,
                                                replaceSelector(&leftMatches.item(i), queryActiveTableSelector(), left),
                                                replaceSelector(&rightMatches.item(i), queryActiveTableSelector(), right));
                extendConditionOwn(cond, no_and, eq.getClear());
            }
            return cond.getClear();
        }
    }
    return NULL;
}

static void appendOptElements(HqlExprArray & target, const HqlExprArray & src)
{
    ForEachItemIn(i, src)
    {
        IHqlExpression & cur = src.item(i);
        //Strip the substring syntax when adding the optional compares to the sort list
        target.append(*removeCommonSubstringRange(&cur));
    }
}

void JoinSortInfo::initSorts()
{
    if (!leftSorts.ordinality())
    {
        appendArray(leftSorts, leftReq);
        appendOptElements(leftSorts, leftOpt);
        appendArray(rightSorts, rightReq);
        appendOptElements(rightSorts, rightOpt);
    }
}

static bool isSameFieldSelected(IHqlExpression * leftExpr, IHqlExpression * rightExpr, IHqlExpression * left, IHqlExpression * right)
{
    if ((leftExpr->getOperator() != no_select) || (rightExpr->getOperator() != no_select))
        return false;
    if (leftExpr->queryChild(1) != rightExpr->queryChild(1))
        return false;
 
    IHqlExpression * leftSelector = leftExpr->queryChild(0);
    IHqlExpression * rightSelector = rightExpr->queryChild(0);
    if (leftSelector == left || rightSelector == right)
        return (leftSelector == left) && (rightSelector == right);
    if (leftSelector == right || rightSelector == left)
        return (leftSelector == right) && (rightSelector == left);
    return isSameFieldSelected(leftSelector, rightSelector, left, right);
}


static bool hasNeverMatchCompare(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right)
{
    switch (expr->getOperator())
    {
    case no_and:
        return hasNeverMatchCompare(expr->queryChild(0), left, right) ||
               hasNeverMatchCompare(expr->queryChild(1), left, right);
    case no_ne:
    case no_gt:
    case no_lt:
        return isSameFieldSelected(expr->queryChild(0), expr->queryChild(1), left, right);
    default:
        return false;
    }
}

bool JoinSortInfo::neverMatchSelf() const
{
    if (!extraMatch)
        return false;
    if (!recordTypesMatch(lhs, rhs))
        return false;

    return hasNeverMatchCompare(extraMatch, left, right);
}


extern HQL_API bool joinHasRightOnlyHardMatch(IHqlExpression * expr, bool allowSlidingMatch)
{
    JoinSortInfo joinInfo(expr);
    joinInfo.findJoinSortOrders(false);
    return joinInfo.hasHardRightNonEquality();
}

IHqlExpression * createImpureOwn(IHqlExpression * expr)
{
    return createValue(no_impure, expr->getType(), expr);
}

IHqlExpression * getNormalizedFilename(IHqlExpression * filename)
{
    NullErrorReceiver errorProcessor;
    OwnedHqlExpr folded = foldHqlExpression(errorProcessor, filename, NULL, HFOloseannotations);
    return normalizeFilenameExpr(folded);
}

bool canBeSlidingJoin(IHqlExpression * expr)
{
    if (expr->hasAttribute(hashAtom) || expr->hasAttribute(lookupAtom) || expr->hasAttribute(smartAtom)|| expr->hasAttribute(allAtom))
        return false;
    if (expr->hasAttribute(rightouterAtom) || expr->hasAttribute(fullouterAtom) ||
        expr->hasAttribute(leftonlyAtom) || expr->hasAttribute(rightonlyAtom) || expr->hasAttribute(fullonlyAtom))
        return false;
    if (expr->hasAttribute(atmostAtom))
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
    keepBest = false;
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
                IAtom * name = cur->queryName();
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
                else if (name == bestAtom)
                    keepBest = true;
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
    if (childIndex == 0)
    {
        args.append(*LINK(newChild));
        unwindChildren(args, expr, 1);
    }
    else
    {
        unwindChildren(args, expr);
        args.replace(*LINK(newChild), childIndex);
    }
    return expr->clone(args);
}


IHqlExpression * createIf(IHqlExpression * cond, IHqlExpression * left, IHqlExpression * right)
{
    assertex(right);
    if (left->isDataset() || right->isDataset())
        return createDataset(no_if, cond, createComma(left, right));

    if (left->isDictionary() || right->isDictionary())
        return createDictionary(no_if, cond, createComma(left, right));

    if (left->isDatarow() || right->isDatarow())
        return createRow(no_if, cond, createComma(left, right));

    ITypeInfo * leftType = left->queryType();
    ITypeInfo * rightType = right->queryType();
    Owned<ITypeInfo> type = ::getPromotedECLType(leftType, rightType);
    if ((isStringType(type) || isUnicodeType(type)) && (leftType->getStringLen() != rightType->getStringLen()))
        type.setown(getStretchedType(UNKNOWN_LENGTH, type));

    return createValue(no_if, type.getClear(), cond, left, right);
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
                OwnedHqlExpr matched = scope->lookupSymbol(field->queryId());
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
    if (!(getChildDatasetType(expr) & childdataset_hasdataset))
    {
        HqlExprArray args;
        unwindChildren(args, expr);
        args.replace(*LINK(newChild), whichChild);
        return expr->clone(args);
    }

    IHqlExpression * oldChild = expr->queryChild(whichChild);
    HqlMapSelectorTransformer mapper(oldChild, newChild);
    HqlExprArray args;
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if (i == whichChild)
            args.append(*LINK(newChild));
        else
            args.append(*mapper.transformRoot(cur));
    }

    return expr->clone(args);
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
    SearchHintVisitor(IAtom * _name) : name(_name) {}

    virtual IHqlExpression * visit(IHqlExpression * hint) 
    {
        return hint->queryAttribute(name);
    }

    IAtom * name;
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
    for (;;)
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

IHqlExpression * queryHint(IHqlExpression * expr, IAtom * name)
{
    SearchHintVisitor visitor(name);
    return walkHints(expr, visitor);
}

void gatherHints(HqlExprCopyArray & target, IHqlExpression * expr)
{
    GatherHintVisitor visitor(target);
    walkHints(expr, visitor);
}

IHqlExpression * queryHintChild(IHqlExpression * expr, IAtom * name, unsigned idx)
{
    IHqlExpression * match = queryHint(expr, name);
    if (match)
        return match->queryChild(idx);
    return NULL;
}

void unwindHintAttrs(HqlExprArray & args, IHqlExpression * expr)
{
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        if ((cur->queryName() == hintAtom) && cur->isAttribute())
            args.append(*LINK(cur));
    }
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
    case no_chooseds:
    case no_choose:
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
    case no_allnodes:
    case no_thisnode:
    case no_keydiff:
    case no_keypatch:
    case no_datasetfromdictionary:
        return 0;
    case no_setresult:
        if (expr->queryChild(0)->isAction())
            return 1;
        return 0;
    case no_compound_selectnew:
    case no_libraryselect:
    case no_definesideeffect:
        return 1;
    case no_libraryscopeinstance:
        {
            //It would be very nice to be able to cache this, but because the arguments are associated with the
            //no_funcdef instead of the no_libraryscope it is a bit tricky.  It could be optimized onto the 
            //no_libraryscopeinstance I guess.
            IHqlExpression * libraryFuncDef = expr->queryDefinition();
            IHqlExpression * library = libraryFuncDef->queryChild(0);
            if (library->hasAttribute(_noStreaming_Atom))
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
    case no_projectrow:
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
        if (isKeyedJoin(expr) && !expr->hasAttribute(_complexKeyed_Atom))
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
    case no_orderedactionlist:
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
        return expr->hasAttribute(_distributed_Atom);
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
    case no_datasetfromdictionary:
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
    case no_datasetfromdictionary:
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
    case no_orderedactionlist:
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
    case no_definesideeffect:
    //case no_callsideeffect:       //??
        return true;
    case no_soapcall:
    case no_newsoapcall:
    case no_if:
    case no_null:
    case no_choose:
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
    case no_choose:
    case no_chooseds:
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

//This function gathers information about the number of fields etc. in a record.
//MORE: Should this calculate numFields, numExpandedFields, numDatasetFields all in a single function and
//then cache it as a property of a no_record?   Only if it becomes a significant bottleneck.
void gatherRecordStats(HqlRecordStats & stats, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                gatherRecordStats(stats, expr->queryChild(i));
            break;
        }
    case no_ifblock:
        return gatherRecordStats(stats, expr->queryChild(1));
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            switch (type->getTypeCode())
            {
            case type_table:
            case type_groupedtable:
                stats.fields++;
                stats.unknownSizeFields++;
                break;
            case type_dictionary:
                stats.fields++;
                stats.unknownSizeFields++;
                break;
            case type_row:
                gatherRecordStats(stats, expr->queryRecord());
                break;
            default:
                stats.fields++;
                //HPCC-17606 add: if (type->getSize() == UNKNOWN_LENGTH) stats.unknownSizeFields++;
                break;
            }
            break;
        }
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        break;
    default:
        UNIMPLEMENTED;
    }
}

unsigned getVarSizeFieldCount(IHqlExpression * expr, bool expandRows)
{
    //MORE: Is it worth caching the results of these functions as attributes?
    switch (expr->getOperator())
    {
    case no_record:
        {
            unsigned count = 0;
            ForEachChild(i, expr)
                count += getVarSizeFieldCount(expr->queryChild(i), expandRows);
            return count;
        }
    case no_ifblock:
        return getVarSizeFieldCount(expr->queryChild(1), expandRows);
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            if (expandRows)
            {
                if (type->getTypeCode() == type_row)
                    return getVarSizeFieldCount(expr->queryRecord(), expandRows);
            }
            if (isArrayRowset(type))
                return 0;
            return isUnknownSize(type) ? 1 : 0;
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

void getSimpleFields(HqlExprArray &out, IHqlExpression *record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_expr:
            break;
        case no_field:
            switch (cur->queryType()->getTypeCode())
            {
            case type_record:
            case type_row:
                {
                    IHqlExpression *nested = cur->queryRecord();
                    if (nested)
                        getSimpleFields(out, nested);
                    break;
                }
            case type_table:
            case type_groupedtable:
            case type_alien:
            case type_any:
            case type_dictionary:
                throwUnexpected();
            default:
                out.append(*LINK(cur));
                break;
            }
            break;
        case no_record:
            getSimpleFields(out, cur);
            break;
        default:
            throwUnexpected();
        }
    }
}

unsigned isSimpleRecord(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_expr:
            break;
        case no_field:
            switch (cur->queryType()->getTypeCode())
            {
            case type_record:
            case type_row:
                {
                    IHqlExpression *nested = cur->queryRecord();
                    if (nested && !isSimpleRecord(nested))
                        return false;
                    break;
                }
            case type_table:
            case type_groupedtable:
            case type_alien:
            case type_any:
            case type_dictionary:
                return false;
            }
            break;
        case no_record:
            if (!isSimpleRecord(cur))
                return false;
            break;
        default:
            return false;
        }
    }
    return record->numChildren()>0;
}

bool isTrivialSelectN(IHqlExpression * expr)
{
    if (expr->getOperator() == no_index || expr->getOperator() == no_selectnth)
    {
        IHqlExpression * index = expr->queryChild(1);
        if (matchesConstantValue(index, 1))
            return hasSingleRow(expr->queryChild(0));
    }
    return false;
}

IHqlExpression * queryAttributeChild(IHqlExpression * expr, IAtom * name, unsigned idx)
{
    IHqlExpression * match = expr->queryAttribute(name);
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
        return (int)getIntValue(queryAttributeChild(set, sequenceAtom, 0), 0);
    }
    return 0;
}


IHqlExpression * querySequence(IHqlExpression * expr)
{
    IHqlExpression * seq = expr->queryAttribute(sequenceAtom);
    if (seq)
        return seq->queryChild(0);
    if (expr->queryValue())
        return expr;
    return NULL;
}

IHqlExpression * queryResultName(IHqlExpression * expr)
{
    IHqlExpression * name = expr->queryAttribute(namedAtom);
    if (name)
        return name->queryChild(0);
    return NULL;
}


bool remainingChildrenMatch(IHqlExpression * left, IHqlExpression * right, unsigned first)
{
    if (left->numChildren() != right->numChildren())
        return false;
    ForEachChildFrom(i, left, first)
    {
        if (left->queryChild(i) != right->queryChild(i))
            return false;
    }
    return true;
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
    if (isGrouped(child) && !expr->hasAttribute(groupedAtom))
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
        if (!expr->hasAttribute(fewAtom) || first)
            return NULL;
    }

    //choosen(sort(x,a,local),n) -> do the topn local, but need to reapply the global choosen
    if (expr->hasAttribute(localAtom))
    {
        if (!child->hasAttribute(localAtom))
            return NULL;
    }
    else
    {
        if (child->hasAttribute(localAtom))
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

void DependenciesUsed::addResultRead(IHqlExpression * wuid, IHqlExpression * seq, IHqlExpression * name, bool isGraphResult)
{
    if (!isGraphResult)
        if (!seq || !seq->queryValue())
            return;         //Can be called in parser when no sequence has been allocated
    OwnedHqlExpr result = createExprAttribute(resultAtom, LINK(seq), LINK(name), LINK(wuid));
    if (resultsWritten.find(*result) == NotFound)
        appendUniqueExpr(resultsRead, LINK(result));
}

void DependenciesUsed::addResultWrite(IHqlExpression * seq, IHqlExpression * name, bool isGraphResult)
{
    if (!isGraphResult)
        if (!seq || !seq->queryValue())
            return;         //Can be called in parser when no sequence has been allocated
    OwnedHqlExpr result = createExprAttribute(resultAtom, LINK(seq), LINK(name));
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
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, nameAtom, 0);
            IHqlExpression * wuid = expr->queryAttribute(wuidAtom);
            addResultRead(wuid, sequence, name, false);
        }
        break;
    case no_getgraphresult:
        if (flags & GatherGraphResultRead)
            addResultRead(NULL, expr->queryChild(1), expr->queryChild(2), true);
        break;
    case no_setgraphresult:
        if (flags & GatherGraphResultWrite)
            addResultWrite(expr->queryChild(1), expr->queryChild(2), true);
        break;
    case no_getresult:
        if (flags & GatherResultRead)
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            IHqlExpression * wuid = expr->queryAttribute(wuidAtom);
            addResultRead(wuid, sequence, name, false);
        }
        break;
    case no_ensureresult:
    case no_setresult:
    case no_extractresult:
        if (flags & GatherResultWrite)
        {
            IHqlExpression * sequence = queryAttributeChild(expr, sequenceAtom, 0);
            IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
            addResultWrite(sequence, name, false);
        }
        break;
    case no_definesideeffect:
        if (flags & GatherResultWrite)
        {
            addResultWrite(expr->queryAttribute(_uid_Atom), NULL, false);
        }
        break;
    case no_callsideeffect:
        if (flags & GatherResultRead)
        {
            addResultRead(NULL, expr->queryAttribute(_uid_Atom), NULL, false);
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

static HqlTransformerInfo selectConsistencyCheckerInfo("SelectConsistencyChecker");
class SelectConsistencyChecker  : public NewHqlTransformer
{
public:
    SelectConsistencyChecker() : NewHqlTransformer(selectConsistencyCheckerInfo)
    {
    }

    virtual void analyseExpr(IHqlExpression * expr)
    {
        if (alreadyVisited(expr))
            return;

        if (expr->getOperator() == no_select)
            checkSelect(expr);

        NewHqlTransformer::analyseExpr(expr);
    }

    virtual void analyseSelector(IHqlExpression * expr)
    {
        if (expr->getOperator() == no_select)
            checkSelect(expr);

        NewHqlTransformer::analyseSelector(expr);
    }

protected:
    void checkSelect(IHqlExpression * expr)
    {
        IHqlExpression * ds = expr->queryChild(0);
        if (ds->getOperator() == no_activetable)
            return;

        IHqlExpression * field = expr->queryChild(1);
        IHqlExpression * record = ds->queryRecord();
        assertex(record);
        IHqlSimpleScope * scope = record->querySimpleScope();
        OwnedHqlExpr match = scope->lookupSymbol(field->queryId());
        if (match != field)
        {
            EclIR::dbglogIR(2, field, match.get());
            throw MakeStringException(ERR_RECURSIVE_DEPENDENCY, "Inconsistent select - field doesn't match parent record's field");
        }
    }
};


void checkSelectConsistency(IHqlExpression * expr)
{
    SelectConsistencyChecker checker;
    checker.analyse(expr, 0);
}

//---------------------------------------------------------------------------

static HqlTransformerInfo parameterDependencyCheckerInfo("ParameterDependencyChecker");
class ParameterDependencyChecker  : public NewHqlTransformer
{
public:
    ParameterDependencyChecker() : NewHqlTransformer(parameterDependencyCheckerInfo), foundParameter(false)
    {
    }

    virtual void analyseExpr(IHqlExpression * expr)
    {
        if (expr->isFullyBound() || alreadyVisited(expr) || foundParameter)
            return;

        if (expr->getOperator() == no_param)
        {
            foundParameter = true;
            return;
        }

        NewHqlTransformer::analyseExpr(expr);
    }

    bool isDependent(IHqlExpression * expr)
    {
        analyse(expr, 0);
        return foundParameter;
    }

protected:
    bool foundParameter;
};


//Is 'expr' really dependent on a parameter - expr->isFullyBound() can give false negatives.
bool isDependentOnParameter(IHqlExpression * expr)
{
    if (expr->isFullyBound())
        return false;
    ParameterDependencyChecker checker;
    return checker.isDependent(expr);
}

bool isTimed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_externalcall:
    {
        IHqlExpression * funcdef = expr->queryExternalDefinition();
        assertex(funcdef);
        IHqlExpression * body = funcdef->queryChild(0);
        if (body->hasAttribute(timeAtom))
            return true;
        break;
    }
    case no_call:
    {
        IHqlExpression * funcdef = expr->queryBody()->queryFunctionDefinition();
        assertex(funcdef);
        IHqlExpression * body = funcdef->queryChild(0);
        if (body && body->hasAttribute(timeAtom))
            return true;
        break;
    }
    }
    return false;
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
        if (!isNewSelector(expr))
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
        if (expr->hasAttribute(beforeAtom))
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
        for (;;)
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
                if (includeVirtual || !expr->hasAttribute(virtualAtom))
                    count++;
                break;
            }
            break;
        }
    }
    return count;
}

static unsigned getFieldNumberFromRecord(IHqlExpression * record, IHqlExpression * field, bool & matched)
{
    matched = false;

    unsigned fieldNum = 0;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (cur == field)
            {
                matched = true;
                return fieldNum;
            }
            if (cur->isDatarow())
                fieldNum += countTotalFields(cur->queryRecord(), false);
            else
                fieldNum++;
            break;
        case no_ifblock:
            fieldNum += getFieldNumberFromRecord(cur->queryChild(1), field, matched);
            if (matched)
                return fieldNum;
            break;
        case no_record:
            fieldNum += getFieldNumberFromRecord(cur, field, matched);
            if (matched)
                return fieldNum;
            break;
        }
    }
    return fieldNum;
}

unsigned getFieldNumber(IHqlExpression * ds, IHqlExpression * selector)
{
    assertex(selector->getOperator() == no_select);

    IHqlExpression * parent = selector->queryChild(0);
    IHqlExpression * field = selector->queryChild(1);
    unsigned fieldNum = 0;
    if (parent != ds)
    {
        assertex(parent->isDatarow());
        fieldNum = getFieldNumber(ds, parent);
    }

    bool matched;
    fieldNum += getFieldNumberFromRecord(parent->queryRecord(), field, matched);
    assertex(matched);
    return fieldNum;
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
    for (;;)
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

static void expandHintValue(StringBuffer & s, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    node_operator childOp = no_none;
    switch (op)
    {
    case no_constant:
        expr->queryValue()->getStringValue(s);
        break;
    case no_comma:
        expandHintValue(s, expr->queryChild(0));
        expandHintValue(s.append(","), expr->queryChild(1));
        break;
    case no_range:
        expandHintValue(s, expr->queryChild(0));
        expandHintValue(s.append(".."), expr->queryChild(1));
        break;
    case no_rangefrom:
        expandHintValue(s, expr->queryChild(0));
        s.append("..");
        break;
    case no_rangeto:
        expandHintValue(s.append(".."), expr->queryChild(0));
        break;
    case no_list:
        {
            s.append("[");
            ForEachChild(i, expr)
            {
                if (i)
                    s.append(",");
                expandHintValue(s, expr->queryChild(i));
            }
            s.append("]");
            break;
        }
    case no_attr:
        s.append(expr->queryName());
        break;
    default:
        s.append("?");
        break;
    }
}

void getHintNameValue(IHqlExpression * attr, StringBuffer &name, StringBuffer &value)
{
    name.set(str(attr->queryName()));
    ForEachChild(i, attr)
    {
        if (i)
            value.append(",");
        expandHintValue(value, attr->queryChild(i));
    }
    if (value.length() == 0)
        value.append("1");
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

StringBuffer & getStringValue(StringBuffer & out, IHqlExpression * expr, const char * dft, bool utf8)
{
    if (expr)
    {
        if (expr->getOperator() == no_translated)
            expr = expr->queryChild(0);
        IValue * value = expr->queryValue();
        if (value)
        {
            if (utf8)
                value->getUTF8Value(out);
            else
                value->getStringValue(out);
            return out;
        }
    }
    if (dft)
        out.append(dft);
    return out;
}

StringBuffer & getStringValue(StringBuffer & out, IHqlExpression * expr, const char * dft)
{
    return getStringValue(out, expr, dft, false);
}

StringBuffer & getUTF8Value(StringBuffer & out, IHqlExpression * expr, const char * dft)
{
    return getStringValue(out, expr, dft, true);
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


bool recordContainsNestedRow(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (recordContainsNestedRow(cur))
                return true;
            break;
        case no_ifblock:
            if (recordContainsNestedRow(cur->queryChild(1)))
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
    if (!root || (root->getOperator() != no_select))
        return NULL;
    if (!followActiveSelectors && !isNewSelector(root))
        return NULL;

    IHqlExpression * ds = root->queryChild(0);
    for (;;)
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
    {
        IHqlExpression * arg0 = op->queryChild(0);
        if (arg0->isBoolean())
            return LINK(arg0);
    }

    if (opKind == no_constant)
        return createConstant(!op->queryValue()->getBoolValue());

    if (opKind == no_alias_scope)
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

    Owned<ITypeInfo> boolType = makeBoolType();
    return createValue(no_not, LINK(boolType), ensureExprType(op, boolType));
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
                    if (!value && !canOmit)
                        throwError1(HQLERR_FieldHasNoDefaultValue, str(cur->queryId()));
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


IHqlExpression * createTransformForField(IHqlExpression * field, IHqlExpression * value)
{
    OwnedHqlExpr record = createRecord(field);
    OwnedHqlExpr self = getSelf(record);
    OwnedHqlExpr target = createSelectExpr(LINK(self), LINK(field));
    OwnedHqlExpr assign = createAssign(LINK(target), LINK(value));
    return createValue(no_transform, makeTransformType(record->getType()), assign.getClear());
}

IHqlExpression * convertScalarToRow(IHqlExpression * value, ITypeInfo * fieldType)
{
    if (!fieldType)
        fieldType = value->queryType();
    OwnedHqlExpr field = createField(unnamedId, LINK(fieldType), NULL, NULL);
    OwnedHqlExpr record = createRecord(field);

    OwnedHqlExpr dataset;
    OwnedHqlExpr attribute;
    if (splitResultValue(dataset, attribute, value))
    {
        OwnedHqlExpr transform = createTransformForField(field, attribute);
        OwnedHqlExpr ds = createDataset(no_newusertable, LINK(dataset), createComma(LINK(record), LINK(transform)));
        return createRow(no_selectnth, LINK(ds), getSizetConstant(1));
    }

    OwnedHqlExpr transform = createTransformForField(field, value);
    return createRow(no_createrow, transform.getClear());
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
    IHqlExpression * seqAttr = setResult->queryAttribute(sequenceAtom);
    IHqlExpression * aliasAttr = setResult->queryAttribute(namedAtom);
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
    case type_dictionary:
        return createDictionary(no_workunit_dataset, LINK(queryOriginalRecord(valueType)), createComma(LINK(seqAttr), LINK(aliasAttr)));
    case type_row:
    case type_record:
         return createRow(no_getresult, LINK(queryOriginalRecord(valueType)), createComma(LINK(seqAttr), LINK(aliasAttr)));
    }

    return createValue(no_getresult, valueType.getLink(), LINK(seqAttr), LINK(aliasAttr));
}


//---------------------------------------------------------------------------------------------------------------------

IHqlExpression * convertScalarToGraphResult(IHqlExpression * value, ITypeInfo * fieldType, IHqlExpression * represents, unsigned seq)
{
    OwnedHqlExpr row = convertScalarToRow(value, fieldType);
    OwnedHqlExpr ds = createDatasetFromRow(LINK(row));
    HqlExprArray args;
    args.append(*LINK(ds));
    args.append(*LINK(represents));
    args.append(*getSizetConstant(seq));
    args.append(*createAttribute(rowAtom));
    return createValue(no_setgraphresult, makeVoidType(), args);
}

IHqlExpression * createScalarFromGraphResult(ITypeInfo * scalarType, ITypeInfo * fieldType, IHqlExpression * represents, unsigned seq)
{
    OwnedHqlExpr counterField = createField(unnamedId, LINK(fieldType), NULL, NULL);
    OwnedHqlExpr counterRecord = createRecord(counterField);
    HqlExprArray args;
    args.append(*LINK(counterRecord));
    args.append(*LINK(represents));
    args.append(*getSizetConstant(seq));
    args.append(*createAttribute(rowAtom));
    OwnedHqlExpr counterResult = createRow(no_getgraphresult, args);
    OwnedHqlExpr select = createNewSelectExpr(LINK(counterResult), LINK(counterField));
    return ensureExprType(select, scalarType);
}

IAtom * queryCsvEncoding(IHqlExpression * mode)
{
    if (mode)
    {
        if (mode->queryAttribute(asciiAtom))
            return asciiAtom;
        if (mode->queryAttribute(ebcdicAtom))
            return ebcdicAtom;
        if (mode->queryAttribute(unicodeAtom))
            return unicodeAtom;
    }
    return NULL;
}

IAtom * queryCsvTableEncoding(IHqlExpression * tableExpr)
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

bool isRightTrim(IHqlExpression * expr)
{
    if (expr->getOperator() != no_trim)
        return false;
    if (expr->hasAttribute(leftAtom) || expr->hasAttribute(allAtom))
        return false;
    return true;
}

bool isOpRedundantForCompare(IHqlExpression * expr)
{
    if (isRightTrim(expr))
    {
        ITypeInfo * baseType = expr->queryChild(0)->queryType();
        if (baseType->getTypeCode() == type_data)
            return false;
        return true;
    }
    return false;
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

        IHqlExpression * leftBody = left->queryBody(true);
        IHqlExpression * rightBody = right->queryBody(true);
        annotate_kind leftAK = left->getAnnotationKind();
        annotate_kind rightAK = right->getAnnotationKind();
        if (leftBody != rightBody)
        {
            if ((left == leftBody) || (right == rightBody))
                return foundDifference();  // one side has an annotation, the other doesn't
            return debugFindFirstDifference(leftBody, rightBody);
        }
        if (leftAK != rightAK)
            return foundDifference();  // different annotation
        return foundDifference();  // some difference in the annotation details
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

void debugTrackDifference(IHqlExpression * expr)
{
    static IHqlExpression * prev;
    if (prev && prev != expr)
        debugFindFirstDifference(prev, expr);
    prev = expr;
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
        defType.setown(makeTableType(makeRowType(makeRecordType())));
        childDefRecord.setown(::createMetaRecord(fieldRecord, callback));
        break;
    case type_groupedtable:
        {
            ITypeInfo * tableType = makeTableType(makeRowType(makeRecordType()));
            defType.setown(makeGroupedTableType(tableType));
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
    if (cur->hasAttribute(blobAtom))
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
    for (;;)
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


bool canCreateRtlTypeInfo(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (!canCreateRtlTypeInfo(cur))
                return false;
            break;
        case no_field:
            switch (cur->queryType()->getTypeCode())
            {
            case type_row:
                if (!canCreateRtlTypeInfo(cur->queryRecord()))
                    return false;
                break;
            case type_table:
            case type_groupedtable:
                if (cur->hasAttribute(countAtom) || cur->hasAttribute(sizeofAtom))
                    return false;
                break;
            case type_alien:
                return false;
            }
            break;
        case no_ifblock:
            return false;
        }
    }
    return true;
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
    return createBlankString();
}

int compareAtoms(IInterface * const * pleft, IInterface * const * pright)
{
    IAtom * left = static_cast<IAtom *>(*pleft);
    IAtom * right = static_cast<IAtom *>(*pright);

    return stricmp(str(left), str(right));
}

int compareScopesByName(IHqlScope * left, IHqlScope * right)
{
    const char * leftName = str(left->queryName());
    const char * rightName = str(right->queryName());
    if (leftName && rightName)
        return stricmp(leftName, rightName);
    if (leftName)
        return +1;
    if (rightName)
        return -1;
    return 0;
}

int compareSymbolsByName(IHqlExpression * left, IHqlExpression * right)
{
    return stricmp(str(left->queryName()), str(right->queryName()));
}

int compareSymbolsByName(IInterface * const * pleft, IInterface * const * pright)
{
    IHqlExpression * left = static_cast<IHqlExpression *>(*pleft);
    IHqlExpression * right = static_cast<IHqlExpression *>(*pright);

    return stricmp(str(left->queryName()), str(right->queryName()));
}

int compareScopesByName(IInterface * const * pleft, IInterface * const * pright)
{
    IHqlScope * left = static_cast<IHqlScope *>(*pleft);
    IHqlScope * right = static_cast<IHqlScope *>(*pright);
    return compareScopesByName(left, right);
}

class ModuleExpander
{
public:
    ModuleExpander(HqlLookupContext & _ctx, bool _expandCallsWhenBound, node_operator _outputOp) 
        : ctx(_ctx), expandCallsWhenBound(_expandCallsWhenBound), outputOp(_outputOp) {}

    IHqlExpression * createExpanded(IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, const char * prefix, IIdAtom *matchId);

protected:
    HqlLookupContext ctx;
    bool expandCallsWhenBound;
    node_operator outputOp;
};

IHqlExpression * ModuleExpander::createExpanded(IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, const char * prefix, IIdAtom *matchId)
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
        IIdAtom * name = cur.queryId();
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
            lowername.clear().append(prefix).append(lower(name)).toLowerCase();

            node_operator op = no_none;
            if (outputOp == no_output)
            {
                if (value->isDictionary())
                {
                    value.setown(createDataset(no_datasetfromdictionary, value.getClear()));
                    value.setown(createDataset(no_selectfields, value.getClear(), createValue(no_null)));
                    op = no_output;
                }
                else if (value->isDataset())
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
            case no_enum:
                op = no_none;
                break;
            }

            if (value->isScope())
            {
                lowername.append(".");
                OwnedHqlExpr child = createExpanded(value, value, lowername.str(), matchId);
                if (child->getOperator() != no_null)
                    outputs.append(*child.getClear());
            }
            else if (!matchId || lower(name)==lower(matchId))
            {
                if (op != no_none)
                {
                    outputs.append(*createValue(op, makeVoidType(), LINK(value), createAttribute(namedAtom, createConstant(lowername))));
                }
                else if (value->isAction())
                    outputs.append(*LINK(value));
            }
        }
    }

    return createActionList(outputs);
}


IHqlExpression * createEvaluateOutputModule(HqlLookupContext & ctx, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, bool expandCallsWhenBound, node_operator outputOp, IIdAtom *id)
{
    ModuleExpander expander(ctx, expandCallsWhenBound, outputOp);
    return expander.createExpanded(scopeExpr, ifaceExpr, NULL, id);
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
    IIdAtom * moduleName = NULL;
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

                IIdAtom * name = symbols.item(i).queryId();
                OwnedHqlExpr failure = createValue(no_stored, makeVoidType(), createConstant(str(lower(name))));

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
    case no_variance:   newop = no_vargroup; break;
    case no_covariance: newop = no_covargroup; break;
    case no_correlation:newop = no_corrgroup; break;
    default:
        return NULL;
    }

    OwnedHqlExpr field;
    if ((newop == no_mingroup || newop == no_maxgroup) && (arg->getOperator() == no_select))
        field.set(arg->queryChild(1));                  // inherit maxlength etc...
    else
        field.setown(createField(valueId, expr->getType(), NULL));

    IHqlExpression * aggregateRecord = createRecord(field);

    HqlExprArray valueArgs;
    ForEachChildFrom(i1, expr, 1)
    {
        IHqlExpression * cur = expr->queryChild(i1);
        //keyed is currently required on the aggregate operator
        if (!cur->isAttribute() || (cur->queryName() == keyedAtom))
            valueArgs.append(*LINK(cur));
    }

    IHqlExpression * newValue = createValue(newop, expr->getType(), valueArgs);
    IHqlExpression * assign = createAssign(createSelectExpr(getSelf(aggregateRecord), LINK(field)), newValue);
    IHqlExpression * transform = createValue(no_newtransform, makeTransformType(aggregateRecord->getType()), assign);

    //remove grouping if dataset happens to be grouped...
    dataset->Link();
    if (dataset->queryType()->getTypeCode() == type_groupedtable)
        dataset = createDataset(no_group, dataset, NULL);

    HqlExprArray args;
    args.append(*dataset);
    args.append(*aggregateRecord);
    args.append(*transform);
    ForEachChild(i2, expr)
    {
        IHqlExpression * cur = expr->queryChild(i2);
        if (cur->isAttribute())
            args.append(*LINK(cur));
    }
    IHqlExpression * project = createDataset(no_newaggregate, args);
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
    ITypeInfo * storeType = type->queryChildType();
    if ((thisBits > bitsRemaining) || !activeType || (storeType != activeType))
    {
        activeType = storeType;
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


extern HQL_API void inheritAttribute(HqlExprArray & attrs, IHqlExpression * donor, IAtom * name)
{
    IHqlExpression * match = donor->queryAttribute(name);
    if (match)
        attrs.append(*LINK(match));
}

IHqlExpression * inheritAttribute(IHqlExpression * expr, IHqlExpression * donor, IAtom * name)
{
    return appendOwnedOperand(expr, LINK(donor->queryAttribute(name)));
}

IHqlExpression * appendAttribute(IHqlExpression * expr, IAtom * attr)
{
    return appendOwnedOperand(expr, createAttribute(attr));
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

IHqlExpression * removeAttribute(IHqlExpression * expr, IAtom * attr)
{
    HqlExprArray args;
    unwindChildren(args, expr);
    if (removeAttribute(args, attr))
        return expr->clone(args);
    return LINK(expr);
}

IHqlExpression * replaceOwnedAttribute(IHqlExpression * expr, IHqlExpression * ownedOperand)
{
    HqlExprArray args;
    unwindChildren(args, expr);
    removeAttribute(args, ownedOperand->queryName());
    args.append(*ownedOperand);
    return expr->clone(args);
}


IHqlExpression * appendLocalAttribute(IHqlExpression * expr)
{
    return appendOwnedOperand(expr, createLocalAttribute());
}

IHqlExpression * removeLocalAttribute(IHqlExpression * expr)
{
    return removeAttribute(expr, localAtom);
}

bool hasOperand(IHqlExpression * expr, IHqlExpression * child)
{
    expr = expr->queryBody();
    ForEachChild(i, expr)
    {
        if (expr->queryChild(i) == child)
            return true;
    }
    return false;
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
    if (expr->hasAttribute(newAtom))
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
        if (expr->hasAttribute(newAtom))
            return doTransformSelect(expr);
        break;
    case no_colon:
    case no_globalscope:
    case no_nothor:
        return LINK(expr);
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
                remove = !expr->hasAttribute(_distributed_Atom);
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
            OwnedHqlExpr field = createField(unnamedId, value->getType(), NULL);
            OwnedHqlExpr transform = createTransformForField(field, value);
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
    if ((leftOp !=no_select) || expr->hasAttribute(newAtom))
    {
        //Ensure selections from dictionaries do not have a separate activity for the row lookup.
        if (leftOp == no_selectmap)
            return false;

        //If this is a selection from an inscope dataset then this must not be assumed to be an input dataset.
        if (expr->isDataset() && !expr->hasAttribute(newAtom))
            return false;

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


bool splitResultValue(SharedHqlExpr & dataset, SharedHqlExpr & attribute, IHqlExpression * value)
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
    if (splitResultValue(dataset, attribute, value))
    {
        args.replace(*dataset.getClear(), 0);
        args.add(*attribute.getClear(), 1);
        return createValue(no_extractresult, makeVoidType(), args);
    }
    if (value->isDataset() || value->isDictionary())
        return createValue(no_output, makeVoidType(), args);
    return createValue(no_setresult, makeVoidType(), args);
}


IHqlExpression * convertSetResultToExtract(IHqlExpression * setResult)
{
    HqlExprAttr dataset, attribute;
    if (splitResultValue(dataset, attribute, setResult->queryChild(0)))
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

void getVirtualFields(HqlExprArray & virtuals, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (cur->hasAttribute(virtualAtom))
                virtuals.append(*LINK(cur));
            break;
        case no_ifblock:
            getVirtualFields(virtuals, cur->queryChild(1));
            break;
        case no_record:
            getVirtualFields(virtuals, cur);
            break;
        }
    }
}

bool containsVirtualFields(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (cur->hasAttribute(virtualAtom))
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
            if (!cur->hasAttribute(virtualAtom))
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

static HqlTransformerInfo fieldPropertyRemoverInfo("FieldAttributeRemover");
class FieldAttributeRemover : public NewHqlTransformer
{
public:
    FieldAttributeRemover(IAtom * _name) : NewHqlTransformer(fieldPropertyRemoverInfo), name(_name) {}

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        //By default fields within the following are not transformed...
        case no_record:
        case no_ifblock:
        case no_select: // Ensure fields used by ifblocks get transformed
            return completeTransform(expr);

        case no_field:
            {
                OwnedHqlExpr transformed = transformField(expr);
                while (transformed->hasAttribute(name))
                    transformed.setown(removeAttribute(transformed, name));
                return transformed.getClear();
            }

        default:
            return NewHqlTransformer::createTransformed(expr);
        }
    }

private:
    IAtom * name;
};

IHqlExpression * removeAttributeFromFields(IHqlExpression * expr, IAtom * name)
{
    FieldAttributeRemover remover(name);
    return remover.transformRoot(expr);
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
            if (expr->hasAttribute(virtualAtom))
                newValue = getVirtualReplacement(expr, expr->queryAttribute(virtualAtom)->queryChild(0), dataset);
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
    for (;;)
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
    for (;;)
    {
        if (!isCast(expr))
            return expr;
        expr = expr->queryChild(0);
    }
}

static bool isSimpleTransformToMergeWith(IHqlExpression * expr, int & varSizeCount)
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

bool isSimpleTransform(IHqlExpression * expr)
{
    int varSizeCount = 0;
    return isSimpleTransformToMergeWith(expr, varSizeCount);
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

bool isConstantDictionary(IHqlExpression * expr)
{
    if (expr->getOperator() == no_null)
        return true;
    if (expr->getOperator() != no_createdictionary)
        return false;
    IHqlExpression * dataset = expr->queryChild(0);
    if (dataset->getOperator() == no_inlinetable)
        return isConstantDataset(dataset);
    return false;
}

bool isProjectableCall(IHqlExpression *expr)
{
    if (expr->getOperator() != no_call)
        return false;
    IHqlExpression * funcdef = expr->queryBody()->queryFunctionDefinition();
    assertex(funcdef);
    IHqlExpression * body = funcdef->queryChild(0);
    assertex(body);
    if ((funcdef->getOperator() == no_funcdef) && (body->getOperator() == no_outofline))
    {
        IHqlExpression * bodycode = body->queryChild(0);
        if (bodycode->getOperator() == no_embedbody && bodycode->hasAttribute(projectedAtom))
            return true;
    }
    return false;
}

inline bool iseol(char c) { return c == '\r' || c == '\n'; }

static unsigned skipSpace(unsigned start, unsigned len, const char * buffer)
{
    while (start < len && isspace((byte)buffer[start]))
        start++;
    return start;
}

static unsigned trimSpace(unsigned len, const char * buffer)
{
    while (len && isspace((byte)buffer[len-1]))
        len--;
    return len;
}

// Supported syntax:
//      #option pure
//      #option library 'value'
//      #option ('pure', true)
//      #option ('library', 'value')
//
// - cur must point to the first non-space character of '#option'
static bool matchOption(unsigned cur, unsigned max, const char * buffer, unsigned lenMatch, const char * match, bool requireValue, unsigned & valueStart, unsigned & valueEnd)
{
    bool openBracket = false;
    bool commaBetweenPair = false;

    valueStart = 0;
    valueEnd = 0;

    if (cur + lenMatch > max)
        return false;
    if ('(' == buffer[cur])
    {
        openBracket = true;
        cur = skipSpace(cur+1, max, buffer);
        // Option inside brackets must have the option name wrapped in single quotes
        if ((cur+lenMatch+3 < max) && ('\'' == buffer[cur]) )
        {
            ++cur;
            if (memicmp(buffer+cur, match, lenMatch) != 0)
                return false;
            cur += lenMatch;
            if ('\'' != buffer[cur])
                return false;
            cur = skipSpace(cur+1, max, buffer);
            if ((cur < max) && (',' == buffer[cur]))
            {
                ++cur;
                commaBetweenPair = true;
            } 
            else if (requireValue)
                return false;
        }
        else  // Not possible to match option name in quotes
            return false;
    } 
    else 
    {
        if (memicmp(buffer+cur, match, lenMatch) != 0)
            return false;
        cur += lenMatch;
        if ((cur < max) && isalnum(buffer[cur]))
            return false;
    }
    cur = skipSpace(cur, max, buffer);
    if (cur < max) 
    {
        if ('\'' == buffer[cur])
        {
            ++cur;
            valueStart = cur;
            while ((cur < max) && ('\'' != buffer[cur]))
                ++cur;
            if (cur >= max) return false;
            valueEnd = cur;
            cur = skipSpace(cur+1, max, buffer);
            if (openBracket && (')' != buffer[cur]))
                return false;
        }
        else
        {
            valueStart = cur;
            if (openBracket)
            {
                while ((cur < max) && (')' != buffer[cur]))
                    ++cur;
                if (cur >= max) return false;
                valueEnd = trimSpace(cur,buffer);
            }
            else
                valueEnd = trimSpace(max, buffer);
        }
    }
    // Note: empty quoted strings rejected here too
    if ((requireValue || commaBetweenPair) && (valueEnd==valueStart))
        return false;

    return true;
}


IHqlExpression * extractCppBodyAttrs(unsigned lenBuffer, const char * buffer)
{
    OwnedHqlExpr attrs;
    unsigned prev = '\n';
    for (unsigned i=0; i < lenBuffer; i++)
    {
        unsigned next = buffer[i];
        bool ignore = false;
        switch (next)
        {
        case '*':
            if ('/' == prev) // Ignore directives in multi-line comments
            {
                i+=2;
                while (i < lenBuffer && ('*' != buffer[i-1] || '/' != buffer[i])) 
                    ++i;
            }
            next = ' '; // treat as whitespace
            break;
        case '/':
            if ('/' == prev) // Ignore directives in single line comments
            {
                ++i;
                while (i < lenBuffer && !iseol(buffer[i]))
                    ++i;
            }
            next = '\n';
            break;
        case ' ': case '\t':
            ignore = true; // allow whitespace in front of #option
            break;
        case '#':
            if (prev == '\n')
            {
                if ((i + 1 + 6 < lenBuffer) && memicmp(buffer+i+1, "option", 6) == 0)
                {
                    unsigned valueStart, valueEnd;
                    unsigned start = skipSpace(i+1+6, lenBuffer, buffer);
                    unsigned end = start;
                    while (end < lenBuffer && !iseol((byte)buffer[end]))
                        end++;
                    i = end;
                    if(matchOption(start, end, buffer, 4, "pure", false, valueStart, valueEnd))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(pureAtom)));
                    else if (matchOption(start, end, buffer, 8, "volatile", false, valueStart, valueEnd))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(volatileAtom)));
                    else if (matchOption(start, end, buffer, 4, "costly", false, valueStart, valueEnd))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(costlyAtom)));
                    else if (matchOption(start, end, buffer, 4, "once", false, valueStart, valueEnd))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(onceAtom)));
                    else if (matchOption(start, end, buffer, 6, "action", false, valueStart, valueEnd))
                        attrs.setown(createComma(attrs.getClear(), createAttribute(actionAtom)));
                    else if (matchOption(start, end, buffer, 6, "source", true, valueStart, valueEnd))
                    {
                        Owned<IValue> restOfLine = createUtf8Value(valueEnd-valueStart, buffer+valueStart, makeUtf8Type(UNKNOWN_LENGTH, NULL));
                        OwnedHqlExpr arg = createConstant(restOfLine.getClear());
                        attrs.setown(createComma(attrs.getClear(), createAttribute(sourceAtom, arg.getClear())));
                    }
                    else if (matchOption(start, end, buffer, 7, "library", true, valueStart, valueEnd))
                    {
                        Owned<IValue> restOfLine = createUtf8Value(valueEnd-valueStart, buffer+valueStart, makeUtf8Type(UNKNOWN_LENGTH, NULL));
                        OwnedHqlExpr arg = createConstant(restOfLine.getClear());
                        attrs.setown(createComma(attrs.getClear(), createAttribute(libraryAtom, arg.getClear())));
                    }
                    else if (matchOption(start, end, buffer, 4, "link", true, valueStart, valueEnd))
                    {
                        Owned<IValue> restOfLine = createUtf8Value(valueEnd-valueStart, buffer+valueStart, makeUtf8Type(UNKNOWN_LENGTH, NULL));
                        OwnedHqlExpr arg = createConstant(restOfLine.getClear());
                        attrs.setown(createComma(attrs.getClear(), createAttribute(linkAtom, arg.getClear())));
                    }
                }
            }
        }
        if (!ignore)
            prev = next;
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
    TempTableTransformer(IErrorReceiver & _errorProcessor, ECLlocation & _location, bool _strictTypeChecking = false)
      : errorProcessor(_errorProcessor), defaultLocation(_location), strictTypeChecking(_strictTypeChecking)
    {}

    IHqlExpression * createTempTableTransform(IHqlExpression * curRow, IHqlExpression * record);

protected:
    void createTempTableAssign(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * curRow, IHqlExpression * expr, unsigned & col, IHqlExpression * selector, HqlMapTransformer & mapper, bool included);
    IHqlExpression * createTempTableTransform(IHqlExpression * self, IHqlExpression * curRow, IHqlExpression * expr, unsigned & col, IHqlExpression * selector, HqlMapTransformer & mapper, bool included);

    void reportWarning(WarnErrorCategory category, IHqlExpression * location, int code,const char *format, ...) __attribute__((format(printf, 5, 6)));
    void reportError(IHqlExpression * location, int code,const char *format, ...) __attribute__((format(printf, 4, 5)));

protected:
    IErrorReceiver & errorProcessor;
    ECLlocation & defaultLocation;
    bool strictTypeChecking;
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
    IHqlExpression * rowPayloadAttr = curRow->queryAttribute(_payload_Atom);
    IHqlExpression * recordPayloadAttr = record->queryAttribute(_payload_Atom);
    if (rowPayloadAttr)
    {
        unsigned rowPayload =  (unsigned) getIntValue(rowPayloadAttr->queryChild(0));
        col++;
        if (recordPayloadAttr)
        {
            unsigned recordPayload =  (unsigned) getIntValue(recordPayloadAttr->queryChild(0));
            if (rowPayload != recordPayload)
                ERRORAT(curRow->queryAttribute(_location_Atom), HQLERR_PayloadMismatch);
        }
        else
            ERRORAT(curRow->queryAttribute(_location_Atom), HQLERR_PayloadMismatch);
    }
    else if (recordPayloadAttr)
        ERRORAT(curRow->queryAttribute(_location_Atom), HQLERR_PayloadMismatch);

    OwnedHqlExpr ret = createTempTableTransform(self, curRow, record, col, self, mapping, true);
    if (queryRealChild(curRow, col))
    {
        StringBuffer s;
        getExprECL(curRow->queryChild(col), s);
        ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_TooManyInitializers, s.str());
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
                    if (expr->isDataset() || expr->isDictionary())
                    {
                        if (src)
                            col++;
                        else
                        {
                            src.set(expr->queryChild(0));
                            if (!src || src->isAttribute())
                                src.set(queryAttributeChild(expr, defaultAtom, 0));
                            if (!src)
                            {
                                ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_NoDefaultProvided, str(expr->queryName()));
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
                                ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_IncompatibleTypesForField, str(expr->queryName()));
                                return;
                            }
                            if (isGrouped(src))
                                castValue.setown(createDataset(no_group, LINK(src)));
                            else
                                castValue.set(src);

                        }
                        else
                        {
                            ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_IncompatibleTypesForField, str(expr->queryName()));
                            return;
                        }
                    }
                    else
                    {
                        if (src && src->isDatarow())
                        {
                            if (!recordTypesMatch(src, target))
                            {
                                ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_IncompatibleTypesForField, str(expr->queryName()));
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
                    if (target->isDictionary() && !castValue->isDictionary())
                        castValue.setown(createDictionary(no_createdictionary, castValue.getClear()));
                }
                else
                {
                    if (expr->isDictionary())
                        castValue.setown(createDictionary(no_null, LINK(record)));
                    else if (expr->isDataset())
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
                        if (!defaultValue || defaultValue->isAttribute())
                            defaultValue = queryAttributeChild(expr, defaultAtom, 0);
                        src.setown(replaceSelfRefSelector(defaultValue, self));
                        if (src)
                            src.setown(mapper.transformRoot(src));
                    }

                    if (!src || src->isAttribute())
                    {
                        if (expr->hasAttribute(virtualAtom))
                            ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_VirtualFieldInTempTable, str(expr->queryName()));
                        else
                            ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_NoDefaultProvided, str(expr->queryName()));
                        return;
                    }
                    if (src->getOperator() == no_recordlist)
                    {
                        ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_IncompatiableInitailiser, str(expr->queryName()));
                        return;
                    }
                    else if (type->isScalar() != src->queryType()->isScalar())
                    {
                        ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_IncompatibleTypesForField, str(expr->queryName()));
                        return;
                    }
                    else if (strictTypeChecking && !type->assignableFrom(src->queryType()))
                    {
                        ERRORAT1(curRow->queryAttribute(_location_Atom), HQLERR_IncompatibleTypesForField, str(expr->queryName()));
                    }
                    castValue.setown(ensureExprType(src, type));
                }
                else
                    castValue.setown(createNullExpr(expr));
            }
            assigns.append(*createAssign(LINK(target), LINK(castValue)));
            mapper.setMapping(target, castValue);
            break;
        }

    case no_ifblock:
        {
            OwnedHqlExpr cond = replaceSelfRefSelector(expr->queryChild(0), selector);
            OwnedHqlExpr mapped = mapper.transformRoot(cond);
            mapped.setown(foldHqlExpression(errorProcessor, mapped, NULL, HFOfoldimpure|HFOforcefold));
            IValue * mappedValue = mapped->queryValue();

            if (included)
            {
                if (!mappedValue)
                    reportWarning(CategoryUnexpected, NULL, HQLWRN_CouldNotConstantFoldIf, HQLWRN_CouldNotConstantFoldIf_Text);
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
    Owned<IError> err = createError(code, errorMsg.str(), str(where->sourcePath), where->lineno, where->column, where->position);
    errorProcessor.report(err);
}

void TempTableTransformer::reportWarning(WarnErrorCategory category, IHqlExpression * location, int code,const char *format, ...)
{
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
    errorProcessor.reportWarning(category, code, errorMsg.str(), str(where->sourcePath), where->lineno, where->column, where->position);
}

IHqlExpression *notePayloadFields(IHqlExpression *record, unsigned payloadCount)
{
    HqlExprArray fields;
    unwindChildren(fields, record);
    unsigned idx = fields.length();
    while (idx && payloadCount)
    {
        IHqlExpression * cur = &fields.item(--idx);
        switch (cur->getOperator())
        {
        case no_record:
            throwUnexpected();
            break;
        case no_field:
        case no_ifblock:
            fields.replace(*appendOwnedOperand(cur, createAttribute(_payload_Atom)), idx);
            payloadCount--;
            break;
        }
    }
    return createRecord(fields);
}

IHqlExpression *getDictionaryKeyRecord(IHqlExpression *record)
{
    IHqlExpression * payload = record->queryAttribute(_payload_Atom);
    unsigned payloadSize = payload ? (unsigned)getIntValue(payload->queryChild(0)) : 0;
    unsigned max = record->numChildren() - payloadSize;
    IHqlExpression *newrec = createRecord();
    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression *child = record->queryChild(idx);
        if (!child->isAttribute() || child->queryName()!=_payload_Atom)  // Strip off the payload attribute
            newrec->addOperand(LINK(child));
    }
    return newrec->closeExpr();
}

IHqlExpression *recursiveStretchFields(IHqlExpression *record)
{
    IHqlExpression *newrec = createRecord();
    ForEachChild (idx, record)
    {
        IHqlExpression *child = record->queryChild(idx);
        if (child->getOperator()==no_field)
        {
            ITypeInfo *fieldType = child->queryType();
            switch (fieldType->getTypeCode())
            {
            case type_row:
            {
                OwnedHqlExpr childType = recursiveStretchFields(child->queryRecord());
                newrec->addOperand(createField(child->queryId(), makeRowType(childType->getType()), NULL, NULL));
                break;
            }
            default:
            {
                Owned<ITypeInfo> stretched = getMaxLengthType(fieldType);
                newrec->addOperand(createField(child->queryId(), stretched.getClear(), NULL, NULL));
                break;
            }
            }
        }
        else
            newrec->addOperand(LINK(child));
    }
    return newrec->closeExpr();
}

IHqlExpression *getDictionarySearchRecord(IHqlExpression *record)
{
    OwnedHqlExpr keyrec = getDictionaryKeyRecord(record);
    return recursiveStretchFields(keyrec);
}

static IHqlExpression *createTransformFromRowValue(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *dict, IHqlExpression * rowValue)
{
    OwnedHqlExpr record = getDictionarySearchRecord(dict->queryRecord());
    TempTableTransformer transformer(errorProcessor, location, true);
    return transformer.createTempTableTransform(rowValue, record);
}


IHqlExpression *createRowForDictExpr(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *expr, IHqlExpression *dict)
{
    OwnedHqlExpr rowValue = createValue(no_rowvalue, makeNullType(), LINK(expr));
    OwnedHqlExpr newTransform = createTransformFromRowValue(errorProcessor, location, dict, rowValue);
    return createRow(no_createrow, newTransform.getClear());
}


IHqlExpression * createSelectMapRow(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression * dict, IHqlExpression *values)
{
    //Always process the expression and create a row since this also validates the search expression
    OwnedHqlExpr newTransform = createTransformFromRowValue(errorProcessor, location, dict, values);

    //If only a single expression is being looked up then create no_selectmap(dict, expr) instead of no_selectmap(dict, row) to avoid unnecessary aliases.
    if (getFlatFieldCount(newTransform->queryRecord()) == 1)
        return createRow(no_selectmap, LINK(dict), LINK(values->queryChild(0)));

    return createRow(no_selectmap, LINK(dict), createRow(no_createrow, newTransform.getClear()));
}

IHqlExpression *createINDictExpr(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *expr, IHqlExpression *dict)
{
    //Always process the expression and create a row since this also validates the search expression
    OwnedHqlExpr row = createRowForDictExpr(errorProcessor, location, expr, dict);

    //If only a single expression is being looked up then create no_indict(expr,dict) instead of no_indict(row,dict) to avoid unnecessary row aliases.
    if (getFlatFieldCount(row->queryRecord()) == 1)
        return createBoolExpr(no_indict, LINK(expr), LINK(dict));

    return createBoolExpr(no_indict, LINK(row), LINK(dict));
}

IHqlExpression *createINDictRow(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression *row, IHqlExpression *dict)
{
    OwnedHqlExpr record = getDictionarySearchRecord(dict->queryRecord());
    Owned<ITypeInfo> rowType = makeRowType(record->getType());
    OwnedHqlExpr castRow = ensureExprType(row, rowType);
    return createBoolExpr(no_indict, castRow.getClear(), LINK(dict));
}

IHqlExpression * convertTempRowToCreateRow(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression * expr)
{
    IHqlExpression * oldValues = expr->queryChild(0);
    IHqlExpression * record = expr->queryChild(1);
    OwnedHqlExpr values = normalizeListCasts(oldValues); // ??? not used
    TempTableTransformer transformer(errorProcessor, location);

    OwnedHqlExpr newTransform = transformer.createTempTableTransform(oldValues, record);
    HqlExprArray children;
    children.append(*LINK(newTransform));
    OwnedHqlExpr ret = createRow(no_createrow, children);
    return expr->cloneAllAnnotations(ret);
}

static IHqlExpression * convertTempTableToInline(IErrorReceiver & errorProcessor, ECLlocation & location, IHqlExpression * expr)
{
    IHqlExpression * oldValues = expr->queryChild(0);
    IHqlExpression * record = expr->queryChild(1);
    OwnedHqlExpr values = normalizeListCasts(oldValues);
    node_operator valueOp = values->getOperator();

    if ((valueOp == no_list) && (values->numChildren() == 0))
        return createDataset(no_null, LINK(record));

    if ((valueOp != no_recordlist) && (valueOp != no_list))
        return LINK(expr);

    TempTableTransformer transformer(errorProcessor, location);
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
                {
                    IHqlExpression * value = queryRealChild(field, 0);
                    if (value)
                        row.append(*LINK(value));
                    else
                    {
                        VStringBuffer msg(HQLERR_FieldHasNoDefaultValue_Text, str(field->queryName()));
                        errorProcessor.reportError(HQLERR_FieldHasNoDefaultValue, msg.str(), str(location.sourcePath), location.lineno, location.column, location.position);
                    }
                }
            }
            cur.setown(createValue(no_rowvalue, makeNullType(), row));
        }
        transforms.append(*transformer.createTempTableTransform(cur, record));
    }

    HqlExprArray children;
    children.append(*createValue(no_transformlist, makeNullType(), transforms));
    children.append(*LINK(record));
    unwindChildren(children, expr, 2);
    OwnedHqlExpr ret = createDataset(no_inlinetable, children);
    return expr->cloneAllAnnotations(ret);
}

IHqlExpression * convertTempTableToInlineTable(IErrorReceiver & errors, ECLlocation & location, IHqlExpression * expr)
{
    return convertTempTableToInline(errors, location, expr);
}

void setPayloadAttribute(HqlExprArray &args)
{
    // Locate a payload attribute in  an initializer value list. If found, move it to front and give it a position
    int payloadPos = -1;
    ForEachItemIn(idx, args)
    {
        IHqlExpression *cur = &args.item(idx);
        if (cur->isAttribute())
        {
            assertex(payloadPos==-1);
            assertex(cur->queryName()==_payload_Atom);
            payloadPos = idx;
        }
    }
    if (payloadPos != -1)
    {
        args.remove(payloadPos);
        args.add(*createAttribute(_payload_Atom, createConstant((__int64) args.length()-payloadPos)), 0);
    }
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
    case type_dictionary:
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


bool getFoldedConstantText(StringBuffer& ret, IHqlExpression * expr)
{
    OwnedHqlExpr folded = foldExprIfConstant(expr);

    IValue * value = folded->queryValue();
    if (!value)
        return false;

    value->getStringValue(ret);
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
    bool sortPayload = sortIndexPayload ? !expr->hasAttribute(sort_KeyedAtom) : expr->hasAttribute(sort_AllAtom);
    if (sortPayload)
    {
        max = buildRecord->numChildren();
        //If the last field is an implicit fpos, then they will all have the same value, so no point sorting.
        if (queryLastField(buildRecord)->hasAttribute(_implicitFpos_Atom))
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


int compareLibraryParameterOrder(IHqlExpression * left, IHqlExpression * right)
{
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
    return stricmp(str(left->queryName()), str(right->queryName()));
}


static int compareLibraryParameterOrder(IInterface * const * pleft, IInterface * const * pright)
{
    IHqlExpression * left = static_cast<IHqlExpression *>(*pleft);
    IHqlExpression * right = static_cast<IHqlExpression *>(*pright);
    return compareLibraryParameterOrder(left, right);
}

LibraryInputMapper::LibraryInputMapper(IHqlExpression * _libraryInterface)
: libraryInterface(_libraryInterface)
{
    assertex(libraryInterface->getOperator() == no_funcdef);
    scopeExpr.set(libraryInterface->queryChild(0));
    streamingAllowed = !scopeExpr->hasAttribute(_noStreaming_Atom);  // ?? is this in the correct place, probably, just nasty

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
            IIdAtom * nestedName = createMangledName(expr, &cur);

            //default values are handled elsewhere - lost from the mapped values here.
            HqlExprArray attrs;
            OwnedHqlExpr renamed = createParameter(nestedName, nextParameter++, cur.getType(), attrs);
            expandParameter(renamed, nextParameter);
        }
    }
    else
        realParameters.append(*LINK(expr));
}


unsigned LibraryInputMapper::findParameter(IIdAtom * searchId)
{
    IAtom * searchName = lower(searchId);
    ForEachItemIn(i, realParameters)
    {
        if (realParameters.item(i).queryName() == searchName)
            return i;
    }
    return NotFound;
}

IHqlExpression * LibraryInputMapper::resolveParameter(IIdAtom * search)
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
        
        inputExprs.append(*createSymbol(cur->queryId(), result, ob_private));
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

            OwnedHqlExpr named = createSymbol(cur.queryId(), LINK(mapped), ob_private);
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
            
            OwnedHqlExpr childValue = valueScope->lookupSymbol(cur.queryId(), LSFpublic, lookupCtx);
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
            IHqlExpression * id = queryAttributeChild(expr, externalAtom, 0);
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

static ErrorSeverity getWarningAction(unsigned errorCode, const HqlExprArray & overrides, unsigned first, ErrorSeverity defaultSeverity)
{
    //warnings are assumed to be infrequent, so don't worry about efficiency here.
    const unsigned max = overrides.ordinality();
    for (unsigned i=first; i < max; i++)
    {
        IHqlExpression & cur = overrides.item(i);
        if (matchesConstantValue(cur.queryChild(0), errorCode))
            return getCheckSeverity(cur.queryChild(1)->queryName());
    }
    return defaultSeverity;
}

//---------------------------------------------------------------------------

ErrorSeverityMapper::ErrorSeverityMapper(IErrorReceiver & _errorProcessor) : IndirectErrorReceiver(_errorProcessor)
{
    firstActiveMapping = 0;
    activeSymbol = NULL;
    for (unsigned category = 0; category < CategoryMax; category++)
        categoryAction[category] = SeverityUnknown;
}


bool ErrorSeverityMapper::addCommandLineMapping(const char * mapping)
{
    if (!mapping)
        return true;

    unsigned len = strlen(mapping);
    if (len == 0)
        return true;

    const char * equals = strchr(mapping, '=');
    const char * value;
    if (equals)
    {
        value = equals+1;
        len = equals-mapping;
    }
    else if (mapping[len-1] == '+')
    {
        len--;
        value = "error";
    }
    else if (mapping[len-1] == '-')
    {
        value = "ignore";
        len--;
    }
    else
        value = "error";

    StringAttr category(mapping, len);
    return addMapping(category, value);
 }

bool ErrorSeverityMapper::addMapping(const char * category, const char * value)
{
    if (!category || !*category)
    {
        ERRLOG("Error: No warning category supplied");
        return false;
    }

    //Ignore mappings with no action
    if (!value || !*value)
        return true;

    IAtom * action = createAtom(value);
    ErrorSeverity severity = getSeverity(action);
    if (severity == SeverityUnknown)
    {
        ERRLOG("Error: Invalid warning severity '%s'", value);
        return false;
    }

    if (isdigit(*category))
    {
        unsigned errorCode = atoi(category);
        addOnWarning(errorCode, action);
        return true;
    }

    WarnErrorCategory cat = getCategory(category);
    if (cat != CategoryUnknown)
    {
        categoryAction[cat] = severity;
        return true;
    }

    ERRLOG("Error: Mapping doesn't specify a valid warning code or category '%s'", category);
    return false;
}

void ErrorSeverityMapper::addOnWarning(unsigned code, IAtom * action)
{
    severityMappings.append(*createAttribute(onWarningAtom, getSizetConstant(code), createAttribute(action)));
}

void ErrorSeverityMapper::addOnWarning(IHqlExpression * setMetaExpr)
{
    IHqlExpression * code = setMetaExpr->queryChild(1);
    IHqlExpression * action = setMetaExpr->queryChild(2);
    if (isStringType(code->queryType()))
    {
        StringBuffer text;
        getStringValue(text, code, NULL);
        WarnErrorCategory cat = getCategory(text);
        ErrorSeverity severity = getSeverity(action->queryName());
        if (cat == CategoryUnknown)
            throwError1(HQLERR_InvalidErrorCategory, text.str());

        categoryAction[cat] = severity;
    }
    else
    {
        severityMappings.append(*createAttribute(onWarningAtom, LINK(code), LINK(action)));
    }
}

unsigned ErrorSeverityMapper::processMetaAnnotation(IHqlExpression * expr)
{
    unsigned prevMax = severityMappings.ordinality();
    gatherMetaAttributes(severityMappings, onWarningAtom, expr);
    return prevMax;
}

void ErrorSeverityMapper::restoreLocalOnWarnings(unsigned prevMax)
{
    severityMappings.trunc(prevMax);
}

void ErrorSeverityMapper::pushSymbol(ErrorSeverityMapperState & saved, IHqlExpression * _symbol)
{
    saveState(saved);
    setSymbol(_symbol);
}

void ErrorSeverityMapper::saveState(ErrorSeverityMapperState & saved) const
{
    saved.firstActiveMapping = firstActiveMapping;
    saved.maxMappings = severityMappings.ordinality();
    saved.symbol = activeSymbol;
}

void ErrorSeverityMapper::setSymbol(IHqlExpression * _symbol)
{
    firstActiveMapping = severityMappings.ordinality();
    activeSymbol = _symbol;
}

void ErrorSeverityMapper::restoreState(const ErrorSeverityMapperState & saved)
{
    severityMappings.trunc(saved.maxMappings);
    firstActiveMapping = saved.firstActiveMapping;
    activeSymbol = saved.symbol;
}

void ErrorSeverityMapper::exportMappings(IWorkUnit * wu) const
{
    IndirectErrorReceiver::exportMappings(wu);

    const unsigned max = severityMappings.ordinality();
    for (unsigned i=firstActiveMapping; i < max; i++)
    {
        IHqlExpression & cur = severityMappings.item(i);
        wu->setWarningSeverity((unsigned)getIntValue(cur.queryChild(0)), getCheckSeverity(cur.queryChild(1)->queryName()));
    }
}


IError * ErrorSeverityMapper::mapError(IError * error)
{
    //An error that is fatal cannot be mapped.
    Owned<IError> mappedError = IndirectErrorReceiver::mapError(error);
    if (!isFatal(mappedError))
    {
        //This takes precedence over mappings in the parent
        ErrorSeverity newSeverity = getWarningAction(mappedError->errorCode(), severityMappings, firstActiveMapping, SeverityUnknown);
        if (newSeverity != SeverityUnknown)
            return mappedError->cloneSetSeverity(newSeverity);

        WarnErrorCategory category = mappedError->getCategory();
        if (categoryAction[category] != SeverityUnknown)
            return mappedError->cloneSetSeverity(categoryAction[category]);

        if (categoryAction[CategoryAll] != SeverityUnknown)
            return mappedError->cloneSetSeverity(categoryAction[CategoryAll]);
    }
    return mappedError.getClear();
}

//---------------------------------------------------------------------------------------------------------------------

bool isGlobalOnWarning(IHqlExpression * expr)
{
    return ((expr->getOperator() == no_setmeta) && (expr->queryChild(0)->queryName() == onWarningAtom));
}

//---------------------------------------------------------------------------

static HqlTransformerInfo globalOnWarningCollectorInfo("GlobalOnWarningCollector");
class GlobalOnWarningCollector : public QuickHqlTransformer
{
public:
    GlobalOnWarningCollector(ErrorSeverityMapper & _mapper) :
        QuickHqlTransformer(globalOnWarningCollectorInfo, NULL), mapper(_mapper)
    {
    }

    virtual void doAnalyse(IHqlExpression * expr)
    {
        IHqlExpression * body = expr->queryBody();
        //Ugly...  If the syntax check is called on something containing a forward module then this code
        //might be called with a placeholder symbol (which has a NULL body).
        if (!body)
            return;

        if (isGlobalOnWarning(body))
            mapper.addOnWarning(body);
        QuickHqlTransformer::doAnalyse(body);
    }

protected:
    ErrorSeverityMapper & mapper;
};


static HqlTransformerInfo warningCollectingTransformerInfo("WarningCollectingTransformer");
class WarningCollectingTransformer : public QuickHqlTransformer
{
public:
    WarningCollectingTransformer(IErrorReceiver & _errs) :
        QuickHqlTransformer(warningCollectingTransformerInfo, &_errs), mapper(_errs)
    {
    }

    virtual void doAnalyse(IHqlExpression * expr)
    {
        switch (expr->getAnnotationKind())
        {
        case annotate_meta:
            {
                unsigned max = mapper.processMetaAnnotation(expr);
                QuickHqlTransformer::doAnalyse(expr);
                mapper.restoreLocalOnWarnings(max);
                return;
            }
        case annotate_warning:
            {
                IError * error = static_cast<CHqlWarningAnnotation *>(expr)->queryWarning();
                Owned<IError> mappedError = mapper.mapError(error);
                mapper.report(mappedError);
                break;
            }
        case annotate_symbol:
            {
                ErrorSeverityMapper::SymbolScope saved(mapper, expr);
                QuickHqlTransformer::doAnalyse(expr);
                return;
            }
        }
        QuickHqlTransformer::doAnalyse(expr);
    }

protected:
    ErrorSeverityMapper mapper;
};


void gatherParseWarnings(IErrorReceiver * errs, IHqlExpression * expr, IErrorArray & orphanedWarnings)
{
    if (!errs || !expr)
        return;

    Owned<IErrorReceiver> deduper = createDedupingErrorReceiver(*errs);

    //First collect any #ONWARNINGs held in the parsed expression tree
    Owned<ErrorSeverityMapper> globalOnWarning = new ErrorSeverityMapper(*deduper);
    GlobalOnWarningCollector globalCollector(*globalOnWarning);
    globalCollector.analyse(expr);

    //Now walk all expressions, outputting warnings and processing local onWarnings
    WarningCollectingTransformer warningCollector(*globalOnWarning);
    warningCollector.analyse(expr);

    ForEachItemIn(i, orphanedWarnings)
    {
        Owned<IError> mappedError = globalOnWarning->mapError(&orphanedWarnings.item(i));
        globalOnWarning->report(mappedError);
    }
}


//---------------------------------------------------------------------------

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

template <class ARRAY>
bool doArraysSame(ARRAY & left, ARRAY & right)
{
    if (left.ordinality() != right.ordinality())
        return false;
    return memcmp(left.getArray(), right.getArray(), left.ordinality() * sizeof(CInterface*)) == 0;
}

bool arraysSame(HqlExprArray & left, HqlExprArray & right)
{
    return doArraysSame(left, right);
}

bool arraysSame(HqlExprCopyArray & left, HqlExprCopyArray & right)
{
    return doArraysSame(left, right);
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
        return expr->hasAttribute(keyedAtom);
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
    for (;;)
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
        return expr->hasAttribute(fieldAtom);
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

    for (;;)
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

        enum { ServiceApi, RtlApi, BcdApi, CApi, CppApi, LocalApi } api = ServiceApi;
        if (body->hasAttribute(eclrtlAtom))
            api = RtlApi;
        else if (body->hasAttribute(bcdAtom))
            api = BcdApi;
        else if (body->hasAttribute(cAtom))
            api = CApi;
        else if (body->hasAttribute(cppAtom))
            api = CppApi;
        else if (body->hasAttribute(localAtom))
            api = LocalApi;

        StringBuffer entrypoint;
        getAttribute(body, entrypointAtom, entrypoint);
        if (entrypoint.length() == 0)
            return false;

        if ((api == ServiceApi) || api == CApi)
        {
            mangled.append(entrypoint); // extern "C"
            return true;
        }

        if (body->hasAttribute(oldSetFormatAtom))
            return false;

        mangled.append("_Z");
        StringBuffer namespaceValue;
        getAttribute(body, namespaceAtom, namespaceValue);
        if (namespaceValue.length())
            mangled.append("N").append(namespaceValue.length()).append(namespaceValue);
        mangled.append(entrypoint.length()).append(entrypoint);
        if (namespaceValue.length())
            mangled.append("E");

        StringBuffer mangledReturn;
        StringBuffer mangledReturnParameters;
        mangleFunctionReturnType(mangledReturn, mangledReturnParameters, retType);

        if (functionBodyUsesContext(body))
            mangled.append("P12ICodeContext");
        else if (body->hasAttribute(globalContextAtom) )
            mangled.append("P18IGlobalCodeContext");
        else if (body->hasAttribute(userMatchFunctionAtom))
            mangled.append("P12IMatchWalker");

        mangled.append(mangledReturnParameters);

        if (formals->numChildren())
        {
            bool hasMeta = getBoolAttribute(body, passParameterMetaAtom, false);
            ForEachChild(i, formals)
            {
                IHqlExpression * param = formals->queryChild(i);
                ITypeInfo *paramType = param->queryType();

                bool isOut = param->hasAttribute(outAtom);
                bool isConst = !param->hasAttribute(noConstAtom);

                if (isOut)
                    mangled.append("R");
                if (!mangleSimpleType(mangled, paramType, isConst, hasMeta))
                    return false;
            }
        }
        else
            mangled.append('v');
        return true;
    }

protected:
    bool mangleSimpleType(StringBuffer & result, ITypeInfo * type, bool hasConst, bool hasMeta)
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
            result.append(lookupRepeat(hasConst ? "PKc" : "Pc"));
            return true;
        case type_varstring:
            result.append(lookupRepeat(hasConst ? "PKc" : "Pc"));
            return true;
        case type_data:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("j");
            result.append(lookupRepeat(hasConst ? "PKv" : "Pv"));
            return true;
        case type_unicode:
            if (type->getSize() == UNKNOWN_LENGTH)
                result.append("j");
            result.append(lookupRepeat(hasConst ? "PKt" : "Pt"));
            return true;
        case type_varunicode:
            result.append(lookupRepeat(hasConst ? "PKt" : "Pt"));
            return true;
        case type_char:
            result.append("c");
            return true;
        case type_enumerated:
            return mangleSimpleType(result, type->queryChildType(), hasConst, hasMeta);
        case type_pointer:
            result.append("P");
            return mangleSimpleType(result, type->queryChildType(), hasConst, hasMeta);
        case type_array:
            result.append("A").append(type->getSize()).append("_");;
            return mangleSimpleType(result, type->queryChildType(), hasConst, hasMeta);
        case type_table:
        case type_groupedtable:
            result.append("j"); // size32_t
            result.append(lookupRepeat(hasConst ? "PKv" : "Pv")); // [const] void *
            return true;
        case type_set:
            result.append("b"); // bool
            result.append("j"); // unsigned
            result.append(lookupRepeat(hasConst ? "PKv" : "Pv")); // *
            return true;
        case type_row:
            if (hasMeta)
                result.append(lookupRepeat("R15IOutputMetaData"));
            result.append(lookupRepeat("PKh"));  // Does not seem to depend on const
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
        case type_record:
            result.append(lookupRepeat("R15IOutputMetaData"));
            return true;
        }
        throwUnexpected();
    }

    StringArray repeatsSeen;
    StringBuffer thisRepeat;
    const char *lookupRepeat(const char *typeStr)
    {
        ForEachItemIn(idx, repeatsSeen)
        {
            if (streq(repeatsSeen.item(idx), typeStr))
                return thisRepeat.appendf("S%d_", idx).str();
        }
        repeatsSeen.append(typeStr);
        return typeStr;
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
// Assuming Windows on ARM64 will have the same mangling
#ifdef __64BIT__
        pointerBaseCode.set("E");
#endif
    }

    bool mangle(StringBuffer & mangled, IHqlExpression * funcdef)
    {
        IHqlExpression *body = funcdef->queryChild(0);
        IHqlExpression *formals = funcdef->queryChild(1);

        enum { ServiceApi, RtlApi, BcdApi, CApi, LocalApi } api = ServiceApi;
        if (body->hasAttribute(eclrtlAtom))
            api = RtlApi;
        else if (body->hasAttribute(bcdAtom))
            api = BcdApi;
        else if (body->hasAttribute(cAtom))
            api = CApi;
        else if (body->hasAttribute(localAtom))
            api = LocalApi;

        StringBuffer entrypoint;
        getAttribute(body, entrypointAtom, entrypoint);
        if (entrypoint.length() == 0)
            return false;

        if ((api == ServiceApi) || api == CApi)
        {
            mangled.append(entrypoint); // extern "C"
            return true;
        }

        if (body->hasAttribute(oldSetFormatAtom))
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

        if (functionBodyUsesContext(body))
            mangled.append("PVICodeContext@@");
        else if (body->hasAttribute(globalContextAtom) )
            mangled.append("PVIGlobalCodeContext@@");
        else if (body->hasAttribute(userMatchFunctionAtom))
            mangled.append("PVIMatchWalker@@");

        if (mangledReturnParameters.length())
            mangled.append(mangledReturnParameters);

        ForEachChild(i, formals)
        {
            IHqlExpression * param = formals->queryChild(i);
            ITypeInfo *paramType = param->queryType();

            bool isOut = param->hasAttribute(outAtom);
            bool isConst = !param->hasAttribute(noConstAtom);

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
    IHqlExpression * xpathAttr = field->queryAttribute(xpathAtom);
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
        IHqlExpression * namedAttr = field->queryAttribute(namedAtom);
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

    IHqlExpression * xpath = field->queryAttribute(xpathAtom);
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

    void build(IHqlExpression * record, bool &hasMixedContent, unsigned keyedCount) const;
    void build(IHqlExpression * record, unsigned keyedCount) const {bool mixed; build(record, mixed, keyedCount);}


protected:
    void extractName(StringBuffer & name, StringBuffer * itemName, StringBuffer * valueName, IHqlExpression * field, const char * defaultItemName) const;

protected:
    ISchemaBuilder & builder;
    bool useXPath;
};



void EclXmlSchemaBuilder::build(IHqlExpression * record, bool &hasMixedContent, unsigned keyedCount) const
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
                        unsigned updateMixed=0;
                        builder.beginRecord(name, false, &updateMixed);
                        bool mixed = false;
                        build(cur->queryRecord(), (name.length()) ? mixed : hasMixedContent, 0);
                        if (mixed)
                            builder.updateMixedRecord(updateMixed, true);
                        builder.endRecord(name);
                        break;
                    }
                case type_set:
                    {
                        extractName(name.clear(), &childName.clear(), NULL, cur, "Item");
                        if (name.length())
                            builder.addSetField(name, childName, *type);
                        else
                            hasMixedContent = true;
                        break;
                    }
                case type_dictionary:
                case type_table:
                case type_groupedtable:
                    {
                        extractName(name.clear(), &childName.clear(), NULL, cur, "Row");
                        ITypeInfo * singleFieldType = (useXPath && name.length() && childName.length()) ? containsSingleSimpleFieldBlankXPath(cur->queryRecord()) : NULL;
                        if (!singleFieldType || !builder.addSingleFieldDataset(name, childName, *singleFieldType))
                        {
                            unsigned updateMixed = 0;
                            bool mixed = false;
                            if (builder.beginDataset(name, childName, false, &updateMixed))
                            {
                                build(cur->queryRecord(), (name.length()) ? mixed : hasMixedContent, 0);
                                if (mixed)
                                    builder.updateMixedRecord(updateMixed, true);
                            }
                            builder.endDataset(name, childName);
                        }
                        break;
                    }
                case type_alien:
                    type = queryAlienType(type)->queryLogicalType();
                    //fallthrough
                default:
                    extractName(name.clear(), NULL, NULL, cur, NULL);
                    if (name.length())
                        builder.addField(name, *type, i < keyedCount);
                    else
                        hasMixedContent = true;
                    break;
                }
                break;
            }
        case no_ifblock:
            builder.beginIfBlock();
            build(cur->queryChild(1), hasMixedContent, 0);
            builder.endIfBlock();
            break;
        case no_record:
            build(cur, hasMixedContent, 0);
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


void getRecordXmlSchema(StringBuffer & result, IHqlExpression * record, bool useXPath, unsigned keyedCount)
{
    XmlSchemaBuilder xmlbuilder(false);
    EclXmlSchemaBuilder builder(xmlbuilder, useXPath);
    builder.build(record, keyedCount);
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
                OwnedHqlExpr storedName = createConstant(str(formal->queryName()));
                OwnedHqlExpr stored = createValue(no_stored, makeVoidType(), storedName.getClear());
                HqlExprArray colonArgs;
                colonArgs.append(*LINK(nullValue));
                colonArgs.append(*LINK(stored));
                args.append(*createWrapper(no_colon, formal->queryType(), colonArgs));
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
    bool processFieldValue(IHqlExpression * optField, ITypeInfo * lhsType, IHqlExpression * rhs);

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

bool ConstantRowCreator::processFieldValue(IHqlExpression * optLhs, ITypeInfo * lhsType, IHqlExpression * rhs)
{
    size32_t lenLhs = lhsType->getStringLen();
    size32_t sizeLhs  = lhsType->getSize();
    node_operator rhsOp = rhs->getOperator();

    switch (lhsType->getTypeCode())
    {
    case type_packedint:
    {
        if (!rhs->queryValue())
            return false;
        unsigned orig = out.length();
        void *tgt = out.reserve(9);
        if (lhsType->isSigned())
            rtlSetPackedSigned(tgt, rhs->queryValue()->getIntValue());
        else
            rtlSetPackedUnsigned(tgt, rhs->queryValue()->getIntValue());
        unsigned actualSize = rtlGetPackedSize(tgt);
        out.setLength(orig+actualSize);
        return true;
    }
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
        if (rhsOp == no_list)
        {
            ITypeInfo * elemType = lhsType->queryChildType();
            out.append(false);
            unsigned patchOffset = out.length();
            out.reserve(sizeof(size32_t));
            const size_t startOffset = out.length();
            ForEachChild(i, rhs)
            {
                if (!processFieldValue(NULL, elemType, rhs->queryChild(i)))
                    return false;
            }
            const size_t setLength = out.length() - startOffset;
            out.writeDirect(patchOffset, sizeof(size32_t), &setLength);
            byte * patchPos = (byte *)out.bufferBase() + patchOffset;
            rtlWriteSize32t(patchPos, setLength);
            return true;
        }
        return false;
    case type_row:
        if (rhsOp == no_null)
            return createConstantNullRow(out, queryOriginalRecord(lhsType));
        if (rhsOp == no_createrow)
            return createConstantRow(out, rhs->queryChild(0));
        return false;
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        {
            assertex(optLhs);
            IHqlExpression * field = optLhs->queryChild(1);
            if (!field->hasAttribute(countAtom) && !field->hasAttribute(sizeofAtom))
            {
                if (field->hasAttribute(_linkCounted_Atom))
                {
                    if (rhsOp == no_null)
                    {
                        rtlWriteSize32t(out.reserve(sizeof(size32_t)), 0);
                        memset(out.reserve(sizeof(byte * *)), 0, sizeof(byte * *));
                        return true;
                    }
                }
                else
                {
                    switch (rhsOp)
                    {
                    case no_null:
                    {
                        rtlWriteSize32t(out.reserve(sizeof(size32_t)), 0);
                        return true;
                    }
                    case no_inlinetable:
                    {
                        unsigned patchOffset = out.length();
                        out.reserve(sizeof(size32_t));
                        unsigned startOffset = out.length();
                        IHqlExpression * transforms = rhs->queryChild(0);
                        ForEachChild(i, transforms)
                        {
                            if (!createConstantRow(out, transforms->queryChild(i)))
                                return false;
                        }
                        byte * patchPos = (byte *)out.bufferBase() + patchOffset;
                        rtlWriteSize32t(patchPos, out.length() - startOffset);
                        return true;
                    }
                    }
                }
            }
            return false;
        }
    }

    if ((lenLhs != UNKNOWN_LENGTH) && (lenLhs > maxSensibleInlineElementSize))
        return false;

    OwnedHqlExpr castRhs = ensureExprType(rhs, lhsType);
    IValue * castValue = castRhs->queryValue();
    if (!castValue)
        return false;
    
    if (optLhs && mapper)
        mapper->setMapping(optLhs, castRhs);

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
    case type_varunicode:
    {
        if (sizeLhs == UNKNOWN_LENGTH)
        {
            void * target = out.reserve((lenValue+1)*sizeof(UChar));
            castValue->toMem(target);
        }
        else
        {
            UChar * target = (UChar *) out.reserve(sizeLhs);
            for (size32_t pos = 0; pos < sizeLhs/sizeof(UChar); pos++)
                target[pos] = (UChar) ' ';
            castValue->toMem(target);
        }
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
                void * target = out.reserve(sizeLhs);
                memset(target, ' ', sizeLhs);   // spaces expand better in the c++
                castValue->toMem(target);
            }
            return true;
        }
    case type_utf8:
    case type_unicode:
    case type_qstring:
        {
            if (lenLhs == UNKNOWN_LENGTH)
                rtlWriteInt4(out.reserve(sizeof(size32_t)), lenValue);
            castValue->toMem(out.reserve(castValue->getSize()));
            return true;
        }
    }
    return false;
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

            return processFieldValue(match->queryChild(0), expr->queryType(), match->queryChild(1));
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

    if (recordContainsIfBlock(transform->queryRecord()))
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

bool createConstantField(MemoryBuffer & target, IHqlExpression * field, IHqlExpression * value)
{
    ConstantRowCreator builder(target);
    return builder.processFieldValue(field, field->queryType(), value);
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


IError * annotateExceptionWithLocation(IException * e, IHqlExpression * location)
{
    StringBuffer errorMsg;
    e->errorMessage(errorMsg);
    unsigned code = e->errorCode();
    return createError(code, errorMsg.str(), str(location->querySourcePath()), location->getStartLine(), location->getStartColumn(), 0);
}

StringBuffer & appendLocation(StringBuffer & s, IHqlExpression * location, const char * suffix)
{
    if (location)
    {
        int line = location->getStartLine();
        int column = location->getStartColumn();
        s.append(str(location->querySourcePath()));
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

static bool doReportDroppedFields(IHqlSimpleScope * newScope, IHqlExpression * oldRecord, IErrorReceiver &err, ECLlocation &location)
{
    bool allDropped = true; // until we find one that isn't
    ForEachChild(i, oldRecord)
    {
        IHqlExpression * cur = oldRecord->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            allDropped = doReportDroppedFields(newScope, cur, err, location) && allDropped;
            break;
        case no_ifblock:
            allDropped = doReportDroppedFields(newScope, cur->queryChild(1), err, location) && allDropped;
            break;
        case no_field:
            {
                OwnedHqlExpr newField = newScope->lookupSymbol(cur->queryId());
                if (!newField)
                {
                    VStringBuffer msg("Field %s is present in DFS file but not in ECL definition", str(cur->queryId()));
                    err.reportWarning(CategoryInformation, HQLINFO_FieldNotPresentInECL, msg.str(), str(location.sourcePath), location.lineno, location.column, location.position);
                }
                else
                {
                    allDropped = false;
                    if (newField->queryType() != cur->queryType())
                    {
                        VStringBuffer msg("Field %s type mismatch: DFS reports ", str(cur->queryId()));
                        cur->queryType()->getECLType(msg).append(" but ECL declared ");
                        newField->queryType()->getECLType(msg);
                        err.reportWarning(CategoryDFS, HQLWRN_DFSlookupTypeMismatch, msg.str(), str(location.sourcePath), location.lineno, location.column, location.position);
                    }
                }
            }
        }
    }
    return allDropped;
}

void reportDroppedFields(IHqlExpression * newRecord, IHqlExpression * oldRecord, IErrorReceiver &err, ECLlocation &location)
{
    if (doReportDroppedFields(newRecord->querySimpleScope(), oldRecord, err, location))
    {
        err.reportWarning(CategoryDFS, HQLWRN_NoFieldsMatch, "No matching fields found in ECL definition", str(location.sourcePath), location.lineno, location.column, location.position);
    }
}

//---------------------------------------------------------------------------------------------------------------------

static void createMappingAssigns(HqlExprArray & assigns, IHqlExpression * selfSelector, IHqlExpression * oldSelector, IHqlSimpleScope * oldScope, IHqlExpression * newRecord, bool replaceMissingWithDefault, IErrorReceiver &err, ECLlocation &location)
{
    ForEachChild(i, newRecord)
    {
        IHqlExpression * cur = newRecord->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            createMappingAssigns(assigns, selfSelector, oldSelector, oldScope, cur, replaceMissingWithDefault, err, location);
            break;
        case no_ifblock:
            createMappingAssigns(assigns, selfSelector, oldSelector, oldScope, cur->queryChild(1), replaceMissingWithDefault, err, location);
            break;
        case no_field:
            {
                OwnedHqlExpr oldSelected;
                OwnedHqlExpr oldField = oldScope->lookupSymbol(cur->queryId());
                if (!oldField)
                {
                    assertex(replaceMissingWithDefault);
                    oldSelected.setown(createNullExpr(cur));
                    VStringBuffer msg("Field %s is not present in DFS file - default value will be used", str(cur->queryId()));
                    err.reportWarning(CategoryInformation, HQLWRN_FieldNotPresentInDFS, msg.str(), str(location.sourcePath), location.lineno, location.column, location.position);
                }
                else
                {
                    oldSelected.setown(createSelectExpr(LINK(oldSelector), LINK(oldField)));
                }
                OwnedHqlExpr selfSelected = createSelectExpr(LINK(selfSelector), LINK(cur));
                if (selfSelected->queryRecord() != oldSelected->queryRecord())
                {
                    if (!oldSelected->isDatarow())
                    {
                        assertex(replaceMissingWithDefault);
                        VStringBuffer msg("Field %s cannot be mapped - incompatible type ", str(cur->queryId()));
                        cur->queryType()->getECLType(msg).append(" (expected ");
                        getFriendlyTypeStr(oldSelected->queryType(),msg).append(')');
                        err.reportError(HQLERR_DFSlookupIncompatible, msg.str(), str(location.sourcePath), location.lineno, location.column, location.position);
                    }
                    OwnedHqlExpr childSelf = getSelf(cur);
                    OwnedHqlExpr childTransform = createMappingTransform(childSelf, oldSelected, replaceMissingWithDefault, err, location);
                    OwnedHqlExpr createRowExpr = createRow(no_createrow, childTransform.getClear());
                    assigns.append(*createAssign(selfSelected.getClear(), createRowExpr.getClear()));
                }
                else
                {
                    if (!cur->queryType()->assignableFrom(oldSelected->queryType()))
                    {
                        assertex(replaceMissingWithDefault);
                        VStringBuffer msg("Field %s cannot be mapped - incompatible type ", str(cur->queryId()));
                        cur->queryType()->getECLType(msg).append(" (expected ");
                        getFriendlyTypeStr(oldSelected->queryType(),msg).append(')');
                        err.reportError(HQLERR_DFSlookupIncompatible, msg.str(), str(location.sourcePath), location.lineno, location.column, location.position);
                    }
                    assigns.append(*createAssign(selfSelected.getClear(), oldSelected.getClear()));
                }
            }
        }
    }
}

IHqlExpression * createMappingTransform(IHqlExpression * selfSelector, IHqlExpression * inSelector, bool replaceMissingWithDefault, IErrorReceiver &err, ECLlocation &location)
{
    HqlExprArray assigns;
    IHqlExpression * selfRecord = selfSelector->queryRecord();
    IHqlExpression * inRecord = inSelector->queryRecord();
    createMappingAssigns(assigns, selfSelector, inSelector, inRecord->querySimpleScope(), selfRecord, replaceMissingWithDefault, err, location);
    return createValue(no_transform, makeTransformType(selfRecord->getType()), assigns);

}


//---------------------------------------------------------------------------------------------------------------------

IHqlExpression * expandMacroDefinition(IHqlExpression * expr, HqlLookupContext & ctx, bool reportError)
{
    assertex(expr->isMacro());

    Owned<IProperties> macroParms = createProperties();
    IHqlExpression * macroBodyExpr;
    if (expr->getOperator() == no_funcdef)
    {
        IHqlExpression * formals = expr->queryChild(1);
        IHqlExpression * defaults = expr->queryChild(2);
        ForEachChild(i, formals)
        {
            IHqlExpression* formal = formals->queryChild(i);
            IHqlExpression* def = queryDefaultValue(defaults, i);

            StringBuffer curParam;
            if (!def || !getFoldedConstantText(curParam, def))
            {
                if (reportError)
                    ctx.errs->reportError(HQLERR_CannotSubmitMacroX, "Cannot submit a MACRO with parameters that do not have default values", NULL, 1, 0, 0);
                return NULL;
            }
            macroParms->setProp(str(formal->queryName()), curParam.str());
        }
        macroBodyExpr = expr->queryChild(0);
    }
    else
        macroBodyExpr = expr;

    IFileContents * macroContents = static_cast<IFileContents *>(macroBodyExpr->queryUnknownExtra());
    size32_t len = macroContents->length();

    //Strangely some macros still have the ENDMACRO on the end, and others don't.  This should be removed really.
    StringBuffer macroText;
    macroText.append(len, macroContents->getText());
    if ((len >= 8) && strieq(macroText.str()+(len-8),"ENDMACRO"))
        macroText.setLength(len-8);
    //Now append a semi colon since that is how macros are normally called.
    macroText.append(";");

    //This might be cleaner if it was implemented by parsing the text myModule.myAttribute().
    //It would make implementing default parameters easy.  However it could introduce other problems
    //with implicitly importing myModule.
    Owned<IFileContents> mappedContents = createFileContentsFromText(macroText.length(), macroText.str(), macroContents->querySourcePath(), false, NULL);
    Owned<IHqlScope> scope = createPrivateScope();
    if (queryLegacyImportSemantics())
        importRootModulesToScope(scope, ctx);
    return parseQuery(scope, mappedContents, ctx, NULL, macroParms, true, true);
}

static IHqlExpression * transformAttributeToQuery(IHqlExpression * expr, HqlLookupContext & ctx, bool syntaxCheck)
{
    if (expr->isMacro())
        return expandMacroDefinition(expr, ctx, true);

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
                const char * name = str(expr->queryName());
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
        OwnedHqlExpr main = scope->lookupSymbol(createIdAtom("main"), LSFpublic, ctx);
        if (main)
            return main.getClear();

        if (!syntaxCheck)
        {
            StringBuffer msg;
            const char * name = scope->queryFullName();
            msg.appendf("Module %s does not EXPORT an attribute main()", name ? name : "");
            ctx.errs->reportError(HQLERR_CannotSubmitModule, msg.str(), NULL, 1, 0, 0);
            return NULL;
        }
    }

    return LINK(expr);
}

IHqlExpression * convertAttributeToQuery(IHqlExpression * expr, HqlLookupContext & ctx, bool syntaxCheck)
{
    OwnedHqlExpr query = LINK(expr);
    for (;;)
    {
        OwnedHqlExpr transformed = transformAttributeToQuery(query, ctx, syntaxCheck);
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

//---------------------------------------------------------------------------------------------------------------------

/*
Aliases are nasty...they can occur in two different situations
i) The user specifies TABLE(x) to create an alias
ii) The scope checking spots that an alias is being implicitly created.

1) exists(join(ds, ds, left.id*3=right.id));
ds_1 := table(ds);
ds(exists(ds_1(ds_1.id=ds.id*3)));

a) ds is a table
b) ds is a filtered table.
c) ds is an implicitly normalized dataset (ds.child);
d) ds is a projected table
e) ds is a filtered projected table.

2) ds(exists(join(child, child, left.id*3=right.id)));
child_1 = table(ds.child);
ds(exists(child(exists(child1(child_1.id = child.id*3)));

a) ds is a table
b) ds is a filtered table.
c) ds is an implicitly normalized dataset (ds.child);
d) ds is a projected table
e) ds is a filtered projected table.

When either of these occurs a no_dataset_alias node is added to the tree with a unique id.  We don't want to modify
any of the input datasets - because we want them to stay common as long as possible - otherwise code like
ds(field in ds(filter))  would cause ds to become split in two - and it should mean the same thing.

For implicit aliases they will be added around the dataset that is ambiguous.
- It would be simpler to add them around the table that is ambiguous (Table is a dataset that defines a column list)
  but that means that sort, filters etc. aren't commoned up.
- When the code is actually generated the base table is modified - which ensures no ambiguous expressions are
  actually present when generating.

E.g,
x := ds(a <> 0);
x(b in set(x(c <> 0), b))
becomes
x := ds(a <> 0);
x' = table(x);
x'(b in set(x(c <> 0), b))

To avoid that the aliases is not added around a dataset that has already been aliased in the dataset that uses it.

When the expression comes to be generated/evaluated, the underlying table of the dataset expression is modified to
include a unique id.  The root table doesn't need to be modified because no selectors for that can be in scope.

*/

IHqlExpression * queryTableOrSplitter(IHqlExpression * expr)
{
    for (;;)
    {
        node_operator op = expr->getOperator();
        if (op == no_compound)
            expr = expr->queryChild(1);
        else if (definesColumnList(expr))
            return expr;
        else if (op == no_split)
            return expr;
        else
            expr = expr->queryChild(0);
    }
}

//Convert no_dataset_alias(expr, uid) to expr'
IHqlExpression * normalizeDatasetAlias(IHqlExpression * expr)
{
    IHqlExpression * uid = expr->queryAttribute(_uid_Atom);
    assertex(uid);
    IHqlExpression * dataset = expr->queryChild(0);
    IHqlExpression * table = queryTableOrSplitter(dataset);

    //If the alias is based on a splitter then we need to ensure the splitter expression stays the same - otherwise
    //if won't be commoned up.  So add a alias with a _normalized_Atom to ensure everything followed that will be
    //unique.  Otherwise add a unique id onto the underlying table to ensure unique expressions.
    OwnedHqlExpr newTable;
    node_operator tableOp = table->getOperator();
    if ((tableOp == no_split) || (tableOp == no_rows))
        newTable.setown(createDataset(no_dataset_alias, LINK(table), createComma(createUniqueId(), createAttribute(_normalized_Atom))));
    else
        newTable.setown(appendOwnedOperand(table, LINK(uid)));
    return replaceDataset(dataset, table, newTable);
}

//---------------------------------------------------------------------------------------------------------------------

//This should only be called on source activities, and on inline datasets.
IHqlExpression * normalizeAnyDatasetAliases(IHqlExpression * expr)
{
    //It is useful to also be able to call this on no_sum(aliased-dataset)
    if (!containsDatasetAliasLocally(expr) && !expr->isAggregate())
        return LINK(expr);

    node_operator op = expr->getOperator();
    IHqlExpression * selector = NULL;
    switch (getChildDatasetType(expr))
    {
    case childdataset_none:
        if ((op == no_select) && isNewSelector(expr))
            break;
        return LINK(expr);
    case childdataset_dataset:
    case childdataset_dataset_noscope:
    case childdataset_datasetleft:
    case childdataset_top_left_right:
        selector = expr->queryChild(0)->queryNormalizedSelector();
        break;
    case childdataset_left:
    case childdataset_leftright:
    case childdataset_many:
    case childdataset_many_noscope:
        break;
    default:
        return LINK(expr);
        throwUnexpected();
    }

    bool same = true;
    HqlExprArray args;
    unsigned max = getNumChildTables(expr);
    for (unsigned i=0; i < max; i++)
    {
        IHqlExpression * dataset = expr->queryChild(i);
        OwnedHqlExpr newDataset = normalizeAnyDatasetAliases(dataset);
        if (dataset != newDataset)
            same = false;
        args.append(*newDataset.getClear());
    }

    OwnedHqlExpr transformed;
    if (same)
        transformed.set(expr);
    else
    {
        if (selector)
        {
            assertex(max == 1);
            replaceSelectors(args, expr, max, selector, args.item(0).queryNormalizedSelector());
        }
        else
            unwindChildren(args, expr, max);
        transformed.setown(expr->clone(args));
    }

    if ((op == no_dataset_alias) && !transformed->hasAttribute(_normalized_Atom))
        return normalizeDatasetAlias(transformed);
    return transformed.getClear();
}

bool userPreventsSort(IHqlExpression * noSortAttr, node_operator side)
{
    if (!noSortAttr)
        return false;

    IHqlExpression * child = noSortAttr->queryChild(0);
    if (!child)
        return true;

    IAtom * name = child->queryName();
    if (side == no_left)
        return name == leftAtom;
    if (side == no_right)
        return name == rightAtom;
    throwUnexpected();
}

//-------------------------------------------------------------------------------------------------

IHqlExpression * queryTransformAssign(IHqlExpression * transform, IHqlExpression * searchField)
{
    while (transform->getOperator() == no_alias_scope)
        transform = transform->queryChild(0);

    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        if (cur->getOperator() == no_alias_scope)
            cur = cur->queryChild(0);
        switch (cur->getOperator())
        {
        case no_assignall:
            {
                IHqlExpression * ret = queryTransformAssign(cur, searchField);
                if (ret)
                    return ret;
                break;
            }
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0)->queryChild(1);
                if (lhs == searchField)
                    return cur;
                if (lhs->queryId() == searchField->queryId())
                    return cur;
                break;
            }
        }
    }
    return NULL;
}

IHqlExpression * queryTransformAssignValue(IHqlExpression * transform, IHqlExpression * searchField)
{
    IHqlExpression * value = queryTransformAssign(transform, searchField);
    if (value)
        return value->queryChild(1);
    return NULL;
}

//-------------------------------------------------------------------------------------------------

IHqlExpression * convertSetToExpression(bool isAll, size32_t len, const void * ptr, ITypeInfo * setType)
{
    HqlExprArray results;
    const byte *presult = (const byte *) ptr;
    const byte *presult_end = presult + len;

    if (isAll)
        return createValue(no_all, LINK(setType));

    ITypeInfo * elementType = setType->queryChildType();
    switch(elementType->getTypeCode())
    {
        case type_unicode:
            while (presult < presult_end)
            {
                const size32_t numUChars = *((size32_t *) presult);
                presult += sizeof(size32_t);
                results.append(*createConstant(createUnicodeValue(numUChars, presult, LINK(elementType))));
                presult += numUChars*sizeof(UChar);
            };
            break;
        case type_string:
            while (presult < presult_end)
            {
                const size32_t numUChars = *((size32_t *) presult);
                presult += sizeof(size32_t);
                results.append(*createConstant(createStringValue( (const char*)presult, (unsigned)numUChars)));
                presult += numUChars;
            };
            break;
        default:
            UNIMPLEMENTED;
    }
    return createValue(no_list, LINK(setType), results);
}

//-------------------------------------------------------------------------------------------------

void getFieldTypeInfo(FieldTypeInfoStruct &out, ITypeInfo *type)
{
    assertex(type);
    type_t tc = type->getTypeCode();
    if (tc == type_record)
        type = queryUnqualifiedType(type);

    if (tc == type_alien)
    {
        ITypeInfo * physicalType = queryAlienType(type)->queryPhysicalType();
        if (physicalType->getSize() != UNKNOWN_LENGTH)
        {
            //Don't use the generated class for xml generation since it will generate physical rather than logical
            out.fieldType |= (RFTMalien|RFTMinvalidxml|RFTMnoserialize);
            type = physicalType;
            tc = type->getTypeCode();
        }
        else
        {
            out.fieldType |= RFTMunknownsize;
            //can't work out the size of the field - to keep it as unknown for the moment.
            //until the alien field type is supported
        }
    }
    out.fieldType |= tc;
    out.length = type->getSize();
    out.locale = nullptr;
    out.className = nullptr;
    if (out.length == UNKNOWN_LENGTH)
    {
        out.fieldType |= RFTMunknownsize;
        out.length = 0;
    }

    switch (tc)
    {
    case type_boolean:
        out.className = "RtlBoolTypeInfo";
        break;
    case type_real:
        out.className ="RtlRealTypeInfo";
        break;
    case type_date:
    case type_enumerated:
    case type_int:
        out.className = "RtlIntTypeInfo";
        if (!type->isSigned())
            out.fieldType |= RFTMunsigned;
        break;
    case type_swapint:
        out.className = "RtlSwapIntTypeInfo";
        if (!type->isSigned())
            out.fieldType |= RFTMunsigned;
        break;
    case type_packedint:
        out.className = "RtlPackedIntTypeInfo";
        if (!type->isSigned())
            out.fieldType |= RFTMunsigned;
        break;
    case type_decimal:
        out.className = "RtlDecimalTypeInfo";
        if (!type->isSigned())
            out.fieldType |= RFTMunsigned;
        out.length = type->getDigits() | (type->getPrecision() << 16);
        break;
    case type_char:
        out.className = "RtlCharTypeInfo";
        break;
    case type_data:
        out.className = "RtlDataTypeInfo";
        break;
    case type_qstring:
        out.className = "RtlQStringTypeInfo";
        out.length = type->getStringLen();
        break;
    case type_varstring:
        out.className = "RtlVarStringTypeInfo";
        if (type->queryCharset() && type->queryCharset()->queryName()==ebcdicAtom)
            out.fieldType |= RFTMebcdic;
        out.length = type->getStringLen();
        break;
    case type_string:
        out.className = "RtlStringTypeInfo";
        if (type->queryCharset() && type->queryCharset()->queryName()==ebcdicAtom)
            out.fieldType |= RFTMebcdic;
        break;
    case type_bitfield:
        {
        out.className = "RtlBitfieldTypeInfo";
        unsigned size = type->queryChildType()->getSize();
        unsigned bitsize = type->getBitSize();
        unsigned offset = (unsigned)getIntValue(queryAttributeChild(type, bitfieldOffsetAtom, 0),-1);
        bool isLastBitfield = (queryAttribute(type, isLastBitfieldAtom) != NULL);
        if (isLastBitfield)
            out.fieldType |= RFTMislastbitfield;
        if (!type->isSigned())
            out.fieldType |= RFTMunsigned;
        out.length = size | (bitsize << 8) | (offset << 16);
        break;
        }
    case type_record:
        {
            IHqlExpression * record = ::queryRecord(type);
            out.className = "RtlRecordTypeInfo";
            out.length = getMinRecordSize(record);
            if (!isFixedSizeRecord(record))
                out.fieldType |= RFTMunknownsize;
            break;
        }
    case type_row:
        {
            out.className = "RtlRowTypeInfo";
            if (hasLinkCountedModifier(type))
                out.fieldType |= RFTMlinkcounted;
            break;
        }
    case type_table:
    case type_groupedtable:
        {
            out.className = "RtlDatasetTypeInfo";
            if (hasLinkCountedModifier(type))
            {
                out.fieldType |= RFTMlinkcounted;
                out.fieldType &= ~RFTMunknownsize;
            }
            break;
        }
    case type_dictionary:
        {
            out.className = "RtlDictionaryTypeInfo";
            out.fieldType |= RFTMnoserialize;
            if (hasLinkCountedModifier(type))
            {
                out.fieldType |= RFTMlinkcounted;
                out.fieldType &= ~RFTMunknownsize;
            }
            break;
        }
    case type_set:
        out.className = "RtlSetTypeInfo";
        break;
    case type_unicode:
        out.className = "RtlUnicodeTypeInfo";
        out.locale = str(type->queryLocale());
        out.length = type->getStringLen();
        break;
    case type_varunicode:
        out.className = "RtlVarUnicodeTypeInfo";
        out.locale = str(type->queryLocale());
        out.length = type->getStringLen();
        break;
    case type_utf8:
        out.className = "RtlUtf8TypeInfo";
        out.locale = str(type->queryLocale());
        out.length = type->getStringLen();
        break;
    case type_blob:
    case type_pointer:
    case type_class:
    case type_array:
    case type_void:
    case type_alien:
    case type_none:
    case type_any:
    case type_pattern:
    case type_rule:
    case type_token:
    case type_feature:
    case type_event:
    case type_null:
    case type_scope:
    case type_transform:
    default:
        out.className = "RtlUnimplementedTypeInfo";
        out.fieldType |= (RFTMcontainsunknown|RFTMinvalidxml|RFTMnoserialize);
        break;
    }
}

bool checkXpathIsNonScalar(const char *xpath)
{
    return (strpbrk(xpath, "/?*[]<>")!=NULL); //anything other than a single tag/attr name cannot name a scalar field
}

unsigned buildRtlRecordFields(IRtlFieldTypeDeserializer &deserializer, unsigned &idx, const RtlFieldInfo * * fieldsArray, IHqlExpression *record, IHqlExpression *rowRecord)
{
    unsigned typeFlags = 0;
    ForEachChild(i, record)
    {
        unsigned fieldFlags = 0;
        IHqlExpression * field = record->queryChild(i);
        switch (field->getOperator())
        {
        case no_ifblock:
            typeFlags |= RFTMnoserialize;
            break;
        case no_field:
        {
            ITypeInfo *fieldType = field->queryType();
            switch (fieldType->getTypeCode())
            {
            case type_alien:
                //MORE:::
                break;
            case type_row:
                //Backward compatibility - should revisit
                fieldType = fieldType->queryChildType();
                break;
            case type_bitfield:
                UNIMPLEMENTED;
                break;
            }

            const RtlTypeInfo *type = buildRtlType(deserializer, fieldType);
            typeFlags |= type->fieldType & RFTMinherited;
            StringBuffer lowerName;
            lowerName.append(field->queryName()).toLowerCase();

            StringBuffer xpathName, xpathItem;
            switch (field->queryType()->getTypeCode())
            {
            case type_set:
                extractXmlName(xpathName, &xpathItem, NULL, field, "Item", false);
                break;
            case type_dictionary:
            case type_table:
            case type_groupedtable:
                extractXmlName(xpathName, &xpathItem, NULL, field, "Row", false);
                //Following should be in the type processing, and the type should include the information
                if (field->hasAttribute(sizeAtom) || field->hasAttribute(countAtom))
                    fieldFlags |= RFTMinvalidxml;
                break;
            default:
                extractXmlName(xpathName, NULL, NULL, field, NULL, false);
                break;
            }
            //Format of the xpath field is (nested-item 0x01 repeated-item)
            if (xpathItem.length())
                xpathName.append(xpathCompoundSeparatorChar).append(xpathItem);
            if (xpathName.charAt(0) == '@')
                fieldFlags |= RFTMhasxmlattr;
            if (checkXpathIsNonScalar(xpathName))
                fieldFlags |= RFTMhasnonscalarxpath;
            const char *xpath = xpathName.str();
            if (strcmp(lowerName, xpath)==0)
                xpath = nullptr;

            MemoryBuffer defaultInitializer;
            IHqlExpression *defaultValue = queryAttributeChild(field, defaultAtom, 0);
            if (defaultValue)
            {
                LinkedHqlExpr targetField = field;
                if (fieldType->getTypeCode() == type_bitfield)
                    targetField.setown(createField(field->queryId(), LINK(fieldType->queryChildType()), NULL));

                if (!createConstantField(defaultInitializer, targetField, defaultValue))
                    UNIMPLEMENTED;  // MORE - fail more gracefully!
            }
            fieldsArray[idx] = deserializer.addFieldInfo(lowerName, xpath, type, fieldFlags, (const char *) defaultInitializer.detach());
            typeFlags |= fieldFlags & RFTMinherited;
            idx++;
            break;
        }
        case no_record:
            typeFlags |= buildRtlRecordFields(deserializer, idx, fieldsArray, field, rowRecord);
            break;
        }
    }
    return typeFlags;
}

const RtlTypeInfo *buildRtlType(IRtlFieldTypeDeserializer &deserializer, ITypeInfo *type)
{
    assertex(type);
    switch (type->getTypeCode())
    {
    case type_alien:
        //MORE:::
        break;
    case type_row:
        //Backward compatibility - should revisit
        return buildRtlType(deserializer, type->queryChildType());
    //case type_bitfield:
        //fieldKey contains a field with a type annotated with offsets/isLastBitfield
        //OwnedHqlExpr fieldKey = getRtlFieldKey(field, rowRecord);
        //return buildRtlType(deserializer, fieldKey->queryType());
    }

    const RtlTypeInfo * found = deserializer.lookupType(type);
    if (found)
        return found;

    FieldTypeInfoStruct info;
    getFieldTypeInfo(info, type);

    switch (info.fieldType & RFTMkind)
    {
    case type_record:
        {
            IHqlExpression * record = ::queryRecord(type);
            unsigned numFields = getFlatFieldCount(record);
            info.fieldsArray = new const RtlFieldInfo * [numFields+1];
            unsigned idx = 0;
            info.fieldType |= buildRtlRecordFields(deserializer, idx, info.fieldsArray, record, record);
            info.fieldsArray[idx] = nullptr;
            break;
        }
    case type_row:
        {
            info.childType = buildRtlType(deserializer, ::queryRecordType(type));
            break;
        }
    case type_table:
    case type_groupedtable:
        {
            info.childType = buildRtlType(deserializer, ::queryRecordType(type));
            break;
        }
    case type_dictionary:
        return nullptr;  // MORE - does this leak?
    case type_set:
        info.childType = buildRtlType(deserializer, type->queryChildType());
        break;
    }
    if (info.childType)
        info.fieldType |= info.childType->fieldType & RFTMinherited;

    return deserializer.addType(info, type);
}


