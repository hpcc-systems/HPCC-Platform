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

#include "platform.h"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jmisc.hpp"

#include "hql.hpp"

#include "hqlfunc.hpp"

#include "hqlcpp.ipp"
#include "hqlwcpp.hpp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqlcatom.hpp"
#include "hqlcerrors.hpp"
#include "hqlthql.hpp"

#define INTEGER_SEARCH_THRESHOLD                    30      // above this, a table search is generated.
#define MAX_NUM_NOBREAK_CASE                        80      // maximum number of case: without a break - compiler workaround
#define INLINE_COMPARE_THRESHOLD                    2       // above this, a loop is generated
#define SWITCH_TABLE_DENSITY_THRESHOLD              3       // % used before use array index.
#define RANGE_DENSITY_THRESHOLD                     30      // % used before to use array index with range checking
#define MAX_NESTED_CASES                            8       // to stop C++ compiler running out of scopes.

//===========================================================================

static IIdAtom * searchDataTableAtom;
static IIdAtom * searchEStringTableAtom;
static IIdAtom * searchQStringTableAtom;
static IIdAtom * searchStringTableAtom;
static IIdAtom * searchVStringTableAtom;

//===========================================================================

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    searchDataTableAtom = createIdAtom("searchDataTable");
    searchEStringTableAtom = createIdAtom("searchEStringTable");
    searchQStringTableAtom = createIdAtom("searchQStringTable");
    searchStringTableAtom = createIdAtom("searchStringTable");
    searchVStringTableAtom = createIdAtom("searchVStringTable");
    return true;
}
MODULE_EXIT()
{
}

//===========================================================================

//Case helper functions...
void cvtChooseListToPairs(HqlExprArray & target, IHqlExpression * from, unsigned base)
{
    unsigned max = from->numChildren();
    unsigned idx;
    target.ensure(max);
    for (idx = 0; idx < max; idx++)
    {
        IHqlExpression * v1 = createConstant(createIntValue(idx+base, LINK(unsignedType)));
        IHqlExpression * v2 = from->queryChild(idx);
        ITypeInfo * type = v2->queryType();
        target.append(* createValue(no_mapto, LINK(type), v1, LINK(v2)));
    }
}

void cvtIndexListToPairs(HqlExprArray & target, IHqlExpression * from)
{
    unsigned max = from->numChildren();
    unsigned idx;
    target.ensure(max);
    for (idx = 0; idx < max; idx++)
    {
        IHqlExpression * v1 = from->queryChild(idx);
        IHqlExpression * v2 = createConstant(createIntValue(idx+1, LINK(unsignedType)));
        ITypeInfo * type = v2->queryType();
        target.append(* createValue(no_mapto, LINK(type), LINK(v1), v2));
    }
}

void cvtInListToPairs(HqlExprArray & target, IHqlExpression * from, bool valueIfMatch)
{
    unsigned max = from->numChildren();
    unsigned idx;
    IHqlExpression * tValue = queryBoolExpr(valueIfMatch);
    ITypeInfo * type = queryBoolType();
    target.ensure(max);
    for (idx = 0; idx < max; idx++)
    {
        IHqlExpression * v1 = from->queryChild(idx);
        target.append(* createValue(no_mapto, LINK(type), LINK(v1), LINK(tValue)));
    }
}

IHqlExpression * createNotFoundValue()
{
    OwnedITypeInfo int4 = makeIntType(4, true);
    return createConstant(int4->castFrom(true, -1));
}

//===========================================================================

static int compareValues(IHqlExpression * lexpr, IHqlExpression * rexpr)
{
    return lexpr->queryValue()->compare(rexpr->queryValue());
}

static int comparePair(IHqlExpression * lexpr, IHqlExpression * rexpr)
{
    return compareValues(lexpr->queryChild(0), rexpr->queryChild(0));
}

static int comparePair(IInterface * const * left, IInterface * const * right)
{
    IHqlExpression * lexpr = (IHqlExpression *)*left;
    IHqlExpression * rexpr = (IHqlExpression *)*right;
    return comparePair(lexpr, rexpr);
}

HqlCppCaseInfo::HqlCppCaseInfo(HqlCppTranslator & _translator) : translator(_translator)
{
    complexCompare = false;
    constantCases = true;
    constantValues = true;
    indexType.setown(makeIntType(sizeof(int), true));
}

void HqlCppCaseInfo::addPair(IHqlExpression * expr)
{
    IHqlExpression * compareExpr = expr->queryChild(0);
    IHqlExpression * resultExpr = expr->queryChild(1);
    if (allResultsMatch && pairs.ordinality())
    {
        if (pairs.tos().queryChild(1) != resultExpr)
            allResultsMatch = false;
    }

    pairs.append(*LINK(expr));
    
    if (!compareExpr->queryValue())
    {
        constantCases = false;
    }
    else if (constantCases)
    {
        if (!lowestCompareExpr || compareValues(compareExpr, lowestCompareExpr) < 0)
            lowestCompareExpr.set(compareExpr);
        if (!highestCompareExpr || compareValues(compareExpr, highestCompareExpr) > 0)
            highestCompareExpr.set(compareExpr);
    }

    if (!expr->queryChild(1)->queryValue())
        constantValues = false;

    if (cond && !complexCompare)
    {
        ITypeInfo * valueType = compareExpr->queryType();
        if (valueType != cond->queryType())
            complexCompare = isCompare3Valued(compareExpr->queryType());
    }

    updateResultType(resultExpr);

}

void HqlCppCaseInfo::addPairs(HqlExprArray & _pairs)
{
    pairs.ensure(_pairs.ordinality());
    ForEachItemIn(idx, _pairs)
        addPair(&_pairs.item(idx));
}

bool HqlCppCaseInfo::canBuildStaticList(ITypeInfo * type)
{
    switch (type->getTypeCode())
    {
    case type_int:
        return isStandardSizeInt(type);
    case type_swapint:
        return false;
    default:
        return isFixedSize(type);
    }
}

bool HqlCppCaseInfo::buildAssign(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    ExprAssignProcessor assigner(target);
    return buildProcess(ctx, assigner);
}

bool HqlCppCaseInfo::buildProcess(BuildCtx & ctx, IExprProcessor & target)
{
    if (pairs.ordinality() == 0)
    {
        target.process(translator, ctx, defaultValue);
    }
    else if (cond.get() && constantCases)
    {
        processBranches();

        BuildCtx subctx(ctx);
        CHqlBoundExpr test;
        buildSwitchCondition(ctx, test);

        if (complexCompare)
        {
            if (pairs.ordinality() > INLINE_COMPARE_THRESHOLD)
            {
                if (okToAlwaysEvaluateDefault(target) || hasLibraryChop())
                    buildLoopChopMap(subctx, target, test);
                else
                    buildGeneralAssign(subctx, target);
            }
            else 
            {
                if (okToAlwaysEvaluateDefault(target))
                    buildChop3Map(subctx, target, test);
                else
                    buildGeneralAssign(subctx, target);
            }
        }
        else
        {
            ITypeInfo * condType = test.queryType()->queryPromotedType();
            if (!queryBuildArrayLookup(subctx, target, test))
            {
                if (condType->getTypeCode() != type_real)
                {
                    OwnedHqlExpr search = test.getTranslatedExpr();
                    if (constantValues && (condType->getTypeCode() == type_int) && (pairs.ordinality() > INTEGER_SEARCH_THRESHOLD) && canBuildStaticList(resultType))
                        buildIntegerSearchMap(subctx, target, search);
                    else
                        buildSwitchMap(subctx, target, test.expr);
                }
                else
                {
                    if (okToAlwaysEvaluateDefault(target) && pairs.ordinality() > 4)
                    {
                        target.process(translator, ctx, defaultValue);
                        buildChop2Map(subctx, target, test, 0, pairs.ordinality());
                    }
                    else
                        buildGeneralAssign(subctx, target);
                }
            }
        }
    }
    else
        buildGeneralAssign(ctx, target);

    return true;
}

bool HqlCppCaseInfo::buildReturn(BuildCtx & ctx)
{
    ExprReturnProcessor processor;
    buildProcess(ctx, processor);
    return true;
}

void HqlCppCaseInfo::buildChop3Map(BuildCtx & ctx, IExprProcessor & target, CHqlBoundExpr & test, IHqlExpression * temp, unsigned start, unsigned end)
{
    if ((end - start) <= 2)
        buildChop2Map(ctx, target, test, start, end);
    else
    {
        unsigned mid = start + (end - start) / 2;
        generateCompareVar(ctx, temp, test, queryCompare(mid));
        OwnedHqlExpr test1 = createValue(no_eq, makeBoolType(), LINK(temp), getZero());
        OwnedHqlExpr test2 = createValue(no_lt, makeBoolType(), LINK(temp), getZero());

        BuildCtx subctx(ctx);
        IHqlStmt * if1 = subctx.addFilter(test1);                   // if (test == 0)
        target.process(translator, subctx, queryReturn(mid));  //   target = value(n)
        subctx.selectElse(if1);                                                         // else
        IHqlStmt * if2 = subctx.addFilter(test2);                   //   if (test < 0)
        buildChop3Map(subctx, target, test, temp, start, mid);      //       repeat for start..mid
        subctx.selectElse(if2);                                                         //   else
        buildChop3Map(subctx, target, test, temp, mid+1, end);      //       repeat for min..end
    }
}


void HqlCppCaseInfo::buildChop3Map(BuildCtx & ctx, IExprProcessor & target, CHqlBoundExpr & test)
{
    target.process(translator, ctx, defaultValue);
    if (getNumPairs() <= 2)
    {
        buildChop2Map(ctx, target, test, 0, getNumPairs());
    }
    else
    {
        //need to hack it because there is no signed integer type
        OwnedHqlExpr tempVar = ctx.getTempDeclare(indexType, NULL);
        buildChop3Map(ctx, target, test, tempVar, 0, getNumPairs());
    }
}

void HqlCppCaseInfo::buildChop2Map(BuildCtx & ctx, IExprProcessor & target, CHqlBoundExpr & test, unsigned start, unsigned end)
{
    BuildCtx subctx(ctx);
    
    if ((end - start) <= 3)             // (1,2,3) avg(2) cf. (2,2,3) avg(2.3)
    {
        //optimize the case where they all create the same value
        bool same = true;
        unsigned index;
        for (index=start+1; index < end; ++index)
        {
            if (queryReturn(index-1) != queryReturn(index))
            {
                same = false;
                break;
            }
        }
        
        if (same)
        {
            CompoundBuilder cb(no_or);
            for (index=start; index < end; ++index)
            {
                IHqlExpression * compare = queryCompare(index);
                IHqlExpression * cond  = createCompareExpr(no_eq, test.getTranslatedExpr(), LINK(compare));
                cb.addOperand(cond);
            }
            OwnedHqlExpr compound = cb.getCompound();
            translator.buildFilter(subctx, compound);
            target.process(translator, subctx, queryReturn(start));
        }
        else
        {
            IHqlStmt * stmt = NULL;
            for (index=start; index < end; ++index)
            {
                if (stmt)
                    subctx.selectElse(stmt);
                
                IHqlExpression * compare = queryCompare(index);
                OwnedHqlExpr cond  = createCompareExpr(no_eq, test.getTranslatedExpr(), LINK(compare));
                CHqlBoundExpr bound;
                translator.buildExpr(subctx, cond, bound);
                stmt = subctx.addFilter(bound.expr);
                target.process(translator, subctx, queryReturn(index));
            }
        }
    }
    else
    {
        unsigned mid = start + (end - start) / 2;
        
        IHqlExpression * compare = queryCompare(mid);
        OwnedHqlExpr cond  = createCompareExpr(no_lt, test.getTranslatedExpr(), LINK(compare));
        CHqlBoundExpr bound;
        translator.buildExpr(subctx, cond, bound);
        IHqlStmt * stmt = subctx.addFilter(bound.expr);

        buildChop2Map(subctx, target, test, start, mid);
        subctx.selectElse(stmt);
        buildChop2Map(subctx, target, test, mid, end);
    }
}

IHqlExpression * HqlCppCaseInfo::buildIndexedMap(BuildCtx & ctx, const CHqlBoundExpr & test)
{
    ITypeInfo * compareType = test.queryType()->queryPromotedType();
    type_t compareTypeCode = compareType->getTypeCode();

    HqlExprArray values;
    IHqlExpression * dft = queryActiveTableSelector();  // value doesn't matter as long as it will not occur
    __int64 lower = getIntValue(lowerTableBound, 0);
    unsigned num = (unsigned)((getIntValue(upperTableBound, 0)-lower)+1);

    CHqlBoundExpr indexExpr;
    switch (compareTypeCode)
    {
        case type_int:
            indexExpr.set(test);
            break;
        case type_string:
            indexExpr.expr.setown(createValue(no_index, makeCharType(), LINK(test.expr), getZero()));
            indexExpr.expr.setown(createValue(no_cast, makeIntType(1, false), LINK(indexExpr.expr)));
            break;
        default:
            throwUnexpectedType(compareType);
    }

    if (useRangeIndex && (num != 1))
        translator.ensureSimpleExpr(ctx, indexExpr);

    OwnedHqlExpr mapped;
    OwnedHqlExpr castDefaultValue = ensureExprType(defaultValue, resultType);
    ITypeInfo * retType = resultType;
    //if num == pairs.ordinality() and all results are identical, avoid the table lookup.
    if (allResultsMatch && (num == pairs.ordinality()))
    {
        mapped.set(pairs.item(0).queryChild(1));
    }
    else
    {
        values.ensure(num);
        unsigned idx;
        for (idx = 0; idx < num; idx++)
            values.append(*LINK(dft));

        ForEachItemIn(idx2, pairs)
        {
            IHqlExpression & cur = pairs.item(idx2);
            IValue * value = cur.queryChild(0)->queryValue();
            unsigned replaceIndex;
            switch (compareTypeCode)
            {
            case type_int:
                replaceIndex = (unsigned)(value->getIntValue()-lower);
                break;
            case type_string:
                {
                    StringBuffer temp;
                    value->getStringValue(temp);
                    replaceIndex = (unsigned)((unsigned char)temp.charAt(0)-lower);
                    break;
                }
            default:
                throwUnexpectedType(compareType);
            }

            IHqlExpression * mapTo = cur.queryChild(1);
            if (mapTo->getOperator() != no_constant)
                throwUnexpected();
            if (replaceIndex >= num)
                translator.reportWarning(CategoryIgnored, HQLWRN_CaseCanNeverMatch, "CASE entry %d can never match the test condition", replaceIndex);
            else
                values.replace(*ensureExprType(mapTo, resultType),replaceIndex);
        }

        //Now replace the placeholders with the default values.
        for (idx = 0; idx < num; idx++)
        {
            if (&values.item(idx) == dft)
                values.replace(*LINK(castDefaultValue),idx);
        }

        // use a var string type to get better C++ generated...
        ITypeInfo * storeType = getArrayElementType(resultType);
        ITypeInfo * listType = makeArrayType(storeType, values.ordinality());
        OwnedHqlExpr lvalues = createValue(no_list, listType, values);

        CHqlBoundExpr boundTable;
        translator.buildExpr(ctx, lvalues, boundTable);

        LinkedHqlExpr tableIndex = indexExpr.expr;
        if (getIntValue(lowerTableBound, 0))
            tableIndex.setown(createValue(no_sub, tableIndex->getType(), LINK(tableIndex), LINK(lowerTableBound)));

        IHqlExpression * ret = createValue(no_index, LINK(retType), LINK(boundTable.expr), LINK(tableIndex));
        mapped.setown(createTranslatedOwned(ret));
    }

    if (useRangeIndex)
    {
        if (num != 1)
        {
            OwnedHqlExpr testValue = indexExpr.getTranslatedExpr();
            OwnedHqlExpr aboveLower = createCompare(no_ge, testValue, lowestCompareExpr);
            OwnedHqlExpr belowUpper = createCompare(no_le, testValue, highestCompareExpr);
            OwnedHqlExpr inRange = createValue(no_and, makeBoolType(), aboveLower.getClear(), belowUpper.getClear());
            mapped.setown(createValue(no_if, LINK(retType), inRange.getClear(), LINK(mapped), LINK(defaultValue)));
        }
        else
        {
            assertex(allResultsMatch);
            OwnedHqlExpr testValue = indexExpr.getTranslatedExpr();
            OwnedHqlExpr inRange = createCompare(no_eq, testValue, lowestCompareExpr);
            mapped.setown(createValue(no_if, LINK(retType), inRange.getClear(), LINK(mapped), LINK(defaultValue)));
        }
    }

    return mapped.getClear();

}

void HqlCppCaseInfo::buildLoopChopMap(BuildCtx & ctx, IExprProcessor & target, CHqlBoundExpr & test)
{
    //Declare a table that contains all the strings...
    ITypeInfo * compareType = queryCompareType();
    type_t ctc = compareType->getTypeCode();
    if ((ctc == type_data) && !hasLibraryChop())
    {
         buildGeneralAssign(ctx, target);
         return;
    }

    OwnedHqlExpr values = createCompareList();
    CHqlBoundExpr boundTable;
    translator.buildExpr(ctx, values, boundTable);

    OwnedHqlExpr midVar = createVariable(LINK(indexType));

    if (hasLibraryChop())
    {
        ITypeInfo * tableEntryType = makeReferenceModifier(LINK(compareType));
        ITypeInfo * tableType = makePointerType(tableEntryType);
        HqlExprArray args;

        IIdAtom * func;
        switch (ctc)
        {
        case type_data: 
            func = searchDataTableAtom; 
            break;
        case type_varstring: 
            func = searchVStringTableAtom;
            break;
        case type_qstring:
            func = searchQStringTableAtom;
            break;
        case type_string:
            if (compareType->queryCharset()->queryName() == asciiAtom)
                func = searchStringTableAtom;
            else if (compareType->queryCharset()->queryName() == ebcdicAtom)
                func = searchEStringTableAtom;
            else
                UNIMPLEMENTED;
            break;
        case type_unicode:
            func = searchUnicodeTableId;
            break;
        case type_utf8:
            func = searchUtf8TableId;
            break;
        case type_varunicode:
            func = searchVUnicodeTableId;
            break;
        default:
            throwUnexpectedType(compareType);
        }
        args.append(*getSizetConstant(values->numChildren()));
        if ((ctc != type_varstring) && (ctc != type_varunicode))
            args.append(*getSizetConstant(values->queryChild(0)->queryType()->getStringLen()));
        args.append(*createValue(no_address, tableType, createValue(no_index, LINK(tableEntryType), LINK(boundTable.expr), getZero())));
        if ((ctc != type_varstring) && (ctc != type_varunicode))
            args.append(*translator.getBoundLength(test));
        args.append(*ensureIndexable(test.expr));
        if ((ctc==type_unicode) || (ctc == type_varunicode) || (ctc == type_utf8))
            args.append(*createConstant(str(compareType->queryLocale())));

        OwnedHqlExpr call = translator.bindTranslatedFunctionCall(func, args);
        OwnedHqlExpr search = createTranslated(call);

        bool includeDefaultInResult = false;
        OwnedHqlExpr resultExpr = createResultsExpr(midVar, true, &includeDefaultInResult);

        OwnedHqlExpr simpleResult = queryCreateSimpleResultAssign(search, resultExpr);
        if (simpleResult)
            target.process(translator, ctx, simpleResult);
        else
        {
            ctx.addDeclare(midVar, NULL);
            translator.buildAssignToTemp(ctx, midVar, search);
            if (includeDefaultInResult)
            {
                target.process(translator, ctx, resultExpr);
            }
            else
            {
                OwnedHqlExpr compare = createBoolExpr(no_ne, LINK(midVar), createNotFoundValue());

                BuildCtx subctx(ctx);
                IHqlStmt * stmt = subctx.addFilter(compare);
                target.process(translator, subctx, resultExpr);
                subctx.selectElse(stmt);
                target.process(translator, subctx, defaultValue);
            }
        }
    }
    else
    {
#if 0
        //Keep this as a reminder of what was needed to fix qstrings.
        if (ctc == type_qstring)
        {
            Linked<ITypeInfo> compareType = queryCompareType();
            compareType.setown(makeArrayType(compareType.getLink()));
            table = createValue(no_typetransfer, compareType.getLink(), table);
        }
#endif

        OwnedHqlExpr resultExpr = createResultsExpr(midVar, false);
        //Now generate the code that performs the binary chop..
        ctx.addDeclare(midVar, NULL);
        OwnedHqlExpr compareVar = ctx.getTempDeclare(indexType, NULL);
        OwnedHqlExpr leftVar = ctx.getTempDeclare(indexType, NULL);
        OwnedHqlExpr rightVar = ctx.getTempDeclare(indexType, NULL);

        OwnedHqlExpr numPairs = getSizetConstant(getNumPairs());
        ctx.addAssign(leftVar, queryZero());
        ctx.addAssign(rightVar, numPairs);
        target.process(translator,ctx, defaultValue);

        OwnedHqlExpr loopc = createBoolExpr(no_lt, LINK(leftVar), LINK(rightVar));
        OwnedHqlExpr mid = createValue(no_div, LINK(indexType), createValue(no_add, LINK(indexType), LINK(leftVar), LINK(rightVar)), createConstant(indexType->castFrom(false, 2)));
        OwnedHqlExpr mid_p1 = createValue(no_add, LINK(indexType), LINK(midVar), createConstant(indexType->castFrom(false, 1)));
        OwnedHqlExpr curelem = createValue(no_index, LINK(compareType), boundTable.getTranslatedExpr(), createTranslated(mid_p1));
        OwnedHqlExpr test1 = createBoolExpr(no_lt, LINK(compareVar), getZero());
        OwnedHqlExpr test2 = createBoolExpr(no_gt, LINK(compareVar), getZero());

        OwnedHqlExpr order = createValue(no_order, LINK(indexType), test.getTranslatedExpr(), LINK(curelem));
        BuildCtx loopctx(ctx);
        loopctx.addLoop(loopc, NULL, true);
            translator.buildAssignToTemp(loopctx, midVar, mid);
            translator.buildAssignToTemp(loopctx, compareVar, order);

            BuildCtx subctx(loopctx);
            IHqlStmt * if1 = subctx.addFilter(test1);                                    // if (test < 0)
            translator.buildAssignToTemp(subctx, rightVar, midVar);                     //   right = mid;
            subctx.selectElse(if1);                                                                          // else
            IHqlStmt * if2 = subctx.addFilter(test2);                                    //     if (test > 0)
            translator.buildAssignToTemp(subctx, leftVar, mid_p1);                      //       left = mid + 1;
            subctx.selectElse(if2);                                                                          //     else
            //generate the default assignment...
            target.process(translator, subctx, resultExpr);
            subctx.addBreak();
    }
}

void HqlCppCaseInfo::buildIntegerSearchMap(BuildCtx & ctx, IExprProcessor & target, IHqlExpression * test)
{
    ITypeInfo * compareType = queryCompareType();

    HqlExprArray args;
    args.append(*createCompareList());
    args.append(*LINK(test));

    IIdAtom * func;
    if (compareType->isSigned())
    {
        if (compareType->getSize() > 4)
            func = searchTableInteger8Id;
        else
            func = searchTableInteger4Id;
    }
    else
    {
        if (compareType->getSize() > 4)
            func = searchTableUInteger8Id;
        else
            func = searchTableUInteger4Id;
    }

    OwnedHqlExpr search = translator.bindFunctionCall(func, args);

    OwnedHqlExpr midVar = createVariable(LINK(indexType));
    bool includeDefaultInResult = false;
    OwnedHqlExpr resultExpr = createResultsExpr(midVar, true, &includeDefaultInResult);

    OwnedHqlExpr simpleResult = queryCreateSimpleResultAssign(search, resultExpr);
    if (simpleResult)
        target.process(translator, ctx, simpleResult);
    else
    {
        ctx.addDeclare(midVar, NULL);
        translator.buildAssignToTemp(ctx, midVar, search);
        if (includeDefaultInResult)
        {
            target.process(translator, ctx, resultExpr);
        }
        else
        {
            OwnedHqlExpr compare = createBoolExpr(no_ne, LINK(midVar), createNotFoundValue());

            BuildCtx subctx(ctx);
            IHqlStmt * stmt = subctx.addFilter(compare);
            target.process(translator, subctx, resultExpr);
            subctx.selectElse(stmt);
            target.process(translator, subctx, defaultValue);
        }
    }
}

void HqlCppCaseInfo::buildSwitchCondition(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    OwnedHqlExpr simpleCond = complexCompare ? getSimplifyCompareArg(cond) : LINK(cond);
    translator.buildCachedExpr(ctx, simpleCond, bound);
}

void HqlCppCaseInfo::buildSwitchMap(BuildCtx & ctx, IExprProcessor & target, IHqlExpression * test)
{
    bool isCharCompare = (test->queryType()->getTypeCode() == type_string);
    Owned<ITypeInfo> unsigned1Type = makeIntType(1, false);
    
    LinkedHqlExpr cond = test;
    if (isCharCompare)
        cond.setown(createValue(no_implicitcast, LINK(unsigned1Type), createValue(no_index, makeCharType(), LINK(test), getZero())));
    
    BuildCtx subctx(ctx);
    IHqlStmt * stmt = subctx.addSwitch(cond);
    
    // are all the comparisons against constant values?  If so we optimize it...
    unsigned sameCount = 0;
    ForEachItemIn(idx, pairs)
    {
        IHqlExpression & cur = pairs.item(idx);
        LinkedHqlExpr compare = cur.queryChild(0);
        IHqlExpression * branchValue = cur.queryChild(1);

        if (isCharCompare)
        {
            //Coded this way to avoid problems with ascii v ebcdic strings
            const byte * data = reinterpret_cast<const byte *>((char *)compare->queryValue()->queryValue());
            byte value = data[0];
            Owned<IValue> cast;
            if (value < 127)
                cast.setown(createCharValue(value, true));
            else
                cast.setown(createIntValue(data[0], LINK(unsigned1Type)));
            compare.setown(createConstant(cast.getClear()));
        }
        
        bool same = false;
        bool skip = false;
        if (idx != pairs.ordinality()-1)
        {
            IHqlExpression & next = pairs.item(idx+1);
            if (next.queryChild(1) == branchValue)
                same = true;
            //if save item is included twice, remove one so no error
            //from the case statement
            if (next.queryChild(0) == cur.queryChild(0))
            {
                skip = true;
#ifdef _DEBUG
                throwUnexpected();      // should have been removed as a duplicate 
#endif
            }
        }

        if (compare->queryValue()->rangeCompare(cond->queryType()) != 0)
        {
            StringBuffer ecl;
            getExprECL(compare, ecl);
            translator.reportWarning(CategoryIgnored, HQLWRN_CaseCanNeverMatch, "CASE entry %s can never match the test condition", ecl.str());
            skip = true;
        }

        if (sameCount >= MAX_NUM_NOBREAK_CASE)
            same = false;

        if (!skip)
        {
            subctx.addCase(stmt, compare);
            
            // common up case labels that produce the same result
            if (!same)
            {
                sameCount = 1;
                target.process(translator, subctx, branchValue);
            }
            else
                sameCount++;
        }
    }
    
    if (target.canContinue())
    {
        subctx.addDefault(stmt);
        target.process(translator, subctx, defaultValue);
    }
    else
        target.process(translator, ctx, defaultValue);
}


void HqlCppCaseInfo::buildGeneralAssign(BuildCtx & ctx, IExprProcessor & target)
{
    unsigned max = pairs.ordinality();

    Owned<BuildCtx> subctx = new BuildCtx(ctx);
    CHqlBoundExpr pureCond;
    if (cond.get())
        translator.buildCachedExpr(*subctx, cond, pureCond);
    
    OwnedHqlExpr testMore;

    if (target.canContinue() && (max > MAX_NESTED_CASES))
    {
        //Too many nested blocks cause the compiler indigestion...
        Owned<ITypeInfo> t = makeBoolType();
        testMore.setown(ctx.getTempDeclare(t, queryBoolExpr(false)));
    }

    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression & cur = pairs.item(idx);
        IHqlExpression * curtest = cur.queryChild(0);
        IHqlExpression * curvalue = cur.queryChild(1);

        OwnedHqlExpr compare;
        if (!pureCond.expr)
            compare.setown(ensureExprType(curtest, queryBoolType()));
        else
            compare.setown(createBoolExpr(no_eq, pureCond.getTranslatedExpr(), LINK(curtest)));

        if (target.canContinue())
        {
            if (idx && ((idx % MAX_NESTED_CASES) == 0))
            {
                translator.buildAssignToTemp(*subctx,testMore,queryBoolExpr(true));
                subctx.setown(new BuildCtx(ctx));
                subctx->addFilter(testMore);
                translator.buildAssignToTemp(*subctx,testMore,queryBoolExpr(false));
            }

            IHqlStmt * test = translator.buildFilterViaExpr(*subctx, compare);
            target.process(translator, *subctx, curvalue);


            subctx->selectElse(test);
        }
        else
        {
            BuildCtx ifctx(*subctx);
            translator.buildFilter(ifctx, compare);
            target.process(translator, ifctx, curvalue);
        }
    }

    target.process(translator, *subctx, defaultValue);
}

void HqlCppCaseInfo::buildGeneralReturn(BuildCtx & ctx)
{
    unsigned max = pairs.ordinality();

    CHqlBoundExpr pureCond;
    if (cond.get())
        translator.buildCachedExpr(ctx, cond, pureCond);
    
    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression & cur = pairs.item(idx);
        IHqlExpression * curtest = cur.queryChild(0);
        IHqlExpression * curvalue = cur.queryChild(1);

        OwnedHqlExpr compare;
        if (!pureCond.expr)
            compare.setown(ensureExprType(curtest, queryBoolType()));
        else
            compare.setown(createBoolExpr(no_eq, pureCond.getTranslatedExpr(), LINK(curtest)));

        BuildCtx subctx(ctx);
        translator.buildFilter(subctx, compare);
        translator.buildReturn(subctx, curvalue);
    }

    translator.buildReturn(ctx, defaultValue);
}

bool HqlCppCaseInfo::okToAlwaysEvaluateDefault(const IExprProcessor & target)
{
    if (target.canReturn())
        return false;
    if (!canRemoveGuard(defaultValue))
        return false;
    return true;
}

ITypeInfo * HqlCppCaseInfo::queryCompareType()
{
    return cond->queryType()->queryPromotedType();
}

IHqlExpression * HqlCppCaseInfo::createCompareList()
{
    //NB: All cases need to have the same type because they are stored in a table
    Linked<ITypeInfo> promoted = pairs.item(0).queryChild(0)->queryType();
    unsigned max = pairs.ordinality();
    for (unsigned idx1 = 1; idx1 < max; idx1++)
    {
        ITypeInfo * type = pairs.item(idx1).queryChild(0)->queryType();
        promoted.setown(getPromotedECLType(promoted, type));
    }

    HqlExprArray values;
    values.ensure(pairs.ordinality());
    ForEachItemIn(idx, pairs)
    {
        IHqlExpression & pair = pairs.item(idx);
        IHqlExpression * compare = pair.queryChild(0);
        values.append(*ensureExprType(compare, promoted));
    }

    Linked<ITypeInfo> compareType = queryCompareType();
    switch (compareType->getTypeCode())
    {
    case type_string:
    case type_data:
    case type_varstring:
    case type_qstring:
    case type_utf8:
        compareType.setown(makePointerType(makeCharType(false)));
        break;
    case type_unicode:
    case type_varunicode:
        compareType.setown(makePointerType(makeClassType("UChar")));
        break;
    }
    return createValue(no_list, makeArrayType(compareType.getLink()), values);
}


IHqlExpression * HqlCppCaseInfo::createResultsExpr(IHqlExpression * matchVar, bool canIncludeDefault, bool * includedDefault)
{
    if (includedDefault)
        *includedDefault = false;

    //Look at all the return results and see what relation we can create
    //This can generate one of the following:
    //1. A single value.
    //2. A function of the result. e.g., IN...
    //3. A lookup in another table. [ all values are constant ]
    //4. A nested map statement
    bool areSame = true;
    bool areLinear = true;
    bool areConstant = true;
    __int64 linearStart = 0;
    __int64 linearMultiple = 1;
    ITypeInfo * retType = resultType;

    switch (retType->getTypeCode())
    {
    case type_int:
    case type_boolean:
        break;
    case type_string:
    case type_data:
//      areConstant = false;        // a temporary hack to stop incorrect table being generated.
        areLinear = false;
        break;
    default:
        areLinear = false;
        break;
    }

    IHqlExpression * prevValue = NULL;
    HqlExprArray values;
    values.ensure(pairs.ordinality());
    ForEachItemIn(idx, pairs)
    {
        IHqlExpression & pair = pairs.item(idx);
        IHqlExpression * value = pair.queryChild(1);
        IValue * cvalue = value->queryValue();

        if (cvalue)
        {
            if (areLinear)
            {
                __int64 val = cvalue->getIntValue();
                if (idx == 0)
                    linearStart = val;
                else if (idx == 1)
                    linearMultiple = (val - linearStart);
                else if (val != linearStart + idx * linearMultiple)
                    areLinear = false;
            }
        }
        else
        {
            areConstant = false;
            areLinear = false;
        }
        
        if (idx > 0)
        {
            if (areSame && (prevValue != value))
                areSame = false;
        }

        values.append(*ensureExprType(value, resultType));
        prevValue = value;
    }

    if (areSame)
        return LINK(prevValue);

    if (areLinear)
    {
        IHqlExpression * ret = ensureExprType(matchVar, resultType);
        if (linearMultiple != 1)
            ret = createValue(no_mul, LINK(resultType), ret, createConstant(resultType->castFrom(true, linearMultiple)));
        if (linearStart != 0)
            ret = createValue(no_add, LINK(resultType), ret, createConstant(resultType->castFrom(true, linearStart)));
        return ret;
    }

    unsigned firstMatchEntry = 0;
    if (canIncludeDefault)
    {
        //If all the values are constant, then can add the default as an extra 0th entry, because -1 will be the index for the default
        if (areConstant && defaultValue->isConstant() && defaultValue->queryType() == values.item(0).queryType())
        {
            firstMatchEntry = 1;
            values.add(*LINK(defaultValue), 0);
            *includedDefault = true;
        }
    }

    // easy way to create a value list...
    ITypeInfo * storeType = getArrayElementType(retType);
    OwnedHqlExpr newlist = createValue(no_list, makeSetType(storeType), values);
    if (areConstant && canBuildStaticList(resultType))
    {
        IHqlExpression * index = adjustValue(matchVar, 1+firstMatchEntry);
        return createValue(no_index, LINK(retType), LINK(newlist), index, createAttribute(noBoundCheckAtom));
    }

    //Need to generate a case (switch integer case 1: ..... )
    HqlExprArray choosePairs;
    cvtChooseListToPairs(choosePairs, newlist, 0);

    *includedDefault = true;
    IHqlExpression * caseExpr = createOpenValue(no_case, LINK(retType));
    caseExpr->addOperand(LINK(matchVar));
    ForEachItemIn(idx2, choosePairs)
        caseExpr->addOperand(&choosePairs.item(idx2));
    choosePairs.kill(true);
    caseExpr->addOperand(LINK(defaultValue));
    return caseExpr->closeExpr();
}


void HqlCppCaseInfo::generateCompareVar(BuildCtx & ctx, IHqlExpression * target, CHqlBoundExpr & test, IHqlExpression * other)
{
    OwnedHqlExpr compare = createValue(no_order, makeIntType(4, true), test.getTranslatedExpr(), LINK(other));
    translator.buildAssignToTemp(ctx, target, compare);
}
    
unsigned HqlCppCaseInfo::getNumPairs()
{
    return pairs.ordinality();
}

bool HqlCppCaseInfo::hasLibraryChop()
{
    ITypeInfo * compareType = queryCompareType();
    type_t ctc = compareType->getTypeCode();

    switch (ctc)
    {
    case type_data:
        return canBuildStaticList(promotedElementType);
    case type_string:
    case type_varstring:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return true;
    }
    return false;
}


void HqlCppCaseInfo::processBranches()
{
    sortPairs();
    removeDuplicates();
    promoteTypes();
}

void HqlCppCaseInfo::promoteTypes()
{
    Owned<ITypeInfo> promoted = pairs.item(0).queryChild(0)->getType();

    unsigned max = pairs.ordinality();
    for (unsigned idx1 = 1; idx1 < max; idx1++)
    {
        ITypeInfo * type = pairs.item(idx1).queryChild(0)->queryType();

        if (isStringType(promoted) && isStringType(type))
        {
            if (promoted->getStringLen() != type->getStringLen())
            {
                promoted.setown(::getPromotedECLType(promoted, type));
                promoted.setown(getStretchedType(UNKNOWN_LENGTH, promoted));
            }
        }

        promoted.setown(::getPromotedECLType(promoted, type));
    }
    promotedElementType.set(promoted);

    if (isStringType(promoted))
        promoted.setown(getStretchedType(UNKNOWN_LENGTH, promoted));
    ITypeInfo * testType = queryCompareType();
    if ((testType->queryCharset() != promoted->queryCharset()) || (testType->queryLocale() != promoted->queryLocale()))
        cond.setown(ensureExprType(cond, promoted));
}


bool HqlCppCaseInfo::canBuildArrayLookup(const CHqlBoundExpr & test)
{
    if (!constantValues || pairs.ordinality() <= 3)
        return false;

    ITypeInfo * condType = test.queryType()->queryPromotedType();

    //MORE: Also support this for high density tables that don't start at 0... - checking upper and lower bounds
    unsigned bitSize = condType->getBitSize();
    if ((bitSize && (bitSize <= 8) && !condType->isSigned()))
    {
        unsigned limit = (1 << bitSize);
        //use case if enough items, or above a certain density...
        if (pairs.ordinality() * 100 >= limit * SWITCH_TABLE_DENSITY_THRESHOLD)
        {
            if ((condType->getTypeCode() == type_int) || (condType->getTypeCode() == type_string))
            {
                lowerTableBound.setown(getSizetConstant(0));
                upperTableBound.setown(getSizetConstant(limit-1));
                return true;
            }
        }
    }

    //Currently this condition is restricted because it revealed inconsistencies in the implementation
    //HPCC-18745 is opened to address that later.
    //Change to:    if (condType->isInteger() && !isUnknownSize(resultType))
    if (condType->isInteger() && resultType->isInteger())
    {
        unsigned __int64 range = getIntValue(highestCompareExpr, 0) - getIntValue(lowestCompareExpr, 0) + 1;
        if (pairs.ordinality() * 100 >= range  * RANGE_DENSITY_THRESHOLD)
        {
            useRangeIndex = true;
            lowerTableBound.set(lowestCompareExpr);
            upperTableBound.set(highestCompareExpr);
            return true;
        }
    }

    return false;
}

bool HqlCppCaseInfo::queryBuildArrayLookup(BuildCtx & ctx, IExprProcessor & target, const CHqlBoundExpr & test)
{
    if (canBuildArrayLookup(test) && canBuildStaticList(resultType) && defaultValue->isConstant())
    {
        BuildCtx subctx(ctx);
        OwnedHqlExpr ret = buildIndexedMap(subctx, test);
        target.process(translator, ctx, ret);
        return true;
    }
    return false;
}

void HqlCppCaseInfo::removeDuplicates()
{
    unsigned num = pairs.ordinality();
    if (num > 1)
    {
        num--;
        while (num--)
        {
            IHqlExpression & cur = pairs.item(num);
            IHqlExpression & next = pairs.item(num+1);

            if (cur.queryChild(0) == next.queryChild(0))
            {
                if (cur.queryChild(1) == next.queryChild(1))
                    pairs.remove(num+1);
                else
                {
                    // we need to keep the first in the original list....  Horrid, but it works...
                    unsigned off1 = originalPairs.find(cur);
                    unsigned off2 = originalPairs.find(next);
                    assertex(off1 != NotFound && off2 !=  NotFound);
                    if (off1 < off2)
                        pairs.remove(num+1);
                    else
                        pairs.remove(num);
                }
            }
        }
    }
}


IHqlExpression * HqlCppCaseInfo::queryCreateSimpleResultAssign(IHqlExpression * search, IHqlExpression * resultExpr)
{
    IHqlExpression * trueExpr = queryBoolExpr(true);
    IHqlExpression * falseExpr = queryBoolExpr(false);
    if (resultExpr == trueExpr && defaultValue == falseExpr)
        return createBoolExpr(no_ne, LINK(search), createNotFoundValue());
    if (resultExpr == falseExpr && defaultValue == trueExpr)
        return createBoolExpr(no_eq, LINK(search), createNotFoundValue());
    return NULL;
}

IHqlExpression * HqlCppCaseInfo::queryCompare(unsigned index)
{
    return pairs.item(index).queryChild(0);
}


IHqlExpression * HqlCppCaseInfo::queryReturn(unsigned index)
{
    return pairs.item(index).queryChild(1);
}
        

void HqlCppCaseInfo::setCond(IHqlExpression * expr)
{
    cond.set(expr);
    if (isCompare3Valued(expr->queryType()))
        complexCompare = true;
}


void HqlCppCaseInfo::setDefault(IHqlExpression * expr)
{
    defaultValue.set(expr);
}

void HqlCppCaseInfo::sortPairs()
{
    appendArray(originalPairs, pairs);
    pairs.sort(comparePair);
}

void HqlCppCaseInfo::updateResultType(IHqlExpression * expr)
{
    ITypeInfo * curResultType = expr->queryType();
    if (resultType)
        resultType.setown(::getPromotedECLType(resultType, curResultType));
    else
        resultType.set(curResultType);
}
