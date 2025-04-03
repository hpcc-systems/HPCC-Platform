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
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"
#include "eclrtl_imp.hpp"
#include "rtlkey.hpp"

#include "hql.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlutil.hpp"
#include "hqlthql.hpp"

#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlopt.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqltrans.ipp"
#include "hqlpmap.hpp"
#include "hqlttcpp.ipp"
#include "hqlcfilter.hpp"
#include "hqlcse.ipp"

const char * BuildFilterState::getSetName(bool createValueSets)
{
    if (!setNames.isItem(numActiveSets))
    {
        StringBuffer name;
        getUniqueId(name.append("set"));
        setNames.append(*new StringAttrItem(name.str()));

        StringBuffer s;
        funcctx.setNextConstructor();
        if (createValueSets)
            funcctx.addQuoted(s.append("Owned<IValueSet> ").append(name).append(";"));
        else
            funcctx.addQuoted(s.append("Owned<IStringSet> ").append(name).append(";"));
    }

    return setNames.item(numActiveSets++).text;
}


void BuildFilterState::popSetName()
{
    numActiveSets--;
}


//---------------------------------------------------------------------------------------------------------------------

CppFilterExtractor::CppFilterExtractor(IHqlExpression * _tableExpr, HqlCppTranslator & _translator, int _numKeyableFields, bool _isDiskRead, bool _createValueSets)
    : FilterExtractor(_translator, _tableExpr, _numKeyableFields, _isDiskRead, _createValueSets, _translator.queryOptions().allKeyedFiltersOptional), translator(_translator)
{
    if (createValueSets)
    {
        addRangeFunc = addRawRangeId;
        killRangeFunc = killRawRangeId;
    }
    else
    {
        addRangeFunc = addRangeId;
        killRangeFunc = killRangeId;
    }
}

void CppFilterExtractor::callAddAll(BuildCtx & ctx, IHqlExpression * targetVar)
{
    HqlExprArray args;
    args.append(*LINK(targetVar));
    translator.callProcedure(ctx, addAllId, args);
}

bool CppFilterExtractor::createGroupingMonitor(BuildCtx ctx, const char * listName, IHqlExpression * expr, unsigned & maxField)
{
    switch (expr->getOperator())
    {
    case no_if:
        {
            IHqlExpression * cond = expr->queryChild(0);
            if (expr->queryChild(2)->isConstant() && isIndependentOfScope(cond))
            {
                BuildCtx subctx(ctx);
                translator.buildFilter(subctx, expr->queryChild(0));
                createGroupingMonitor(subctx, listName, expr->queryChild(1), maxField);
                return true;        // may still be keyed
            }
            break;
        }
    case no_select:
        {
            size32_t offset = 0;
            ForEachItemIn(i, keyableSelects)
            {
                IHqlExpression & cur = keyableSelects.item(i);
                size32_t curSize = cur.queryType()->getSize();
                if (!createValueSets && curSize == UNKNOWN_LENGTH)
                    break;
                if (expr == &cur)
                {
                    maxField = i+1;
                    if (createValueSets)
                    {
                        StringBuffer type;
                        translator.buildRtlFieldType(type, expr->queryChild(1), queryRecord(tableExpr));
                        ctx.addQuotedF("%s->append(FFkeyed, createWildFieldFilter(%u, %s));", listName, i, type.str());
                    }
                    else
                    {
                        //MORE: Check the type of the field is legal.
                        ctx.addQuotedF("%s->append(createWildKeySegmentMonitor(%u, %u, %u));", listName, i, offset, curSize);
                    }
                    return true;
                }
                offset += curSize;
            }
            break;
        }
    case no_constant:
        return true;
    }
    ctx.addReturn(queryBoolExpr(false));
    return false;
}

void CppFilterExtractor::buildKeySegmentInExpr(BuildFilterState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind)
{
    //Generally this slightly increases the code size, but reduces the number of
    //temporary sets which is generally more efficient.
    OwnedHqlExpr simplified = querySimplifyInExpr(&thisKey);
    if (simplified)
    {
        OwnedHqlExpr folded = foldHqlExpression(simplified);
        buildKeySegmentExpr(buildState, selectorInfo, ctx, target, *folded, filterKind);
        return;
    }

    IHqlExpression * expandedSelector = selectorInfo.expandedSelector;
    ITypeInfo * fieldType = expandedSelector->queryType();
    unsigned curSize = fieldType->getSize();
    createStringSet(ctx, target, curSize, expandedSelector);

    OwnedHqlExpr targetVar = createVariable(target, makeVoidType());
    IHqlExpression * lhs = thisKey.queryChild(0);
    OwnedHqlExpr values = normalizeListCasts(thisKey.queryChild(1));

    IIdAtom * func = addRangeFunc;
    if (thisKey.getOperator() == no_notin)
    {
        callAddAll(ctx, targetVar);
        func = killRangeFunc;
    }

    if (values->getOperator() != no_list)
    {
        //iterate through the set
        BuildCtx subctx(ctx);
        CHqlBoundExpr boundCurElement;
        Owned<IHqlCppSetCursor> cursor = translator.createSetSelector(ctx, values);

        bool done = false;
        CHqlBoundExpr isAll;
        cursor->buildIsAll(subctx, isAll);
        if (isAll.expr->queryValue())
        {
            if (isAll.expr->queryValue()->getBoolValue())
            {
                callAddAll(subctx, targetVar);
                done = true;
                //If ALL allowed exceptions then we would need to do more....
            }
        }
        else
        {
            IHqlStmt * stmt = subctx.addFilter(isAll.expr);
            callAddAll(subctx, targetVar);
            subctx.selectElse(stmt);
        }

        if (!done)
        {
            cursor->buildIterateLoop(subctx, boundCurElement, false);
            OwnedHqlExpr curValue = boundCurElement.getTranslatedExpr();

            OwnedHqlExpr test = createBoolExpr(no_eq, LINK(lhs), LINK(curValue));
            OwnedHqlExpr promoted = getExplicitlyPromotedCompare(test);
            OwnedHqlExpr subrange, compare, normalized;
            extractCompareInformation(subctx, promoted, subrange, compare, normalized, expandedSelector);
            CHqlBoundExpr boundSubLength;
            buildSubRange(ctx, subrange, boundSubLength);

            if (compare)
                translator.buildFilter(subctx, compare);

            HqlExprArray args;
            args.append(*LINK(targetVar));
            unsigned srcSize = normalized->queryType()->getSize();
            if (srcSize < curSize && curSize != UNKNOWN_LENGTH)
            {
                OwnedHqlExpr lengthExpr = getSizetConstant(srcSize);
                OwnedHqlExpr rangeLower = getRangeLimit(fieldType, lengthExpr, normalized, -1);
                OwnedHqlExpr rangeUpper = getRangeLimit(fieldType, lengthExpr, normalized, +1);

                CHqlBoundExpr boundLower, boundUpper;
                translator.buildExpr(subctx, rangeLower, boundLower);
                translator.buildExpr(subctx, rangeUpper, boundUpper);
                args.append(*getPointer(boundLower.expr));
                args.append(*getPointer(boundUpper.expr));
            }
            else
            {
                OwnedHqlExpr address = getMonitorValueAddress(subctx, expandedSelector, normalized);
                args.append(*LINK(address));
                args.append(*LINK(address));
            }
            if (boundSubLength.expr)
                args.append(*LINK(boundSubLength.expr));
            translator.callProcedure(subctx, func, args);
        }
    }
    else
    {
        CHqlBoundExpr boundSubLength;
        ForEachChild(idx2, values)
        {
            BuildCtx subctx(ctx);
            IHqlExpression * cur = values->queryChild(idx2);
            OwnedHqlExpr test = createBoolExpr(no_eq, LINK(lhs), LINK(cur));
            OwnedHqlExpr promoted = getExplicitlyPromotedCompare(test);
            OwnedHqlExpr subrange, compare, normalized;
            extractCompareInformation(subctx, promoted, subrange, compare, normalized, expandedSelector);
            //This never changes (it is a function of the lhs), so only evaluate it first time around this loop
            if (!boundSubLength.expr)
                buildSubRange(ctx, subrange, boundSubLength);
            if (compare)
                translator.buildFilter(subctx, compare);

            OwnedHqlExpr address = getMonitorValueAddress(subctx, expandedSelector, normalized);
            HqlExprArray args;
            args.append(*LINK(targetVar));
            args.append(*LINK(address));
            args.append(*LINK(address));
            if (boundSubLength.expr)
                args.append(*LINK(boundSubLength.expr));
            translator.callProcedure(subctx, func, args);
        }
    }
}

void CppFilterExtractor::noteKeyedFieldUsage(SourceFieldUsage * fieldUsage)
{
    IHqlExpression * tableSelector = tableExpr->queryNormalizedSelector();
    ForEachItemIn(idx, keyableSelects)
    {
        IHqlExpression * selector = &keyableSelects.item(idx);
        ForEachItemIn(cond, keyed.conditions)
        {
            KeyCondition & cur = keyed.conditions.item(cond);
            if ((cur.selector == selector) && !cur.isWild)
            {
                fieldUsage->noteKeyedSelect(selector, tableSelector);
                break;
            }
        }
    }
}

static IHqlExpression * createCompareRecast(node_operator op, IHqlExpression * value, IHqlExpression * recastValue)
{
    if (recastValue->queryValue())
        return LINK(queryBoolExpr(op == no_ne));
    return createValue(op, makeBoolType(), LINK(value), LINK(recastValue));
}


void CppFilterExtractor::extractCompareInformation(BuildCtx & ctx, IHqlExpression * expr, SharedHqlExpr & subrange, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector)
{
    extractCompareInformation(ctx, expr->queryChild(0), expr->queryChild(1), subrange, compare, normalized, expandedSelector);
}


void CppFilterExtractor::extractCompareInformation(BuildCtx & ctx, IHqlExpression * lhs, IHqlExpression * value, SharedHqlExpr & subrange, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector)
{
    //For substring matching the set of values should match the type of the underlying field.
    if (createValueSets)
    {
        IHqlExpression * base = queryStripCasts(lhs);
        if (base->getOperator() == no_substring)
            subrange.set(base->queryChild(1));
    }

    LinkedHqlExpr compareValue = value->queryBody();
    OwnedHqlExpr recastValue;
    if ((lhs->getOperator() != no_select) || (lhs->queryType() != compareValue->queryType()))
    {
        OwnedHqlExpr temp  = castToFieldAndBack(lhs, compareValue);
        if (temp != compareValue)
        {
            //Force into a temporary variable since it will be used more than once, and reapply the field casting/
            compareValue.setown(translator.buildSimplifyExpr(ctx, compareValue));
            //cast to promoted type because sometimes evaluating can convert string to string<n>
            Owned<ITypeInfo> promotedType = getPromotedECLType(lhs->queryType(), compareValue->queryType());
            compareValue.setown(ensureExprType(compareValue, promotedType));
            recastValue.setown(castToFieldAndBack(lhs, compareValue));
        }
    }

    normalized.setown(invertTransforms(lhs, compareValue));
    normalized.setown(foldHqlExpression(normalized));

    if (recastValue && recastValue != compareValue)
        compare.setown(createCompareRecast(no_eq, compareValue, recastValue));
}

void CppFilterExtractor::createStringSet(BuildCtx & ctx, const char * target, unsigned size, IHqlExpression * selector)
{
    assertex(selector->getOperator() == no_select);
    if (createValueSets)
    {
        StringBuffer type;
        translator.buildRtlFieldType(type, selector->queryChild(1), queryRecord(tableExpr));
        ctx.addQuotedF("%s.setown(createValueSet(%s));", target, type.str());
    }
    else
    {
        if (onlyHozedCompares)
            ctx.addQuotedF("%s.setown(createRtlStringSet(%u));", target, size);
        else
        {
            ITypeInfo * type = selector->queryType();
            bool isBigEndian = !type->isInteger() || !isLittleEndian(type);
            ctx.addQuotedF("%s.setown(createRtlStringSetEx(%u,%d,%d));", target, size, isBigEndian, type->isSigned());
        }
    }
}

bool CppFilterExtractor::buildSubRange(BuildCtx & ctx, IHqlExpression * range, CHqlBoundExpr & bound)
{
    if (!createValueSets)
        return false;

    if (!range)
        return false;

    IHqlExpression * limit;
    switch (range->getOperator())
    {
    case no_rangeto:
        limit = range->queryChild(0);
        break;
    case no_range:
        assertex(matchesConstValue(range->queryChild(0), 1));
        limit = range->queryChild(1);
        break;
    case no_constant:
        limit = range;
        assertex(matchesConstValue(range, 1));
        break;
    default:
        throwUnexpected();
    }
    translator.buildExpr(ctx, limit, bound);
    return true;
}


void CppFilterExtractor::buildKeySegmentCompareExpr(BuildFilterState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * targetSet, IHqlExpression & thisKey)
{
    OwnedHqlExpr targetVar = createVariable(targetSet, makeVoidType());
    createStringSet(ctx, targetSet, selectorInfo.size, selectorInfo.expandedSelector);

    if (!exprReferencesDataset(&thisKey, tableExpr))
    {
        BuildCtx subctx(ctx);
        translator.buildFilter(subctx, &thisKey);
        callAddAll(subctx, targetVar);
        return;
    }

    OwnedHqlExpr subrange;
    OwnedHqlExpr compare;
    OwnedHqlExpr normalized;
    BuildCtx subctx(ctx);
    extractCompareInformation(subctx, &thisKey, subrange, compare, normalized, selectorInfo.expandedSelector);
    OwnedHqlExpr address = getMonitorValueAddress(subctx, selectorInfo.expandedSelector, normalized);

    HqlExprArray args;
    args.append(*LINK(targetVar));

    CHqlBoundExpr boundSubLength;
    buildSubRange(ctx, subrange, boundSubLength);

    node_operator op = thisKey.getOperator();
    switch (op)
    {
    case no_eq:
        if (compare)
            translator.buildFilter(subctx, compare);
        args.append(*LINK(address));
        args.append(*LINK(address));
        if (boundSubLength.expr)
            args.append(*LINK(boundSubLength.expr));
        translator.callProcedure(subctx, addRangeFunc, args);
        break;
    case no_ne:
        subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
        if (compare)
            translator.buildFilter(subctx, compare);
        args.append(*LINK(address));
        args.append(*LINK(address));
        if (boundSubLength.expr)
            args.append(*LINK(boundSubLength.expr));
        translator.callProcedure(subctx, killRangeFunc, args);
        break;
    case no_le:
        args.append(*createValue(no_nullptr, makeVoidType()));
        args.append(*LINK(address));
        if (boundSubLength.expr)
            args.append(*LINK(boundSubLength.expr));
        translator.callProcedure(subctx, addRangeFunc, args);
        break;
    case no_lt:
        // e) no_lt.  If isExact add < value else add <= value
        if (compare)
        {
            OwnedHqlExpr invCompare = getInverse(compare);
            IHqlStmt * cond = translator.buildFilterViaExpr(subctx, invCompare);
            //common this up...
            args.append(*createValue(no_nullptr, makeVoidType()));
            args.append(*LINK(address));
            if (boundSubLength.expr)
                args.append(*LINK(boundSubLength.expr));
            translator.callProcedure(subctx, addRangeFunc, args);
            subctx.selectElse(cond);
            args.append(*LINK(targetVar));
        }
        subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
        args.append(*LINK(address));
        args.append(*createValue(no_nullptr, makeVoidType()));
        if (boundSubLength.expr)
            args.append(*LINK(boundSubLength.expr));
        translator.callProcedure(subctx, killRangeFunc, args);
        break;
    case no_ge:
        // d) no_ge.  If isExact add >= value else add > value
        if (compare)
        {
            OwnedHqlExpr invCompare = getInverse(compare);
            IHqlStmt * cond = translator.buildFilterViaExpr(subctx, invCompare);
            //common this up...
            subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
            args.append(*createValue(no_nullptr, makeVoidType()));
            args.append(*LINK(address));
            if (boundSubLength.expr)
                args.append(*LINK(boundSubLength.expr));
            translator.callProcedure(subctx, killRangeFunc, args);
            subctx.selectElse(cond);
            args.append(*LINK(targetVar));
        }
        args.append(*LINK(address));
        args.append(*createValue(no_nullptr, makeVoidType()));
        if (boundSubLength.expr)
            args.append(*LINK(boundSubLength.expr));
        translator.callProcedure(subctx, addRangeFunc, args);
        break;
    case no_gt:
        subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
        args.append(*createValue(no_nullptr, makeVoidType()));
        args.append(*LINK(address));
        if (boundSubLength.expr)
            args.append(*LINK(boundSubLength.expr));
        translator.callProcedure(subctx, killRangeFunc, args);
        break;
    case no_between:
    case no_notbetween:
        {
            //NB: This should only be generated for substring queries.  User betweens are converted
            //to two separate comparisons to cope with range issues.
            args.append(*LINK(address));

            CHqlBoundExpr rhs2;
            OwnedHqlExpr adjustedUpper = invertTransforms(thisKey.queryChild(0), thisKey.queryChild(2));
            OwnedHqlExpr foldedUpper = foldHqlExpression(adjustedUpper);
            OwnedHqlExpr hozedValue = getHozedKeyValue(foldedUpper);
            IIdAtom * name = hozedValue->queryId();
            if ((name != createRangeHighId) && (name != createQStrRangeHighId))
                hozedValue.setown(ensureExprType(hozedValue, selectorInfo.expandedSelector->queryType()));
            translator.buildExpr(subctx, hozedValue, rhs2);
            translator.ensureHasAddress(subctx, rhs2);
            args.append(*getPointer(rhs2.expr));
            if (op == no_between)
            {
                if (boundSubLength.expr)
                    args.append(*LINK(boundSubLength.expr));
                translator.callProcedure(subctx, addRangeFunc, args);
            }
            else
            {
                subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
                if (boundSubLength.expr)
                    args.append(*LINK(boundSubLength.expr));
                translator.callProcedure(subctx, killRangeFunc, args);
            }
            break;
        }
    default:
        throwUnexpectedOp(op);
    }
}


//Note this function may change the incoming ctx if filterKind is not NoMonitorFilter
void CppFilterExtractor::buildKeySegmentExpr(BuildFilterState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * requiredSet, IHqlExpression & thisKey, MonitorFilterKind filterKind)
{
    const char * targetSet = requiredSet;
    StringBuffer s;
    unsigned curSize = selectorInfo.size;
    node_operator op = thisKey.getOperator();

    BuildCtx subctx(ctx);
    BuildCtx * appendCtx = &ctx;
    StringBuffer createMonitorText;

    switch (op)
    {
    case no_in:
    case no_notin:
        {
            if (!targetSet)
                targetSet = buildState.getSetName(createValueSets);
            buildKeySegmentInExpr(buildState, selectorInfo, ctx, targetSet, thisKey, filterKind);
            break;
        }
    case no_if:
        {
            MonitorFilterKind childFilter = targetSet ? NoMonitorFilter : filterKind;
            IHqlStmt * ifStmt = translator.buildFilterViaExpr(subctx, thisKey.queryChild(0));
            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, *thisKey.queryChild(1), childFilter);
            subctx.selectElse(ifStmt);
            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, *thisKey.queryChild(2), childFilter);
            break;
        }
    case no_and:
        {
            HqlExprArray matches;
            OwnedHqlExpr invariant = unwindConjunction(matches, &thisKey);
            unsigned numMatches = matches.ordinality();

            if (!targetSet && numMatches > 1)
                targetSet = buildState.getSetName(createValueSets);

            IHqlStmt * ifStmt = NULL;
            if (invariant)
            {
                ifStmt = translator.buildFilterViaExpr(subctx, invariant);
                if (filterKind == MonitorFilterSkipEmpty)
                    ctx.set(subctx);
            }

            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, matches.item(0), NoMonitorFilter);
            for (unsigned i=1; i< numMatches; i++)
            {
                IHqlExpression & cur = matches.item(i);
                const char * curTarget = buildState.getSetName(createValueSets);
                BuildCtx childctx(subctx);
                buildKeySegmentExpr(buildState, selectorInfo, childctx, curTarget, cur, MonitorFilterSkipAll);
                if (createValueSets)
                    childctx.addQuotedF("%s->intersectSet(%s);", targetSet, curTarget);
                else
                    childctx.addQuotedF("%s.setown(rtlIntersectSet(%s,%s));", targetSet, targetSet, curTarget);
                buildState.popSetName();
            }

            if (invariant && (filterKind != MonitorFilterSkipEmpty))
            {
                subctx.selectElse(ifStmt);
                if (targetSet)
                    createStringSet(subctx, targetSet, curSize, selectorInfo.selector);
                else
                    buildEmptyKeySegment(buildState, subctx, selectorInfo);
            }
            break;
        }
    case no_or:
        {
            HqlExprArray matches;
            OwnedHqlExpr invariant = unwindConjunction(matches, &thisKey);
            unsigned numMatches = matches.ordinality();

            if (invariant)
            {
                if (filterKind == MonitorFilterSkipAll)
                {
                    OwnedHqlExpr test = getInverse(invariant);
                    translator.buildFilter(subctx, test);
                    ctx.set(subctx);
                }
                else
                {
                    IHqlStmt * ifStmt = translator.buildFilterViaExpr(subctx, invariant);
                    if (targetSet)
                    {
                        createStringSet(subctx, targetSet, curSize, selectorInfo.selector);
                        OwnedHqlExpr targetVar = createVariable(targetSet, makeVoidType());
                        callAddAll(subctx, targetVar);
                    }
                    subctx.selectElse(ifStmt);
                }
            }

            appendCtx = &subctx;
            if (!targetSet && numMatches > 1)
                targetSet = buildState.getSetName(createValueSets);

            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, matches.item(0), NoMonitorFilter);
            for (unsigned i=1; i < numMatches; i++)
            {
                IHqlExpression & cur = matches.item(i);
                const char * curTarget = buildState.getSetName(createValueSets);
                BuildCtx childctx(subctx);
                buildKeySegmentExpr(buildState, selectorInfo, childctx, curTarget, cur, MonitorFilterSkipEmpty);
                if (createValueSets)
                    childctx.addQuotedF("%s->unionSet(%s);", targetSet, curTarget);
                else
                    childctx.addQuotedF("%s.setown(rtlUnionSet(%s, %s));", targetSet, targetSet, curTarget);
                buildState.popSetName();
            }
            break;
        }
    case no_eq:
        {
            if (!targetSet)
            {
                if (buildSingleKeyMonitor(createMonitorText, selectorInfo, subctx, thisKey))
                    break;
                targetSet = buildState.getSetName(createValueSets);
            }
            buildKeySegmentCompareExpr(buildState, selectorInfo, ctx, targetSet, thisKey);
            break;
        }
    default:
        {
            if (!targetSet)
                targetSet = buildState.getSetName(createValueSets);
            buildKeySegmentCompareExpr(buildState, selectorInfo, ctx, targetSet, thisKey);
            break;
        }
    }

    if (targetSet && !requiredSet)
    {
        if (createValueSets)
            createMonitorText.appendf("createFieldFilter(%u, %s)", selectorInfo.fieldIdx, targetSet);
        else
            createMonitorText.appendf("createKeySegmentMonitor(%s, %s.getClear(), %u, %u, %u)",
                                      boolToText(selectorInfo.keyedKind != KeyedYes), targetSet, selectorInfo.fieldIdx, selectorInfo.offset, selectorInfo.size);


        buildState.popSetName();
    }

    if (createMonitorText.length())
    {
        if (buildState.listName)
        {
            if (createValueSets)
                appendCtx->addQuotedF("%s->append(%s, %s);", buildState.listName, selectorInfo.getFFOptions(), createMonitorText.str());
            else
                appendCtx->addQuotedF("%s->append(%s);", buildState.listName, createMonitorText.str());
        }
        else
            appendCtx->addQuotedF("return %s;", createMonitorText.str());
    }
}


IHqlExpression * CppFilterExtractor::getMonitorValueAddress(BuildCtx & ctx, IHqlExpression * selector, IHqlExpression * _value)
{
    LinkedHqlExpr value = _value;
    CHqlBoundExpr bound;
    ITypeInfo * type = selector->queryType();
    bool castViaRow = isUnknownSize(type);
    if (castViaRow)
    {
        IHqlExpression * field = selector->queryChild(1);
        OwnedHqlExpr record = createRecord(field);
        OwnedHqlExpr self = createSelector(no_self, record, nullptr);
        OwnedHqlExpr assign = createValue(no_assign, makeVoidType(), createNewSelectExpr(LINK(self), LINK(field)), LINK(value));
        OwnedHqlExpr transform = createValue(no_transform, makeTransformType(record->getType()), LINK(assign));
        OwnedHqlExpr row = createRow(no_createrow, LINK(transform));
        translator.buildAnyExpr(ctx, row, bound);
    }
    else
    {
        if (!createValueSets)
        {
            //Need to ensure old segmonitors for varstrings are filled with \0s
            switch (type->getTypeCode())
            {
            case type_varstring: case type_varunicode:
                {
                    assertex(type->getSize() != UNKNOWN_LENGTH);
                    CHqlBoundTarget tempTarget;
                    translator.createTempFor(ctx, type, tempTarget, typemod_none, FormatNatural);
                    //clear the variable.
                    HqlExprArray args;
                    args.append(*getPointer(tempTarget.expr));
                    args.append(*getZero());
                    args.append(*getSizetConstant(type->getSize()));
                    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(memsetId, args);
                    ctx.addExpr(call);
                    //then assign over the top
                    translator.buildExprAssign(ctx, tempTarget, value);
                    bound.setFromTarget(tempTarget);
                    break;
                }
            }
        }
        if (!bound.expr)
        {
            translator.buildExpr(ctx, value, bound);
            translator.ensureHasAddress(ctx, bound);
        }
    }
    return getPointer(bound.expr);
}

bool CppFilterExtractor::buildSingleKeyMonitor(StringBuffer & createMonitorText, KeySelectorInfo & selectorInfo, BuildCtx & ctx, IHqlExpression & thisKey)
{
    if (selectorInfo.subrange)
        return false;

    BuildCtx subctx(ctx);
    OwnedHqlExpr subrange, compare, normalized;

    StringBuffer funcName;
    extractCompareInformation(subctx, &thisKey, subrange, compare, normalized, selectorInfo.expandedSelector);
    if (compare || subrange)
        return false;

    if (createValueSets)
    {
        StringBuffer type;
        translator.buildRtlFieldType(type, selectorInfo.selector->queryChild(1), tableExpr->queryRecord());

        //MORE: Need to ensure it is exactly the correct format - e.g. variable length strings are length prefixed
        OwnedHqlExpr address = getMonitorValueAddress(subctx, selectorInfo.expandedSelector, normalized);
        StringBuffer addrText;
        translator.generateExprCpp(addrText, address);

        createMonitorText.appendf("createFieldFilter(%u, %s, %s)", selectorInfo.fieldIdx, type.str(), addrText.str());
    }
    else
    {
        ITypeInfo * type = selectorInfo.expandedSelector->queryType();
        type_t tc = type->getTypeCode();
        if ((tc == type_int) || (tc == type_swapint))
        {
            if (isLittleEndian(type))
            {
                if (type->isSigned())
                    funcName.append("createSingleLittleSignedKeySegmentMonitor");
                else if (type->getSize() != 1)
                    funcName.append("createSingleLittleKeySegmentMonitor");
            }
            else
            {
                if (type->isSigned())
                    funcName.append("createSingleBigSignedKeySegmentMonitor");
                else
                    funcName.append("createSingleKeySegmentMonitor");
            }
        }

        if (!funcName.length())
            funcName.append("createSingleKeySegmentMonitor");

        OwnedHqlExpr address = getMonitorValueAddress(subctx, selectorInfo.expandedSelector, normalized);
        StringBuffer addrText;
        translator.generateExprCpp(addrText, address);

        createMonitorText.append(funcName)
                         .appendf("(%s, %u, %u, %u, %s)",
                                  boolToText(selectorInfo.keyedKind != KeyedYes), selectorInfo.fieldIdx, selectorInfo.offset, selectorInfo.size, addrText.str());
    }
    return true;
}

KeyedKind getKeyedKind(HqlCppTranslator & translator, KeyConditionArray & matches)
{
    KeyedKind keyedKind = KeyedNo;
    ForEachItemIn(i, matches)
    {
        KeyCondition & cur = matches.item(i);
        if (cur.keyedKind != keyedKind)
        {
            if (keyedKind == KeyedNo)
                keyedKind = cur.keyedKind;
            else
                translator.throwError1(HQLERR_InconsistentKeyedOpt, str(cur.selector->queryChild(1)->queryName()));
        }
    }
    return keyedKind;
}

void CppFilterExtractor::buildEmptyKeySegment(BuildFilterState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo)
{
    StringBuffer s;
    if (createValueSets)
    {
        StringBuffer type;
        translator.buildRtlFieldType(type, selectorInfo.selector->queryChild(1), queryRecord(selectorInfo.selector->queryChild(0)));
        ctx.addQuoted(s.appendf("%s->append(%s, createEmptyFieldFilter(%u, %s));", buildState.listName, selectorInfo.getFFOptions(), selectorInfo.fieldIdx, type.str()));
    }
    else
        ctx.addQuoted(s.appendf("%s->append(createEmptyKeySegmentMonitor(%s, %u, %u, %u));", buildState.listName, boolToText(selectorInfo.keyedKind != KeyedYes), selectorInfo.fieldIdx, selectorInfo.offset, selectorInfo.size));
}


void CppFilterExtractor::buildKeySegment(BuildFilterState & buildState, BuildCtx & ctx, unsigned whichField, unsigned curSize)
{
    IHqlExpression * selector = &keyableSelects.item(whichField);
    IHqlExpression * expandedSelector = &expandedSelects.item(whichField);
    IHqlExpression * field = selector->queryChild(1);
    KeyConditionArray matches;
    bool isImplicit = true;
    bool prevWildWasKeyed = buildState.wildWasKeyed;
    buildState.wildWasKeyed = false;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.selector == selector)
        {
            cur.generated = true;
            if (cur.isWild)
            {
                isImplicit = false;
                if (cur.wasKeyed)
                    buildState.wildWasKeyed = true;
                else if (buildState.implicitWildField && !ignoreUnkeyed)
                {
                    StringBuffer s, keyname;
                    translator.throwError3(HQLERR_WildFollowsGap, getExprECL(field, s).str(), str(buildState.implicitWildField->queryChild(1)->queryName()), queryKeyName(keyname));
                }
            }
            else
            {
                matches.append(OLINK(cur));
                if (buildState.implicitWildField && !ignoreUnkeyed)
                {
                    StringBuffer s,keyname;
                    if (cur.isKeyed())
                        translator.throwError3(HQLERR_KeyedFollowsGap, getExprECL(field, s).str(), str(buildState.implicitWildField->queryChild(1)->queryName()), queryKeyName(keyname));
                    else if (!buildState.doneImplicitWarning)
                    {
                        translator.WARNING3(CategoryEfficiency, HQLWRN_KeyedFollowsGap, getExprECL(field, s).str(), str(buildState.implicitWildField->queryChild(1)->queryName()), queryKeyName(keyname));
                        buildState.doneImplicitWarning = true;
                    }
                }
            }
        }
    }
    if (buildState.wildWasKeyed && (matches.ordinality() == 0))
    {
        StringBuffer keyname;
        translator.WARNING2(CategoryFolding, HQLWRN_FoldRemoveKeyed, str(field->queryName()), queryKeyName(keyname));
    }

    StringBuffer s;
    KeyedKind keyedKind = getKeyedKind(translator, matches);
    if (whichField >= firstOffsetField)
        translator.throwError1(HQLERR_KeyedNotKeyed, getExprECL(field, s).str());
    KeySelectorInfo selectorInfo(keyedKind, selector, nullptr, expandedSelector, buildState.curFieldIdx, buildState.curOffset, curSize);

    bool ignoreKeyedExtend = false;
    if ((keyedKind == KeyedExtend) && buildState.wildPending() && !ignoreUnkeyed)
    {
        if (keyedKind == KeyedExtend)
        {
            if (prevWildWasKeyed)
                buildState.wildWasKeyed = true;
            else
            {
                StringBuffer keyname;
                translator.WARNING2(CategoryEfficiency, HQLERR_OptKeyedFollowsWild, getExprECL(field, s).str(), queryKeyName(keyname));
            }
        }
        //previous condition folded so always true, so keyed,opt will always be a wildcard.

        if (!allowDynamicFormatChange)
            ignoreKeyedExtend = true;
        isImplicit = false;
    }

    if (matches.ordinality() && !ignoreKeyedExtend)
    {
        if (buildState.wildPending() && !ignoreUnkeyed)
            buildState.clearWild();

        HqlExprArray args;
        ForEachItemIn(i, matches)
        {
            KeyCondition & cur = matches.item(i);
            args.append(*LINK(cur.expr));
        }

        OwnedHqlExpr fullExpr = createBalanced(no_and, queryBoolType(), args);
        BuildCtx subctx(ctx);
        buildKeySegmentExpr(buildState, selectorInfo, subctx, NULL, *fullExpr, ignoreUnkeyed ? MonitorFilterSkipAll : NoMonitorFilter);
    }
    else
    {
        if (isImplicit)
        {
            buildState.implicitWildField.set(selector);
            buildState.doneImplicitWarning = false;
        }

        if (buildState.wildPending() && noMergeSelects.contains(*selector))
            buildState.clearWild();

        if (!buildState.wildPending())
            buildState.wildOffset = buildState.curOffset;
    }
    buildState.curOffset += selectorInfo.size;
    buildState.curFieldIdx++;
}

void CppFilterExtractor::spotSegmentCSE(BuildCtx & ctx)
{
    //This could make things much better, but needs some thought
    HqlExprArray conditions;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.expr)
            conditions.append(*LINK(cur.expr));
    }

    HqlExprArray associated;
    IHqlExpression * selector = tableExpr->queryNormalizedSelector();
    translator.traceExpressions("beforeSegmentCse", conditions);
    spotScalarCSE(conditions, associated, NULL, selector, translator.queryOptions().spotCseInIfDatasetConditions);
    translator.traceExpressions("afterSegmentCse", conditions);

    unsigned curCond = 0;
    ForEachItemIn(i, conditions)
    {
        IHqlExpression * cur = &conditions.item(i);
        switch (cur->getOperator())
        {
        case no_alias:
            translator.buildStmt(ctx, cur);
            break;
        case no_alias_scope:
            translator.expandAliasScope(ctx, cur);
            cur = cur->queryChild(0);
            //fallthrough
        default:
            for (;;)
            {
                if (!keyed.conditions.isItem(curCond))
                    throwUnexpected();
                KeyCondition & keyCond = keyed.conditions.item(curCond++);
                if (keyCond.expr)
                {
                    keyCond.expr.set(cur);
                    break;
                }
            }
            break;
        }
    }
    for (;;)
    {
        if (!keyed.conditions.isItem(curCond))
            break;
        KeyCondition & keyCond = keyed.conditions.item(curCond++);
        assertex(!keyCond.expr);
    }
}


void CppFilterExtractor::buildSegments(BuildCtx & ctx, const char * listName, bool _ignoreUnkeyed)
{
    translator.useInclude("rtlkey.hpp");
    ignoreUnkeyed = _ignoreUnkeyed;

    if (translator.queryOptions().spotCSE)
        spotSegmentCSE(ctx);

    BuildFilterState buildState(ctx, listName);
    ForEachItemIn(idx, keyableSelects)
    {
        IHqlExpression * selector = &keyableSelects.item(idx);
        IHqlExpression * expandedSelector = &expandedSelects.item(idx);
        IHqlExpression * field = selector->queryChild(1);
        unsigned curSize = expandedSelector->queryType()->getSize();
        assertex(createValueSets || curSize != UNKNOWN_LENGTH);

        //MORE: Should also allow nested record structures, and allow keying on first elements.
        //      and field->queryType()->getSize() doesn't work for alien datatypes etc.
        if(!field->hasAttribute(virtualAtom))
            buildKeySegment(buildState, ctx, idx, curSize);
    }

    //check that all keyed entries have been matched
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (!cur.generated)
            translator.throwError1(HQLERR_OnlyKeyFixedField, str(cur.selector->queryChild(1)->queryId()));
    }
}


IHqlExpression * CppFilterExtractor::getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary)
{
    IHqlExpression * constExpr = FilterExtractor::getRangeLimit(fieldType, lengthExpr, value, whichBoundary);
    if (constExpr)
        return constExpr;

    type_t ftc = fieldType->getTypeCode();
    unsigned fieldLength = fieldType->getStringLen();
    IIdAtom * func;
    if (whichBoundary < 0)
    {
        switch (ftc)
        {
        case type_qstring:
            func = createQStrRangeLowId;
            break;
        case type_string:
            func = createStrRangeLowId;
            break;
        case type_data:
            func = createDataRangeLowId;
            break;
        case type_unicode:
            func = createUnicodeRangeLowId;
            break;
        default:
            func = createRangeLowId;
            break;
        }
    }
    else
    {
        switch (ftc)
        {
        case type_qstring:
            func = createQStrRangeHighId;
            break;
        case type_string:
            func = createStrRangeHighId;
            break;
        case type_data:
            func = createDataRangeHighId;
            break;
        case type_unicode:
            func = createUnicodeRangeHighId;
            break;
        default:
            func = createRangeHighId;
            break;
        }
    }

    HqlExprArray args;
    args.append(*getSizetConstant(fieldLength));
    args.append(*LINK(lengthExpr));
    args.append(*LINK(value));

    //Note: I can't change the return type of the function - because if fixed length then wrong call is made, and variable length is worse code.
    OwnedHqlExpr call = translator.bindFunctionCall(func, args);
    return createValue(no_typetransfer, LINK(fieldType), LINK(call));
}


static HqlTransformerInfo selectSpotterInfo("SelectSpotter");
CppFilterExtractor::SelectSpotter::SelectSpotter(const HqlExprArray & _selects) : NewHqlTransformer(selectSpotterInfo), selects(_selects)
{
    hasSelects = false;
}

void CppFilterExtractor::SelectSpotter::analyseExpr(IHqlExpression * expr)
{
    if (hasSelects || alreadyVisited(expr))
        return;
    if (selects.find(*expr) != NotFound)
    {
        hasSelects = true;
        return;
    }
    NewHqlTransformer::analyseExpr(expr);
}
