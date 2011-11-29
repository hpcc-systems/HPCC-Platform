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

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"

#include "hql.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlutil.hpp"
#include "hqlpmap.hpp"

#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlopt.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqllib.ipp"
#include "hqlresource.hpp"
#include "hqlregex.ipp"
#include "hqlsource.ipp"
#include "hqlcse.ipp"
#include "hqlgraph.ipp"
#include "hqlscope.hpp"
#include "hqlccommon.hpp"

#include "eclhelper.hpp"

#include "deffield.hpp"

//===========================================================================

class SteppingCondition
{
public:
    SteppingCondition(bool _onlyEquality, IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * _rowsid);

    bool extractSteppingCondition(IHqlExpression * expr, IHqlExpression * sortOrder);
    IHqlExpression * createEqualitySortList();
    bool matchedAny() { return equalities.ordinality() != 0 || compareLhs; }
    IHqlExpression * queryRangeLeftSelector() { return compareLhs; }
    IHqlExpression * queryExtraFilter() { return extraCompare; }
    IHqlExpression * queryGlobalCompare() { return globalCompare; }

    IHqlExpression * getMaxLeftBeforeRight() { return getNegative(lhsDelta); }
    IHqlExpression * getMaxRightBeforeLeft() { return getNegative(rhsDelta); }

protected:
    void clearRangeMatch()
    {
        compareLhs.clear();
    }

    bool containsRowsLeft(IHqlExpression * expr);
    bool extractCondition(IHqlExpression * expr, IHqlExpression * searchField);
    bool extractCondition(HqlExprArray & args, IHqlExpression * searchField);
    bool extractComparison(IHqlExpression * lhs, IHqlExpression * rhs, IHqlExpression * searchField, bool isEqual = false);
    bool isLeftRightInvariant(IHqlExpression * expr);
    IHqlExpression * simplifyArgument(IHqlExpression * expr, SharedHqlExpr & delta, bool invert);

protected:
    HqlExprArray equalities;
    HqlExprAttr compareLhs;
    HqlExprAttr lhsDelta;               // left.x >= right.x + lhsDelta
    HqlExprAttr rhsDelta;               // right.x >= left.x + rhsDelta
    OwnedHqlExpr extraCompare;
    OwnedHqlExpr globalCompare;
    LinkedHqlExpr left;
    LinkedHqlExpr right;
    OwnedHqlExpr rowsLeftExpr;
    bool onlyEquality;
    bool explicitStepped;
};


SteppingCondition::SteppingCondition(bool _onlyEquality, IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * _rowsid) : left(_left), right(_right), onlyEquality(_onlyEquality)
{
    explicitStepped = false;
    rowsLeftExpr.setown(createDataset(no_rows, LINK(left), LINK(_rowsid)));
}

IHqlExpression * SteppingCondition::createEqualitySortList()
{
    return createValueSafe(no_sortlist, makeSortListType(NULL), equalities);
}

bool SteppingCondition::extractSteppingCondition(IHqlExpression * expr, IHqlExpression * sortOrder)
{
    HqlExprArray args, stepArgs;
    expr->unwindList(args, no_and);

    explicitStepped = false;;
    ForEachItemIn(i1, args)
    {
        IHqlExpression & cur = args.item(i1);
        if (cur.getOperator() == no_assertstepped)
        {
            explicitStepped = true;
            cur.queryChild(0)->unwindList(stepArgs, no_and);
        }
    }

    //The merge order defines the order that the stepping fields are processed in.
    HqlExprArray order, expandedOrder;
    unwindChildren(order, sortOrder);
    expandRowSelectors(expandedOrder, order);
    bool foundStepped = explicitStepped;
    ForEachItemIn(i2, expandedOrder)
    {
        IHqlExpression * cur = &expandedOrder.item(i2);
        if (explicitStepped)
        {
            if (!extractCondition(stepArgs, cur))
            {
                StringBuffer s;
                if (cur->getOperator() == no_select)
                    s.append(cur->queryChild(1)->queryName());
                else
                    getExprECL(cur, s);
                throwError1(HQLERR_SteppingNotMatchSortCondition, s.str());
            }
            if (stepArgs.ordinality() == 0)
                break;
        }
        else
        {
            if (!extractCondition(args, cur))
                break;
            foundStepped = true;
        }
        if (compareLhs)
            break;
    }

    if (stepArgs.ordinality())
        throwError1(HQLERR_SteppingNotMatchSortCondition, "");

    //Walk the list of non stepped condition, and retain any that are dependent on rows(left)
    ForEachItemIn(i3, args)
    {
        IHqlExpression & cur = args.item(i3);
        if (cur.getOperator() != no_assertstepped)
        {
            if (containsRowsLeft(&cur))
                extendConditionOwn(globalCompare, no_and, LINK(&cur));
            else
                extendConditionOwn(extraCompare, no_and, LINK(&cur));
        }
    }

    return foundStepped;
}


bool SteppingCondition::containsRowsLeft(IHqlExpression * expr)
{
    OwnedHqlExpr null = createDataset(no_null, LINK(left->queryRecord()));
    OwnedHqlExpr replaceLeft = replaceExpression(expr, rowsLeftExpr, null);
    return replaceLeft != expr;
}

bool SteppingCondition::isLeftRightInvariant(IHqlExpression * expr)
{
    //MORE: is his good enough?
    OwnedHqlExpr replaceLeft = replaceSelector(expr, left, right);
    OwnedHqlExpr replaceRight = replaceSelector(expr, right, left);
    if (expr == replaceLeft && expr == replaceRight)
        return true;
    return false;
}


void adjustValue(SharedHqlExpr & total, IHqlExpression * value, bool invert)
{
    if (total)
        total.setown(adjustBoundIntegerValues(total, value, invert));
    else if (!invert)
        total.set(value);
    else
        total.setown(getNegative(value));
}


IHqlExpression * SteppingCondition::simplifyArgument(IHqlExpression * expr, SharedHqlExpr & delta, bool invert)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_cast:
        case no_implicitcast:
            if (!castPreservesValueAndOrder(expr))
                return expr;
            expr = expr->queryChild(0);
            break;
        case no_add:
            {
                IHqlExpression * lhs = expr->queryChild(0);
                IHqlExpression * rhs = expr->queryChild(1);
                if (isLeftRightInvariant(rhs))
                {
                    adjustValue(delta, rhs, invert);
                    expr = lhs;
                }
                else if (isLeftRightInvariant(lhs))
                {
                    adjustValue(delta, lhs, invert);
                    expr = rhs;
                }
                else
                    return expr;
                break;
            }
        case no_sub:
            {
                IHqlExpression * lhs = expr->queryChild(0);
                IHqlExpression * rhs = expr->queryChild(1);
                if (isLeftRightInvariant(rhs))
                {
                    adjustValue(delta, rhs, !invert);
                    expr = lhs;
                }
                else
                    return expr;
                break;
            }
        default:
            return expr;
        }
    }
}


bool SteppingCondition::extractComparison(IHqlExpression * lhs, IHqlExpression * rhs, IHqlExpression * searchField, bool isEqual)
{
    OwnedHqlExpr lhsSelect;
    OwnedHqlExpr delta;
    IHqlExpression * simpleLhs = simplifyArgument(lhs, delta, true);
    IHqlExpression * simpleRhs = simplifyArgument(rhs, delta, false);

    OwnedHqlExpr searchRightField = replaceSelector(searchField, left, right);
    if ((simpleLhs == searchField) && (simpleRhs == searchRightField))
    {
        compareLhs.set(searchField);
        if (!delta)
            delta.setown(getZero());
        if (lhsDelta)
        {
            StringBuffer s;
            throwError1(HQLERR_SteppedMultiRange, getExprECL(searchField,s).str());
        }
        lhsDelta.set(delta);
        if (isEqual)
            rhsDelta.setown(getNegative(delta));
        return true;
    }
    if ((simpleLhs == searchRightField) && (simpleRhs == searchField))
    {
        compareLhs.set(searchField);
        if (!delta)
            delta.setown(getZero());
        if (rhsDelta)
        {
            StringBuffer s;
            throwError1(HQLERR_SteppedMultiRange, getExprECL(searchRightField,s).str());
        }
        rhsDelta.set(delta);
        if (isEqual)
            lhsDelta.setown(getNegative(delta));
        return true;
    }
    return false;
}

bool SteppingCondition::extractCondition(IHqlExpression * expr, IHqlExpression * searchField)
{
    //Search for LEFT.someSelect = right.someSelect
    node_operator op = expr->getOperator();
    assertex(op != no_and);

    if (op == no_eq)
    {
        IHqlExpression * lhs = expr->queryChild(0);
        IHqlExpression * rhs = expr->queryChild(1);
        if (lhs == searchField)
        {
            OwnedHqlExpr replaced = replaceSelector(rhs, right, left);
            if (replaced == lhs)
            {
                equalities.append(*LINK(lhs));
                return true;
            }
        }
        if (rhs == searchField)
        {
            OwnedHqlExpr replaced = replaceSelector(lhs, right, left);
            if (replaced == rhs)
            {
                equalities.append(*LINK(rhs));
                return true;
            }
        }
    }

    if (!onlyEquality)
    {
        //left.x + d1 >= right.wpos + d2                (d1, d2 may be subtracted, and may be implicit casts in the expression)
        //normalize to left.x >= right.x + delta
        //left => maxRightAfterLeft = -delta;    right => maxRightBeforeLeft = -delta;

        switch (op)
        {
        case no_ge:
            return extractComparison(expr->queryChild(0), expr->queryChild(1), searchField);
        case no_le:
            return extractComparison(expr->queryChild(1), expr->queryChild(0), searchField);
        case no_between:
            if (extractComparison(expr->queryChild(0), expr->queryChild(1), searchField))
            {
                if (extractComparison(expr->queryChild(2), expr->queryChild(0), searchField))
                    return true;
                clearRangeMatch();
            }
            break;
        case no_eq:
            return extractComparison(expr->queryChild(0), expr->queryChild(1), searchField, true);
        }
    }
    return false;
}


bool SteppingCondition::extractCondition(HqlExprArray & args, IHqlExpression * searchField)
{
    assertex(!compareLhs);
    UnsignedArray matched;

    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        if (extractCondition(&cur, searchField))
            matched.append(i);
    }

    if (compareLhs)
    {
        //Only matched in one direction
        if (!lhsDelta || !rhsDelta)
        {
            if (explicitStepped)
                throwError(HQLERR_SteppedRangeOnlyOneDirection);
            matched.kill();
        }
        else
        {
            ForEachItemInRev(i2, matched)
            {
                IHqlExpression & cur = args.item(matched.item(i2));
                extendConditionOwn(extraCompare, no_and, LINK(&cur));
            }
        }
    }

    ForEachItemInRev(i2, matched)
        args.remove(matched.item(i2));

    return matched.ordinality() != 0;
}

//---------------------------------------------------------------------------

void SteppingFieldSelection::expandTransform(IHqlExpression * expr)
{
    IHqlExpression * parent = expr->queryChild(0)->queryNormalizedSelector();

    TableProjectMapper mapper(expr);
    if (!mapper.isMappingKnown())
        throwError(HQLERR_CantProjectStepping);

    fields.setown(mapper.expandFields(fields, ds, parent));
    fields.setown(expandCreateRowSelectors(fields));
    ds.set(parent);
}

void SteppingFieldSelection::extractFields(SteppingFieldSelection & steppingFields)
{
    steppingFields.ds.set(ds);
    HqlExprArray args;
    ForEachChild(i, fields)
    {
        IHqlExpression * cur = fields->queryChild(i);
        args.append(*extractSelect(cur));
    }
    steppingFields.fields.setown(fields->clone(args));
}


static void throwTooComplexToStep(IHqlExpression * expr)
{
    StringBuffer ecl;
    getExprECL(expr, ecl, true, false);
    throwError1(HQLERR_TooComplexToStep, ecl.str());
}

IHqlExpression * SteppingFieldSelection::extractSelect(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_filepos:
        case no_file_logicalname:
            //MORE: We should really catch more problems like this...
            throwError(HQLERR_NoSteppingOnPayload);
        case no_cast:
        case no_implicitcast:
            {
                if (!castPreservesValueAndOrder(expr))
                {
                    switch (expr->queryChild(0)->getOperator())
                    {
                    case no_filepos:
                    case no_file_logicalname:
                        throwError(HQLERR_NoSteppingOnPayload);
                    default:
                        throwTooComplexToStep(expr);
                    }
                }
                expr = expr->queryChild(0);
                break;
            }
        case no_add:
        case no_sub:
            {
                //cope with biasing on indexes.
                IHqlExpression * rhs = expr->queryChild(1);
                switch (rhs->getOperator())
                {
                case no_constant:
                    break;
                default:
                    throwTooComplexToStep(expr);
                }
                expr = expr->queryChild(0);
                break;
            }
        case no_select:
            return LINK(expr);
        default:
            throwTooComplexToStep(expr);
        }
    }
}

void SteppingFieldSelection::gatherFieldOffsetSizes(HqlCppTranslator & translator, UnsignedArray & result)
{
    //A pseudo context in somewhere that will never be generated.
    BuildCtx ctx(*translator.queryCode(), _internal_Atom);
    ctx.addGroup();
    translator.bindTableCursor(ctx, ds, "x");

    CHqlBoundExpr bound;
    StringBuffer s;
    ForEachChild(i, fields)
    {
        IHqlExpression * cur = fields->queryChild(i);
        assertex(cur->getOperator() == no_select);
        Owned<IReferenceSelector> selector = translator.buildActiveReference(ctx, cur);
        selector->getOffset(ctx, bound);
        IValue * offsetValue = bound.expr->queryValue();
        if (offsetValue)
            result.append((unsigned)offsetValue->getIntValue());
        else
            throwError1(HQLERR_SteppedVariableOffset, getExprECL(cur, s).str());

        selector->getSize(ctx, bound);
        IValue * sizeValue = bound.expr->queryValue();
        if (sizeValue)
            result.append((unsigned)sizeValue->getIntValue());
        else
            throwError1(HQLERR_SteppedVariableSize, getExprECL(cur, s).str());
    }
}

IHqlExpression * SteppingFieldSelection::generateSteppingMeta(HqlCppTranslator & translator)
{
    OwnedHqlExpr normalFields = replaceSelector(fields, ds, queryActiveTableSelector());
    OwnedHqlExpr key = createAttribute(_steppedMeta_Atom, LINK(ds->queryRecord()->queryBody()), LINK(normalFields));
    BuildCtx declarectx(*translator.queryCode(), declareAtom);

    HqlExprAssociation * match = declarectx.queryMatchExpr(key);
    if (match)
        return match->queryExpr();

    BuildCtx classctx(declarectx);
    StringBuffer s, s2;
    StringBuffer memberName, offsetName;
    unique_id_t id = translator.getUniqueId();
    appendUniqueId(memberName.append("st"), id);
    appendUniqueId(offsetName.append("so"), id);

    UnsignedArray offsets;
    gatherFieldOffsetSizes(translator, offsets);

    unsigned lenOffsets = offsets.ordinality();
    s.clear();
    s.append("CFieldOffsetSize ").append(offsetName).append("[").append(lenOffsets/2).append("] = {");
    for (unsigned i=0; i < lenOffsets; i += 2)
    {
        if (i) s.append(",");
        s.append("{").append(offsets.item(i)).append(",").append(offsets.item(i+1)).append("}");
    }
    s.append("};");
    declarectx.setNextPriority(SteppedPrio);
    declarectx.addQuoted(s);

    //MORE: This might be better commoned up globally, depending of number of instances
    classctx.setNextPriority(SteppedPrio);
    classctx.addQuotedCompound(s.clear().append("struct C").append(memberName).append(" : public ISteppingMeta"), s2.append(" ").append(memberName).append(";").str());
    translator.doBuildUnsignedFunction(classctx, "getNumFields", lenOffsets/2);

    classctx.addQuoted(s.clear().append("virtual const CFieldOffsetSize * queryFields() { return ").append(offsetName).append("; }"));

    //compare function.
    {
        StringBuffer compareName;
        translator.getUniqueId(compareName.append("c"));
        OwnedITypeInfo intType = makeIntType(4, true);
        OwnedHqlExpr result = createVariable("ret", LINK(intType));

        BuildCtx comparectx(classctx);
        comparectx.addQuotedCompound("class Compare : public IRangeCompare", s2.clear().append(" ").append(compareName).append(";"));
        translator.doBuildUnsignedFunction(comparectx, "maxFields", lenOffsets/2);

        comparectx.addQuotedCompound("virtual int docompare(const void * _left,const void * _right, unsigned numFields) const");
        comparectx.addQuoted("const byte * left = (const byte *)_left;");
        comparectx.addQuoted("const byte * right = (const byte *)_right;");
        comparectx.addQuoted("int ret;");

        comparectx.addQuoted(s.clear().append("if (numFields < 1) return 0;"));
        OwnedHqlExpr selSeq = createDummySelectorSequence();
        BoundRow * left = translator.bindTableCursor(comparectx, ds, "left", no_left, selSeq);
        BoundRow * right = translator.bindTableCursor(comparectx, ds, "right", no_right, selSeq);
        ForEachChild(i, fields)
        {
            IHqlExpression * cur = fields->queryChild(i);
            if (i)
                comparectx.addQuoted(s.clear().append("if (ret || (numFields < ").append(i+1).append(")) return ret;"));
            OwnedHqlExpr lhs = replaceSelector(cur, ds, left->querySelector());
            OwnedHqlExpr rhs = replaceSelector(cur, ds, right->querySelector());
            OwnedHqlExpr order = createValue(no_order, makeIntType(4, true), LINK(lhs), LINK(rhs));
            translator.buildAssignToTemp(comparectx, result, order);
        }
        comparectx.addReturn(result);
    
        classctx.addQuoted(s.clear().append("virtual IRangeCompare * queryCompare() { return &").append(compareName).append("; }"));
    }

    //distance function - very similar to compare
    {
        StringBuffer distanceName;
        translator.getUniqueId(distanceName.append("c"));
        OwnedITypeInfo intType = makeIntType(4, true);
        OwnedHqlExpr result = createVariable("ret", LINK(intType));

        BuildCtx distancectx(classctx);
        distancectx.addQuotedCompound("class Distance : public IDistanceCalculator", s2.clear().append(" ").append(distanceName).append(";"));
        distancectx.addQuotedCompound("virtual unsigned getDistance(unsigned __int64 & distance, const void * _before, const void * _after, unsigned numFields) const");
        distancectx.addQuoted("const byte * before = (const byte *)_before;");
        distancectx.addQuoted("const byte * after = (const byte *)_after;");

        OwnedHqlExpr selSeq = createDummySelectorSequence();
        OwnedITypeInfo distanceType = makeIntType(8, false);
        OwnedHqlExpr distanceExpr = createVariable("distance", LINK(distanceType));
        BoundRow * left = translator.bindTableCursor(distancectx, ds, "before", no_left, selSeq);
        BoundRow * right = translator.bindTableCursor(distancectx, ds, "after", no_right, selSeq);
        ForEachChild(i, fields)
        {
            IHqlExpression * cur = fields->queryChild(i);
            distancectx.addQuoted(s.clear().append("if (numFields < ").append(i+1).append(") return DISTANCE_EXACT_MATCH;"));
            OwnedHqlExpr lhs = replaceSelector(cur, ds, left->querySelector());
            OwnedHqlExpr rhs = replaceSelector(cur, ds, right->querySelector());
            OwnedHqlExpr compare = createBoolExpr(no_ne, LINK(lhs), LINK(rhs));
            BuildCtx subctx(distancectx);
            translator.buildFilter(subctx, compare);

            OwnedHqlExpr value;
            if (lhs->queryType()->isInteger())
                value.setown(createValue(no_sub, LINK(distanceType), ensureExprType(rhs, distanceType), ensureExprType(lhs, distanceType)));
            else
                value.setown(getSizetConstant(1));
            translator.buildAssignToTemp(subctx, distanceExpr, value);
            subctx.addQuotedF("return %u;", i+1);
        }
            
        distancectx.addQuoted("return DISTANCE_EXACT_MATCH;");
    
        classctx.addQuoted(s.clear().append("virtual IDistanceCalculator * queryDistance() { return &").append(distanceName).append("; }"));
    }

    StringBuffer resultText;
    if (translator.queryOptions().spanMultipleCpp)
    {
        translator.createAccessFunctions(resultText, declarectx, SteppedPrio, "ISteppingMeta", memberName);
        resultText.append("()");
    }
    else
        resultText.append(memberName);

    OwnedHqlExpr func = createVariable(resultText.str(), makeVoidType());
    declarectx.associateExpr(key, func);
    return func;
}

void SteppingFieldSelection::generateSteppingMetaMember(HqlCppTranslator & translator, BuildCtx & ctx, const char * name)
{
    IHqlExpression * func = generateSteppingMeta(translator);

    StringBuffer s;
    s.clear().append("virtual ISteppingMeta * query").append(name).append("() { return &");
    translator.generateExprCpp(s, func);
    s.append(";}");
    ctx.addQuoted(s);
}

IHqlExpression * SteppingFieldSelection::invertTransform(IHqlExpression * expr, IHqlExpression * select)
{
    LinkedHqlExpr result = select;
    loop
    {
        node_operator op = expr->getOperator();
        switch (op)
        {
        case no_cast:
        case no_implicitcast:
            {
                IHqlExpression * uncast = expr->queryChild(0);
                result.setown(ensureExprType(result, uncast->queryType()));
                expr = uncast;
                break;
            }
        case no_add:
        case no_sub:
            {
                node_operator newOp = (op == no_add) ? no_sub : no_add;
                IHqlExpression * rhs = expr->queryChild(1);
                result.setown(createValue(newOp, expr->getType(), LINK(result), LINK(rhs)));
                expr = expr->queryChild(0);
                break;
            }
        case no_select:
            return result.getLink();
        default:
            throwUnexpectedOp(op);
        }
    }
}


void SteppingFieldSelection::set(IHqlExpression * _ds, IHqlExpression * _fields)
{
    ds.set(_ds);
    fields.set(_fields);
}

void SteppingFieldSelection::setStepping(IHqlExpression * expr)
{
    ds.set(expr->queryNormalizedSelector());
    fields.set(expr->queryChild(1));
}


//---------------------------------------------------------------------------

bool HqlCppTranslator::buildNWayInputs(CIArrayOf<ABoundActivity> & inputs, BuildCtx & ctx, IHqlExpression * input)
{
    if (input->getOperator() == no_datasetlist)
    {
        IHqlExpression * record = input->queryChild(0);
        ForEachChild(i, input)
        {
            IHqlExpression * cur = input->queryChild(i);
            if (!recordTypesMatch(cur->queryRecord(), record))
                throwError(HQLERR_InconsistentNaryInput);
            inputs.append(*buildCachedActivity(ctx, cur));
        }
        return false;
    }

    inputs.append(*buildCachedActivity(ctx, input));
    return true;
}


ABoundActivity * HqlCppTranslator::doBuildActivityRowsetRange(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * rowset, IHqlExpression * inputSelection)
{
    bool isNWayInput;
    ThorActivityKind kind;
    const char * argName;
    CIArrayOf<ABoundActivity> inputs;
    IHqlExpression * graphId = NULL;

    switch (rowset->getOperator())
    {
    case no_getgraphloopresultset:
        {
            kind = TAKnwaygraphloopresultread;
            argName = "NWayGraphLoopResultRead";
            isNWayInput = true;
            graphId = rowset->queryChild(1);
            break;
        }
    case no_datasetlist:
        {
            kind = TAKnwayinput;
            argName = "NWayInput";
            isNWayInput = false;
            ForEachChild(i, rowset)
                inputs.append(*buildCachedActivity(ctx, rowset->queryChild(i)));
            break;
        }
    default:
        throwError(HQLERR_UnsupportedRowsetRangeParam);
    }

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, argName);
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    OwnedITypeInfo castType = makeSetType(LINK(unsignedType));
    OwnedHqlExpr castList = ensureExprType(inputSelection, castType);
    OwnedHqlExpr normalized = normalizeListCasts(castList);

    BuildCtx funcctx(instance->startctx);
    funcctx.addQuotedCompound("virtual void getInputSelection(bool & __isAllResult, size32_t & __lenResult, void * & __result)");
    doBuildFunctionReturn(funcctx, castType, normalized);

    if ((kind == TAKnwaygraphloopresultread) && isGrouped(rowset))
        doBuildBoolFunction(instance->classctx, "grouped", true);
    if (graphId && targetRoxie())
        instance->addAttributeInt("_graphId", getIntValue(graphId->queryChild(0)));

    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, inputs)
        buildConnectInputOutput(ctx, instance, &inputs.item(idx2), 0, idx2, NULL, isNWayInput);

    return instance->getBoundActivity();
}

ABoundActivity * HqlCppTranslator::doBuildActivityRowsetRange(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    IHqlExpression * inputSelection = expr->queryChild(1);
    return doBuildActivityRowsetRange(ctx, expr, ds, inputSelection);
}


ABoundActivity * HqlCppTranslator::doBuildActivityRowsetIndex(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    if (dataset->getOperator() == no_getgraphloopresultset)
    {
        throwUnexpected();      // this should have been translated elsewhere...
        OwnedHqlExpr newExpr = createDataset(no_getgraphloopresult, LINK(dataset->queryRecord()), createComma(LINK(dataset->queryChild(1)), LINK(expr->queryChild(1))));
        return buildActivity(ctx, newExpr, false);
    }

    CIArrayOf<ABoundActivity> inputs;
    bool isNWayInput = buildNWayInputs(inputs, ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnwayselect, expr, "NWaySelect");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    doBuildUnsignedFunction(instance->startctx, "getInputIndex", expr->queryChild(1));

    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, inputs)
        buildConnectInputOutput(ctx, instance, &inputs.item(idx2), 0, idx2, NULL, isNWayInput);

    return instance->getBoundActivity();
}



ABoundActivity * HqlCppTranslator::doBuildActivityNWayMerge(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    CIArrayOf<ABoundActivity> inputs;
    bool isNWayInput = buildNWayInputs(inputs, ctx, dataset);

    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, TAKnwaymerge, expr, "NWayMerge");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    IHqlExpression * sortOrder = expr->queryChild(1);
    instance->startctx.addQuoted("virtual ICompare * queryCompare() { return &compare; }");

    //NOTE: left is used instead of dataset in sort list
    DatasetReference dsRef(dataset, no_left, querySelSeq(expr));        
    buildCompareClass(instance->nestedctx, "compare", sortOrder, dsRef);

    if (expr->hasProperty(dedupAtom))
        doBuildBoolFunction(instance->classctx, "dedup", true);

    SteppingFieldSelection stepping;
    IHqlExpression * left = dsRef.querySelector();
    stepping.set(left, sortOrder);
    stepping.generateSteppingMetaMember(*this, instance->classctx, "SteppingMeta");

    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, inputs)
        buildConnectInputOutput(ctx, instance, &inputs.item(idx2), 0, idx2, NULL, isNWayInput);

    return instance->getBoundActivity();
}



ABoundActivity * HqlCppTranslator::doBuildActivityNWayMergeJoin(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * dataset = expr->queryChild(0);
    CIArrayOf<ABoundActivity> inputs;
    bool isNWayInput = buildNWayInputs(inputs, ctx, dataset);
    node_operator op = expr->getOperator();

    ThorActivityKind kind = (op == no_mergejoin) ? TAKnwaymergejoin : TAKnwayjoin;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, "NWayMergeJoin");
    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    IHqlExpression * mofn = expr->queryProperty(mofnAtom);
    bool leftonly = expr->hasProperty(leftonlyAtom);
    bool leftouter = expr->hasProperty(leftouterAtom);
    IHqlExpression * selSeq = querySelSeq(expr);
    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
    IHqlExpression * transform = (op == no_nwayjoin) ? expr->queryChild(2) : NULL;
    IHqlExpression * sortOrder = (op == no_nwayjoin) ? expr->queryChild(3) : expr->queryChild(2);

    OwnedHqlExpr left = createSelector(no_left, dataset, selSeq);
    OwnedHqlExpr right = createSelector(no_right, dataset, selSeq);

    SteppingCondition stepCondition(false, left, right, rowsid);
    stepCondition.extractSteppingCondition(expr->queryChild(1), sortOrder);

    if (!stepCondition.matchedAny())
        throwError(HQLERR_JoinNotMatchSortCondition);

    OwnedHqlExpr equalityList = stepCondition.createEqualitySortList();
    IHqlExpression * rangeSelect = stepCondition.queryRangeLeftSelector();
    IHqlExpression * internalFlags = queryPropertyChild(expr, internalFlagsAtom, 0);
    IHqlExpression * skew = expr->queryProperty(skewAtom);

    //Now generate all the helper functions....
    bool createClearRow = true;//(!leftouter && !leftonly);
    StringBuffer flags;
    flags.append("|MJFhasdistance");
    if (leftouter)
        flags.append("|MJFleftouter");
    else if (leftonly)
        flags.append("|MJFleftonly");
    else if (mofn)
        flags.append("|MJFmofn");
    else
        flags.append("|MJFinner");

    if (expr->hasProperty(dedupAtom)) flags.append("|MJFdedup");
    if (expr->hasProperty(steppedAtom)) flags.append("|MJFstepped");
    if (transform) flags.append("|MJFtransform");
    if (rangeSelect) flags.append("|MJFhasrange");
    if (expr->hasProperty(assertAtom) && generateAsserts()) flags.append("|MJFassertsorted");
    if (stepCondition.queryGlobalCompare()) flags.append("|MJFglobalcompare");
    if (createClearRow) flags.append("|MJFhasclearlow");
    if (skew) flags.append("|MJFhaspartition");
    if (internalFlags) flags.append("|").append(getIntValue(internalFlags, 0));

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getJoinFlags", flags.str()+1);


    //NOTE: left is used instead of dataset in sort list
    DatasetReference leftRef(dataset, no_left, querySelSeq(expr));

    unsigned numEqualFields = equalityList->numChildren();
    doBuildUnsignedFunction(instance->classctx, "numEqualFields", numEqualFields);
    doBuildUnsignedFunction(instance->classctx, "numOrderFields", sortOrder->numChildren());

    //virtual ICompare * queryEqualCompare()
    {
        buildCompareClass(instance->nestedctx, "equalCompare", equalityList, leftRef);
        instance->classctx.addQuoted("virtual ICompare * queryEqualCompare() { return &equalCompare; }");
    }

    //virtual ICompareEq * queryExactCompare()
    {
        buildCompareEqMember(instance->classctx, "EqualCompareEq", equalityList, leftRef);
    }

    //virtual ICompareEq * queryPartitionCompareEq()
    if (skew)
    {
        HqlExprArray skewArgs;
        unwindChildren(skewArgs, skew);
        OwnedHqlExpr skewOrder = createValue(no_sortlist, makeSortListType(NULL), skewArgs);
        DatasetReference datasetRef(dataset);
        buildCompareEqMember(instance->classctx, "PartitionCompareEq", skewOrder, leftRef);
    }

    //virtual ISteppingMeta * querySteppingMeta()
    {
        SteppingFieldSelection stepping;
        stepping.set(left, sortOrder);
        stepping.generateSteppingMetaMember(*this, instance->classctx, "SteppingMeta");
    }

    //virtual IOutputMetaData * queryInputMeta()
    {
        MetaInstance inputmeta(*this, dataset);
        buildMetaInfo(inputmeta);

        StringBuffer s;
        s.append("virtual IOutputMetaData * queryInputMeta() { return &").append(inputmeta.queryInstanceObject()).append("; }");
        instance->classctx.addQuoted(s);
    }

    //NOTE: left is used instead of dataset in sort list
    //virtual ICompare * queryMergeCompare()
    {
        buildCompareClass(instance->nestedctx, "mergeCompare", sortOrder, leftRef);
        instance->classctx.addQuoted("virtual ICompare * queryMergeCompare() { return &mergeCompare; }");
    }

    if (createClearRow)
    {
        BuildCtx funcctx(instance->startctx);
        OwnedHqlExpr func = getClearRecordFunction(dataset->queryRecord(), -1);
        StringBuffer s;
        generateExprCpp(s.append("virtual size32_t createLowInputRow(ARowBuilder & crSelf) { return "), func).append("(crSelf, ctx); }");
        funcctx.addQuoted(s);
    }

    if (rangeSelect)
    {
        OwnedITypeInfo rangeType = makeIntType(8, false);
        OwnedITypeInfo distanceType = makeIntType(8, true);
        OwnedHqlExpr rangeValue = ensureExprType(rangeSelect, rangeType);
        OwnedHqlExpr bias;
        if (rangeSelect->queryType()->isSigned())
        {
            bias.setown(getHozedBias(rangeSelect->queryType()));
            rangeValue.setown(createValue(no_add, rangeValue->getType(), LINK(rangeValue), ensureExprType(bias, rangeType)));
        }

        if (sortOrder->numChildren() != numEqualFields + 1)
            throwError(HQLERR_SortOrderMustMatchJoinFields);

        //virtual unsigned __int64 extractRangeValue(const void * input);               // distance is assumed to be unsigned, code generator must bias if not true.
        {
            BuildCtx extractCtx(instance->startctx);
            extractCtx.addQuotedCompound("unsigned __int64 extractRangeValue(const void * _left)");
            extractCtx.addQuoted("const byte * left = (const byte *)_left;");
            bindTableCursor(extractCtx, dataset, "left", no_left, selSeq);
            buildReturn(extractCtx, rangeValue);
        }

        //virtual void adjustRangeValue(void * self, const void * input, __int64 delta);        // implementation must ensure field doesn't go -ve.
        {
            BuildCtx adjustCtx(instance->startctx);
            adjustCtx.addQuotedCompound("void adjustRangeValue(ARowBuilder & crSelf, const void * _left, __int64 delta)");
            ensureRowAllocated(adjustCtx, "crSelf");
            adjustCtx.addQuoted("const byte * left = (const byte *)_left;");

            BoundRow * self = bindSelf(adjustCtx, dataset, "crSelf");
            bindTableCursor(adjustCtx, dataset, "left", no_left, selSeq);
            ForEachChild(i, equalityList)
            {
                IHqlExpression * cur = equalityList->queryChild(i);
                OwnedHqlExpr target = replaceSelector(cur, left, self->querySelector());
                buildAssign(adjustCtx, target, cur);
            }
            OwnedHqlExpr target = replaceSelector(rangeSelect, left, self->querySelector());
            OwnedHqlExpr delta = createVariable("delta", LINK(distanceType));
            OwnedHqlExpr castDelta = ensureExprType(delta, rangeType);
            OwnedHqlExpr minusDelta = getNegative(delta);
            OwnedHqlExpr cond = createBoolExpr(no_or, 
                                            createBoolExpr(no_ge, LINK(delta), ensureExprType(queryZero(), distanceType)),
                                            createBoolExpr(no_ge, LINK(rangeValue), ensureExprType(minusDelta, rangeType)));
            OwnedHqlExpr firstValue = bias ? getNegative(bias) : getZero();
            OwnedHqlExpr assignValue = createValue(no_if, rangeSelect->getType(), 
                                                          LINK(cond), 
                                                          createValue(no_add, rangeSelect->getType(), LINK(rangeSelect), ensureExprType(delta, rangeSelect->queryType())),
                                                          ensureExprType(firstValue, rangeSelect->queryType()));
            buildAssign(adjustCtx, target, assignValue);
        }

        //virtual __int64 maxRightBeforeLeft()
        {
            BuildCtx rBeforeLctx(instance->startctx);
            rBeforeLctx.addQuotedCompound("virtual __int64 maxRightBeforeLeft()");
            OwnedHqlExpr mrbl = stepCondition.getMaxRightBeforeLeft();
            buildReturn(rBeforeLctx, mrbl);
        }

        //virtual __int64 maxLeftBeforeRight()
        {
            BuildCtx lBeforeRctx(instance->startctx);
            lBeforeRctx.addQuotedCompound("virtual __int64 maxLeftBeforeRight()");
            OwnedHqlExpr mlbr = stepCondition.getMaxLeftBeforeRight();
            buildReturn(lBeforeRctx, mlbr);
        }
    }

    //virtual ICompareEq * queryNonSteppedCompare()
    IHqlExpression * compare = stepCondition.queryExtraFilter();
    if (compare)
        buildCompareEqMemberLR(instance->nestedctx, "NonSteppedCompare", compare, dataset, dataset, selSeq);

    //virtual INaryCompareEq * queryGlobalCompare() = 0;
    IHqlExpression * globalCompare = stepCondition.queryGlobalCompare();
    if (globalCompare)
        buildNaryCompareMember(instance->startctx, "GlobalCompare", globalCompare, dataset, selSeq, rowsid);

    //virtual size32_t transform(ARowBuilder & crSelf, unsigned _num, const void * * _rows)
    if (transform)
    {
        BuildCtx transformctx(instance->startctx);
        transformctx.addQuotedCompound("virtual size32_t transform(ARowBuilder & crSelf, unsigned numRows, const void * * _rows)");
        ensureRowAllocated(transformctx, "crSelf");
        transformctx.addQuoted("const unsigned char * left = (const unsigned char *) _rows[0];");
        transformctx.addQuoted("const unsigned char * right = (const unsigned char *) _rows[1];");
        transformctx.addQuoted("unsigned char * * rows = (unsigned char * *) _rows;");

        bindTableCursor(transformctx, dataset, "left", no_left, selSeq);
        bindTableCursor(transformctx, dataset, "right", no_right, selSeq);
        bindRows(transformctx, no_left, selSeq, rowsid, dataset, "numRows", "rows", options.mainRowsAreLinkCounted);
        BoundRow * selfCursor = bindSelf(transformctx, expr, "crSelf");

        associateSkipReturnMarker(transformctx, queryZero(), selfCursor);
        doTransform(transformctx, transform, selfCursor);
        buildReturnRecordSize(transformctx, selfCursor);
    }

    if (mofn)
    {
        doBuildUnsignedFunction(instance->startctx, "getMinMatches", mofn->queryChild(0));
        if (queryRealChild(mofn, 1))
            doBuildUnsignedFunction(instance->startctx, "getMaxMatches", mofn->queryChild(1));
    }

    if (leftonly)
    {
        //Create a function to apply a delta to the last field, it assumes that overflow isn't going to be a problem.
        IHqlExpression * lastJoinField = equalityList->queryChild(numEqualFields-1);
        if (lastJoinField->queryType()->isInteger())
        {
            BuildCtx transformctx(instance->startctx);
            transformctx.addQuotedCompound("virtual bool createNextJoinValue(ARowBuilder & crSelf, const void * _value)");
            ensureRowAllocated(transformctx, "crSelf");
            transformctx.addQuoted("const byte * value = (const byte *)_value;");

            BoundRow * self = bindSelf(transformctx, dataset, "crSelf");
            bindTableCursor(transformctx, dataset, "value", no_left, selSeq);
            ForEachChild(i, equalityList)
            {
                IHqlExpression * cur = equalityList->queryChild(i);
                OwnedHqlExpr target = replaceSelector(cur, left, self->querySelector());
                LinkedHqlExpr source = cur;
                if (i == numEqualFields-1)
                    source.setown(adjustValue(cur, 1));
                buildAssign(transformctx, target, source);
            }
            buildReturn(transformctx, queryBoolExpr(true));
        }
    }


    buildInstanceSuffix(instance);

    ForEachItemIn(idx2, inputs)
        buildConnectInputOutput(ctx, instance, &inputs.item(idx2), 0, idx2, NULL, isNWayInput);

    return instance->getBoundActivity();
}



//---------------------------------------------------------------------------


/*

  Stepping info.
  Assume we have 
  a) an index read, stepped on [doc, wpos, wip]
  b) an index read, stepped on [doc, wpos]
  c) an index read, stepped on [doc, wpos, wip]
  d) mergejoin(a,b, merge[doc, wpos, wip], left.doc = right.doc));
  e) join(d, c, stepped(left.doc = right.doc, right.wpos in range left.wpos - 5, left.wpos + 10), sorted([doc, wpos, wip]);
  f) SORT(e, [doc, wpos, wip], RANGE(left.wpos - right.wpos between [-5, 5]))       
     // could push top and right scope for range, but not very nice..., introduce a new no_sort keyword regardless of syntax.


We have
    a) static stepping = [doc,wpos,wip], dynamic matchee
    b) static stepping = [doc, wpos], dyamic matches
    c) same as a
    d) static stepping = [doc, wpos, wip]
       dynamic = dynamic for input#0 intersected with own static stepping.
       because a merge, all fields used in the merge can be stepped.
    e) static = [doc, wpos], because those are the conditions used in the join condition, and each of those values is either assigned left.x or right.x inside the transform
       sorting = [doc], or possibly [doc, wpos] if assignment self.wpos = left.wpos in transform
       stepping on [doc, wpos] is handled by adjusting the requested value by the maximum (delta1, delta2), since it is either assigned left/right.  This should be a separate constant
       so the self.x := left.x can be optimized to delta1, but fairly insignificant.
    f) static = [doc, wpos] - from sort criteria, and field referenced in the proximity condition
       dynamic = [doc, wpos] after intersection with output from e.
       sorted by [doc, wpos, wip] again.

More on JOIN:
  * Write code to allow nesting ((a JOIN b) JOIN c) with different deltas for each level.
  * Do all the seeks before creating any of the records.  Probably need to find the first candidate in parallel, and then recursively create the transforms.

    seek(n) = seek(applyDelta(min(values[1..n-1], minRightBeforeLeft);
    if fail, adjust match, by minRightBeforeLeft, and start seeking on 1 again.
    once you've got a match, go off and create the instances.

  For arbitrary nesting
    (a w/x b) w/y (c w/z d)
    Seek(a)
    seek(b, matcha-x);
    seek(c, min(a,b)-(y+z));
    seek(d, c, z);
    could optionally check that (a, b) w/y (c, d), but probably better to just handle via the post filter.

Indexes and shuffle information:

  i := rawindex
  p := project(i, logicalindex);
  st := stepped(p, [a,b,c,d,e]);
  e := project(st, p2());
  f := compoundindexread;

Need to locate stepped
i) walk up to work out what is projected, and down.  Probably simplest done using a recursive function - should be relatively simple.  Don't merge with the index definition any more.
ii) Implement should be ok.  Have a flag to indicate if we spotted a STEPPED() identifier.  Complain if not a read.

Note:
  for search "a and b and date > x" it is much better to step (a,b) first before date because of condition complexity

  */
