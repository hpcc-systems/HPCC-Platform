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
#include "hqlexpr.hpp"
#include "hqlattr.hpp"
#include "hqlfunc.hpp"
#include "hqlcpputil.hpp"
#include "hqlfold.hpp"
#include "hqlthql.hpp"

#include "hqlstmt.hpp"
#include "hqlwcpp.hpp"
#include "hqlcpp.ipp"
#include "hqltcppc.ipp"
#include "hqlhtcpp.ipp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlccommon.hpp"
#include "hqlpmap.hpp"
#include "hqlutil.hpp"
#include "hqlinline.hpp"
#include "hqlusage.hpp"

#define LIMIT_FOR_GET       (NULL)

//#define TraceExprPrintLog(x, expr)                PrintLog(x ": %s", expr->toString(StringBuffer()).str());

static void normalizeAdditions(IHqlExpression * expr, HqlExprAttr & var, HqlExprAttr & fixed)
{
    switch (expr->getOperator())
    {
    case no_constant:
        extendAdd(fixed, expr);
        break;
    case no_add:
        normalizeAdditions(expr->queryChild(0), var, fixed);
        normalizeAdditions(expr->queryChild(1), var, fixed);
        break;
    default:
        extendAdd(var, expr);
        break;
    }
}

static bool needToNormalize(IHqlExpression * expr, bool insideAdd)
{
    switch (expr->getOperator())
    {
    case no_constant:
        return insideAdd;
    case no_add:
        if (needToNormalize(expr->queryChild(0), true))
            return true;
        if (!insideAdd && expr->queryChild(1)->getOperator() == no_constant)
            return false;
        return needToNormalize(expr->queryChild(1), true);
    default:
        return false;
    }
}


IHqlExpression * normalizeAdditions(IHqlExpression * expr)
{
    if (!needToNormalize(expr, false))
        return LINK(expr);
    HqlExprAttr var;
    HqlExprAttr fixed;
    normalizeAdditions(expr, var, fixed);
    if (fixed)
        extendAdd(var, fixed);
    return var.getClear();
}



IHqlExpression * ensureType(IHqlExpression * expr, ITypeInfo * type)
{
    if (expr->queryType() != type)
        return createValue(no_implicitcast, LINK(type), expr);
    return expr;
}

static bool isVerySimpleLength(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_variable:
    case no_constant:
    case no_select:
        return true;
    }
    return false;
}
bool isSimpleLength(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_variable:
    case no_constant:
    case no_select:
        return true;
    case no_add:
        if (isVerySimpleLength(expr->queryChild(0)) &&
            (expr->queryChild(1)->getOperator() == no_constant))
            return true;
        break;
    case no_sub:
        if (isVerySimpleLength(expr->queryChild(0)))
            if (isSimpleLength(expr->queryChild(1)))
                    return true;
        break;
    }
    return false;
}

void ensureSimpleLength(HqlCppTranslator & translator, BuildCtx & ctx, CHqlBoundExpr & bound)
{
    OwnedHqlExpr length = translator.getBoundLength(bound);
    if (isSimpleLength(length))
        return;

    OwnedHqlExpr tempLen = createTranslatedOwned(translator.getBoundLength(bound));
    CHqlBoundExpr boundLength;
    translator.buildSimpleExpr(ctx, tempLen, boundLength);
    bound.length.set(boundLength.expr);
}

//---------------------------------------------------------------------------

static IHqlExpression * createSizeExpression(IHqlExpression * varSize, unsigned fixedSize)
{
    if (!varSize)
        return getSizetConstant(fixedSize);

    OwnedHqlExpr total = ensureType(LINK(varSize), sizetType);
    if (fixedSize)
        return adjustValue(total, (int)fixedSize);
    return total.getClear();
}

void SizeStruct::add(const SizeStruct & other)
{
    assertex(self == other.self);
    addFixed(other.fixedSize);
    if (other.varSize)
        addVariableExpr(other.varMinSize, other.varSize);
}

void SizeStruct::addVariableExpr(unsigned _varMinSize, IHqlExpression * expr)
{
    varMinSize += _varMinSize;
    if (varSize)
        varSize.setown(createValue(no_add, varSize.getClear(), LINK(expr)));
    else
        varSize.set(expr);
}

void SizeStruct::addVariable(unsigned _varMinSize, IHqlExpression * column)
{
    OwnedHqlExpr expr = createValue(no_sizeof, makeIntType(4,false), LINK(column));
    addVariableExpr(_varMinSize, expr);
}


void SizeStruct::buildSizeExpr(HqlCppTranslator & translator, BuildCtx & ctx, BoundRow * row, CHqlBoundExpr & bound)
{
    if (varSize)
    {
        OwnedHqlExpr temp = getSizeExpr(row);
        translator.buildCachedExpr(ctx, temp, bound);
    }
    else
        bound.expr.setown(getSizetConstant(fixedSize));
}


void SizeStruct::forceToTemp(node_operator op, IHqlExpression * selector)
{
    varSize.setown(createValue(op, LINK(sizetType), LINK(selector)));
    fixedSize = 0;
}


IHqlExpression * SizeStruct::getSizeExpr(BoundRow * row) const
{
#if 0
    IHqlExpression * bound = row->queryDataset();
    if ((bound->getOperator() == no_translated) && queryRealChild(bound, 1))
        return createTranslated(bound->queryChild(1));
#endif

    OwnedHqlExpr total;
    if (row)
    {
        assertex(self != NULL);
        OwnedHqlExpr mapped = normalizeAdditions(varSize);
        total.setown(row->bindToRow(mapped, self));
    }
    else
        total.set(varSize);
    return createSizeExpression(total, fixedSize);
}


bool SizeStruct::isWorthCommoning() const
{
    if (varSize && varSize->getOperator() == no_add)
    {
        if (varSize->queryChild(0)->getOperator() == no_add)
            return true;
    }
    return false;
}

//---------------------------------------------------------------------------

/* In param: _column is not linked */
CMemberInfo::CMemberInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column)
{ 
    container = _container;
    prior = _prior;
    column.set(_column);
    if (column->isRecord())
        column.setown(createRow(no_null, LINK(column)));
    hasVarOffset = false;
    isOffsetCached = false;
    seq = 0;
}

void CMemberInfo::addVariableSize(size32_t varMinSize, SizeStruct & size)
{
    Owned<IHqlExpression> self = createSelectorExpr();
    size.addVariable(varMinSize, self);
}


void CMemberInfo::buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    bound.expr.setown(getColumnAddress(translator, ctx, selector, queryPhysicalType()));
}

void CMemberInfo::calcAllCachedOffsets()
{
    assertex(container);
    container->calcAllCachedOffsets();
}

void CMemberInfo::calcCachedOffsets(const SizeStruct & offset, SizeStruct & sizeSelf)
{
    if (!isOffsetCached)
    {
        isOffsetCached = true;
        cachedOffset.set(offset);
        cachedSize.clear(offset.querySelf());
        calcCachedSize(offset, sizeSelf);
    }
    else
    {
        sizeSelf.set(cachedSize);
    }
}


void CMemberInfo::calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf)
{
    gatherSize(cachedSize);
    sizeSelf.set(cachedSize);
}


void CMemberInfo::getOffsets(SizeStruct & offset, SizeStruct & accessorOffset) const
{
    offset.set(cachedOffset);
    accessorOffset.set(cachedAccessorOffset);
}


bool CMemberInfo::checkCompatibleIfBlock(HqlExprCopyArray & conditions)
{
    return false;
}

StringBuffer & CMemberInfo::expandSelectPathText(StringBuffer & out, bool isLast) const
{
    bool isField = (column->getOperator() == no_field);
    if (container)
        container->expandSelectPathText(out, isLast && !isField);
    if (isField)
    {
        out.append(column->queryName());
        if (!isLast)
            out.append('.');
    }
    return out;
}


unsigned CMemberInfo::getTotalFixedSize()
{
    return cachedSize.getFixedSize();
}

unsigned CMemberInfo::getTotalMinimumSize()
{
    return cachedSize.getMinimumSize();
}

void CContainerInfo::calcAllCachedOffsets()
{
    if (container)
        CMemberInfo::calcAllCachedOffsets();
    else
    {
        OwnedHqlExpr self = getRelativeSelf();
        SizeStruct offset(self);
        SizeStruct size(self);
        calcCachedOffsets(offset, size);

        if (usesAccessClass())
        {
            SizeStruct finalAccessorOffset(self);
            OwnedHqlExpr sizeSelf = createValue(no_sizeof, LINK(sizetType), getRelativeSelf());
            if (bindOffsetsFromClass(finalAccessorOffset, false))
            {
                StringBuffer name;
                name.append("off[").append(nextSeq()).append("]");
                OwnedHqlExpr newOffset = createVariable(name, LINK(sizetType));
                finalAccessorOffset.set(0, newOffset);
            }
            accessorSize.set(finalAccessorOffset);
            cachedSize.set(0, sizeSelf);

            SizeStruct tempOffset;
            bindSizesFromOffsets(tempOffset, cachedSize);
        }
    }
}

void CContainerInfo::calcCachedChildrenOffsets(const SizeStruct & startOffset, SizeStruct & sizeSelf)
{
    //Optimize one special case of ifblocks.
    //Sometimes you have a header with some fields, followed by a set of mutually exclusive ifblocks.
    //Spot any trailing mutually exclusive ifblocks and don't update
    HqlExprCopyArray conditions;
    unsigned maxOffsetUpdate = children.ordinality();
    while (maxOffsetUpdate > 0)
    {
        CMemberInfo & cur = children.item(maxOffsetUpdate-1);
        if (!cur.checkCompatibleIfBlock(conditions))
            break;
        maxOffsetUpdate--;
    }

    SizeStruct offset(startOffset);
    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);

        SizeStruct sizeChild(offset.querySelf());

        cur.calcCachedOffsets(offset, sizeChild);
        if (offset.isWorthCommoning())
        {
            Owned<IHqlExpression> child = cur.createSelectorExpr();
            offset.forceToTemp(no_offsetof, child);
        }
        sizeSelf.add(sizeChild);
        if (idx < maxOffsetUpdate)
            offset.add(sizeChild);
    }
}

void CContainerInfo::calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf)
{
    SizeStruct childOffset(offset);
    if (childOffset.isWorthCommoning())
    {
        Owned<IHqlExpression> self = createSelectorExpr();
        childOffset.forceToTemp(no_offsetof, self);
    }
    calcCachedChildrenOffsets(childOffset, cachedSize);

    //Ensure that a record with no fields has a meta size > 0 (can be created by implicit project code)
    if (cachedSize.isEmpty())
    {
        IHqlExpression * record = column->queryRecord();
        if (record->hasAttribute(_nonEmpty_Atom))
            cachedSize.addFixed(1);
    }

    if (cachedSize.isFixedSize())
        sizeSelf.set(cachedSize);
    else
        addVariableSize(cachedSize.getMinimumSize(), sizeSelf);
}

bool CMemberInfo::bindOffsetsFromClass(SizeStruct & accessorOffset, bool prevVariableSize)
{
    if (prevVariableSize)
    {
        seq = container->nextSeq();
        StringBuffer name;
        name.append("off[").append(seq).append("]");
        OwnedHqlExpr newOffset = createVariable(name, LINK(sizetType));
        cachedAccessorOffset.set(0, newOffset);
    }
    else
        cachedAccessorOffset.set(accessorOffset);

    if (!cachedAccessorOffset.isFixedSize())
    {
        Owned<IHqlExpression> child = createSelectorExpr();
        cachedOffset.forceToTemp(no_offsetof, child);
    }
    else
        assertex(!cachedOffset.queryVarSize() || cachedOffset.queryVarSize()->getOperator() != no_add);

    accessorOffset.set(cachedAccessorOffset);
    accessorOffset.addFixed(cachedSize.getFixedSize());
    return !cachedSize.isFixedSize();
}

bool CContainerInfo::bindOffsetsFromClass(SizeStruct & accessorOffset, bool prevVariableSize)
{
    //MORE: ifblocks need further work if the offsets are not recalculated for trailing ifblocks
    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        bool thisVariableSize = cur.bindOffsetsFromClass(accessorOffset, prevVariableSize);
        if (idx == 0)
            cur.getOffsets(cachedOffset, cachedAccessorOffset);

        prevVariableSize = thisVariableSize;
    }

    return prevVariableSize;
}

void CMemberInfo::bindSizesFromOffsets(SizeStruct & thisOffset, const SizeStruct & nextOffset)
{
    if (!cachedSize.isFixedSize())
    {
        assertex(nextOffset.queryVarSize()->getOperator() != no_add);
        OwnedHqlExpr sub = createValue(no_sub, LINK(sizetType), nextOffset.getSizeExpr(NULL), cachedOffset.getSizeExpr(NULL));
        cachedSize.set(0, sub);
    }
    thisOffset.set(cachedOffset);
}

void CContainerInfo::bindSizesFromOffsets(SizeStruct & thisOffset, const SizeStruct & nextOffset)
{
    SizeStruct curOffset(nextOffset);
    ForEachItemInRev(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        cur.bindSizesFromOffsets(curOffset, curOffset);
    }
    CMemberInfo::bindSizesFromOffsets(thisOffset, nextOffset);
}

unsigned CContainerInfo::getTotalFixedSize()
{
    if (!isOffsetCached)
        calcAllCachedOffsets();
    if (cachedSize.isFixedSize())
        return cachedSize.getFixedSize();
    unsigned size = 0;
    ForEachItemIn(idx, children)
        size += children.item(idx).getTotalFixedSize();
    return size;
}

unsigned CContainerInfo::getTotalMinimumSize()
{
    if (!isOffsetCached)
        calcAllCachedOffsets();
    if (cachedSize.isFixedSize())
        return cachedSize.getFixedSize();
    unsigned size = 0;
    ForEachItemIn(idx, children)
        size += children.item(idx).getTotalMinimumSize();
    return size;
}

void CIfBlockInfo::calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf)
{
    calcCachedChildrenOffsets(offset, cachedSize);

    if (cachedSize.isFixedSize())
        sizeSelf.set(cachedSize);
    else
    {
        addVariableSize(0, sizeSelf);
//      if (alwaysPresent)
//          sizeSelf.addFixed(cachedSize.getFixedSize());
    }
}

bool CIfBlockInfo::checkCompatibleIfBlock(HqlExprCopyArray & conditions)
{
    ForEachItemIn(i, conditions)
    {
        if (!areExclusiveConditions(condition, &conditions.item(i)))
            return false;
    }
    conditions.append(*condition);
    return true;
}

unsigned CIfBlockInfo::getTotalFixedSize()
{
    if (alwaysPresent)
        return CContainerInfo::getTotalFixedSize();
    return 0;
}

unsigned CIfBlockInfo::getTotalMinimumSize()
{
    return 0;
}

void CBitfieldContainerInfo::calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf)
{
    cachedSize.addFixed(column->queryType()->getSize());
    sizeSelf.set(cachedSize);

    SizeStruct sizeBitfields(sizeSelf.querySelf());
    calcCachedChildrenOffsets(offset, sizeBitfields);
    assertex(sizeBitfields.isFixedSize() && sizeBitfields.getFixedSize() == 0);
}



void CMemberInfo::gatherSize(SizeStruct & target)
{
    assertex(!"Should be implemented for non-containers");
}

void CMemberInfo::gatherOffset(SizeStruct & target, IHqlExpression * selector)
{
    if (!isOffsetCached)
        calcAllCachedOffsets();
    target.set(cachedOffset);
}


void CMemberInfo::getSizeExpr(SizeStruct & target)
{
    if (!isOffsetCached)
        calcAllCachedOffsets();
    target.set(cachedSize);
}


size32_t CMemberInfo::getContainerTrailingFixed()
{
    SizeStruct size;
    assertex(container);
    container->addTrailingFixed(size, this);
    return size.getFixedSize();
}


void CMemberInfo::buildConditionFilter(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    OwnedHqlExpr condition = getConditionSelect(translator, ctx, selector->queryRootRow());
    if (condition)
        translator.buildFilter(ctx, condition);
}

void CMemberInfo::buildOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    if (!cachedAccessorOffset.isEmpty())
    {
        OwnedHqlExpr value;
        if (cachedAccessorOffset.queryVarSize())
        {
            IHqlExpression * accessor = selector->queryRootRow()->ensureAccessor(translator, ctx);
            assertex(accessor);
            value.setown(createValue(no_select, LINK(sizetType), LINK(accessor), LINK(cachedAccessorOffset.queryVarSize())));
        }
        bound.expr.setown(createSizeExpression(value, cachedAccessorOffset.getFixedSize()));
    }
    else
    {
        SizeStruct totalSize;
        gatherOffset(totalSize, selector->queryExpr());     //this

        totalSize.buildSizeExpr(translator, ctx, selector->queryRootRow(), bound);
    }
}

void callDeserializeGetN(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, IHqlExpression * boundSize, IHqlExpression * address)
{
    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*LINK(boundSize));
    args.append(*LINK(address));
    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(deserializerReadNId, args);
    ctx.addExpr(call);
}

IHqlExpression * callDeserializerGetSize(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper)
{
    HqlExprArray args;
    args.append(*LINK(helper));
    OwnedHqlExpr call = translator.bindFunctionCall(deserializerReadSizeId, args);
    OwnedHqlExpr sizeVariable = ctx.getTempDeclare(sizetType, call);
    return LINK(sizeVariable);
}

void callDeserializerSkipInputSize(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, IHqlExpression * size)
{
    CHqlBoundExpr bound;
    translator.buildExpr(ctx, size, bound);
    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*LINK(bound.expr));
    translator.callProcedure(ctx, deserializerSkipNId, args);
}

void callDeserializerSkipInputTranslatedSize(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, IHqlExpression * size)
{
    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*LINK(size));
    translator.callProcedure(ctx, deserializerSkipNId, args);
}

void CMemberInfo::associateSizeOf(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * rawSize, size32_t extraSize)
{
    //Use the size just calculated for the field
    OwnedHqlExpr sizeOfExpr = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()));
    OwnedHqlExpr srcSize = adjustValue(rawSize, extraSize);
    ctx.associateExpr(sizeOfExpr, srcSize);
}

void CMemberInfo::doBuildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IHqlExpression * boundSize)
{
    if (!boundSize->isConstant())
    {
        OwnedHqlExpr unboundSize = createTranslated(boundSize);
        checkAssignOk(translator, ctx, selector, unboundSize, 0);
    }

    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryPhysicalType());
    callDeserializeGetN(translator, ctx, helper, boundSize, address);

    associateSizeOf(ctx, selector, boundSize, 0);
}


void CMemberInfo::doBuildSkipInput(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, size32_t size)
{
    HqlExprArray args;
    args.append(*LINK(helper));

    if (size == UNKNOWN_LENGTH)
    {
        OwnedHqlExpr sizeVariable = callDeserializerGetSize(translator, ctx, helper);
        callDeserializerSkipInputTranslatedSize(translator, ctx, helper, sizeVariable);
    }
    else
    {
        OwnedHqlExpr sizeExpr = getSizetConstant(size);
        callDeserializerSkipInputTranslatedSize(translator, ctx, helper, sizeExpr);
    }
}


IHqlExpression * CMemberInfo::createSelfPeekDeserializer(HqlCppTranslator & translator, IHqlExpression * helper)
{
    IHqlExpression * size = column->queryProperty(EPsize);
    LinkedHqlExpr maxSize = size->queryChild(2);
    if (!maxSize || maxSize->isAttribute())
        maxSize.setown(getSizetConstant(MAX_RECORD_SIZE));
    HqlExprArray peekArgs;
    peekArgs.append(*LINK(helper));
    peekArgs.append(*LINK(maxSize));
    return translator.bindTranslatedFunctionCall(deserializerPeekId, peekArgs);
}

void CMemberInfo::gatherMaxRowSize(SizeStruct & totalSize, IHqlExpression * newSize, size32_t fixedExtra, IReferenceSelector * selector)
{
    if (!isOffsetCached)
        calcAllCachedOffsets();

    totalSize.set(cachedOffset);
    totalSize.addFixed(fixedExtra);

    //Total size of the record is
    //offset+<size-this>+<any-following-fixed-size>
    if (newSize)
    {
        IHqlExpression * size = newSize;
        if (size->getOperator() == no_translated)
            size = size->queryChild(0);

        IValue * fixedNewSize = size->queryValue();
        if (fixedNewSize)
            totalSize.addFixed((size32_t)fixedNewSize->getIntValue());
        else
            totalSize.addVariableExpr(0, newSize);
    }

    BoundRow * row = selector->queryRootRow();
    if (!row->isSerialization())
    {
        if (container)
            container->addTrailingFixed(totalSize, this);
    }
}


void CMemberInfo::checkAssignOk(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * newSize, unsigned fixedExtra)
{
    //If no size beyond the constant value then this can't be increasing the size of the row => no need to check
    if (matchesConstantValue(newSize, 0))
        return;

    CHqlBoundExpr bound;
    SizeStruct totalSize;
    gatherMaxRowSize(totalSize, newSize, fixedExtra, selector);

    BoundRow * row = selector->queryRootRow();
    if (row->isSerialization())
    {
        //<any-following-fixed-size> is unknown at this point.
        //equals (eventualMaximum - fixed-to-this-point)
        //note, offset - leadingFixed should never go negative
        container->subLeadingFixed(totalSize, this);
        totalSize.buildSizeExpr(translator, ctx, row, bound);
        //NOTE: The final fixed size is unknown at this point, and is implemented as a callback.
        bound.expr.setown(createValue(no_add, bound.expr->getType(), row->getFinalFixedSizeExpr(), LINK(bound.expr)));

        HqlExprArray args2;
        args2.append(*LINK(row->queryBound()));
        args2.append(*LINK(bound.expr));
        translator.callProcedure(ctx, ensureRowAvailableId, args2);
    }
    else
    {
        unsigned maxRowSize = row->getMaxSize();
        unsigned fixedSize = totalSize.getFixedSize();

        //This removes calls that can be constant folded - a bit confusing in the generated code sometimes..
        if (!row->queryBuilder() && !totalSize.queryVarSize())
            return;

        totalSize.buildSizeExpr(translator, ctx, row, bound);

        IValue * value = bound.expr->queryValue();
        if (value)
        {
            unsigned constSize = (unsigned)value->getIntValue();
            if (constSize <= getMinRecordSize(row->queryRecord()))
                return;
        }

        StringBuffer fieldname;
        fieldname.append(column->queryName()).toLowerCase();

        if (row->queryBuilder())
        {
            HqlExprArray args2;
            args2.append(*LINK(row->queryBuilder()));
            args2.append(*LINK(bound.expr));
            args2.append(*createConstant(unknownVarStringType->castFrom(fieldname.length(), fieldname.str())));
            OwnedHqlExpr call = translator.bindFunctionCall(ensureCapacityId, args2);

            bool combined = false;
            if (bound.expr->isConstant())
            {
                //Try and merge all calls to ensureCapacity with a constant value
                IHqlExpression * marker = row->queryBuilderEnsureMarker();
                HqlStmtExprAssociation * match = static_cast<HqlStmtExprAssociation *>(ctx.queryAssociation(marker, AssocStmt, NULL));
                if (match)
                {
                    //Check the previous call to ensureCapacity() wasn't outside of a condition
                    //otherwise modifying it could cause problems with the other branches
                    if (ctx.hasAssociation(*match, false))
                    {
                        ctx.replaceExpr(match->stmt, call);
                        combined = true;
                    }
                }

                if (!combined)
                {
                    IHqlStmt * stmt = ctx.addExpr(call);
                    ctx.associateOwn(* new HqlStmtExprAssociation(marker, stmt));
                    combined = true;
                }
            }
            
            if (!combined)
                ctx.addExpr(call);
        }
        else
        {
            OwnedHqlExpr max = getSizetConstant(maxRowSize);

            HqlExprArray args2;
            args2.append(*LINK(bound.expr));
            args2.append(*LINK(max));
            args2.append(*createConstant(unknownVarStringType->castFrom(fieldname.length(), fieldname.str())));
            translator.callProcedure(ctx, checkFieldOverflowId, args2);
        }
    }
}

void CMemberInfo::defaultSetColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    CHqlBoundTarget boundTarget;
    boundTarget.expr.setown(getColumnRef(translator, ctx, selector));
    translator.buildExprAssign(ctx, boundTarget, value);
}

void CMemberInfo::ensureTargetAvailable(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, size32_t thisSize)
{
    OwnedHqlExpr minSize = getSizetConstant(thisSize);
    checkAssignOk(translator, ctx, selector, minSize, 0);
}


IHqlExpression * CMemberInfo::getColumnAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, ITypeInfo * columnType, size32_t delta)
{
    ITypeInfo * type;
    if (isTypePassedByAddress(columnType) && !hasReferenceModifier(columnType))
    {
        type = makeReferenceModifier(LINK(columnType));
    }
    else
        type = makePointerType(LINK(columnType));

    CHqlBoundExpr bound;
    getColumnOffset(translator, ctx, selector, bound);
    bound.expr.setown(adjustValue(bound.expr, delta));

    IHqlExpression * rowAddr = getPointer(selector->queryRootRow()->queryBound());
//  IValue * value = bound.expr->queryValue();
//  if (value && value->getIntValue()== 0)
//      return createValue(no_typetransfer, type, rowAddr));
    return createValue(no_add, type, rowAddr, bound.expr.getClear());
}

void CMemberInfo::getColumnOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & tgt)
{
    if (!isOffsetCached)
        calcAllCachedOffsets();
    if (!cachedOffset.isWorthCommoning())
        buildOffset(translator, ctx, selector, tgt);
    else
    {
        OwnedHqlExpr expr = createValue(no_offsetof, LINK(sizetType), LINK(selector->queryExpr()));
        translator.buildExpr(ctx, expr, tgt);
    }
}

IHqlExpression * CMemberInfo::getColumnRef(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryType());

    return convertAddressToValue(address, queryType());
}

IHqlExpression * CMemberInfo::getCondition(BuildCtx & ctx)
{ 
    if (container)
        return container->getCondition(ctx); 
    return NULL;
}

IHqlExpression * CMemberInfo::getConditionSelect(HqlCppTranslator & translator, BuildCtx & ctx, BoundRow * row)
{ 
    OwnedHqlExpr cond = getCondition(ctx);
    cond.setown(row->bindToRow(cond, queryRootSelf()));

    if (row->isConditional())
        extendConditionOwn(cond, no_and, createTranslated(row->queryBound()));
    if (cond)
    {
        //Force the ifblock condition to be evaluated in a temporary - since it will be reused several times.
        CHqlBoundExpr bound;
        HqlExprAssociation * match = ctx.queryMatchExpr(cond);
        if (match)
            bound.expr.set(match->queryExpr());
        else
            translator.buildSimpleExpr(ctx, cond, bound);
        IValue * boundValue = bound.expr->queryValue();
        if (boundValue)
        {
            if (boundValue->getBoolValue())
                return NULL;
            return LINK(bound.expr);
        }
        return bound.getTranslatedExpr();
    }
    return cond.getClear();
}


ITypeInfo * CMemberInfo::queryPhysicalType()
{
    return queryType();
}

IReferenceSelector * CMemberInfo::getSelector(BuildCtx & ctx, IReferenceSelector * parentSelector)
{
    return parentSelector->select(ctx, column);
}

void CMemberInfo::getXPath(StringBuffer & out)
{
    if (container)
        container->getContainerXPath(out);
    IHqlExpression * xpath = column->queryAttribute(xpathAtom);
    if (xpath)
        xpath->queryChild(0)->queryValue()->getStringValue(out);
    else
    {
        IHqlExpression * named = column->queryAttribute(namedAtom);
        if (named)
            named->queryChild(0)->queryValue()->getStringValue(out);
        else
        {
            StringBuffer temp;
            temp.append(str(column->queryName())).toLowerCase();
            out.append(temp);
        }
    }
}


IHqlExpression * CMemberInfo::createSelectorExpr()
{
    return createSelectExpr(container->getRelativeSelf(), column.getLink());
}

IHqlExpression * CMemberInfo::makeConditional(HqlCppTranslator & translator, BuildCtx & ctx, BoundRow * row, IHqlExpression * value)
{
    OwnedHqlExpr cond = getConditionSelect(translator, ctx, row);
    if (cond)
    {
        IValue * condValue = cond->queryValue();
        if (!condValue)
            return createValue(no_if, value->getType(), cond.getClear(), LINK(value), createNullExpr(value));
        else if (!condValue->getBoolValue())
            return createNullExpr(value);
    }
    return LINK(value);
}

bool CMemberInfo::hasFixedOffset()
{
    return !hasVarOffset;
}

bool CMemberInfo::isConditional()
{ 
    if (container)
        return container->isConditional(); 
    return false;
}

IHqlExpression * CMemberInfo::queryParentSelector(IHqlExpression * selector)
{
    return selector->queryChild(0);
}

IHqlExpression * CMemberInfo::queryRootSelf()
{
    return container->queryRootSelf();
}


ITypeInfo * CMemberInfo::queryType() const
{
    return column->queryType();
}

bool CMemberInfo::requiresTemp()                                                                                                    
{ 
    return isConditional(); 
}

void CMemberInfo::setOffset(bool _hasVarOffset)
{
    hasVarOffset = _hasVarOffset;

}

AColumnInfo * CMemberInfo::lookupColumn(IHqlExpression * search)
{
    throwUnexpected();
}


//---------------------------------------------------------------------------
/* In param: _column is NOT linked */
CContainerInfo::CContainerInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : 
    CMemberInfo(_container, _prior, _column)
{
    fixedSize = true;
    isDynamic = false;
}


void CContainerInfo::addChild(CMemberInfo * child)      
{ 
    isOffsetCached = false;
    children.append(*LINK(child)); 
    registerChild(child);
}

void CContainerInfo::buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction)
{
    BuildCtx condctx(ctx);
    buildConditionFilter(translator, condctx, selector);

    if (children.ordinality() == 0)
    {
        if (column->queryRecord()->hasAttribute(_nonEmpty_Atom))
        {
            //Clear on an empty record that has the _nonEmpty_attrbute clears the implicit byte
            Owned<ITypeInfo> dummyType = makeIntType(1, false);
            OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, dummyType);
            OwnedHqlExpr dummyTarget = convertAddressToValue(address, dummyType);
            translator.buildAssignToTemp(ctx, dummyTarget, queryZero());
        }
        return;
    }

    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        Owned<IReferenceSelector> ds = cur.getSelector(ctx, selector);
        cur.buildClear(translator, condctx, ds, direction);
    }
}

void CContainerInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        Owned<IReferenceSelector> ds = cur.getSelector(ctx, selector);
        cur.buildDeserialize(translator, ctx, ds, helper, serializeForm);
    }
}

void CContainerInfo::buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        Owned<IReferenceSelector> ds = cur.getSelector(ctx, selector);
        cur.buildSerialize(translator, ctx, ds, helper, serializeForm);
    }
}

void CContainerInfo::buildSizeOf(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    if (!isOffsetCached)
        calcAllCachedOffsets();
    if (container && !usesAccessClass())
        usesAccessClass();
    if (!container && usesAccessClass())
    {
        IHqlExpression * accessor = selector->queryRootRow()->ensureAccessor(translator, ctx);
        assertex(accessor);
        if (accessorSize.queryVarSize())
            bound.expr.setown(createValue(no_select, LINK(sizetType), LINK(accessor), LINK(accessorSize.queryVarSize())));
        bound.expr.setown(createSizeExpression(bound.expr, accessorSize.getFixedSize()));
    }
    else
        cachedSize.buildSizeExpr(translator, ctx, selector->queryRootRow(), bound);
}


bool CContainerInfo::prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state)
{
    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        if (!cur.prepareReadAhead(translator, state))
            return false;
    }
    return true;
}

bool CContainerInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    ForEachItemIn(idx, children)
    {
        CMemberInfo & cur = children.item(idx);
        if (!cur.buildReadAhead(translator, ctx, state))
            return false;
    }
    return true;
}

IHqlExpression * CContainerInfo::createSelectorExpr()
{
    if (!container)
        return getRelativeSelf();
    return CMemberInfo::createSelectorExpr();
}

void CContainerInfo::setRow(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IReferenceSelector * source)
{
    if (!recordTypesMatch(selector->queryType(), source->queryType()))
        throwError(HQLERR_RecordNotCompatible);

    CHqlBoundExpr targetAddress, sourceAddress, length;
    source->buildAddress(ctx, sourceAddress);

    IHqlExpression * sourceExpr = source->queryExpr();
    OwnedHqlExpr rowExpr = sourceExpr->isDataset() ? ensureActiveRow(sourceExpr) : LINK(sourceExpr);
    OwnedHqlExpr size = createSizeof(rowExpr);
    translator.buildExpr(ctx, size, length);
    ensureSimpleLength(translator, ctx, length);

    calcAllCachedOffsets();

    //If copying from one identical record to another then the source record must be large enough,
    //so only need to check it it is a child record....
    if (!cachedSize.isFixedSize())
    {
        OwnedHqlExpr translatedLength = length.getTranslatedExpr();
        checkAssignOk(translator, ctx, selector, translatedLength, 0);
    }

    buildAddress(translator, ctx, selector, targetAddress);

    HqlExprArray args;
    if (recordRequiresLinkCount(column->queryRecord()))
    {
        args.append(*LINK(targetAddress.expr));
        args.append(*LINK(length.expr));
        args.append(*LINK(sourceAddress.expr));
        args.append(*translator.buildMetaParameter(column));
        translator.callProcedure(ctx, rtlCopyRowLinkChildrenId, args);
    }
    else
    {
        args.append(*LINK(targetAddress.expr));
        args.append(*LINK(sourceAddress.expr));
        args.append(*LINK(length.expr));
        translator.callProcedure(ctx, memcpyId, args);
    }

    //Use the size just calculated for the field
    associateSizeOf(ctx, selector, length.expr, 0);
}


void CContainerInfo::addTrailingFixed(SizeStruct & size, CMemberInfo * cur)
{
    if (container)
        container->addTrailingFixed(size, this);

    unsigned max = children.ordinality();
    unsigned match = children.find(*cur);
    for (unsigned i=match+1; i < max; i++)
        size.addFixed(children.item(i).getTotalMinimumSize());
}

void CContainerInfo::subLeadingFixed(SizeStruct & size, CMemberInfo * cur)
{
    if (container)
        container->subLeadingFixed(size, this);

    unsigned match = children.find(*cur);
    for (unsigned i=0; i < match; i++)
        size.addFixed((unsigned)-(int)children.item(i).getTotalFixedSize());
}

IHqlExpression * CContainerInfo::getCondition(BuildCtx & ctx)
{
    if (container)
        return container->getCondition(ctx);
    return NULL;
}

void CContainerInfo::getContainerXPath(StringBuffer & out)
{
    if (container)
        container->getContainerXPath(out);
    if (column->getOperator() == no_field)
    {
        StringBuffer temp;
        IHqlExpression * xpath = column->queryAttribute(xpathAtom);
        if (xpath)
            xpath->queryChild(0)->queryValue()->getStringValue(temp);
        else
        {
            IHqlExpression * named = column->queryAttribute(namedAtom);
            if (named)
                named->queryChild(0)->queryValue()->getStringValue(temp);
            else
                temp.append(str(column->queryName())).toLowerCase();
        }
        unsigned len = temp.length();
        if (len && (temp.charAt(len-1) != '/'))
            temp.append('/');
        out.append(temp);
    }
}

unsigned CContainerInfo::nextSeq()
{
    if (container)
        return container->nextSeq();
    return ++seq;
}


bool CContainerInfo::isConditional()
{
    if (container)
        return container->isConditional();
    return false;
}

void CContainerInfo::registerChild(CMemberInfo * child) 
{ 
    container->registerChild(child); 
}


//---------------------------------------------------------------------------

CRecordInfo::CRecordInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) :
    CContainerInfo(_container, _prior, _column)
{
    useAccessClass = false;
}

void CRecordInfo::buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)     
{
    CHqlBoundExpr temp;
    buildExpr(translator, ctx, selector, temp);
    translator.assign(ctx, target, temp);
}


void CRecordInfo::buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr size = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()), NULL);
    CHqlBoundExpr boundSize;
    translator.buildExpr(ctx, size, boundSize);

    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryType());

    bound.length.set(boundSize.expr);
    bound.expr.setown(convertAddressToValue(address, queryType()));
}

void CRecordInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    translator.buildCompoundAssign(ctx, selector->queryExpr(), value);
}


IHqlExpression * CRecordInfo::getRelativeSelf()
{
    if (!cachedSelf)
    {
        if (container)
            cachedSelf.setown(createSelectExpr(container->getRelativeSelf(), LINK(column)));
        else
            cachedSelf.setown(createSelector(no_self, column->queryRecord(), NULL));
    }
    return LINK(cachedSelf);
}

IHqlExpression * CRecordInfo::queryRootSelf()
{
    if (container)
        return container->queryRootSelf();
    if (!cachedSelf)
        cachedSelf.setown(createSelector(no_self, column->queryRecord(), NULL));
    return cachedSelf;
}


AColumnInfo * CRecordInfo::lookupColumn(IHqlExpression * search)
{
    return map.find(search);
}

void CRecordInfo::registerChild(CMemberInfo * child)    
{ 
    map.replaceOwn(*child);
}

//---------------------------------------------------------------------------

CIfBlockInfo::CIfBlockInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) :
    CContainerInfo(_container, _prior, _column)
{
    OwnedHqlExpr self = container->getRelativeSelf();
    condition.setown(replaceSelector(column->queryChild(0), querySelfReference(), self));
    alwaysPresent = (condition->queryValue() && condition->queryValue()->getBoolValue());
}

void CIfBlockInfo::buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)        
{
    throwUnexpected();
}

void CIfBlockInfo::buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    throwUnexpected();
}

void CIfBlockInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    //MORE: This should really associate offset of the ifblock with the offset of its first child as well.
    CHqlBoundExpr boundOffset;
    buildOffset(translator, ctx, selector, boundOffset);

    //NB: Sizeof(ifblock) has an unusual representation...
    OwnedHqlExpr sizeOfIfBlock = createValue(no_sizeof, makeIntType(4,false), createSelectExpr(LINK(selector->queryExpr()), LINK(column)));
    CHqlBoundTarget cachedSize;
    cachedSize.expr.setown(ctx.getTempDeclare(sizetType, queryZero()));

    //MORE: Should also conditionally set a variable to the size of the ifblock to simplify subsequent generated code
    OwnedHqlExpr cond = selector->queryRootRow()->bindToRow(condition, queryRootSelf());
    CHqlBoundExpr bound;
    translator.buildSimpleExpr(ctx, cond, bound);
    BuildCtx condctx(ctx);
    condctx.addFilter(bound.expr);

    //MORE: This test could be avoided if the first child is *actually* variable length
    ensureTargetAvailable(translator, condctx, selector, CContainerInfo::getTotalMinimumSize());
    CContainerInfo::buildDeserialize(translator, condctx, selector, helper, serializeForm);

    //Avoid recalculating the size outside of the ifblock()
    translator.buildExprAssign(condctx, cachedSize, sizeOfIfBlock);

    ctx.associateExpr(sizeOfIfBlock, cachedSize.expr);
}

void CIfBlockInfo::buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr cond = selector->queryRootRow()->bindToRow(condition, queryRootSelf());
    CHqlBoundExpr bound;
    translator.buildSimpleExpr(ctx, cond, bound);
    BuildCtx condctx(ctx);
    condctx.addFilter(bound.expr);
    CContainerInfo::buildSerialize(translator, condctx, selector, helper, serializeForm);
}

bool CIfBlockInfo::prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state)
{
    gatherSelectExprs(state.requiredValues, condition);
    return CContainerInfo::prepareReadAhead(translator, state);
}

bool CIfBlockInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    try
    {
        OwnedHqlExpr mappedCondition = quickFullReplaceExpressions(condition, state.requiredValues, state.mappedValues);
        //Early check to see if all the values have been mapped, rather than relying on exception processing.
        if (!containsSelector(mappedCondition, queryRootSelf()))
        {
            BuildCtx condctx(ctx);
            translator.buildFilter(condctx, mappedCondition);
            return CContainerInfo::buildReadAhead(translator, condctx, state);
        }
    }
    catch (IException * e)
    {
        //yuk yuk yuk!!  Could't resolve the test condition for very unusual reason, e.g., based on a variable length string.
        e->Release();
    }
    return false;
}

void CIfBlockInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    throwUnexpected();
}

IHqlExpression * CIfBlockInfo::getCondition(BuildCtx & ctx)
{
    IHqlExpression * containerCond = container->getCondition(ctx);
    if (alwaysPresent)
        return containerCond;

    if (containerCond)
        return createBoolExpr(no_and, LINK(condition), containerCond);
    return LINK(condition);
}

IHqlExpression * CIfBlockInfo::getRelativeSelf()
{
    return container->getRelativeSelf();
}

IReferenceSelector * CIfBlockInfo::getSelector(BuildCtx & ctx, IReferenceSelector * parentSelector)
{
    return LINK(parentSelector);
}

bool CIfBlockInfo::isConditional()
{
    if (alwaysPresent)
        return CMemberInfo::isConditional();
    return true;
}


IHqlExpression * CIfBlockInfo::queryParentSelector(IHqlExpression * selector)
{
    return selector;
}

//---------------------------------------------------------------------------

CColumnInfo::CColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CMemberInfo(_container, _prior, _column)
{
}

void CColumnInfo::buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)
{
    BoundRow * row = selector->queryRootRow();
    if (isConditional() || row->isConditional())
    {
        CHqlBoundExpr boundCondition;

        OwnedHqlExpr condition = getConditionSelect(translator, ctx, row);

        //calculate the address (and throw it away) so that indexing calculations aren't
        //included in the conditional code.
        calcCurrentOffset(translator, ctx, selector);

        // now generate if (a) b else c
        BuildCtx condctx(ctx);
        IHqlStmt * stmt = NULL;
        if (condition)
            stmt = translator.buildFilterViaExpr(condctx, condition);

        buildColumnAssign(translator, condctx, selector, target);

        if (stmt)
        {
            condctx.selectElse(stmt);
            IHqlExpression * dft = column->queryChild(0);
            if (dft && !dft->isAttribute() && dft->isConstant())
                translator.buildExprAssign(condctx, target, dft);
            else
                translator.buildClear(condctx, target);
        }
    }
    else
    {
        buildColumnAssign(translator, ctx, selector, target);
    }
}

void CColumnInfo::calcCurrentOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryPhysicalType());
}

void CColumnInfo::gatherSize(SizeStruct & target)
{
    if (isConditional())
        addVariableSize(queryType()->getSize(), target);
    else
        target.addFixed(queryType()->getSize());
}


void CColumnInfo::buildSizeOf(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    //Need to be careful that buildSizeOfUnbound() is called within a test for the dataset being present
    //otherwise with alien datatypes it can incorrectly access the current row.
    BoundRow * row = selector->queryRootRow();
    if (!row->isConditional())
    {
        OwnedHqlExpr value = ensureType(buildSizeOfUnbound(translator, ctx, selector), sizetType);
        value.setown(makeConditional(translator, ctx, row, value));
        translator.buildExpr(ctx, value, bound);
    }
    else
    {
        OwnedHqlExpr cond = getConditionSelect(translator, ctx, row);
        CHqlBoundTarget tempTarget;
        translator.createTempFor(ctx, sizetType, tempTarget, typemod_none, FormatNatural);

        BuildCtx subctx(ctx);
        IHqlStmt * ifStmt = translator.buildFilterViaExpr(subctx, cond);
        OwnedHqlExpr value = ensureType(buildSizeOfUnbound(translator, subctx, selector), sizetType);
        translator.buildExprAssign(subctx, tempTarget, value);
        subctx.selectElse(ifStmt);
        translator.buildExprAssign(subctx, tempTarget, queryZero());

        bound.setFromTarget(tempTarget);
    }
}


IHqlExpression * CColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    ITypeInfo * type = queryType();
    unsigned typeSize = type->getSize();
    return getSizetConstant(typeSize);
}


static __int64 maxIntValue[] =  { I64C(0), I64C(0x7F), I64C(0x7FFF), I64C(0x7FFFFF), I64C(0x7FFFFFFF), I64C(0x7FFFFFFFFF), I64C(0x7FFFFFFFFFFF), I64C(0x7FFFFFFFFFFFFF), I64C(0x7FFFFFFFFFFFFFFF) };
void CColumnInfo::buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction)
{
    OwnedHqlExpr null;
    ITypeInfo * type = queryType();
    if (direction != 0)
    {
        switch (type->getTypeCode())
        {
        case type_int:
        case type_swapint:
            if (type->isSigned())
            {
                __int64 value = maxIntValue[type->getSize()];
                if (direction == -1)
                    value = 0-(value+1);
                null.setown(createConstant(type->castFrom(true, value)));
            }
            break;
        case type_decimal:
            if (type->isSigned())
            {
                size32_t size = type->getSize();
                byte * temp = (byte *)alloca(size);
                memset(temp, 0x99, size-1);
                if ((type->getDigits() & 1) != 0)
                    temp[0] = 0x99;
                else
                    temp[0] = 0x09;
                if (direction == 1)
                    temp[size-1] = 0x90;
                else
                    temp[size-1] = 0x9F;

                null.setown(createConstant(createValueFromMem(LINK(type), temp)));
            }
            else if (direction == 1)
            {
                size32_t size = type->getSize();
                byte * temp = (byte *)alloca(size);
                memset(temp, 0x99, size);
                if ((type->getDigits() & 1) != 0)
                    temp[0] = 0x99;
                else
                    temp[0] = 0x09;
                null.setown(createConstant(createValueFromMem(LINK(type), temp)));
            }
            break;
        case type_real:
            //MORE: ? assign +/-inf?
            break;
        case type_string:
        case type_data:
        case type_varstring:
            {
                size32_t size = type->getSize();
                size32_t len = type->getStringLen();
                if (size == UNKNOWN_LENGTH)
                {
                    assertex(direction < 0);
                    null.setown(createConstant(type->castFrom(0, (const char *)NULL)));
                }
                else
                {
                    MemoryAttr buffer(size);
                    memset(buffer.bufferBase(), direction < 0 ? 0x00 : 0xff, size);
                    null.setown(createConstant(type->castFrom(len, (const char *)buffer.get())));
                }
                break;
            }
        case type_unicode:
        case type_varunicode:
            {
                assertex(direction < 0);
                size32_t size = type->getSize();
                size32_t len = type->getStringLen();
                if (size == UNKNOWN_LENGTH)
                {
                    null.setown(createConstant(type->castFrom(0, (const UChar *)NULL)));
                }
                else
                {
                    MemoryAttr buffer(size);
                    memset(buffer.bufferBase(), 0x00, size);
                    null.setown(createConstant(type->castFrom(len, (const UChar *)buffer.get())));
                }
                break;
            }
        }
    }
    if (!null)
        null.setown(createNullExpr(column));
    setColumn(translator, ctx, selector, null);
}

void CColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    CHqlBoundExpr boundSize;
    OwnedHqlExpr unboundSize = ensureType(buildSizeOfUnbound(translator, ctx, selector), sizetType);
    translator.buildExpr(ctx, unboundSize, boundSize);

    doBuildDeserialize(translator, ctx, selector, helper, boundSize.expr);
}

void CColumnInfo::buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr sizeOfExpr = createValue(no_sizeof, LINK(sizetType), LINK(selector->queryExpr()));
    CHqlBoundExpr boundSize;
    translator.buildExpr(ctx, sizeOfExpr, boundSize);

    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*LINK(boundSize.expr));
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));
    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(serializerPutId, args);
    ctx.addExpr(call);
}


bool CColumnInfo::prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state)
{
    return true;
}

bool CColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    size32_t columnSize = queryType()->getSize();
    if ((columnSize != UNKNOWN_LENGTH) && state.requiredValues.ordinality())
    {
        OwnedHqlExpr selector = createSelectorExpr();
        unsigned match = state.requiredValues.find(*selector);
        if (match != NotFound)
        {
            OwnedHqlExpr tempVariable = ctx.getTempDeclare(queryType(), NULL);

            HqlExprArray args;
            args.append(*LINK(state.helper));
            args.append(*getSizetConstant(columnSize));
            args.append(*getPointer(tempVariable));
            OwnedHqlExpr call = translator.bindTranslatedFunctionCall(deserializerReadNId, args);
            ctx.addExpr(call);

            OwnedHqlExpr translated = createTranslated(tempVariable);
            state.setMapping(match, translated);
            return true;
        }
    }

    doBuildSkipInput(translator, ctx, state.helper, column->queryType()->getSize());
    return true;
}

void CColumnInfo::buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    BoundRow * row = selector->queryRootRow();
    if (isConditional() || row->isConditional())
    {
        OwnedHqlExpr condition = getConditionSelect(translator, ctx, row);
        if (condition)
        {
            //Unusual, but if this is inside a fileview translation helper there is no context, so use blocked
            //format to avoid temporaries that require an allocator.
            //A better fix long term would be to pass the format to get() and buildExpr
            ExpressionFormat format = ctx.queryMatchExpr(codeContextMarkerExpr) ? FormatNatural : FormatBlockedDataset;

            //MORE: Can conditionally code retrieval be improved for some types of field...
            CHqlBoundTarget tempTarget;
            translator.createTempFor(ctx, queryLogicalType(), tempTarget, typemod_none, format);
            buildAssign(translator, ctx, selector, tempTarget);
            bound.setFromTarget(tempTarget);
            return;
        }
    }

    buildColumnExpr(translator, ctx, selector, bound);
}


bool CColumnInfo::isFixedSize()
{
    return (queryType()->getSize() != UNKNOWN_LENGTH);
}


void CColumnInfo::buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)
{
    CHqlBoundExpr bound;
    buildColumnExpr(translator, ctx, selector, bound);
    translator.assign(ctx, target, bound);
}

void CColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    bound.expr.setown(getColumnRef(translator, ctx, selector));
}

void CColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    if (isConditional())
    {
        OwnedHqlExpr size = getSizetConstant(queryType()->getSize());
        checkAssignOk(translator, ctx, selector, size, 0);

        CHqlBoundTarget tgt;
        OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryType());
        tgt.expr.setown(convertAddressToValue(address, queryType()));

        translator.buildExprAssign(ctx, tgt, value);
    }
    else
        defaultSetColumn(translator, ctx, selector, value);
}

//---------------------------------------------------------------------------

CSpecialIntColumnInfo::CSpecialIntColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CSpecialIntColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    ITypeInfo * type = queryType();
    IIdAtom * func = readIntId[type->getSize()][type->isSigned()];

    HqlExprArray args;
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));
    bound.expr.setown(translator.bindTranslatedFunctionCall(func, args));

    ITypeInfo * promoted = type->queryPromotedType();
    if (promoted != bound.expr->queryType())
        bound.expr.setown(createValue(no_typetransfer, LINK(promoted), bound.expr.getClear()));
}

void CSpecialIntColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    ITypeInfo * type = queryType();
    ITypeInfo * promoted = type->queryPromotedType();
    IIdAtom * func = writeIntId[type->getSize()];

    if (isConditional())
    {
        OwnedHqlExpr size = getSizetConstant(type->getSize());
        checkAssignOk(translator, ctx, selector, size, 0);
    }

    HqlExprArray args;
    CHqlBoundExpr bound;
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));

    ITypeInfo * valueType = value->queryType();
    LinkedHqlExpr castValue = value;
    if (valueType->getTypeCode() != type_int || type->getTypeCode() != type_int)
        castValue.setown(ensureExprType(value, promoted));

    translator.buildExpr(ctx, castValue, bound);
    args.append(*bound.expr.getLink());

    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(func, args);
    ctx.addExpr(call);
}

//---------------------------------------------------------------------------

CPackedIntColumnInfo::CPackedIntColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CPackedIntColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    ITypeInfo * type = queryType();
    IIdAtom * func = type->isSigned() ? getPackedSignedId : getPackedUnsignedId;

    HqlExprArray args;
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));
    bound.expr.setown(translator.bindTranslatedFunctionCall(func, args));

    ITypeInfo * promoted = type->queryPromotedType();
    if (promoted != bound.expr->queryType())
        bound.expr.setown(createValue(no_typetransfer, LINK(promoted), bound.expr.getClear()));
}

void CPackedIntColumnInfo::gatherSize(SizeStruct & target)
{
    addVariableSize(1, target);
}


IHqlExpression * CPackedIntColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    HqlExprArray args;
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));
    return createValue(no_translated, LINK(sizetType), translator.bindTranslatedFunctionCall(getPackedSizeId, args));
}

void CPackedIntColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr size = getSizetConstant(queryType()->queryPromotedType()->getSize()+1);    // an over-estimate, but more efficient than working out exactly.
    checkAssignOk(translator, ctx, selector, size, 0);

    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));
    translator.buildTranslatedFunctionCall(ctx, deserializerReadPackedIntId, args);
}

bool CPackedIntColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    Owned<ITypeInfo> tempType = makeDataType(queryType()->queryPromotedType()->getSize()+1);    // an over-estimate
    OwnedHqlExpr tempVar = ctx.getTempDeclare(tempType, NULL);

    HqlExprArray args;
    args.append(*LINK(state.helper));
    args.append(*getPointer(tempVar));
    translator.buildTranslatedFunctionCall(ctx, deserializerReadPackedIntId, args);
    return true;
}

void CPackedIntColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    ITypeInfo * type = queryType();
    IIdAtom * func = type->isSigned() ? setPackedSignedId : setPackedUnsignedId;

    OwnedHqlExpr size = getSizetConstant(type->queryPromotedType()->getSize()+1);   // an over-estimate, but more efficient than working out exactly.
    checkAssignOk(translator, ctx, selector, size, 0);

    HqlExprArray args;
    CHqlBoundExpr bound;
    args.append(*getColumnAddress(translator, ctx, selector, queryPhysicalType()));

    OwnedHqlExpr castValue = ensureExprType(value, type);
    translator.buildExpr(ctx, castValue, bound);
    args.append(*LINK(bound.expr));

    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(func, args);
    ctx.addExpr(call);
}

//---------------------------------------------------------------------------

CSpecialStringColumnInfo::CSpecialStringColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CSpecialStringColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, sizetType);
    OwnedHqlExpr addressStr = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    bound.length.setown(convertAddressToValue(address, sizetType));
    bound.expr.setown(convertAddressToValue(addressStr, queryType()));

#if 0
    //Following improves code for transforms, disable until I finish regression testing
    ensureSimpleLength(translator, ctx, bound);
    OwnedHqlExpr boundSize = translator.getBoundSize(bound);
    associateSizeOf(ctx, selector, boundSize, sizeof(size32_t));
#endif
}

void CSpecialStringColumnInfo::gatherSize(SizeStruct & target)
{
    addVariableSize(sizeof(size32_t), target);
}


void CSpecialStringColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, sizetType);
    OwnedHqlExpr addressStr = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    OwnedHqlExpr targetLength = convertAddressToValue(address, sizetType);
    OwnedHqlExpr sizeSizet = getSizetConstant(sizeof(size32_t));
    callDeserializeGetN(translator, ctx, helper, sizeSizet, address);

    if (queryType()->getTypeCode() == type_utf8)
    {
        BoundRow * row = selector->queryRootRow();

        CHqlBoundExpr boundOffset;
        getColumnOffset(translator, ctx, selector, boundOffset);

        SizeStruct fixedRowSize;
        CHqlBoundExpr boundFixedRowSize;
        gatherMaxRowSize(fixedRowSize, NULL, sizeof(size32_t), selector);
        fixedRowSize.buildSizeExpr(translator, ctx, row, boundFixedRowSize);

        assertex(!row->isSerialization() && row->queryBuilder());

        IIdAtom * func = deserializerReadUtf8Id;
        HqlExprArray args;
        args.append(*LINK(helper));
        args.append(*LINK(row->queryBuilder()));
        args.append(*adjustValue(boundOffset.expr, sizeof(size32_t)));
        args.append(*LINK(boundFixedRowSize.expr));
        args.append(*LINK(targetLength));
        OwnedHqlExpr call = translator.bindTranslatedFunctionCall(func, args);
        OwnedHqlExpr sizeVariable = ctx.getTempDeclare(sizetType, call);
        associateSizeOf(ctx, selector, sizeVariable, sizeof(size32_t));
    }
    else
    {

        CHqlBoundExpr bound;
        bound.length.setown(translator.ensureSimpleTranslatedExpr(ctx, targetLength));
        bound.expr.setown(convertAddressToValue(addressStr, queryType()));

        OwnedHqlExpr boundSize = translator.getBoundSize(bound);
        if (queryType()->getTypeCode() == type_qstring)
            boundSize.setown(translator.ensureSimpleTranslatedExpr(ctx, boundSize));
        OwnedHqlExpr unboundSize = createTranslated(boundSize);

        checkAssignOk(translator, ctx, selector, unboundSize, sizeof(size32_t));

        callDeserializeGetN(translator, ctx, helper, boundSize, addressStr);

        associateSizeOf(ctx, selector, boundSize, sizeof(size32_t));
    }
}


bool CSpecialStringColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    OwnedHqlExpr lengthVariable = callDeserializerGetSize(translator, ctx, state.helper);

    if (column->queryType()->getTypeCode() == type_utf8)
    {
        HqlExprArray args;
        args.append(*LINK(state.helper));
        args.append(*LINK(lengthVariable));
        translator.callProcedure(ctx, deserializerSkipUtf8Id, args);
    }
    else
    {
        OwnedHqlExpr size = translator.getBoundSize(column->queryType(), lengthVariable, NULL);
        callDeserializerSkipInputTranslatedSize(translator, ctx, state.helper, size);
    }
    return true;
}

IHqlExpression * CSpecialStringColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, sizetType);
    OwnedHqlExpr addressStr = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    OwnedHqlExpr length = convertAddressToValue(address, sizetType);
    assertex(length->queryType()->queryTypeBase() == sizetType);

    OwnedHqlExpr boundSize = translator.getBoundSize(column->queryType(), length, addressStr);
    return createValue(no_translated, LINK(sizetType), adjustValue(boundSize, sizeof(size32_t)));
}

void CSpecialStringColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, sizetType, 0);
    OwnedHqlExpr addressStr = getColumnAddress(translator, ctx, selector, queryType(), sizeof(size32_t));

    ITypeInfo * columnType = column->queryType();
    CHqlBoundExpr bound;
    OwnedHqlExpr value = ensureExprType(_value, columnType);
    translator.buildExpr(ctx, value, bound);
    ensureSimpleLength(translator, ctx, bound);

    OwnedHqlExpr length = createValue(no_translated, LINK(sizetType), translator.getBoundLength(bound));
    OwnedHqlExpr size = createValue(no_translated, LINK(sizetType), translator.getBoundSize(bound));
    checkAssignOk(translator, ctx, selector, size, sizeof(size32_t));
    CHqlBoundTarget boundTarget;
    boundTarget.expr.setown(convertAddressToValue(address, sizetType));
    translator.buildExprAssign(ctx, boundTarget, length);

    translator.buildBlockCopy(ctx, addressStr, bound);

    //Use the size just calulated for the field
    OwnedHqlExpr boundSize = translator.getBoundSize(bound);
    associateSizeOf(ctx, selector, boundSize, sizeof(size32_t));
}

//---------------------------------------------------------------------------

CSpecialVStringColumnInfo::CSpecialVStringColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CSpecialVStringColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryType(), 0);
    bound.expr.setown(convertAddressToValue(address, queryType()));
}

void CSpecialVStringColumnInfo::gatherSize(SizeStruct & target)
{
    size32_t varMinSize = (queryType()->getTypeCode() == type_varunicode) ? sizeof(UChar) : 1;
    addVariableSize(varMinSize, target);
}


IHqlExpression * CSpecialVStringColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    CHqlBoundExpr bound;
    buildColumnExpr(translator, ctx, selector, bound);
    IHqlExpression * length = translator.getBoundSize(bound);
    return createTranslatedOwned(length);
}

void CSpecialVStringColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    BoundRow * row = selector->queryRootRow();

    CHqlBoundExpr boundOffset;
    getColumnOffset(translator, ctx, selector, boundOffset);

    SizeStruct fixedRowSize;
    CHqlBoundExpr boundFixedRowSize;
    gatherMaxRowSize(fixedRowSize, NULL, 0, selector);
    fixedRowSize.buildSizeExpr(translator, ctx, row, boundFixedRowSize);

    assertex(!row->isSerialization() && row->queryBuilder());

    IIdAtom * func;
    switch (queryType()->getTypeCode())
    {
    case type_varstring:
        func = deserializerReadVStrId;
        break;
    case type_varunicode:
        func = deserializerReadVUniId;
        break;
    default:
        throwUnexpected();
    }

    HqlExprArray args;
    args.append(*LINK(helper));
    args.append(*LINK(row->queryBuilder()));
    args.append(*LINK(boundOffset.expr));
    args.append(*LINK(boundFixedRowSize.expr));
    OwnedHqlExpr call = translator.bindTranslatedFunctionCall(func, args);
    OwnedHqlExpr sizeVariable = ctx.getTempDeclare(sizetType, call);
    associateSizeOf(ctx, selector, sizeVariable, 0);
}


bool CSpecialVStringColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    HqlExprArray args;
    args.append(*LINK(state.helper));

    IIdAtom * func;
    switch (queryType()->getTypeCode())
    {
    case type_varstring:
        func = deserializerSkipVStrId;
        break;
    case type_varunicode:
        func = deserializerSkipVUniId;
        break;
    default:
        throwUnexpected();
    }
    translator.callProcedure(ctx, func, args);
    return true;
}


void CSpecialVStringColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value)
{
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryType(), 0);
    OwnedHqlExpr target = convertAddressToValue(address, queryType());

    ITypeInfo * columnType = column->queryType();
    OwnedHqlExpr value = ensureExprType(_value, columnType);
    //Allow a direct assignment from a string, rather than creating a temp varstring.
    switch (value->getOperator())
    {
    case no_cast: case no_implicitcast:
        ITypeInfo * type = value->queryType();
        ITypeInfo * prevType = value->queryChild(0)->queryType();
        if ((type->getTypeCode() == type_varstring) && (prevType->getTypeCode() == type_string) &&
            (type->queryCharset() == prevType->queryCharset()))
            value.set(value->queryChild(0));
        break;
    }

    CHqlBoundExpr bound;
    translator.buildExpr(ctx, value, bound);
//  ensureSimpleLength(translator, ctx, bound);

    OwnedHqlExpr length = translator.getBoundLength(bound);
    OwnedHqlExpr targetSize = adjustValue(length, 1);
    if (isUnicodeType(columnType))
        targetSize.setown(createValue(no_mul, targetSize->getType(), LINK(targetSize), getSizetConstant(2)));

    OwnedHqlExpr translatedLength = createTranslated(targetSize);
    checkAssignOk(translator, ctx, selector, translatedLength, 0);
    HqlExprArray args;
    if (columnType->getTypeCode() == type_varunicode)
    {
        translator.buildBlockCopy(ctx, address, bound);
    }
    else if (bound.expr->queryType()->getTypeCode() == type_varstring)
    {
        args.append(*target.getLink());
        args.append(*translator.getElementPointer(bound.expr));     
        translator.callProcedure(ctx, strcpyId, args);
    }
    else
    {
        args.append(*LINK(targetSize));
        args.append(*target.getLink());
        args.append(*LINK(length));
        args.append(*translator.getElementPointer(bound.expr));
        translator.callProcedure(ctx, str2VStrId, args);
        associateSizeOf(ctx, selector, LINK(targetSize), 0);
    }
}

//---------------------------------------------------------------------------

CAlienColumnInfo::CAlienColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
    self.setown(container->getRelativeSelf());
}

void CAlienColumnInfo::buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    Linked<ITypeInfo> physicalType = queryPhysicalType();
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, physicalType, 0);
    OwnedHqlExpr value = convertAddressToValue(address, physicalType);
    bound.expr.set(value->queryChild(0));
}

IHqlExpression * CAlienColumnInfo::doBuildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper)
{
    ITypeInfo * type = queryType();
    IHqlAlienTypeInfo * alien = queryAlienType(type);
    unsigned size = alien->getPhysicalTypeSize();
    
    if (size != UNKNOWN_LENGTH)
        return getSizetConstant(size);

    BoundRow * cursor = selector->queryRootRow();
    IHqlExpression * lengthAttr = alien->queryLengthFunction();
    if (!lengthAttr->isFunctionDefinition())
    {
        OwnedHqlExpr absoluteLength = replaceSelector(lengthAttr, querySelfReference(), self);
        OwnedHqlExpr value = cursor->bindToRow(absoluteLength, queryRootSelf());
        return ensureExprType(value, sizetType);
    }

    HqlExprArray args;
    if (lengthAttr->queryChild(0))
    {
        OwnedHqlExpr address;
        Owned<ITypeInfo> physicalType = getPhysicalSourceType();
        if (helper)
            address.setown(createSelfPeekDeserializer(translator, helper));
        else
            address.setown(getColumnAddress(translator, ctx, selector, physicalType, 0));

        OwnedHqlExpr value = convertAddressToValue(address, physicalType);
        args.append(*createTranslated(value));
    }
    OwnedHqlExpr expr = createBoundFunction(NULL, lengthAttr, args, NULL, true);
    OwnedHqlExpr absoluteExpr = replaceSelector(expr, querySelfReference(), self);
    OwnedHqlExpr value = cursor->bindToRow(absoluteExpr, queryRootSelf());
    return ensureExprType(value, sizetType);
}

IHqlExpression * CAlienColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    return doBuildSizeOfUnbound(translator, ctx, selector, NULL);
}

void CAlienColumnInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr unboundSize = doBuildSizeOfUnbound(translator, ctx, selector, helper);
        
    CHqlBoundExpr boundSize;
    if (unboundSize->isConstant())
        translator.buildSimpleExpr(ctx, unboundSize, boundSize);
    else
        translator.buildTempExpr(ctx, unboundSize, boundSize);
    doBuildDeserialize(translator, ctx, selector, helper, boundSize.expr);
}

bool CAlienColumnInfo::prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state)
{
    return false;  // too complicated to do safetly.  It really needs a rethink...
}

bool CAlienColumnInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    throwUnexpected();
}

void CAlienColumnInfo::gatherSize(SizeStruct & target)
{
    unsigned size = getPhysicalSize();
    if (isConditional())
        addVariableSize(0, target);
    else
    {
        if (size != UNKNOWN_LENGTH)
            target.addFixed(size);
        else
            addVariableSize(0, target);
    }
}



unsigned CAlienColumnInfo::getPhysicalSize()
{
    ITypeInfo * type = queryType();
    IHqlAlienTypeInfo * alien = queryAlienType(type);
    unsigned size = alien->getPhysicalTypeSize();

    if (size == UNKNOWN_LENGTH)
    {
        IHqlExpression * lengthAttr = queryStripCasts(alien->queryLengthFunction());
        if (lengthAttr->isConstant() && !lengthAttr->isFunction())
        {
            OwnedHqlExpr folded = foldHqlExpression(lengthAttr);
            if (folded->queryValue())
                size = (unsigned)folded->queryValue()->getIntValue();
        }
    }
    return size;
}



ITypeInfo * CAlienColumnInfo::queryPhysicalType()
{
    IHqlAlienTypeInfo * alien = queryAlienType(queryType());
    return alien->queryPhysicalType();
}


ITypeInfo * CAlienColumnInfo::getPhysicalSourceType()
{
    ITypeInfo * physicalType = queryPhysicalType();
    if (physicalType->getSize() == UNKNOWN_LENGTH)
        return getStretchedType(INFINITE_LENGTH, physicalType);

    return LINK(physicalType);
}

IHqlExpression * CAlienColumnInfo::getAlienGetFunction(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    ITypeInfo * columnType = queryType();
    IHqlAlienTypeInfo * alien = queryAlienType(columnType);
    IHqlExpression * getFunction = alien->queryLoadFunction();
    Owned<ITypeInfo> physicalType = getPhysicalSourceType();
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, physicalType, 0);
    OwnedHqlExpr physical = convertAddressToValue(address, physicalType);

    HqlExprArray args;
    args.append(*createTranslated(physical));
    OwnedHqlExpr bound = createBoundFunction(NULL, getFunction, args, NULL, true);
    OwnedHqlExpr absoluteBound = replaceSelector(bound, querySelfReference(), self);
    return selector->queryRootRow()->bindToRow(absoluteBound, queryRootSelf());
}

bool CAlienColumnInfo::isFixedSize()
{
    return getPhysicalSize() != UNKNOWN_LENGTH;
}


void CAlienColumnInfo::buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)
{
    OwnedHqlExpr expr = getAlienGetFunction(translator, ctx, selector);
    translator.buildExprAssign(ctx, target, expr);
}

void CAlienColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    BoundRow * cursor = selector->queryRootRow();

    ITypeInfo * columnType = queryType();
    IHqlAlienTypeInfo * alien = queryAlienType(columnType);
    IHqlExpression * getFunction = alien->queryLoadFunction();
    Owned<ITypeInfo> physicalType = getPhysicalSourceType();
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, physicalType);
    OwnedHqlExpr physical = convertAddressToValue(address, physicalType);

    HqlExprArray args;
    args.append(*createTranslated(physical));
    OwnedHqlExpr expr = createBoundFunction(NULL, getFunction, args, NULL, true);
    OwnedHqlExpr absoluteExpr = replaceSelector(expr, querySelfReference(), self);
    OwnedHqlExpr selectedExpr = cursor->bindToRow(absoluteExpr, queryRootSelf());
    translator.buildExpr(ctx, selectedExpr, bound);
}

void CAlienColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    BoundRow * cursor = selector->queryRootRow();
    ITypeInfo * columnType = queryType();
    
    IHqlAlienTypeInfo * alien = queryAlienType(columnType);
    IHqlExpression * setFunction = alien->queryStoreFunction();
    HqlExprArray args;

    CHqlBoundExpr bound;
    translator.buildExpr(ctx, value, bound);
    args.append(*bound.getTranslatedExpr());
    OwnedHqlExpr call = createBoundFunction(NULL, setFunction, args, NULL, true);

    Linked<ITypeInfo> physicalType = queryPhysicalType();
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, physicalType, 0);
    IHqlExpression * tgt = convertAddressToValue(address, physicalType);

    OwnedHqlExpr absoluteCall = replaceSelector(call, querySelfReference(), self);
    OwnedHqlExpr selectedCall = cursor->bindToRow(absoluteCall, queryRootSelf());
    if (physicalType->getSize() == UNKNOWN_LENGTH)
    {
        CHqlBoundExpr boundCall;
        translator.buildExpr(ctx, selectedCall, boundCall);
        OwnedHqlExpr size = translator.getBoundSize(boundCall);
        OwnedHqlExpr translatedSize = createTranslated(size);
        checkAssignOk(translator, ctx, selector, translatedSize, 0);
        translator.buildBlockCopy(ctx, tgt, boundCall);

        //Use the size just calulated for sizeof(target)
        associateSizeOf(ctx, selector, size, 0);
    }
    else
    {
        if (isConditional())
        {
            OwnedHqlExpr size = getSizetConstant(physicalType->getSize());
            checkAssignOk(translator, ctx, selector, size, 0);
        }

        CHqlBoundTarget boundTarget;
        boundTarget.expr.set(tgt);
        translator.buildExprAssign(ctx, boundTarget, selectedCall);
    }

    tgt->Release();
}

//---------------------------------------------------------------------------

CBitfieldContainerInfo::CBitfieldContainerInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) :
    CContainerInfo(_container, _prior, _column)
{
}

//AColumnInfo
void CBitfieldContainerInfo::buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)
{
    throwUnexpected();
}

void CBitfieldContainerInfo::buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction)
{
    BuildCtx condctx(ctx);
    BoundRow * cursor = selector->queryRootRow();
    OwnedHqlExpr condition = getConditionSelect(translator, ctx, cursor);
    if (condition)
        translator.buildFilter(condctx, condition);

    OwnedHqlExpr null = createNullExpr(column);
    setColumn(translator, condctx, selector, null);
}

void CBitfieldContainerInfo::buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm)
{
    OwnedHqlExpr size = getSizetConstant(column->queryType()->getSize());
    doBuildDeserialize(translator, ctx, selector, helper, size);
}

bool CBitfieldContainerInfo::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state)
{
    doBuildSkipInput(translator, ctx, state.helper, column->queryType()->getSize());
    return true;
}

IHqlExpression* CBitfieldContainerInfo::queryParentSelector(IHqlExpression* selector)
{
    return selector;
}


void CBitfieldContainerInfo::buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    throwUnexpected();
}

IReferenceSelector * CBitfieldContainerInfo::getSelector(BuildCtx & ctx, IReferenceSelector * parentSelector)
{
    return LINK(parentSelector);
}

void CBitfieldContainerInfo::noteLastBitfield() 
{
    if (children.ordinality())
        children.tos().noteLastBitfield();
}

void CBitfieldContainerInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    defaultSetColumn(translator, ctx, selector, value);
}

CMemberInfo * CBitfieldContainerInfo::lastBitfield()
{
    if (children.ordinality())
        return &(CMemberInfo &)children.tos();
    return NULL;
}

void CBitfieldContainerInfo::gatherSize(SizeStruct & size)
{
    size.addFixed(column->queryType()->getSize());
}

IHqlExpression * CBitfieldContainerInfo::getRelativeSelf()
{
    return container->getRelativeSelf();
}


bool CBitfieldContainerInfo::isFixedSize()
{
    return true;
}



//---------------------------------------------------------------------------

CBitfieldInfo::CBitfieldInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
    bitOffset = 0;
    isLastBitfield = false;
}

void CBitfieldInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    ITypeInfo * columnType = queryType();
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, queryStorageType(), 0);
    OwnedHqlExpr value = convertAddressToValue(address, queryStorageType());
    if (bitOffset > 0)
        value.setown(createValue(no_rshift, LINK(value), getSizetConstant((int)bitOffset)));

    unsigned __int64 mask = ((unsigned __int64)1<<columnType->getBitSize())-1;
    IValue * maskValue = columnType->queryChildType()->castFrom(false, (__int64)mask);
    bound.expr.setown(createValue(no_band, LINK(value), createConstant(maskValue)));
}

IHqlExpression * CBitfieldInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    return createConstant(0);
}

void CBitfieldInfo::gatherSize(SizeStruct & target)
{
    target.addFixed(0); //!!
}


bool CBitfieldInfo::isFixedSize()
{
    return true;
}

unsigned CBitfieldInfo::queryBitfieldPackSize() const
{
    const CBitfieldContainerInfo * packing = static_cast<const CBitfieldContainerInfo *>(container);
    return packing->queryStorageType()->getSize();
}


void CBitfieldInfo::setBitOffset(unsigned _bitOffset)
{ 
    bitOffset = _bitOffset;
}

void CBitfieldInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    ITypeInfo * columnType = queryType();
    ITypeInfo * storageType = queryStorageType();
    
    OwnedHqlExpr address = getColumnAddress(translator, ctx, selector, storageType, 0);
    IHqlExpression * columnRef = convertAddressToValue(address, storageType);

    unsigned bitSize = columnType->getBitSize();
    unsigned __int64 mask = (((unsigned __int64)1)<<bitSize)-1;
    unsigned __int64 shiftMask = 0;
    if (bitSize + bitOffset < sizeof(__int64) * 8)
        shiftMask = (((unsigned __int64)1)<<(bitSize+bitOffset));
    shiftMask -= (((unsigned __int64)1)<<bitOffset);
    IValue * oldMaskValue = storageType->castFrom(false, (__int64)shiftMask);
    IHqlExpression * oldMask = createConstant(oldMaskValue);
    IHqlExpression * transColumn = createTranslatedOwned(columnRef);
    IHqlExpression * oldValue = createValue(no_band, transColumn, createValue(no_bnot,oldMask));

    IValue * newMaskValue = storageType->castFrom(false, (__int64)mask);
    IHqlExpression * newMask = createConstant(newMaskValue);
    OwnedHqlExpr newValue = createValue(no_band, LINK(storageType), ensureExprType(value, storageType), newMask);
    if (bitOffset > 0)
        newValue.setown(createValue(no_lshift, LINK(storageType), newValue.getClear(), getSizetConstant(bitOffset)));
    if (newValue->isConstant())
        newValue.setown(foldHqlExpression(translator.queryErrorProcessor(), newValue));
    OwnedHqlExpr final = createValue(no_bor, LINK(storageType), oldValue, newValue.getClear());

    CHqlBoundTarget boundTarget;
    boundTarget.expr.set(columnRef);
    translator.buildExprAssign(ctx, boundTarget, final);
}


//---------------------------------------------------------------------------

void CRowReferenceColumnInfo::gatherSize(SizeStruct & target)
{
    if (isConditional())
        addVariableSize(queryType()->getSize(), target);
    else
        target.addFixed(sizeof(void *));
}


IHqlExpression * CRowReferenceColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    size32_t typeSize = sizeof(void *);
    return getSizetConstant(typeSize);
}


//---------------------------------------------------------------------------

CVirtualColumnInfo::CVirtualColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CVirtualColumnInfo::buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    UNIMPLEMENTED;
}

IHqlExpression * CVirtualColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    return getSizetConstant(0);
}

void CVirtualColumnInfo::gatherSize(SizeStruct & target)
{
    //Zero size
}


bool CVirtualColumnInfo::isFixedSize()
{
    return true;
}


void CVirtualColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    IHqlExpression * virtualAttr = column->queryAttribute(virtualAtom);
    OwnedHqlExpr value = getVirtualReplacement(column, virtualAttr->queryChild(0), selector->queryRootRow()->querySelector());
    OwnedHqlExpr cast = ensureExprType(value, column->queryType()->queryPromotedType());
    translator.buildExpr(ctx, cast, bound);
}

void CVirtualColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    UNIMPLEMENTED;
}

//---------------------------------------------------------------------------


//******* For CSV the offset is the field number, not a byte offset ***********
CCsvColumnInfo::CCsvColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, IAtom * _encoding) : CColumnInfo(_container, _prior, _column)
{
    encoding = _encoding;
}

void CCsvColumnInfo::buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    UNIMPLEMENTED;
}

IHqlExpression * CCsvColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    return getSizetConstant(1);
}


void CCsvColumnInfo::gatherSize(SizeStruct & target)
{
    if (isConditional())
        addVariableSize(1, target);
    else
        target.addFixed(1);
}


bool CCsvColumnInfo::isFixedSize()
{
    return true;
}


void CCsvColumnInfo::getName(HqlCppTranslator & translator, BuildCtx & ctx, StringBuffer & out, const char * prefix, IReferenceSelector * selector)
{
    CHqlBoundExpr boundIndex;
    buildOffset(translator, ctx, selector, boundIndex);

    BoundRow * cursor = selector->queryRootRow();
    out.append(prefix);
    cursor->queryBound()->toString(out);
    out.append("[");
    translator.generateExprCpp(out, boundIndex.expr).append("]");
}


void CCsvColumnInfo::buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)
{
    OwnedHqlExpr value = getColumnExpr(translator, ctx, selector);
    translator.buildExprAssign(ctx, target, value);
}

void CCsvColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr value = getColumnExpr(translator, ctx, selector);
    translator.buildExpr(ctx, value, bound);
}

IHqlExpression * CCsvColumnInfo::getColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    StringBuffer lenText, dataText;
    getName(translator, ctx, lenText, "len", selector);
    getName(translator, ctx, dataText, "data", selector);

    Owned<ITypeInfo> type = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
    ITypeInfo * columnType = column->queryType();
    if (encoding == NULL)
    {
        //This is slightly weird legacy semantics - treat the input as UTF8 if a unicode field is used.
        if (isUnicodeType(columnType))
            type.setown(makeUtf8Type(UNKNOWN_LENGTH, NULL));
    }
    else if (encoding == ebcdicAtom || encoding == asciiAtom)
    {
        type.setown(makeStringType(UNKNOWN_LENGTH, getCharset(encoding), NULL));
    }
    else if (encoding == unicodeAtom)
    {
        type.setown(makeUtf8Type(UNKNOWN_LENGTH, NULL));
    }

    type.setown(makeReferenceModifier(type.getClear()));

    if (isUnicodeType(type))
    {
        //This is an ugly fix to change the size to the number of utf8-characters.
        //Better would be to either perform the mapping (and validation) in the engines, or
        //give it a string of encoding utf8 and extend the code generator to correctly handle those
        //string/unicode conversions by using the codepage to codepage mapping function.
        StringBuffer temp;
        temp.appendf("rtlUtf8Length(%s,%s)", lenText.str(), dataText.str());
        lenText.swapWith(temp);
    }

    OwnedHqlExpr length = createQuoted(lenText.str(), LINK(sizetType));
    OwnedHqlExpr data = createQuoted(dataText.str(), type.getClear());

    OwnedHqlExpr value = createTranslated(data, length);
    if (columnType->getTypeCode() == type_boolean)
    {
        HqlExprArray args;
        args.append(*LINK(value));
        value.setown(translator.bindFunctionCall(csvStr2BoolId,args));
    }
    else
        value.setown(ensureExprType(value, columnType));
    return value.getClear();
}

void CCsvColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    UNIMPLEMENTED;
}

//---------------------------------------------------------------------------

static IIdAtom * getXmlReadFunction(ITypeInfo * type, bool hasDefault)
{
    type = type->queryPromotedType();
    if (hasDefault)
    {
        switch (type->getTypeCode())
        {
        case type_unicode:
        case type_varunicode:
            return columnReadUnicodeXId;
        case type_utf8:
            return columnReadUtf8XId;
        case type_int:
        case type_swapint:
        case type_packedint:
            return columnReadIntId;
        case type_data:
            return columnReadDataXId;
        case type_boolean:
            return columnReadBoolId;
        default:
            return columnReadStringXId;
        }
    }
    else
    {
        switch (type->getTypeCode())
        {
        case type_unicode:
        case type_varunicode:
            return columnGetUnicodeXId;
        case type_utf8:
            return columnGetUtf8XId;
        case type_int:
        case type_swapint:
        case type_packedint:
            return columnGetIntId;
        case type_data:
            return columnGetDataXId;
        case type_boolean:
            return columnGetBoolId;
        default:
            return columnGetStringXId;
        }
    }
}

CXmlColumnInfo::CXmlColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column) : CColumnInfo(_container, _prior, _column)
{
}

void CXmlColumnInfo::buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    UNIMPLEMENTED;
}

IHqlExpression * CXmlColumnInfo::buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    UNIMPLEMENTED;
}


void CXmlColumnInfo::gatherSize(SizeStruct & target)
{
    addVariableSize(0, target);
}


bool CXmlColumnInfo::isFixedSize()
{
    return false;
}


void CXmlColumnInfo::buildFixedStringAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target, IHqlExpression * defaultValue, IIdAtom * func)
{
    StringBuffer row,path;
    selector->queryRootRow()->queryBound()->toString(row);
    getXPath(path);
    translator.noteXpathUsed(path);

    HqlExprArray args;
    args.append(*createQuoted(row, makeBoolType()));
    args.append(*target.getTranslatedExpr());
    args.append(*createConstant(path.str()));
    if (defaultValue)
        args.append(*LINK(defaultValue));
    OwnedHqlExpr call = translator.bindFunctionCall(func, args);
    translator.buildStmt(ctx, call);
}

void CXmlColumnInfo::buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target)
{
    Linked<ITypeInfo> type = queryPhysicalType();
    if (type->getSize() != UNKNOWN_LENGTH)
    {
        IIdAtom * func = NULL;
        IHqlExpression * defaultValue = queryAttributeChild(column, xmlDefaultAtom, 0);
        if (!defaultValue)
            defaultValue = queryAttributeChild(column, defaultAtom, 0);

        switch (type->getTypeCode())
        {
        case type_string:
            if (type->queryCharset()->queryName() == asciiAtom)
                func = defaultValue ? columnReadStringId : columnGetStringId;
            break;
        case type_data:
            func = defaultValue ? columnReadDataId : columnGetDataId;
            break;
        case type_qstring:
            func = defaultValue ? columnReadQStringId : columnGetQStringId;
            break;
        }

        if (func)
        {
            buildFixedStringAssign(translator, ctx, selector, target, defaultValue, func);
            return;
        }
    }

    OwnedHqlExpr call = getCallExpr(translator, ctx, selector);
    translator.buildExprAssign(ctx, target, call);
}

void CXmlColumnInfo::buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound)
{
    OwnedHqlExpr call = getCallExpr(translator, ctx, selector);
    translator.buildExpr(ctx, call, bound);
}

void HqlCppTranslator::buildXmlReadChildrenIterator(BuildCtx & ctx, const char * iterTag, IHqlExpression * rowName, SharedHqlExpr & subRowExpr)
{
    StringBuffer s, iterName, subRowName;
    unique_id_t id = getUniqueId();
    appendUniqueId(iterName.append("iter"), id);
    appendUniqueId(subRowName.append("row"), id);

    //XmlChildIterator iterNNN;
    s.clear().append("XmlChildIterator ").append(iterName).append(";");
    ctx.addQuoted(s);

    //iterNN.init(row->getChildIterator("Name/Row"))
    s.clear().append(iterName).append(".initOwn(");
    generateExprCpp(s, rowName).append("->getChildIterator(");
    s.append("\"");
    if (iterTag)
        appendStringAsCPP(s, strlen(iterTag), iterTag, false);
    s.append("\"));");
    ctx.addQuoted(s);

    //IColumnProvider * rowNNN;
    s.clear().append("IColumnProvider * ").append(subRowName).append(";");
    ctx.addQuoted(s);
    //rowNNN = iterNN.first();
    s.clear().append(subRowName).append(" = ").append(iterName).append(".first();");
    ctx.addQuoted(s);

    s.clear().append(subRowName).append(" = ").append(iterName).append(".next()");
    OwnedHqlExpr nextExpr = createQuoted(s.str(), makeVoidType());

    //<Name><Row><.....fields.....></Row>
    subRowExpr.setown(createQuoted(subRowName, makeBoolType()));

    ctx.addLoop(subRowExpr, nextExpr, false);
}

IHqlExpression * CXmlColumnInfo::getXmlDatasetExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    IHqlExpression * expr = column;
    IHqlExpression * rowName = selector->queryRootRow()->queryBound();
    StringBuffer prefix;
    if (container)
        container->getContainerXPath(prefix);
    
    StringBuffer s;
    StringBuffer fieldTag, rowTag, iterTag;
    extractXmlName(fieldTag, &rowTag, NULL, expr, "Row", true, nullptr);

    iterTag.append(prefix);
    if (fieldTag.length())
        iterTag.append(fieldTag).append("/");
    iterTag.append(rowTag);

    //Create the builder for generating a temporary set.
    IHqlExpression * record = expr->queryRecord();
    Owned<IHqlCppDatasetBuilder> builder;
    builder.setown(translator.createLinkedDatasetBuilder(record));
    builder->buildDeclare(ctx);

    //Generate the code to process a child iterator
    OwnedHqlExpr subRowExpr;
    BuildCtx loopctx(ctx);
    translator.buildXmlReadChildrenIterator(loopctx, iterTag.str(), rowName, subRowExpr);

    BoundRow * targetRow = builder->buildCreateRow(loopctx);

    IHqlExpression * path = selector->queryExpr();
    StringBuffer subPrefix;
    BoundRow * selfCursor = translator.bindSelf(loopctx, path, targetRow->queryBound(), targetRow->queryBuilder());
    translator.bindXmlTableCursor(loopctx, path, subRowExpr, no_none, NULL, false);
    OwnedHqlExpr active = ensureActiveRow(path);
    translator.buildAssign(loopctx, selfCursor->querySelector(), active);
    translator.finishSelf(loopctx, selfCursor, targetRow);
    builder->finishRow(loopctx, targetRow);

    CHqlBoundTarget temp;
    translator.createTempFor(ctx, expr, temp);
    builder->buildFinish(ctx, temp);

    return temp.getTranslatedExpr();
}

IHqlExpression * CXmlColumnInfo::getXmlSetExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    IHqlExpression * expr = column;
    IHqlExpression * rowName = selector->queryRootRow()->queryBound();
    StringBuffer prefix;
    if (container)
        container->getContainerXPath(prefix);
    
    ITypeInfo * elementType = expr->queryType()->queryChildType();
    StringBuffer s;
    StringBuffer fieldTag, itemTag, valueTag, iterTag, fullFieldPath;
    extractXmlName(fieldTag, &itemTag, &valueTag, expr, "Item", true, nullptr);

    fullFieldPath.append(prefix).append(fieldTag);
    iterTag.append(prefix);
    if (fieldTag.length() != 0)
        iterTag.append(fieldTag).append("/");
    iterTag.append(itemTag);
    bool checkForAll = (fieldTag.length() != 0);

    //Create the builder for generating a temporary set.
    CHqlBoundTarget temp;
    translator.createTempFor(ctx, expr, temp);
    Owned<IHqlCppSetBuilder> builder = translator.createTempSetBuilder(elementType, temp.isAll);
    builder->buildDeclare(ctx);

    LinkedHqlExpr defaultValue = queryAttributeChild(column, xmlDefaultAtom, 0);
    if (!defaultValue)
        defaultValue.set(queryAttributeChild(column, defaultAtom, 0));
    bool defaultIsAllValue = defaultValue && (defaultValue->getOperator() == no_all);
    if (checkForAll)
    {
        //assign isAll...
        HqlExprArray isAllArgs;
        isAllArgs.append(*LINK(rowName));
        isAllArgs.append(*createConstant(fullFieldPath.str()));
        if (defaultValue)
            isAllArgs.append(*createConstant(defaultIsAllValue));
        OwnedHqlExpr isAll = translator.bindFunctionCall(defaultValue ? columnReadSetIsAllId : columnGetSetIsAllId, isAllArgs);
        builder->setAll(ctx, isAll);
    }
    else
        builder->setAll(ctx, queryBoolExpr(false));

    //Generate the code to process a child iterator
    OwnedHqlExpr subRowExpr;
    BuildCtx loopctx(ctx);
    translator.buildXmlReadChildrenIterator(loopctx, iterTag.str(), rowName, subRowExpr);

    HqlExprArray args;
    args.append(*LINK(subRowExpr));
    args.append(*createConstant(valueTag.str()));
    OwnedHqlExpr call = translator.bindFunctionCall(getXmlReadFunction(elementType, false), args);

    Owned<IReferenceSelector> elemselector = builder->buildCreateElement(loopctx);
    elemselector->set(loopctx, call);
    builder->finishElement(loopctx);

    builder->buildFinish(ctx, temp);

    return temp.getTranslatedExpr();
}

IHqlExpression * CXmlColumnInfo::getCallExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector)
{
    Linked<ITypeInfo> type = queryPhysicalType();
    IIdAtom * func = NULL;
    IHqlExpression * defaultValue = queryAttributeChild(column, xmlDefaultAtom, 0);
    if (!defaultValue)
        defaultValue = queryAttributeChild(column, defaultAtom, 0);

    switch (type->getTypeCode())
    {
    case type_dictionary:
    case type_table:
    case type_groupedtable:
        return getXmlDatasetExpr(translator, ctx, selector);
    case type_set:
        return getXmlSetExpr(translator, ctx, selector);
    case type_string:
        if ((type->getSize() != UNKNOWN_LENGTH) && type->queryCharset()->queryName() == asciiAtom)
            func = defaultValue ? columnReadStringId : columnGetStringId;
        break;
    case type_data:
        if (type->getSize() != UNKNOWN_LENGTH)
            func = defaultValue ? columnReadDataId : columnGetDataId;
        break;
    case type_qstring:
        if (type->getSize() != UNKNOWN_LENGTH)
            func = defaultValue ? columnReadQStringId : columnGetQStringId;
        break;
    }

    if (func)
    {
        CHqlBoundTarget tempTarget;
        translator.createTempFor(ctx, type, tempTarget, typemod_none, FormatNatural);
        buildFixedStringAssign(translator, ctx, selector, tempTarget, defaultValue, func);
        return tempTarget.getTranslatedExpr();
    }

    StringBuffer row,path;
    selector->queryRootRow()->queryBound()->toString(row);
    getXPath(path);
    translator.noteXpathUsed(path);

    HqlExprArray args;
    args.append(*createQuoted(row, makeBoolType()));
    args.append(*createConstant(path.str()));
    if (defaultValue)
        args.append(*LINK(defaultValue));
    OwnedHqlExpr call = translator.bindFunctionCall(getXmlReadFunction(type, defaultValue != NULL), args);
    return ensureExprType(call, type);
}

void CXmlColumnInfo::setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value)
{
    UNIMPLEMENTED;
}

//---------------------------------------------------------------------------

inline int doAlign(unsigned value, unsigned align) { return (value + align-1) & ~(align-1); }

ColumnToOffsetMap::ColumnToOffsetMap(IHqlExpression * _key, IHqlExpression * _record, unsigned _id, unsigned _packing, unsigned _maxRecordSize, bool _translateVirtuals, bool _useAccessClass)
: root(NULL, NULL, _record), key(_key), id(_id)
{ 
    record = _record;
    prior = NULL;
    maxAlign = _packing; 
    packing = _packing; 
    fixedSizeRecord = true;
    translateVirtuals = _translateVirtuals;
    defaultMaxRecordSize = _maxRecordSize;
    containsIfBlock = false;
    cachedDefaultMaxSizeUsed = false;
    cachedMaxSize = UNKNOWN_LENGTH;
    root.setOffset(false);
    if (_useAccessClass)
        root.setUseAccessClass();
}

void ColumnToOffsetMap::init(RecordOffsetMap & map)
{
    expandRecord(record, &root, map);
}


void ColumnToOffsetMap::completeActiveBitfields()
{
    if (prior)
        prior->noteLastBitfield();
    packer.reset();
}

CMemberInfo * ColumnToOffsetMap::addColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map)
{
    CMemberInfo * created = NULL;
    switch (column->getOperator())
    {
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        if (column->queryName() == packingAtom)
            packing = (unsigned)column->queryChild(0)->queryValue()->getIntValue();
        break;
    case no_record:
        completeActiveBitfields();
        return expandRecord(column, container, map);
    case no_ifblock:
        {
            completeActiveBitfields();
            CIfBlockInfo * next = new CIfBlockInfo(container, prior, column);
            next->setOffset(!fixedSizeRecord);
            expandRecord(column->queryChild(1), next, map);
            created = next;
            containsIfBlock = true;
        }
        break;
    default:
        {
            if (column->queryType()->getTypeCode() != type_bitfield)
                completeActiveBitfields();

            if (translateVirtuals && column->hasAttribute(virtualAtom) && (column->queryType()->getSize() != UNKNOWN_LENGTH))
                created = new CVirtualColumnInfo(container, prior, column);
            else
                created = createColumn(container, column, map);
            break;
        }
    }

    if (created)
    {
        container->addChild(created);
        prior = created;
        if (!created->isFixedSize())
            fixedSizeRecord = false;
    }

    return created;
}


bool ColumnToOffsetMap::buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper)
{
    //prepare() allows Ifblock and a count/size on a dataset to tag the fields they depend on which need to be read.
    //The fallback (implemented in base class) is to call deserialize on a temporary row instead.  Ugly, but better than failing.
    ReadAheadState state(helper);
    if (queryRootColumn()->prepareReadAhead(translator, state))
    {
        state.addDummyMappings();
        return queryRootColumn()->buildReadAhead(translator, ctx, state);
    }
    return false;
}


void ColumnToOffsetMap::buildAccessor(StringBuffer & accessorName, HqlCppTranslator & translator, BuildCtx & declarectx, IHqlExpression * selector)
{
    StringBuffer typeName;
    translator.buildRtlType(typeName, record->queryType());

    BuildCtx ctx(declarectx);
    ctx.setNextPriority(TypeInfoPrio);
    ctx.addGroup();

    StringBuffer s;
    s.clear().append("RtlRecord rec").append(queryId()).append("(").append(typeName).append(",true);");
    ctx.addQuoted(s);

    if (translator.queryOptions().spanMultipleCpp)
    {
        ctx.set(mainprototypesAtom);
        ctx.addQuoted(s.clear().append("extern RtlRecord rec").append(queryId()).append(";"));
    }

    BuildCtx classctx(ctx);
    unsigned numVarOffsets = getVarSizeFieldCount(record, true);
    accessorName.append("access").append(queryId());
    s.clear().append("struct ").append(accessorName).append(" : public RtlStaticRow<").append(numVarOffsets).append(">");
    classctx.addQuotedCompound(s.str(), ";");

    s.clear().append(accessorName).append("(const void * _row) : RtlStaticRow<").append(numVarOffsets).append(">(rec").append(queryId()).append(", _row) {}");
    classctx.addQuoted(s.str());
}


CMemberInfo * ColumnToOffsetMap::createColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map)
{
    ITypeInfo * type = column->queryType();
    ITypeInfo * promoted = type->queryPromotedType();
    unsigned align = promoted ? promoted->getAlignment() : 1;
    if (align > packing)
        align = packing;
    if (align > maxAlign)
        maxAlign = align;

    bool isFixedOffset = !fixedSizeRecord;
    CMemberInfo * created = NULL;
    switch (type->getTypeCode())
    {
    //MORE: Alien types could be based on bitfields.  If so need a column as a child
    //of the alien column representing the stored value.....
    case type_bitfield:
        {
            CBitfieldContainerInfo * bitContainer;

            unsigned thisBitOffset, thisBits;
            if (!packer.checkSpaceAvailable(thisBitOffset, thisBits, type))
            {
                if (prior)
                    prior->noteLastBitfield();
                ITypeInfo * storeType = type->queryChildType();
                //Use createUniqueId() to ensure that bitfield containers of the same type can be distinguished.
                OwnedHqlExpr value = createValue(no_field, LINK(storeType), createUniqueId());
                bitContainer = new CBitfieldContainerInfo(container, prior, value);
                bitContainer->setOffset(!fixedSizeRecord);
            }
            else
                bitContainer = (CBitfieldContainerInfo *)prior;

            CBitfieldInfo & entry = * new CBitfieldInfo(bitContainer, bitContainer->lastBitfield(), column);
            entry.setBitOffset(thisBitOffset);
            bitContainer->addChild(&entry);
            if (bitContainer != prior)
                created = bitContainer;
            break;
        }
    case type_row:
        {
            if (hasReferenceModifier(type))
                created = new CRowReferenceColumnInfo(container, prior, column);
            else
            {
                CRecordInfo * next = new CRecordInfo(container, prior, column);
                expandRecord(column->queryRecord(), next, map);
                created = next;
            }
            break;
        }
    case type_groupedtable:
    case type_table:
    case type_dictionary:
        {
            IHqlExpression * count = NULL;
            IHqlExpression * size = NULL;

            ForEachChild(i, column)
            {
                IHqlExpression * cur = column->queryChild(i);
                if (cur->isAttribute())
                {
                    IAtom * name = cur->queryName();
                    if (name == countAtom)
                        count = cur->queryChild(0);
                    else if (name == sizeofAtom)
                        size = cur->queryChild(0);
                }
            }

            if (column->hasAttribute(_linkCounted_Atom))
                created = new CChildLinkedDatasetColumnInfo(container, prior, column, map, defaultMaxRecordSize);
            else if (count || size)
                created = new CChildLimitedDatasetColumnInfo(container, prior, column, map, defaultMaxRecordSize);
            else
                created = new CChildDatasetColumnInfo(container, prior, column, map, defaultMaxRecordSize);
            break;
        }
    case type_string:
    case type_data:
    case type_unicode:
    case type_qstring:
    case type_utf8:
        if (type->getSize() == UNKNOWN_LENGTH)
            created = new CSpecialStringColumnInfo(container, prior, column);
        else
            created = new CColumnInfo(container, prior, column);
        break;
    case type_varstring:
    case type_varunicode:
        if (type->getSize() == UNKNOWN_LENGTH)
            created = new CSpecialVStringColumnInfo(container, prior, column);
        else
            created = new CColumnInfo(container, prior, column);
        break;
    case type_int:
    case type_swapint:
        switch (type->getSize())
        {
        case 1: case 2: case 4: case 8:
            created = new CColumnInfo(container, prior, column);
            break;
        default:
            created = new CSpecialIntColumnInfo(container, prior, column);
            break;
        }
        break;
    case type_packedint:
        created = new CPackedIntColumnInfo(container, prior, column);
        break;
    case type_set:
        created = new CChildSetColumnInfo(container, prior, column);
        break;
    default:
        {
            if (type->getTypeCode() == type_alien)
                created = new CAlienColumnInfo(container, prior, column);
            else
                created = new CColumnInfo(container, prior, column);
            break;
        }
    }
    if (created)
        created->setOffset(isFixedOffset);
    return created;
}


unsigned ColumnToOffsetMap::getFixedRecordSize()
{
    SizeStruct size;
    root.getSizeExpr(size);
    assertex(size.isFixedSize());
    return size.getFixedSize();
}

AColumnInfo * ColumnToOffsetMap::queryRootColumn()
{
    return &root;
}


void ColumnToOffsetMap::ensureMaxSizeCached()
{
    if (cachedMaxSize == UNKNOWN_LENGTH)
    {
        bool isKnownSize;
        cachedMaxSize = getMaxRecordSize(record, defaultMaxRecordSize, isKnownSize, cachedDefaultMaxSizeUsed);
    }
}

unsigned ColumnToOffsetMap::getMaxSize()
{
    ensureMaxSizeCached();
    return cachedMaxSize;
}

bool ColumnToOffsetMap::isMaxSizeSpecified()
{
    if (isFixedWidth())
        return true;

    ensureMaxSizeCached();
    return !cachedDefaultMaxSizeUsed;
}

CMemberInfo * ColumnToOffsetMap::expandRecord(IHqlExpression * record, CContainerInfo * container, RecordOffsetMap & map)
{
    assertex(record->getOperator() == no_record);
    unsigned max = record->numChildren();
    unsigned idx;
    bool fixedSize = true;
    prior = NULL;
    for (idx = 0; idx < max; idx++)
    {
        CMemberInfo * created = addColumn(container, record->queryChild(idx), map);
        if (created)
        {
            if (!created->isFixedSize())
                fixedSize = false;
        }
    }
    container->setFixedSize(fixedSize);
    completeActiveBitfields();
    return prior;
}


DynamicColumnToOffsetMap::DynamicColumnToOffsetMap(unsigned _maxRecordSize) : ColumnToOffsetMap(NULL, queryNullRecord(), 0, 0, _maxRecordSize, false, false)
{
    root.setDynamic();
}

void DynamicColumnToOffsetMap::addColumn(IHqlExpression * column, RecordOffsetMap & map)
{
    CMemberInfo * created = ColumnToOffsetMap::addColumn(&root, column, map);
    if (created)
    {
        if (!created->isFixedSize())
            root.setFixedSize(false);
    }
}



//---------------------------------------------------------------------------

static bool cachedCanReadFromCsv(IHqlExpression * record)
{
    record = record->queryBody();
    if (record->queryTransformExtra())
        return true;
    record->setTransformExtra(record);
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_ifblock:
            if (!cachedCanReadFromCsv(cur->queryChild(1)))
                return false;
            break;
        case no_record:
            if (!cachedCanReadFromCsv(cur))
                return false;
            break;
        case no_field:
            {
                ITypeInfo * type = cur->queryType();
                switch (type->getTypeCode())
                {
                case type_row:
                    if (!cachedCanReadFromCsv(cur->queryRecord()))
                        return false;
                    break;
                case type_dictionary:
                case type_table:
                case type_groupedtable:
                case type_set:
                    return false;
                }
                break;
            }
        }
    }
    return true;
}

bool canReadFromCsv(IHqlExpression * record)
{
    TransformMutexBlock block;
    return cachedCanReadFromCsv(record);
}


CsvColumnToOffsetMap::CsvColumnToOffsetMap(IHqlExpression * _record, unsigned _maxRecordSize, bool _translateVirtuals, IAtom * _encoding)
: ColumnToOffsetMap(NULL, _record, 0, 1, _maxRecordSize, _translateVirtuals, false)
{
    encoding = _encoding;
}

CMemberInfo * CsvColumnToOffsetMap::createColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map)
{
    CMemberInfo * created = NULL;

    ITypeInfo * type = column->queryType();
    switch (type->getTypeCode())
    {
    case type_row:
        {
            Owned<CRecordInfo> next = new CRecordInfo(container, prior, column);
            expandRecord(column->queryRecord(), next, map);
            created = next.getClear();
            break;
        }
    case type_table:
    case type_groupedtable:
    case type_set:
        throwError(HQLERR_CsvNotSupportTableSet);
        break;
    default:
        created = new CCsvColumnInfo(container, prior, column, encoding);
        break;
    }
    return created;
}

//---------------------------------------------------------------------------

XmlColumnToOffsetMap::XmlColumnToOffsetMap(IHqlExpression * _record, unsigned _maxRecordSize, bool _translateVirtuals)
: ColumnToOffsetMap(NULL, _record, 0, 1, _maxRecordSize, _translateVirtuals, false)
{
}

CMemberInfo * XmlColumnToOffsetMap::createColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map)
{
    CMemberInfo * created = NULL;
    ITypeInfo * type = column->queryType();
    switch (type->getTypeCode())
    {
    case type_row:
        {
            CRecordInfo * next = new CRecordInfo(container, prior, column);
            expandRecord(column->queryRecord(), next, map);
            created = next;
            break;
        }
    default:
        created = new CXmlColumnInfo(container, prior, column);
        break;
    }

    return created;
}

//---------------------------------------------------------------------------

BoundRow::BoundRow(const BoundRow & other, IHqlExpression * _newBound) : HqlExprAssociation(other.represents)
{
    dataset.set(other.dataset);
    bound.set(_newBound ? _newBound : other.bound.get());
    columnMap = LINK(other.columnMap);
    conditional = other.conditional;
    side = other.side;
    kind = other.kind;
    assertex(bound->queryType()->getTypeCode() != type_void);
    resultAlias = false;
    inherited = other.inherited;
    assertex(!other.accessor); // not currently supported in child queries.
    accessor.set(other.accessor);
    accessorStmt = other.accessorStmt;
}

BoundRow::BoundRow(const BoundRow & other, ColumnToOffsetMap * rawMap, IHqlExpression * _newBound) : HqlExprAssociation(other.represents)
{
    dataset.set(other.dataset);
    bound.set(_newBound ? _newBound : other.bound.get());
    columnMap = LINK(rawMap);
    conditional = other.conditional;
    side = other.side;
    kind = other.kind;
    assertex(bound->queryType()->getTypeCode() != type_void);
    resultAlias = false;
    inherited = other.inherited;
    assertex(!rawMap->usesAccessor());
}


BoundRow::BoundRow(IHqlExpression * _dataset, IHqlExpression * _bound, IHqlExpression * _accessor, ColumnToOffsetMap * _columnMap) : HqlExprAssociation(_dataset)
{
    assertex(_columnMap);
    dataset.set(_dataset);
    bound.set(_bound);
    accessor.set(_accessor);
    assertex(!accessor);
    columnMap = LINK(_columnMap);
    conditional = false;
    side = no_none;
    kind = AssocRow;
    assertex(bound->queryType()->getTypeCode() != type_void);
    resultAlias = false;
    inherited = false;
    assertex(!accessor == !columnMap->usesAccessor());
}


BoundRow::BoundRow(IHqlExpression * _dataset, IHqlExpression * _bound, IHqlExpression * _accessor, ColumnToOffsetMap * _columnMap, node_operator _side, IHqlExpression * selSeq) : HqlExprAssociation(NULL)
{
    assertex(_columnMap);
    dataset.set(_dataset);
    bound.set(_bound);
    accessor.set(_accessor);
    columnMap = LINK(_columnMap);
    conditional = false;
    kind = AssocCursor;
    side = _side;
    if (side == no_none)
        represents.set(_dataset->queryNormalizedSelector());
    else if ((side != no_self) || selSeq)
        represents.setown(createSelector(side, dataset, selSeq));
    else
    {
        OwnedHqlExpr uid = createUniqueSelectorSequence();
        represents.setown(createSelector(no_self, dataset, uid));
    }
    assertex(bound->queryType()->getTypeCode() != type_void);
    resultAlias = false;
    inherited = false;
//    assertex(!columnMap->usesAccessor());
}

BoundRow::~BoundRow()
{
    ::Release(columnMap);
}


/* In: not linked. Return: linked */
IHqlExpression * BoundRow::bindToRow(IHqlExpression * expr, IHqlExpression * exprSelector)
{
    if (kind == AssocCursor)
    {
        OwnedHqlExpr replacement = ensureActiveRow(represents);
        return replaceSelector(expr, exprSelector, replacement);
    }
    OwnedHqlExpr wrapped = createRow(no_newrow, LINK(represents));
    return replaceSelector(expr, exprSelector, wrapped);
}

IHqlExpression * BoundRow::getMappedSelector(BuildCtx & ctx, IReferenceSelector * selector)
{
    return NULL;
}

IHqlExpression * BoundRow::getFinalFixedSizeExpr()      
{ 
    return getSizetConstant(columnMap->getTotalFixedSize());
}

bool BoundRow::isBinary()
{
    return (columnMap->getFormat() == MapFormatBinary);
}

IHqlExpression * BoundRow::queryBuilderEnsureMarker()
{
    if (!builderEnsureMarker)
        builderEnsureMarker.setown(createAttribute(ensureCapacityAtom, LINK(represents)));
    return builderEnsureMarker;
}

void BoundRow::prepareAccessor(HqlCppTranslator & translator, BuildCtx & ctx)
{
    translator.buildRowAccessor(columnMap);
    StringBuffer className;
    className.append("access").append(columnMap->queryId());
    accessor.setown(createVariable(makeConstantModifier(makeClassType(className))));

    OwnedHqlExpr rowPointer = getPointer(bound);
    accessorStmt = ctx.addDeclare(accessor, rowPointer);
    accessorStmt->setIncluded(false);
}

IHqlExpression * BoundRow::ensureAccessor(HqlCppTranslator & translator, BuildCtx & ctx)
{
    accessorStmt->setIncluded(true);
    return accessor;
}

AColumnInfo * BoundRow::queryRootColumn()   
{ 
    return columnMap->queryRootColumn(); 
}

unsigned BoundRow::getMaxSize()                 
{ 
    return columnMap->getMaxSize(); 
}

//---------------------------------------------------------------------------

NonLocalIndirectRow::NonLocalIndirectRow(const BoundRow & other, ColumnToOffsetMap * rawMap, SerializationRow * _serialization) : BoundRow(other, rawMap, nullptr)
{
    serialization = _serialization;
}

IHqlExpression * NonLocalIndirectRow::getMappedSelector(BuildCtx & ctx, IReferenceSelector * selector)
{
    return serialization->ensureSerialized(ctx, NULL, selector);
}

//---------------------------------------------------------------------------


SerializationRow::SerializationRow(HqlCppTranslator & _translator, IHqlExpression * _dataset, IHqlExpression * _bound, DynamicColumnToOffsetMap * _columnMap, ActivityInstance * _activity) : BoundRow(_dataset, _bound, NULL, _columnMap, no_none, NULL), translator(_translator)
{
    serializedMap = _columnMap;
    extractBuilder = NULL;
    finalFixedSizeExpr.setown(createUnknown(no_callback, LINK(sizetType), sizeAtom, new DelayedSizeGenerator(serializedMap)));
    activity = _activity;
}

IHqlExpression * SerializationRow::ensureSerialized(BuildCtx & ctx, IHqlExpression * colocal, IReferenceSelector * selector)
{
    return ensureSerialized(selector->queryExpr(), colocal, selector->isConditional());
}

IHqlExpression * SerializationRow::ensureSerialized(IHqlExpression * path, IHqlExpression * colocal, bool isConditional)
{
    SharedHqlExpr * mapped = mapping.getValue(path);
    if (mapped)
        return LINK(mapped->get());

    ITypeInfo * pathType = path->queryType();
    Owned<ITypeInfo> unqualifiedType = getFullyUnqualifiedType(pathType);
    Owned<ITypeInfo> serializeType = cloneEssentialFieldModifiers(pathType, unqualifiedType);
    if (colocal && path->isDataset() && 
        (hasLinkedRow(pathType) || hasOutOfLineModifier(pathType)))
        serializeType.setown(setLinkCountedAttr(serializeType, true));
    return addSerializedValue(path, serializeType, colocal, isConditional);
}

IHqlExpression * SerializationRow::addSerializedValue(IHqlExpression * path, ITypeInfo * type, IHqlExpression * colocal, bool isConditional)
{
    IIdAtom * id = NULL;
    if (path->getOperator() == no_select)
        id = path->queryChild(1)->queryId();
    Owned<ITypeInfo> newType = getSimplifiedType(type, isConditional, (colocal == NULL), internalAtom);
    OwnedHqlExpr newSelect = createField(id, newType);

    OwnedHqlExpr deserialized;
    if (colocal)
    {
        extractBuilder->buildAssign(newSelect, path);
        deserialized.set(newSelect);
    }
    else
    {
        OwnedHqlExpr srcValue = ::ensureSerialized(path, internalAtom);
        extractBuilder->buildAssign(newSelect, srcValue);

        Linked<ITypeInfo> evaluateType = type;
        if (evaluateType->getTypeCode() == type_dictionary)
            evaluateType.setown(setLinkCountedAttr(evaluateType, true));

        deserialized.setown(ensureDeserialized(newSelect, evaluateType, internalAtom));
        if (deserialized != newSelect)
            deserialized.setown(createAlias(deserialized, NULL));           // force it to be evaluated once per start
    }
    mapping.setValue(path, deserialized);

    return LINK(deserialized);
}

IHqlExpression * SerializationRow::createField(IIdAtom * id, ITypeInfo * type)
{
    if (!id)
    {
        StringBuffer fieldName;
        fieldName.append("__f").append(numFields()).append("__");
        id = createIdAtom(fieldName);
    }
    IHqlExpression * attr = hasLinkCountedModifier(type) ? getLinkCountedAttr() : NULL;
    OwnedHqlExpr newField = ::createField(id, LINK(type), attr, NULL);
    if (serializedMap->queryRootColumn()->lookupColumn(newField))
        return createField(NULL, type);                     // name clash -> create a new unnamed field

    queryRecord()->addOperand(LINK(newField));
    serializedMap->addColumn(newField, translator.queryRecordMap());
    return createSelectExpr(LINK(querySelector()), LINK(newField));
}

SerializationRow * SerializationRow::create(HqlCppTranslator & _translator, IHqlExpression * _bound, ActivityInstance * activity)
{
    OwnedHqlExpr id = createDataset(no_anon, LINK(queryNullRecord()), createAttribute(serializationAtom, createUniqueId()));
    Owned<DynamicColumnToOffsetMap> map = new DynamicColumnToOffsetMap(_translator.queryOptions().maxRecordSize);           //NB: This is not cached, but it may be shared...
    return new SerializationRow(_translator, id, _bound, map, activity);
}

BoundRow * SerializationRow::clone(IHqlExpression * _newBound)
{
    return new SerializationRow(translator, dataset, _newBound, serializedMap, activity);
}

size32_t SerializationRow::getTotalMinimumSize() const
{
    return serializedMap->getTotalMinimumSize();
}

bool SerializationRow::isFixedSize() const
{
    return serializedMap->isFixedWidth();
}

unsigned SerializationRow::numFields() const
{
    return serializedMap->numRootFields();
}

IHqlExpression * SerializationRow::queryRecord()
{
    if (!record)
        record.setown(createRecord());
    return record;
}

void SerializationRow::finalize()
{
    if (record)
        record.setown(record.getClear()->closeExpr());
}
