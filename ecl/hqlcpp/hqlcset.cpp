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
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "javahash.hpp"
#include "eclhelper.hpp"

#include "hqlfunc.hpp"

#include "hqlattr.hpp"
#include "hqlhtcpp.ipp"
#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlpmap.hpp"
#include "hqlthql.hpp"
#include "hqlcset.ipp"
#include "hqlfold.hpp"
#include "hqltcppc.ipp"
#include "hqlutil.hpp"
#include "hqliter.ipp"

#ifdef CREATE_DEAULT_ROW_IF_NULL
#define CREATE_DEAULT_ROW_IF_NULL_VALUE 1
#else
#define CREATE_DEAULT_ROW_IF_NULL_VALUE 0
#endif

//===========================================================================

IHqlExpression * getOutOfRangeValue(IHqlExpression * indexExpr)
{
    IHqlExpression * dft = indexExpr->queryProperty(defaultAtom);
    if (dft)
        return LINK(dft->queryChild(0));
    else
        return createNullExpr(indexExpr);
}

//===========================================================================

BaseDatasetCursor::BaseDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr * _boundDs) : translator(_translator)
{
    ds.set(_ds);
    record.set(ds->queryRecord());
    if (_boundDs)
        boundDs.set(*_boundDs);
}

BoundRow * BaseDatasetCursor::buildIterateLoop(BuildCtx & ctx, bool needToBreak)
{
    StringBuffer iterName;
    buildIterateClass(ctx, iterName, NULL);

    StringBuffer s, rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);

    //row = iter.first()
    s.clear().append(rowName).append(" = ").append(iterName).append(".first();");
    ctx.addQuoted(s);

    //while (row)
    ctx.addLoop(row, NULL, false);
    BoundRow * cursor = translator.bindTableCursor(ctx, ds, row);

    // row = iter.next();
    ctx.setNextDestructor();
    s.clear().append(rowName).append(" = (byte *)").append(iterName).append(".next();");
    ctx.addQuoted(s);
    return cursor;
}

void BaseDatasetCursor::buildIterateClass(BuildCtx & ctx, SharedHqlExpr & iter, SharedHqlExpr & row)
{
    StringBuffer cursorName, rowName;
    buildIterateClass(ctx, cursorName, NULL);

    iter.setown(createVariable(cursorName.str(), makeBoolType()));
    row.setown(createRow(ctx, "row", rowName));
}

void BaseDatasetCursor::buildIterateMembers(BuildCtx & declarectx, BuildCtx & initctx)
{
    StringBuffer iterName;
    buildIterateClass(declarectx, iterName, &initctx);

    StringBuffer s, rowName;
    OwnedHqlExpr row = createRow(declarectx, "row", rowName);

    //row = iter.first()
    BuildCtx firstctx(declarectx);
    firstctx.addQuotedCompound("virtual bool first()");
    s.clear().append(rowName).append(" = (byte *)").append(iterName).append(".first();");
    firstctx.addQuoted(s);
    s.clear().append("return ").append(rowName).append(" != NULL;");
    firstctx.addQuoted(s);

    //row = iter.first()
    BuildCtx nextctx(declarectx);
    nextctx.addQuotedCompound("virtual bool next()");
    s.clear().append(rowName).append(" = (byte *)").append(iterName).append(".next();");
    nextctx.addQuoted(s);
    s.clear().append("return ").append(rowName).append(" != NULL;");
    nextctx.addQuoted(s);

    //iterate
    translator.bindTableCursor(declarectx, ds, row);
}

BoundRow * BaseDatasetCursor::buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr)
{
    //MORE: Check if the cursor already exists....

    StringBuffer cursorName;
    buildIterateClass(ctx, cursorName, NULL);

    //create a unique dataset and associate it with a call to select
    //set value to be the field selection from the dataset
    StringBuffer s, rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);

    CHqlBoundExpr boundIndex;
    OwnedHqlExpr index = adjustIndexBaseToZero(indexExpr->queryChild(1));
    translator.buildExpr(ctx, index, boundIndex);

    //MORE: CREATE_DEAULT_ROW_IF_NULL - pass the default row to the select() function.
    //row = iter.select(n)
    s.clear().append(rowName).append(" = (byte *)").append(cursorName).append(".select(");
    translator.generateExprCpp(s, boundIndex.expr);
    s.append(");");
    ctx.addQuoted(s);

    bool conditional = !indexExpr->hasProperty(noBoundCheckAtom);
#ifdef CREATE_DEAULT_ROW_IF_NULL
    if (conditional)
    {
        CHqlBoundExpr boundCleared;
        translator.buildDefaultRow(ctx, ds, boundCleared);
        OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);

        BuildCtx subctx(ctx);
        OwnedHqlExpr test = createValue(no_not, makeBoolType(), LINK(row));
        subctx.addFilter(test);
        subctx.addAssign(row, defaultRowPtr);
        conditional = false;
    }
#endif

    BoundRow * cursor = translator.bindRow(ctx, indexExpr, row);
    cursor->setConditional(conditional);
    return cursor;
}


IHqlExpression * BaseDatasetCursor::createRow(BuildCtx & ctx, const char * prefix, StringBuffer & rowName)
{
    translator.getUniqueId(rowName.append(prefix));
    OwnedITypeInfo type;
    if (boundDs.expr && boundDs.expr->queryRecord())
        type.setown(makeConstantModifier(makeRowReferenceType(boundDs)));
    else
        type.setown(makeConstantModifier(makeRowReferenceType(ds)));
    OwnedHqlExpr row = createVariable(rowName, type.getClear());
    ctx.addDeclare(row);
    return row.getClear();
}

//---------------------------------------------------------------------------

BlockDatasetCursor::BlockDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs) : BaseDatasetCursor(_translator, _ds, &_boundDs)
{
    boundDs.set(_boundDs);
    assertex(boundDs.expr->isDatarow() || !isArrayRowset(boundDs.expr->queryType()));       // I don't think this can ever be called at the moment
}

void BlockDatasetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.setown(translator.getBoundCount(boundDs));
}

void BlockDatasetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    if (boundDs.count)
        tgt.expr.setown(createValue(no_ne, makeBoolType(), LINK(boundDs.count), getZero()));
    else
        tgt.expr.setown(createValue(no_ne, makeBoolType(), LINK(boundDs.length), getZero()));
}

void BlockDatasetCursor::buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx)
{
    translator.getUniqueId(cursorName.append("iter"));

    StringBuffer extraParams;
    StringBuffer decl,args;
    if (translator.isFixedRecordSize(record))
    {
        //RtlFixedDatasetCursor cursor(len, data, size)
        decl.append("RtlFixedDatasetCursor");
        extraParams.append(", ").append(translator.getFixedRecordSize(record));
    }
    else
    {
        //RtlVariableDatasetCursor cursor(len, data, recordSize)
        decl.append("RtlVariableDatasetCursor");
        translator.buildMetaForRecord(extraParams.append(", "), record);
    }

    OwnedHqlExpr size = translator.getBoundSize(boundDs);
    decl.append(" ").append(cursorName);
    translator.generateExprCpp(args, size);
    args.append(", ");
    translator.generateExprCpp(args, boundDs.expr);
    args.append(extraParams);

    if (initctx)
    {
        StringBuffer s;
        s.append(cursorName).append(".init(").append(args).append(");");
        initctx->addQuoted(s);
    }
    else
    {
        decl.append("(").append(args).append(")");
    }
    decl.append(";");
    ctx.addQuoted(decl);
}

//---------------------------------------------------------------------------

bool isEmptyDataset(const CHqlBoundExpr & bound)
{
    IValue * value = NULL;
    if (bound.length)
        value = bound.length->queryValue();
    else if (bound.count)
        value = bound.count->queryValue();
    return (value && value->getIntValue() == 0);
}

InlineBlockDatasetCursor::InlineBlockDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs) : BlockDatasetCursor(_translator, _ds, _boundDs)
{
}

BoundRow * InlineBlockDatasetCursor::buildIterateLoop(BuildCtx & ctx, bool needToBreak)
{
    StringBuffer rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);
    if (isEmptyDataset(boundDs))
    {
        ctx.addFilter(queryBoolExpr(false));
        return translator.bindTableCursor(ctx, ds, row);
    }

    StringBuffer s;

    //row = ds;
    OwnedHqlExpr address = getPointer(boundDs.expr);
    OwnedHqlExpr cast = createValue(no_implicitcast, row->getType(), LINK(address));
    ctx.addAssign(row, cast);

    OwnedHqlExpr test;
    if (boundDs.length)
    {
        OwnedHqlExpr length = translator.getBoundLength(boundDs);
        StringBuffer endName;
        OwnedHqlExpr end = createRow(ctx, "end", endName);

        //end = row+length;
        s.clear().append(endName).append(" = ").append(rowName).append("+");
        translator.generateExprCpp(s, length).append(";");
        ctx.addQuoted(s);

        //while (row < end)
        test.setown(createValue(no_lt, makeBoolType(), LINK(row), LINK(end)));
    }
    else if (matchesConstantValue(boundDs.count, 1) && !needToBreak)
    {
        //Optimize count=1, needToBreak = false;
        ctx.addGroup();
        return translator.bindTableCursor(ctx, ds, row);
    }
    else
    {
        OwnedHqlExpr count = translator.getBoundCount(boundDs);

        //count = <n>
        OwnedHqlExpr counter = ctx.getTempDeclare(unsignedType, count);

        //while (count--)
        test.setown(createValue(no_postdec, LINK(counter)));
    }

    ctx.addLoop(test, NULL, false);
    BoundRow * cursor = translator.bindTableCursor(ctx, ds, row);

    //row = row + recordSize
    OwnedHqlExpr size = translator.getRecordSize(cursor->querySelector());
    CHqlBoundExpr boundSize;
    translator.buildExpr(ctx, size, boundSize);

    ctx.setNextDestructor();
    if (translator.queryOptions().optimizeIncrement)
    {
        ctx.addAssignIncrement(row, boundSize.expr);
    }
    else
    {
        OwnedHqlExpr inc = createValue(no_add, row->getType(), LINK(row), LINK(boundSize.expr));
        ctx.addAssign(row, inc);
    }

    return cursor;
}


BoundRow * InlineBlockDatasetCursor::buildSelectFirst(BuildCtx & ctx, IHqlExpression * indexExpr, bool createDefaultRowIfNull)
{
    StringBuffer s, rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);

    BuildCtx subctx(ctx);
    bool conditional = !indexExpr->hasProperty(noBoundCheckAtom);
    if (conditional)
    {
        HqlExprAttr test;
        if (boundDs.count)
        {
            IValue * countValue = boundDs.count->queryValue();
            if (countValue)
            {
                if (countValue->getIntValue() == 0)
                    return NULL;
            }
            else
            {
                OwnedHqlExpr max = createTranslated(boundDs.count);
                test.setown(createCompare(no_ne, max, queryZero()));
            }
        }
        else
        {
            OwnedHqlExpr max = createTranslated(boundDs.length);
            test.setown(createCompare(no_gt, max, queryZero()));
        }
        if (test)
        {
            CHqlBoundExpr boundCleared;
            if (createDefaultRowIfNull)
            {
                translator.buildDefaultRow(ctx, ds, boundCleared);
                conditional = false;
            }
            else
                translator.buildNullRow(ctx, ds, boundCleared);

            OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);
            ctx.addAssign(row, defaultRowPtr);

            translator.buildFilter(subctx, test);
        }
        else
            conditional = false;
    }

    if (isArrayRowset(boundDs.expr->queryType()))
    {
        s.clear().append(rowName).append(" = ");
        translator.generateExprCpp(s, boundDs.expr).append("[0];");
        subctx.addQuoted(s);
    }
    else
    {
        OwnedHqlExpr address = getPointer(boundDs.expr);
        s.clear().append(rowName).append(" = (byte *)(void *)");        // more: should really be const...
        translator.generateExprCpp(s, address);
        s.append(";");
        subctx.addQuoted(s);
    }

    BoundRow * cursor = translator.bindRow(ctx, indexExpr, row);
    cursor->setConditional(conditional);
    return cursor;
}


BoundRow * InlineBlockDatasetCursor::buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr)
{
    assertex(!isArrayRowset(boundDs.expr->queryType()));        // I don't think this can ever be called at the moment

    OwnedHqlExpr index = foldHqlExpression(indexExpr->queryChild(1));
    if (!translator.isFixedRecordSize(record))
    {
        if (matchesConstantValue(index, 1))
            return buildSelectFirst(ctx, indexExpr, CREATE_DEAULT_ROW_IF_NULL_VALUE);
        return BlockDatasetCursor::buildSelect(ctx, indexExpr);
    }
    if (matchesConstantValue(index, 1))
        return buildSelectFirst(ctx, indexExpr, CREATE_DEAULT_ROW_IF_NULL_VALUE);

    //row = NULL
    StringBuffer s, rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);

    //if (index > 0 && (index <= count) or (index * fixedSize <= size)
    //MORE: Need to be very careful about the types...
    OwnedHqlExpr base0Index;
    unsigned fixedSize = translator.getFixedRecordSize(record);

    BuildCtx subctx(ctx);
    bool conditional = !indexExpr->hasProperty(noBoundCheckAtom);
    if (conditional)
    {
        OwnedHqlExpr simpleIndex = translator.buildSimplifyExpr(ctx, index);
        base0Index.setown(adjustIndexBaseToZero(simpleIndex));

        IValue * indexValue = index->queryValue();
        OwnedHqlExpr test;
        if (indexValue)
        {
            if (indexValue->getIntValue() <= 0)
                return NULL;
        }
        else
            test.setown(createCompare(no_gt, simpleIndex, queryZero()));

        IHqlExpression * test2 = NULL;
        if (boundDs.count)
        {
            IValue * countValue = boundDs.count->queryValue();
            if (countValue && indexValue)
            {
                if (indexValue->getIntValue() > countValue->getIntValue())
                    return NULL;
            }
            else
            {
                OwnedHqlExpr max = createTranslated(boundDs.count);
                test2 = createCompare(no_le, simpleIndex, max);
            }
        }
        else
        {
            OwnedHqlExpr max = createTranslated(boundDs.length);
            OwnedHqlExpr offset = multiplyValue(simpleIndex, fixedSize);
            test2 = createCompare(no_le, offset, max);
        }
        extendConditionOwn(test, no_and, test2);
        if (test)
        {
            CHqlBoundExpr boundCleared;
#ifdef CREATE_DEAULT_ROW_IF_NULL
            translator.buildDefaultRow(ctx, ds, boundCleared);
            conditional = false;
#else
            translator.buildNullRow(ctx, ds, boundCleared);
#endif
            OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);
            ctx.addAssign(row, defaultRowPtr);

            translator.buildFilter(subctx, test);
        }
        else
            conditional = false;
    }
    else
    {
        CHqlBoundExpr boundIndex;
        OwnedHqlExpr base0 = adjustIndexBaseToZero(index);
        translator.buildExpr(ctx, base0, boundIndex);
        base0Index.setown(boundIndex.getTranslatedExpr());
    }

    //row = base + index * fixedSize;
    OwnedHqlExpr address = LINK(boundDs.expr);//getPointer(boundDs.expr);
    s.clear().append(rowName).append(" = (byte *)(void *)");        // more: should really be const...
    translator.generateExprCpp(s, address);

    CHqlBoundExpr boundOffset;
    OwnedHqlExpr offset = multiplyValue(base0Index, fixedSize);
    translator.buildExpr(subctx, offset, boundOffset);
    s.append(" + (");
    translator.generateExprCpp(s, boundOffset.expr).append(")");

    s.append(";");
    subctx.addQuoted(s);

    BoundRow * cursor = translator.bindRow(ctx, indexExpr, row);
    cursor->setConditional(conditional);
    return cursor;
}

//---------------------------------------------------------------------------

InlineLinkedDatasetCursor::InlineLinkedDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs) : BaseDatasetCursor(_translator, _ds, &_boundDs)
{
    assertex(boundDs.count != NULL);
    assertex(isArrayRowset(boundDs.expr->queryType()));
}

void InlineLinkedDatasetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(boundDs.count);
}

void InlineLinkedDatasetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.setown(createValue(no_ne, makeBoolType(), LINK(boundDs.count), getZero()));
}

void InlineLinkedDatasetCursor::buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx)
{
    translator.getUniqueId(cursorName.append("iter"));

    //RtlFixedDatasetCursor cursor(len, data, size)
    StringBuffer decl;
    decl.append("RtlLinkedDatasetCursor ").append(cursorName);

    StringBuffer args;
    translator.generateExprCpp(args, boundDs.count);
    args.append(", ");
    translator.generateExprCpp(args, boundDs.expr);

    if (initctx)
    {
        StringBuffer s;
        s.append(cursorName).append(".init(").append(args).append(");");
        initctx->addQuoted(s);
    }
    else
    {
        decl.append("(").append(args).append(")");
    }
    decl.append(";");
    ctx.addQuoted(decl);
}

BoundRow * InlineLinkedDatasetCursor::buildIterateLoop(BuildCtx & ctx, bool needToBreak)
{
    StringBuffer rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);
    if (isEmptyDataset(boundDs))
    {
        ctx.addFilter(queryBoolExpr(false));
        return translator.bindTableCursor(ctx, ds, row);
    }

    if (matchesConstantValue(boundDs.count, 1) && !needToBreak)
    {
        CHqlBoundExpr boundRow;
        boundRow.set(boundDs);
        translator.convertBoundDatasetToFirstRow(ds, boundRow);

        //Optimize count=1, needToBreak = false;
        ctx.addGroup();
        return translator.bindTableCursor(ctx, ds, boundRow.expr);
    }

    StringBuffer cursorName, s;
    translator.getUniqueId(cursorName.append("cur"));

    //row = ds;
    OwnedHqlExpr address = getPointer(boundDs.expr);            // ensure no longer a wrapped item

    s.clear().append("byte * * ").append(cursorName).append(" = ");
    translator.generateExprCpp(s, address).append(";");
    ctx.addQuoted(s);

    OwnedHqlExpr test;
    OwnedHqlExpr count = translator.getBoundCount(boundDs);

    //count = <n>
    OwnedHqlExpr counter = ctx.getTempDeclare(unsignedType, count);

    //while (count--)
    test.setown(createValue(no_postdec, LINK(counter)));

    ctx.addLoop(test, NULL, false);
    ctx.addQuoted(s.clear().append(rowName).append(" = *").append(cursorName).append("++;"));
    BoundRow * cursor = translator.bindTableCursor(ctx, ds, row);

    return cursor;
}

BoundRow * InlineLinkedDatasetCursor::buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr)
{
    OwnedHqlExpr index = foldHqlExpression(indexExpr->queryChild(1));

    //row = NULL
    StringBuffer s, rowName;
    OwnedHqlExpr row = createRow(ctx, "row", rowName);

    //if (index > 0 && (index <= count)
    //MORE: Need to be very careful about the types...
    CHqlBoundExpr boundBase0Index;
    BuildCtx subctx(ctx);
    bool conditional = !indexExpr->hasProperty(noBoundCheckAtom);
    if (conditional)
    {
        IValue * indexValue = index->queryValue();
        if (indexValue)
        {
            if (indexValue->getIntValue() <= 0)
                return NULL;
            if (indexValue->getIntValue() > (size32_t)-1)
                return NULL;
            if (indexValue->queryType()->getSize() > sizeof(size32_t))
                index.setown(ensureExprType(index, sizetType));
        }

        OwnedHqlExpr simpleIndex = translator.buildSimplifyExpr(ctx, index);
        OwnedHqlExpr base0Index = adjustIndexBaseToZero(simpleIndex);
        translator.buildExpr(ctx, base0Index, boundBase0Index);

        OwnedHqlExpr test;
        if (!indexValue)
            test.setown(createCompare(no_gt, simpleIndex, queryZero()));

        IHqlExpression * test2 = NULL;
        IValue * countValue = boundDs.count->queryValue();
        if (countValue && indexValue)
        {
            if (indexValue->getIntValue() > countValue->getIntValue())
                return NULL;
        }
        else
        {
            OwnedHqlExpr max = createTranslated(boundDs.count);
            test2 = createCompare(no_le, simpleIndex, max);
        }
        extendConditionOwn(test, no_and, test2);

        if (test)
        {
            CHqlBoundExpr boundCleared;
#ifdef CREATE_DEAULT_ROW_IF_NULL
            translator.buildDefaultRow(ctx, ds, boundCleared);
            conditional = false;
#else
            translator.buildNullRow(ctx, ds, boundCleared);
#endif
            OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);
            ctx.addAssign(row, defaultRowPtr);

            translator.buildFilter(subctx, test);
        }
        else
            conditional = false;
    }
    else
    {
        OwnedHqlExpr base0 = adjustIndexBaseToZero(index);
        translator.buildExpr(ctx, base0, boundBase0Index);
    }

    //row = base[index]
    OwnedHqlExpr address = getPointer(boundDs.expr);
    OwnedHqlExpr indexedValue = createValue(no_index, row->getType(), LINK(address), LINK(boundBase0Index.expr));
    subctx.addAssign(row, indexedValue);

    //MORE: Should mark as linked if it is.
    BoundRow * cursor = translator.bindRow(ctx, indexExpr, row);
    cursor->setConditional(conditional);
    return cursor;
}

//---------------------------------------------------------------------------
MultiLevelDatasetCursor::MultiLevelDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds)
: BaseDatasetCursor(_translator, _ds, NULL)
{
}


void MultiLevelDatasetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)           
{ 
    throwUnexpected(); 
}

void MultiLevelDatasetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)          
{ 
    throwUnexpected(); 
}

BoundRow * MultiLevelDatasetCursor::buildIterateLoop(BuildCtx & ctx, bool needToBreak)
{
    OwnedHqlExpr breakVar;
    if (needToBreak)
    {
        CHqlBoundTarget bound;
        translator.createTempFor(ctx, boolType, bound, typemod_none, FormatNatural);
        breakVar.set(bound.expr);
        ctx.addAssign(breakVar, queryBoolExpr(false));
    }

    return doBuildIterateLoop(ctx, ds, breakVar, true);
}

BoundRow * MultiLevelDatasetCursor::buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr)
{
    //Declare row for final level, iterate the appropriate number of times, and then assign and break.
    BuildCtx initctx(ctx);

    IHqlExpression * selector = ds->queryNormalizedSelector();
    StringBuffer cursorName;
    translator.getUniqueId(cursorName.append("row"));
    OwnedHqlExpr rowExpr = createVariable(cursorName, makeRowReferenceType(selector));
    initctx.addDeclare(rowExpr);

    CHqlBoundExpr boundCleared;
    translator.buildDefaultRow(initctx, selector, boundCleared);
    OwnedHqlExpr defaultRowPtr = getPointer(boundCleared.expr);
    initctx.addAssign(rowExpr, defaultRowPtr);
    HqlExprAssociation * savedMarker = ctx.associateExpr(queryConditionalRowMarker(), rowExpr);

    CHqlBoundTarget boundCount;
    IHqlExpression * index = indexExpr->queryChild(1);
    bool selectFirst = matchesConstValue(index, 1);
    if (!selectFirst)
    {
        translator.createTempFor(initctx, index, boundCount);
        translator.buildExprAssign(initctx, boundCount, index);
    }

    BuildCtx subctx(ctx);
    buildIterateLoop(subctx, true);
    if (!selectFirst)
    {
        OwnedHqlExpr test = createValue(no_eq, makeBoolType(), createValue(no_predec, LINK(boundCount.expr)), getZero());
        subctx.addFilter(test);
    }

    //Now we have the correct element, assign it to the pointer.  
    //Need to be careful that the row we are pointing at is preserved, and doesn't go out of scope.  (Don't need to worry about  t can't be reused).
    BoundRow * curIter = translator.resolveSelectorDataset(subctx, selector);
    OwnedHqlExpr source = getPointer(curIter->queryBound());
    subctx.addAssign(rowExpr, source);
    subctx.addBreak();

    //Bind the expression as a row - so that the same select expression will get commoned up (e.g. sqagg)
    ctx.removeAssociation(savedMarker);
    return translator.bindRow(ctx, indexExpr, rowExpr);
}

BoundRow * MultiLevelDatasetCursor::doBuildIterateLoop(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * breakVar, bool topLevel)
{
    IHqlExpression * root = queryRoot(expr);

    if (root)
    {
        if (isMultiLevelDatasetSelector(root, false))
            doBuildIterateLoop(ctx, root->queryChild(0), breakVar, false);
    }

    BuildCtx oldctx(ctx);
    BoundRow * row;
    if (root)
    {
        OwnedHqlExpr thisLevel = replaceExpression(expr, root, root->queryNormalizedSelector());
        row = translator.buildDatasetIterate(ctx, thisLevel, breakVar != NULL);
    }
    else
    {
        //Unusual...  Something like (no_select(no_select(somethingComplex)))  Assert on topLevel to prevent recursive stack fault
        //(see dlingle4.xhql for an example)
        assertex(!topLevel);
        root = expr->queryChild(0);
        row = translator.buildDatasetIterate(ctx, expr, breakVar != NULL);
    }

    if (breakVar)
    {
        if (topLevel)
        {
            ctx.addAssign(breakVar, queryBoolExpr(true));
            ctx.setNextDestructor();
            ctx.addAssign(breakVar, queryBoolExpr(false));
        }

        if (isMultiLevelDatasetSelector(root, false))
        {
            oldctx.addFilter(breakVar);
            oldctx.addBreak();
        }
    }

    return row;
}

//---------------------------------------------------------------------------

BaseSetCursor::BaseSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr) : translator(_translator) 
{
    expr.set(_expr);
}

ListSetCursor::ListSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr) : BaseSetCursor(_translator, _expr)
{
}

void ListSetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.setown(getCountExpr());
}

void ListSetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(queryBoolExpr(expr->numChildren() != 0));
}

void ListSetCursor::buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(queryBoolExpr(false));
}

void ListSetCursor::buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & curBound, bool needToBreak)
{
    if (expr->numChildren() == 0)
    {
        ctx.addFilter(queryBoolExpr(false));
        curBound.expr.setown(createNullExpr(expr->queryType()->queryChildType()));
        return;
    }
    if (!needToBreak && (expr->numChildren() == 1))
    {
        translator.buildExpr(ctx, expr->queryChild(0), curBound);
        return;
    }

    CHqlBoundExpr boundList;
    translator.buildSimpleExpr(ctx, expr, boundList);

    OwnedHqlExpr loopVar = ctx.getTempDeclare(unsignedType, NULL);
    OwnedHqlExpr loopTest = createValue(no_lt, makeBoolType(), LINK(loopVar), getCountExpr());
    OwnedHqlExpr inc = createValue(no_postinc, loopVar->getType(), LINK(loopVar));

    translator.buildAssignToTemp(ctx, loopVar, queryZero());
    ctx.addLoop(loopTest, inc, false);
    curBound.expr.setown(createValue(no_index, LINK(expr->queryType()->queryChildType()), LINK(boundList.expr), LINK(loopVar)));
}

void ListSetCursor::buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    CHqlBoundExpr boundList;
    translator.buildSimpleExpr(ctx, expr, boundList);

    UNIMPLEMENTED;
    ctx.addQuoted("create fixed iterate (bound.length, bound.getAddress()");
}

void ListSetCursor::gatherSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & value, HqlExprAttr & cond)
{
    if (expr->numChildren() == 0)
    {
        OwnedHqlExpr null = getOutOfRangeValue(indexExpr);
        translator.buildExpr(ctx, null, value);
        return;
    }
    IHqlExpression * index = indexExpr->queryChild(1);
    if (index->isConstant())
    {
        OwnedHqlExpr folded = foldHqlExpression(index);
        unsigned which = (unsigned)folded->queryValue()->getIntValue()-1;
        if (which < expr->numChildren())
            translator.buildExpr(ctx, expr->queryChild(which), value);
        else 
        {
            OwnedHqlExpr null = getOutOfRangeValue(indexExpr);
            translator.buildExpr(ctx, null, value);
        }
    }
    else
    {
        CHqlBoundExpr boundList;
        translator.buildSimpleExpr(ctx, expr, boundList);

        CHqlBoundExpr boundIndex;
        ITypeInfo * elementType = expr->queryType()->queryChildType();  // not indexExpr->getType() because may now be more specific
        OwnedHqlExpr base0Index = adjustIndexBaseToZero(index);
        if (indexExpr->hasProperty(noBoundCheckAtom))
            translator.buildExpr(ctx, base0Index, boundIndex);
        else
            translator.buildSimpleExpr(ctx, base0Index, boundIndex);
        value.expr.setown(createValue(no_index, LINK(elementType), LINK(boundList.expr), LINK(boundIndex.expr)));

        if (!indexExpr->hasProperty(noBoundCheckAtom))
        {
            ITypeInfo * indexType = boundIndex.expr->queryType();
            //ok to subtract early and remove a check for > 0 on unsigned values because they will wrap and fail upper limit test
            if (indexType->isSigned())
                cond.setown(createBoolExpr(no_ge, LINK(boundIndex.expr), getZero()));
            if (indexType->getCardinality() > expr->numChildren())
                extendConditionOwn(cond, no_and, createBoolExpr(no_lt, LINK(boundIndex.expr), getCountExpr()));
        }
    }
}



void ListSetCursor::buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt)
{
    CHqlBoundExpr value;
    HqlExprAttr cond;
    gatherSelect(ctx, indexExpr, value, cond);
    if (cond)
    {
        translator.buildTempExpr(ctx, indexExpr, tgt);
        return;
        CHqlBoundTarget tempTarget;
        translator.createTempFor(ctx, indexExpr, tempTarget);
        buildAssignSelect(ctx, tempTarget, indexExpr);
        tgt.setFromTarget(tempTarget);
    }
    else
        tgt.set(value);
}



void ListSetCursor::buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr)
{
    CHqlBoundExpr value;
    HqlExprAttr cond;
    gatherSelect(ctx, indexExpr, value, cond);

    if (cond)
    {
        BuildCtx subctx(ctx);
        IHqlStmt * e = subctx.addFilter(cond);
        translator.assign(subctx, target, value);
        subctx.selectElse(e);
        OwnedHqlExpr null = getOutOfRangeValue(indexExpr);
        translator.buildExprAssign(subctx, target, null);
    }
    else
        translator.assign(ctx, target, value);
}


IHqlExpression * ListSetCursor::getCountExpr()
{ 
    return getSizetConstant(expr->numChildren()); 
}


//---------------------------------------------------------------------------

AllSetCursor::AllSetCursor(HqlCppTranslator & _translator) : BaseSetCursor(_translator, NULL)
{
}

void AllSetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    translator.throwError(HQLERR_CountAllSet);
}

void AllSetCursor::buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(queryBoolExpr(true));
}

void AllSetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(queryBoolExpr(true));
}

void AllSetCursor::buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & curBound, bool needToBreak)
{
    translator.throwError(HQLERR_IndexAllSet);
}

void AllSetCursor::buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    translator.throwError(HQLERR_IndexAllSet);
}


void AllSetCursor::buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt)
{
    translator.throwError(HQLERR_IndexAllSet);
}



void AllSetCursor::buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr)
{
    translator.throwError(HQLERR_IndexAllSet);
}



//---------------------------------------------------------------------------

GeneralSetCursor::GeneralSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr, CHqlBoundExpr & boundSet) : BaseSetCursor(_translator, _expr)
{
    isAll.setown(boundSet.getIsAll());
    ITypeInfo * elementType = LINK(expr->queryType()->queryChildType());
    if (!elementType)
        elementType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
    element.setown(createField(valueAtom, elementType, NULL));

    HqlExprArray fields;
    fields.append(*LINK(element));

    ds.setown(createDataset(no_anon, createRecord(fields), LINK(expr)));
    dsCursor.setown(new InlineBlockDatasetCursor(translator, ds, boundSet));
}

void GeneralSetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    checkNotAll(ctx);
    dsCursor->buildCount(ctx, tgt);
}

void GeneralSetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    if (isAll->queryValue())
    {
        if (isAll->queryValue()->getBoolValue())
        {
            tgt.expr.set(queryBoolExpr(true));
            return;
        }
        dsCursor->buildExists(ctx, tgt);
    }
    else
    {
        dsCursor->buildExists(ctx, tgt);
        tgt.expr.setown(createBoolExpr(no_or, LINK(isAll), LINK(tgt.expr)));
    }
}

void GeneralSetCursor::buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(isAll);
}

void GeneralSetCursor::buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & curBound, bool needToBreak)
{
    BoundRow * cursor = dsCursor->buildIterateLoop(ctx, needToBreak);

    OwnedHqlExpr select = createSelectExpr(LINK(cursor->querySelector()), LINK(element));
    translator.buildExpr(ctx, select, curBound);
}

void GeneralSetCursor::buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    UNIMPLEMENTED;
    HqlExprAttr row;
    dsCursor->buildIterateClass(ctx, tgt.expr, row);
}

IHqlExpression * GeneralSetCursor::createDatasetSelect(IHqlExpression * indexExpr)
{
    HqlExprArray args;
    args.append(*LINK(ds));
    unwindChildren(args, indexExpr, 1);
    return createRow(no_selectnth, args);
}

void GeneralSetCursor::buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt)
{
    if (indexExpr->hasProperty(noBoundCheckAtom))
    {
        if (indexExpr->hasProperty(forceAllCheckAtom))
            checkNotAll(ctx);

        OwnedHqlExpr dsIndexExpr = createDatasetSelect(indexExpr);
        BoundRow * cursor = dsCursor->buildSelect(ctx, dsIndexExpr);
        OwnedHqlExpr select = createSelectExpr(LINK(dsIndexExpr), LINK(element));
        translator.buildExpr(ctx, select, tgt);
    }
    else
    {
        translator.buildTempExpr(ctx, indexExpr, tgt);
    }
}



void GeneralSetCursor::buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr)
{
    if (!indexExpr->hasProperty(noBoundCheckAtom) || indexExpr->hasProperty(forceAllCheckAtom))
        checkNotAll(ctx);

    OwnedHqlExpr dsIndexExpr = createDatasetSelect(indexExpr);
    BoundRow * cursor = dsCursor->buildSelect(ctx, dsIndexExpr);

    if (cursor)
    {
        OwnedHqlExpr select = createSelectExpr(LINK(dsIndexExpr), LINK(element));
        if (!cursor->isConditional())
            translator.buildExprAssign(ctx, target, select);
        else
        {
            // if (row) tgt = x else tgt = dft;
            BuildCtx subctx(ctx);
            IHqlStmt * e = subctx.addFilter(cursor->queryBound());
            cursor->setConditional(false);  // yuk!
            translator.buildExprAssign(subctx, target, select);
            cursor->setConditional(true);
            subctx.selectElse(e);
            OwnedHqlExpr null = getOutOfRangeValue(indexExpr);
            translator.buildExprAssign(subctx, target, null);
        }
    }
    else
    {
        OwnedHqlExpr null = getOutOfRangeValue(indexExpr);
        translator.buildExprAssign(ctx, target, null);
    }
}

void GeneralSetCursor::checkNotAll(BuildCtx & ctx)
{
    if (isAll->queryValue())
    {
        if (isAll->queryValue()->getBoolValue())
            translator.throwError(HQLERR_IndexAllSet);
    }
    else
    {
        //MORE: Should only really do this once...
        BuildCtx subctx(ctx);
        subctx.addFilter(isAll);
        IHqlExpression * msg = translator.createFailMessage("Cannot index ALL", NULL, NULL, translator.queryCurrentActivityId(ctx));
        OwnedHqlExpr fail = createValue(no_fail, makeVoidType(), getZero(), msg, getDefaultAttr());
        translator.buildStmt(subctx, fail);
    }
}

bool GeneralSetCursor::isSingleValued()
{
    if (!matchesBoolean(isAll, false))
        return false;
//  return dsCursor->hasSingleRow();
    return false;
}

//---------------------------------------------------------------------------

CreateSetCursor::CreateSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr, IHqlCppDatasetCursor * _dsCursor) : BaseSetCursor(_translator, _expr)
{
    ds.set(expr->queryChild(0));
    value.set(expr->queryChild(1));
    dsCursor.set(_dsCursor);
}

void CreateSetCursor::buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    dsCursor->buildCount(ctx, tgt);
}

void CreateSetCursor::buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    dsCursor->buildExists(ctx, tgt);
}

void CreateSetCursor::buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    tgt.expr.set(queryBoolExpr(false));
}

void CreateSetCursor::buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & curBound, bool needToBreak)
{
    BoundRow * cursor = dsCursor->buildIterateLoop(ctx, needToBreak);
    translator.buildExpr(ctx, value, curBound);
}

void CreateSetCursor::buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt)
{
    UNIMPLEMENTED;
}

IHqlExpression * CreateSetCursor::createDatasetSelect(IHqlExpression * indexExpr)
{
    if (value->getOperator() == no_select && 
        (value->queryChild(0)->queryNormalizedSelector() == ds->queryNormalizedSelector()))
    {
        HqlExprArray args;
        args.append(*LINK(ds));
        unwindChildren(args, indexExpr, 1);
        IHqlExpression * select = createRow(no_selectnth, args);
        return createNewSelectExpr(select, LINK(value->queryChild(1)));
    }
    else
    {
        OwnedHqlExpr field = createField(createIdentifierAtom("__f1__"), value->getType(), NULL);
        IHqlExpression * aggregateRecord = createRecord(field);

        IHqlExpression * assign = createAssign(createSelectExpr(getSelf(aggregateRecord), LINK(field)), LINK(value));
        IHqlExpression * transform = createValue(no_newtransform, makeTransformType(aggregateRecord->getType()), assign);

        HqlExprArray args;
        args.append(*createDataset(no_newusertable, LINK(ds), createComma(aggregateRecord, transform)));
        unwindChildren(args, indexExpr, 1);
        IHqlExpression * select = createRow(no_selectnth, args);
        return createNewSelectExpr(select, LINK(field));
    }
}

void CreateSetCursor::buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt)
{
    OwnedHqlExpr newExpr = createDatasetSelect(indexExpr);
    translator.buildExpr(ctx, newExpr, tgt);
}



void CreateSetCursor::buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr)
{
    OwnedHqlExpr newExpr = createDatasetSelect(indexExpr);
    translator.buildExprAssign(ctx, target, newExpr);
}

bool CreateSetCursor::isSingleValued()
{
    return hasSingleRow(ds);
}

//---------------------------------------------------------------------------

IHqlCppSetCursor * HqlCppTranslator::createSetSelector(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr normalized = normalizeListCasts(expr);

    switch (normalized->getOperator())
    {
    case no_alias_scope:
        {
            unsigned max = normalized->numChildren();
            for (unsigned idx = 1; idx < max; idx++)
                expandAliases(ctx, normalized->queryChild(idx));
            return createSetSelector(ctx, normalized->queryChild(0));
        }
    case no_null:
        return new ListSetCursor(*this, normalized);
    case no_all:
        return new AllSetCursor(*this);
    case no_list:
        if ((normalized->numChildren() == 0) || (normalized->queryType()->queryChildType()->getSize() != UNKNOWN_LENGTH))
            return new ListSetCursor(*this, normalized);
        break; // default
    case no_createset:
        {
            Owned<IHqlCppDatasetCursor> dsCursor = createDatasetSelector(ctx, expr->queryChild(0));
            return new CreateSetCursor(*this, expr, dsCursor);
        }
    }

    CHqlBoundExpr bound;
    buildSimpleExpr(ctx, normalized, bound);
    return new GeneralSetCursor(*this, normalized, bound);
}


//---------------------------------------------------------------------------

IHqlCppDatasetCursor * HqlCppTranslator::createDatasetSelector(BuildCtx & ctx, IHqlExpression * expr)
{
//  OwnedHqlExpr normalized = normalizeDatasetCasts(expr);

    switch (expr->getOperator())
    {
    case no_null:
        break;
    case no_select:
        if (isMultiLevelDatasetSelector(expr, false))
            return new MultiLevelDatasetCursor(*this, expr);
        break;
    }

    CHqlBoundExpr bound;
    buildDataset(ctx, expr, bound, FormatNatural);
    if (bound.expr->isDatarow() || !isArrayRowset(bound.expr->queryType()))
        return new InlineBlockDatasetCursor(*this, expr, bound);
    return new InlineLinkedDatasetCursor(*this, expr, bound);
}


//---------------------------------------------------------------------------

CHqlCppDatasetBuilder::CHqlCppDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record)
: translator(_translator), record(_record)
{
}



DatasetBuilderBase::DatasetBuilderBase(HqlCppTranslator & _translator, IHqlExpression * _record, bool _buildLinkedRows) : CHqlCppDatasetBuilder(_translator, _record)
{
    StringBuffer rowName;
    unique_id_t id = translator.getUniqueId();
    appendUniqueId(instanceName.append("cr"), id);
    builderName.append(instanceName).append(".rowBuilder()");
    rowName.append(instanceName).append(".rowBuilder().row()");     // more!

    IHqlExpression * linkAttr = _buildLinkedRows ? getLinkCountedAttr() : NULL;
    ITypeInfo * rowType = makeRowReferenceType(record);
    if (_buildLinkedRows)
        rowType = makeAttributeModifier(rowType, getLinkCountedAttr());
    OwnedHqlExpr cursorVar = createVariable(rowName.str(), rowType);
    dataset.setown(createDataset(no_anon, LINK(record), createComma(getSelfAttr(), linkAttr)));
}

BoundRow * DatasetBuilderBase::buildCreateRow(BuildCtx & ctx)
{
    StringBuffer s;
    OwnedHqlExpr cond = createQuoted(s.append(instanceName).append(".createRow()"), makeBoolType());

    if (isRestricted())
        ctx.addFilter(cond);
    else
        ctx.addExpr(cond);

    return translator.bindSelf(ctx, dataset, builderName);
}

BoundRow * DatasetBuilderBase::buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput)
{
    StringBuffer serializerInstanceName;
    translator.ensureRowSerializer(serializerInstanceName, ctx, record, deserializerAtom);

    StringBuffer s;
    s.append(instanceName).append(".deserializeRow(*");
    s.append(serializerInstanceName).append(", ");
    translator.generateExprCpp(s, serializedInput).append(");");
    ctx.addQuoted(s);

    return translator.bindSelf(ctx, dataset, builderName);
}


void DatasetBuilderBase::finishRow(BuildCtx & ctx, BoundRow * selfCursor)
{
    OwnedHqlExpr size = createSizeof(selfCursor->querySelector());
    CHqlBoundExpr boundSize;
    translator.buildExpr(ctx, size, boundSize);

    StringBuffer s;
    s.append(instanceName).append(".finalizeRow(");
    translator.generateExprCpp(s, boundSize.expr).append(");");
    ctx.addQuoted(s);
    ctx.removeAssociation(selfCursor);
}


//---------------------------------------------------------------------------


BlockedDatasetBuilder::BlockedDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record) : DatasetBuilderBase(_translator, _record, false)
{
    forceLength = false;
}

void BlockedDatasetBuilder::buildDeclare(BuildCtx & ctx)
{
    StringBuffer decl, extra;

    if (count)
    {
        CHqlBoundExpr boundCount;
        translator.buildExpr(ctx, count, boundCount);
        if (translator.isFixedRecordSize(record))
        {
            //RtlFixedDatasetCreator cursor(len, data, size)
            decl.append("RtlLimitedFixedDatasetBuilder");
            extra.append(translator.getFixedRecordSize(record));
        }
        else
        {
            //RtlVariableDatasetCursor cursor(len, data, recordSize)
            decl.append("RtlLimitedVariableDatasetBuilder");
            translator.buildMetaForRecord(extra, record);
        }

        translator.ensureContextAvailable(ctx);
        decl.append(" ").append(instanceName).append("(").append(extra).append(",");
        translator.generateExprCpp(decl, boundCount.expr).append(",");
        if (forceLength)
        {
            OwnedHqlExpr clearFunc = translator.getClearRecordFunction(record);
            translator.generateExprCpp(decl, clearFunc).append(", ctx);");
        }
        else
            decl.append("NULL,NULL);");
    }
    else
    {
        if (translator.isFixedRecordSize(record))
        {
            //RtlFixedDatasetCreator cursor(len, data, size)
            decl.append("RtlFixedDatasetBuilder");
            extra.append(translator.getFixedRecordSize(record)).append(", 0");
        }
        else
        {
            //RtlVariableDatasetCursor cursor(len, data, recordSize)
            decl.append("RtlVariableDatasetBuilder");
            translator.buildMetaForRecord(extra, record);
        }

        decl.append(" ").append(instanceName).append("(").append(extra).append(");");
    }
    ctx.addQuoted(decl);
}

void BlockedDatasetBuilder::buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    //more: should I do this by really calling a function?
    StringBuffer s;
    s.append(instanceName).append(".getData(");
    translator.generateExprCpp(s, target.length);
    s.append(",");
    OwnedHqlExpr ref = createValue(no_reference, target.getType(), LINK(target.expr));
    translator.generateExprCpp(s, ref);
    s.append(");");
    ctx.addQuoted(s);
}


void BlockedDatasetBuilder::buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    StringBuffer s;
    s.clear().append(instanceName).append(".getSize()");
    bound.length.setown(createQuoted(s.str(), LINK(unsignedType)));
    s.clear().append(instanceName).append(".queryData()");
    bound.expr.setown(createQuoted(s.str(), makeReferenceModifier(dataset->getType())));
}


//---------------------------------------------------------------------------


SingleRowTempDatasetBuilder::SingleRowTempDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, BoundRow * _row) : CHqlCppDatasetBuilder(_translator, _record)
{
    row.set(_row);
    cursor.set(row);
}

void SingleRowTempDatasetBuilder::buildDeclare(BuildCtx & ctx)
{
}

BoundRow * SingleRowTempDatasetBuilder::buildCreateRow(BuildCtx & ctx)
{
    cursor.set(row);
    return row;
}

void SingleRowTempDatasetBuilder::buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    assertex(cursor != NULL);
}


void SingleRowTempDatasetBuilder::buildFinish(BuildCtx & ctx, CHqlBoundExpr & target)
{
    assertex(cursor != NULL);
}


void SingleRowTempDatasetBuilder::finishRow(BuildCtx & ctx, BoundRow * selfCursor)
{
}

//---------------------------------------------------------------------------


InlineDatasetBuilder::InlineDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, IHqlExpression * _size, IHqlExpression * _address) : CHqlCppDatasetBuilder(_translator, _record)
{
    StringBuffer cursorName;
    getUniqueId(cursorName.append("p"));

    ITypeInfo * rowType = makeRowReferenceType(record);
    cursorVar.setown(createVariable(cursorName.str(), rowType));
    dataset.setown(createDataset(no_anon, LINK(record), getSelfAttr()));
    size.set(_size);
    address.set(_address);
}

void InlineDatasetBuilder::buildDeclare(BuildCtx & ctx)
{
    //NB: This is only ever used where the target has already been checked to ensure there is enough room
    //If we wanted to be clever we would need to use a RtlNestedRowBuilder(parent, <start-of-this-row>, ...);
    ctx.addDeclare(cursorVar, address);
}

BoundRow * InlineDatasetBuilder::buildCreateRow(BuildCtx & ctx)
{
    Owned<BoundRow> cursor = translator.createTableCursor(dataset, cursorVar, no_self, NULL);
    ctx.associate(*cursor);
    return cursor;
}

void InlineDatasetBuilder::buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    ctx.addAssign(target.length, size);
}


void InlineDatasetBuilder::buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    bound.length.set(size);
    bound.expr.set(address);
}


void InlineDatasetBuilder::finishRow(BuildCtx & ctx, BoundRow * selfCursor)
{
    CHqlBoundExpr bound;
    translator.getRecordSize(ctx, selfCursor->querySelector(), bound);

    if (translator.queryOptions().optimizeIncrement)
    {
        ctx.addAssignIncrement(selfCursor->queryBound(), bound.expr);
    }
    else
    {
        OwnedHqlExpr inc = createValue(no_add, LINK(selfCursor->queryBound()), LINK(bound.expr));
        ctx.addAssign(selfCursor->queryBound(), inc);
    }
}


//---------------------------------------------------------------------------


LinkedDatasetBuilder::LinkedDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, IHqlExpression * _choosenLimit) : DatasetBuilderBase(_translator, _record, true)
{
    choosenLimit.set(_choosenLimit);
}

void LinkedDatasetBuilder::buildDeclare(BuildCtx & ctx)
{
    StringBuffer decl, allocatorName;

    OwnedHqlExpr curActivityId = translator.getCurrentActivityId(ctx);
    translator.ensureRowAllocator(allocatorName, ctx, record, curActivityId);

    decl.append("RtlLinkedDatasetBuilder ").append(instanceName).append("(");
    decl.append(allocatorName);
    if (choosenLimit)
    {
        CHqlBoundExpr boundLimit;
        translator.buildExpr(ctx, choosenLimit, boundLimit);
        translator.generateExprCpp(decl.append(", "), boundLimit.expr);
    }
    decl.append(");");

    ctx.addQuoted(decl);
}

void LinkedDatasetBuilder::finishRow(BuildCtx & ctx, BoundRow * selfCursor)
{
    OwnedHqlExpr size = translator.getRecordSize(selfCursor->querySelector());
    CHqlBoundExpr boundSize;
    translator.buildExpr(ctx, size, boundSize);

    StringBuffer s;
    s.append(instanceName).append(".finalizeRow(");
    translator.generateExprCpp(s, boundSize.expr).append(");");
    ctx.addQuoted(s);
}

void LinkedDatasetBuilder::buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    //more: should I do this by really calling a function?
    StringBuffer s;

    s.append(instanceName).append(".getcount()");

    if (hasWrapperModifier(target.queryType()))
    {
        translator.generateExprCpp(s.clear(), target.expr);
        s.append(".setown(").append(instanceName).append(".getcount()");
        s.append(",").append(instanceName).append(".linkrows());");
        ctx.addQuoted(s);
    }
    else
    {
        OwnedHqlExpr countExpr = createQuoted(s.str(), LINK(unsignedType));
        ctx.addAssign(target.count, countExpr);
        s.clear().append(instanceName).append(".linkrows()");
        OwnedHqlExpr rowsExpr = createQuoted(s.str(), dataset->getType());
        ctx.addAssign(target.expr, rowsExpr);
    }
}


void LinkedDatasetBuilder::buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound)
{
    StringBuffer s;
    s.clear().append(instanceName).append(".getcount()");
    bound.count.setown(createQuoted(s.str(), LINK(unsignedType)));
    s.clear().append(instanceName).append(".queryrows()");
    bound.expr.setown(createQuoted(s.str(), makeReferenceModifier(dataset->getType())));
}

bool LinkedDatasetBuilder::buildLinkRow(BuildCtx & ctx, BoundRow * sourceRow)
{
    IHqlExpression * sourceRecord = sourceRow->queryRecord();
    if (recordTypesMatch(sourceRecord, record) && sourceRow->isBinary())
    {
        OwnedHqlExpr source = getPointer(sourceRow->queryBound());
        BuildCtx subctx(ctx);
        if (sourceRow->isConditional())
            subctx.addFilter(source);

        if (sourceRow->isLinkCounted())
        {
            StringBuffer s;
            s.append(instanceName).append(".append(");
            translator.generateExprCpp(s, source);
            s.append(");");
            subctx.addQuoted(s);
            return true;
        }

        IHqlExpression * sourceExpr = sourceRow->querySelector();
        OwnedHqlExpr rowExpr = sourceExpr->isDataset() ? ensureActiveRow(sourceExpr) : LINK(sourceExpr);
        OwnedHqlExpr size = createSizeof(rowExpr);
        CHqlBoundExpr boundSize;
        translator.buildExpr(ctx, size, boundSize);

        StringBuffer s;
        s.append(instanceName).append(".cloneRow(");
        translator.generateExprCpp(s, boundSize.expr).append(",");
        translator.generateExprCpp(s, source);
        s.append(");");
        subctx.addQuoted(s);
        return true;
    }
    return false;
}

bool LinkedDatasetBuilder::buildAppendRows(BuildCtx & ctx, IHqlExpression * expr) 
{
    IHqlExpression * sourceRecord = expr->queryRecord();
    if (recordTypesMatch(sourceRecord, record))
    {
        CHqlBoundExpr bound;
        if (!ctx.getMatchExpr(expr, bound))
        {
            bool tryToOptimize = false;
            switch (expr->getOperator())
            {
            case no_select:
                if (isMultiLevelDatasetSelector(expr, false))
                    break;
                if (!hasLinkedRow(expr->queryType()))
                    break;
                tryToOptimize = true;
                break;
            default:
                //Don't speculatively evaluate if the expression isn't pure
                tryToOptimize = alwaysEvaluatesToBound(expr) && expr->isPure();
                break;
            }

            if (tryToOptimize)
                translator.buildDataset(ctx, expr, bound, FormatNatural);
        }

        if (bound.expr)
        {
            if (hasLinkedRow(bound.queryType()))
            {
                OwnedHqlExpr source = getPointer(bound.expr);
                StringBuffer s;
                s.append(instanceName).append(".appendRows(");
                translator.generateExprCpp(s, bound.count);
                s.append(",");
                translator.generateExprCpp(s, source);
                s.append(");");
                ctx.addQuoted(s);
                return true;
            }
        }
    }
    return false;
}


//---------------------------------------------------------------------------

SetBuilder::SetBuilder(HqlCppTranslator & _translator, ITypeInfo * fieldType, IHqlExpression * _allVar) : translator(_translator)
{
    HqlExprArray fields;
    fields.append(*createField(valueAtom, LINK(fieldType), NULL));
    record.setown(createRecord(fields));
    allVar.set(_allVar);
    activeRow = NULL;
}

void SetBuilder::buildDeclare(BuildCtx & ctx)
{
    datasetBuilder->buildDeclare(ctx);
}

IReferenceSelector * SetBuilder::buildCreateElement(BuildCtx & ctx)
{
    activeRow = datasetBuilder->buildCreateRow(ctx);
    OwnedHqlExpr select = createSelectExpr(LINK(activeRow->querySelector()), LINK(record->queryChild(0)));
    return translator.buildReference(ctx, select);
}

void SetBuilder::buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target)
{
    if (target.isAll && (allVar != target.isAll))
    {
        assertex(allVar != NULL);
        ctx.addAssign(target.isAll, allVar);
    }
    datasetBuilder->buildFinish(ctx, target);
}


void SetBuilder::finishElement(BuildCtx & ctx)
{
    datasetBuilder->finishRow(ctx, activeRow);
    activeRow = NULL;
}

void SetBuilder::setAll(BuildCtx & ctx, IHqlExpression * isAll)
{
    if (allVar)
    {
        CHqlBoundExpr bound;
        translator.buildExpr(ctx, isAll, bound);
        ctx.addAssign(allVar, bound.expr);
    }
    else
    {
        if (!matchesBoolean(isAll, false))
            throwUnexpected();
    }
}



TempSetBuilder::TempSetBuilder(HqlCppTranslator & _translator, ITypeInfo * fieldType, IHqlExpression * _allVar) : SetBuilder(_translator, fieldType, _allVar)
{
    datasetBuilder.setown(new BlockedDatasetBuilder(translator, record));
}

InlineSetBuilder::InlineSetBuilder(HqlCppTranslator & _translator, ITypeInfo * fieldType, IHqlExpression * _allVar, IHqlExpression * _size, IHqlExpression * _address) : SetBuilder(_translator, fieldType, _allVar)
{
    datasetBuilder.setown(new InlineDatasetBuilder(translator, record, _size, _address));
}

IHqlCppSetBuilder * HqlCppTranslator::createTempSetBuilder(ITypeInfo * type, IHqlExpression * allVar)
{
    return new TempSetBuilder(*this, type, allVar);
}

IHqlCppSetBuilder * HqlCppTranslator::createInlineSetBuilder(ITypeInfo * type, IHqlExpression * allVar, IHqlExpression * size, IHqlExpression * address)
{
    assertex(allVar);
    return new InlineSetBuilder(*this, type, allVar, size, address);
}

IHqlCppDatasetBuilder * HqlCppTranslator::createBlockedDatasetBuilder(IHqlExpression * record)
{
    return new BlockedDatasetBuilder(*this, record);
}

IHqlCppDatasetBuilder * HqlCppTranslator::createLinkedDatasetBuilder(IHqlExpression * record, IHqlExpression * choosenLimit)
{
    return new LinkedDatasetBuilder(*this, record, choosenLimit);
}

IHqlCppDatasetBuilder * HqlCppTranslator::createSingleRowTempDatasetBuilder(IHqlExpression * record, BoundRow * row)
{
//  if (translator.isFixedRecordSize(record))
        return new SingleRowTempDatasetBuilder(*this, record, row);
    return createBlockedDatasetBuilder(record);
}

IHqlCppDatasetBuilder * HqlCppTranslator::createInlineDatasetBuilder(IHqlExpression * record, IHqlExpression * size, IHqlExpression * address)
{
    assertex(isFixedRecordSize(record));
    return new InlineDatasetBuilder(*this, record, size, address);
}

IHqlCppDatasetBuilder * HqlCppTranslator::createChoosenDatasetBuilder(IHqlExpression * record, IHqlExpression * maxCount)
{
    BlockedDatasetBuilder * builder = new BlockedDatasetBuilder(*this, record);
    builder->setLimit(maxCount, false);
    return builder;
}

IHqlCppDatasetBuilder * HqlCppTranslator::createLimitedDatasetBuilder(IHqlExpression * record, IHqlExpression * maxCount)
{
    BlockedDatasetBuilder * builder = new BlockedDatasetBuilder(*this, record);
    builder->setLimit(maxCount, true);
    return builder;
}

//---------------------------------------------------------------------------

void HqlCppTranslator::doBuildSetAssignAndCast(BuildCtx & ctx, IHqlCppSetBuilder * builder, IHqlExpression * value)
{
    Owned<IHqlCppSetCursor> cursor = createSetSelector(ctx, value);
    CHqlBoundExpr srcIsAll;
    cursor->buildIsAll(ctx, srcIsAll);
    OwnedHqlExpr translated = srcIsAll.getTranslatedExpr();
    builder->setAll(ctx, translated);

    BuildCtx loopctx(ctx);
    CHqlBoundExpr boundCurElement;
    cursor->buildIterateLoop(loopctx, boundCurElement, false);

    Owned<IReferenceSelector> selector = builder->buildCreateElement(loopctx);
    OwnedHqlExpr translatedCurElement = boundCurElement.getTranslatedExpr();
    selector->set(loopctx, translatedCurElement);
    builder->finishElement(loopctx);
}


void HqlCppTranslator::buildSetAssign(BuildCtx & ctx, IHqlCppSetBuilder * builder, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_cast:
        doBuildSetAssignAndCast(ctx, builder, expr->queryChild(0));
        break;
    case no_addsets:
        doBuildSetAssignAndCast(ctx, builder, expr);
        break;
        //MORE: This is wrong because needs to cope with all.
        buildSetAssign(ctx, builder, expr->queryChild(0));
        buildSetAssign(ctx, builder, expr->queryChild(1));
        break;
    case no_all:
        builder->setAll(ctx, queryBoolExpr(true));
        break;
    case no_list:
        {
            unsigned max = expr->numChildren();
            if ((max < 3) || isComplexSet(expr) || !isConstantSet(expr))
            {
                for (unsigned i=0; i < max; i++)
                {
                    //Need a subcontext otherwise sizeof(target-row) gets cached.
                    BuildCtx subctx(ctx);       
                    subctx.addGroup();
                    Owned<IReferenceSelector> selector = builder->buildCreateElement(subctx);
                    selector->set(subctx, expr->queryChild(i));
                    builder->finishElement(subctx);
                }
                builder->setAll(ctx, queryBoolExpr(false));
            }
            else
                doBuildSetAssignAndCast(ctx, builder, expr);
        }
        break;
    case no_createset:
        {
            IHqlExpression * ds = expr->queryChild(0);
            IHqlExpression * value = expr->queryChild(1);
            builder->setAll(ctx, queryBoolExpr(false));
            BuildCtx subctx(ctx);
            BoundRow * cursor = buildDatasetIterate(subctx, ds, false);
            Owned<IReferenceSelector> selector = builder->buildCreateElement(subctx);
            selector->set(subctx, value);
            builder->finishElement(subctx);
            break;
        }
    default:
        doBuildSetAssignAndCast(ctx, builder, expr);
        break;
    }
}


void HqlCppTranslator::buildSetAssignViaBuilder(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * value)
{
    ITypeInfo * to = target.queryType();
    Owned<IHqlCppSetBuilder> builder = createTempSetBuilder(to->queryChildType(), target.isAll);
    builder->buildDeclare(ctx);

    buildSetAssign(ctx, builder, value);

    builder->buildFinish(ctx, target);
}



void HqlCppTranslator::doBuildAssignAddSets(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * value)
{
    IHqlExpression * left = value->queryChild(0);
    IHqlExpression * right = value->queryChild(1);

    assertex(left->queryType() == right->queryType());
    //a poor implementation, but at least it works.
    HqlExprArray args;
    args.append(*LINK(left));
    args.append(*LINK(right));
    OwnedHqlExpr call = bindFunctionCall(appendSetXAtom, args, left->queryType());
    buildExprAssign(ctx, target, call);
}

