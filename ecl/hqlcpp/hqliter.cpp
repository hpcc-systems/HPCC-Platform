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
#include "hqlcse.ipp"
#include "hqliter.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hqlcpp/hqliter.cpp $ $Id: hqliter.cpp 66009 2011-07-06 12:28:32Z ghalliday $");

//===========================================================================

bool isSequenceRoot(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_newkeyindex:
    case no_table:
    case no_select:
        return true;
    }
    return false;
}


bool canBuildSequenceInline(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_cachealias:
            expr = expr->queryChild(1);
            break;
        case no_newkeyindex:
        case no_table:
        case no_select:
            return true;
        case no_sorted:
        case no_distributed:
        case no_preservemeta:
        case no_grouped:
        case no_preload:
        case no_limit:
        case no_keyedlimit:
        case no_choosen:
        case no_filter:
        case no_hqlproject:
        case no_newusertable:
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
        case no_compound_fetch:
        case no_compound_selectnew:
        case no_compound_inline:
        case no_alias_scope:
            expr = expr->queryChild(0);
            break;
        default:
            return false;
        }
    }
}

//-------------------------------------------------------------------

void TransformSequenceBuilder::buildSequence(BuildCtx & ctx, BuildCtx * declarectx, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_cachealias:
        buildSequence(ctx, declarectx, expr->queryChild(1));
        return;
    case no_newkeyindex:
    case no_table:
    case no_fetch:
    case no_select:
    case no_anon:
    case no_pseudods:
        break;
    case no_hqlproject:
    case no_newusertable:
        buildSequence(ctx, NULL, expr->queryChild(0));
        break;
    default:
        buildSequence(ctx, declarectx, expr->queryChild(0));
        break;
    }

    switch (op)
    {
    case no_filter:
        {
            HqlExprAttr cond;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = queryRealChild(expr, i);
                if (cur && i != 0)
                    extendConditionOwn(cond, no_and, LINK(cur));
            }
            OwnedHqlExpr test = getInverse(cond);
            if (translator.queryOptions().foldFilter)
                test.setown(foldScopedHqlExpression(expr->queryChild(0)->queryNormalizedSelector(), test));
            if (translator.queryOptions().spotCSE)
                test.setown(spotScalarCSE(test));
            translator.buildFilteredReturn(ctx, test, failedFilterValue);
        }
        break;
    case no_hqlproject:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            OwnedHqlExpr leftSelect = createSelector(no_left, dataset, querySelSeq(expr));
            OwnedHqlExpr newSelect = ensureActiveRow(dataset->queryNormalizedSelector());
            OwnedHqlExpr transform = replaceSelector(expr->queryChild(1), leftSelect, newSelect);
            //MORE: Calculate cses at this point, but we really need to have removed hqlprojects for cse to work...

            BuildCtx & createctx = declarectx ? *declarectx : ctx;
            Owned<BoundRow> tempRow = translator.declareTempAnonRow(createctx, ctx, expr);

            BuildCtx subctx(ctx);
            translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);
            translator.doInlineTransform(subctx, transform, tempRow);
            translator.bindTableCursor(ctx, expr, tempRow->queryBound());
        }
        break;
    case no_newusertable:
        {
            IHqlExpression * transform = expr->queryChild(2);

            BuildCtx & createctx = declarectx ? *declarectx : ctx;
            Owned<BoundRow> tempRow = translator.declareTempAnonRow(createctx, ctx, expr);

            BuildCtx subctx(ctx);
            translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);

            translator.doInlineTransform(subctx, transform, tempRow);

            translator.bindTableCursor(ctx, expr, tempRow->queryBound());
        }
        break;
    }
}

//------------------------------------------------------------------------------------------

//Gather the different select levels, return the root dataset (if not in scope)
IHqlExpression * gatherSelectorLevels(HqlExprArray & iterators, IHqlExpression * expr)
{
    loop
    {
        IHqlExpression * root = queryRoot(expr);
        if (!root || root->getOperator() != no_select)
            return expr;
        iterators.add(*LINK(expr), 0);
        if (!root->hasProperty(newAtom))
            return NULL;
        expr = root->queryChild(0);
    }
}


void CompoundIteratorBuilder::bindParentCursors(BuildCtx & ctx, CursorArray & cursors)
{
    OwnedHqlExpr colocal = createQuoted("activity", makeVoidType());
    ForEachItemIn(i, cursors)
    {
        BoundRow & cur = cursors.item(i);
        //Very similar to code in the extract builder
        OwnedHqlExpr colocalBound = addMemberSelector(cur.queryBound(), colocal);
        ctx.associateOwn(*cur.clone(colocalBound));
    }
}

void CompoundIteratorBuilder::buildCompoundIterator(BuildCtx & initctx, HqlExprArray & iterators, CursorArray & cursors)
{
    StringBuffer s;

    declarectx.addQuoted("RtlCompoundIterator iter;");
    initctx.addQuoted(s.clear().append("iter.init(").append(iterators.ordinality()).append(");"));

    ForEachItemIn(i, iterators)
    {
        StringBuffer iterName, cursorName;

        createSingleLevelIterator(iterName, cursorName, &iterators.item(i), cursors);

        initctx.addQuoted(s.clear().append("iter.addIter(").append(i).append(",&").append(iterName).append(",&").append(cursorName).append(");"));
    }
}


void CompoundIteratorBuilder::createSingleLevelIterator(StringBuffer & iterName, StringBuffer & cursorName, IHqlExpression * expr, CursorArray & cursors)
{
    LinkedHqlExpr cur = expr;
    IHqlExpression * root = queryRoot(cur);
    assertex(root->getOperator() == no_select);

    //First remove any new attributes from selector, since parent cursor is guaranteed to be in scope at this point.
    IHqlExpression * normalized = root->queryNormalizedSelector();
    if (root != normalized)
        cur.setown(replaceExpression(cur, root, normalized));

    createSingleIterator(iterName, cur, cursors);

    translator.getUniqueId(cursorName.append("row"));
    OwnedHqlExpr row = createVariable(cursorName, makeRowReferenceType(cur));
    declarectx.addDeclare(row);
    cursors.append(*translator.createTableCursor(cur, row, no_none, NULL));
}

void CompoundIteratorBuilder::createSingleIterator(StringBuffer & iterName, IHqlExpression * expr, CursorArray & cursors)
{
    StringBuffer s;
    translator.getUniqueId(iterName.clear().append("iter"));

    //MORE: Nested class/...
    BuildCtx classctx(nestedctx);
    translator.beginNestedClass(classctx, iterName, "IRtlDatasetSimpleCursor", NULL, NULL);
    translator.queryEvalContext(classctx)->ensureHelpersExist();

    if (isArrayRowset(expr->queryType()))
    {
        classctx.addQuoted("byte * * end;");
        classctx.addQuoted("byte * * cur;");
    }
    else
    {
        classctx.addQuoted("byte * end;");
        classctx.addQuoted("byte * cur;");
    }

    IHqlExpression * root = queryRoot(expr);
    if (expr->queryBody() == root)
    {
        BuildCtx firstctx(classctx);
        firstctx.addQuotedCompound("virtual const byte * first()");
        createRawFirstFunc(firstctx, expr, cursors);

        BuildCtx nextctx(classctx);
        nextctx.addQuotedCompound("virtual const byte * next()");
        createRawNextFunc(nextctx, expr, cursors);
    }
    else
    {
        BuildCtx rawfirstctx(classctx);
        rawfirstctx.addQuotedCompound("inline const byte * rawFirst()");
        createRawFirstFunc(rawfirstctx, root, cursors);

        BuildCtx rawnextctx(classctx);
        rawnextctx.addQuotedCompound("virtual const byte * rawNext()");
        createRawNextFunc(rawnextctx, root, cursors);

        OwnedHqlExpr failValue = createTranslatedOwned(createValue(no_nullptr, makeVoidType()));
        TransformSequenceBuilder checkValidBuilder(translator, failValue);
        BuildCtx checkctx(classctx);
        checkctx.addQuotedCompound("inline const byte * checkValid()");
        bindParentCursors(checkctx, cursors);
        if (isArrayRowset(expr->queryType()))
            translator.bindTableCursor(checkctx, root, "(*cur)");
        else
            translator.bindTableCursor(checkctx, root, "cur");
        checkValidBuilder.buildSequence(checkctx, &classctx, expr);
        BoundRow * match = translator.resolveSelectorDataset(checkctx, expr);
        assertex(match);
        OwnedHqlExpr row = getPointer(match->queryBound());
        checkctx.addReturn(row);

        BuildCtx firstctx(classctx);
        firstctx.addQuotedCompound("virtual const byte * first()");
        firstctx.addQuoted("if (!rawFirst()) return NULL;");
        firstctx.addQuotedCompound("for (;;)");
        firstctx.addQuoted("const byte * valid = checkValid(); if (valid) return valid;");
        firstctx.addQuoted("if (!rawNext()) return NULL;");

        BuildCtx nextctx(classctx);
        nextctx.addQuotedCompound("virtual const byte * next()");
        nextctx.addQuotedCompound("for (;;)");
        nextctx.addQuoted("if (!rawNext()) return NULL;");
        nextctx.addQuoted("const byte * valid = checkValid(); if (valid) return valid;");
    }

    translator.endNestedClass();
}

void CompoundIteratorBuilder::createRawFirstFunc(BuildCtx & ctx, IHqlExpression * expr, CursorArray & cursors)
{
    bool isOutOfLine = isArrayRowset(expr->queryType());

    bindParentCursors(ctx, cursors);
    CHqlBoundExpr boundDs;
    translator.buildDataset(ctx, expr, boundDs, queryNaturalFormat(expr->queryType()));

    if (isOutOfLine)
    {
        StringBuffer s;
        s.clear().append("cur = ");     // more: should really be const...
        translator.generateExprCpp(s, boundDs.expr).append(";");
        ctx.addQuoted(s);

        OwnedHqlExpr count = translator.getBoundCount(boundDs);
        s.clear().append("end = cur+");
        translator.generateExprCpp(s, count).append(";");
        ctx.addQuoted(s);
        ctx.addQuoted("return (cur < end) ? *cur : NULL;");
    }
    else
    {
        OwnedHqlExpr address = getPointer(boundDs.expr);
        StringBuffer s;
        s.clear().append("cur = (byte *)");     // more: should really be const...
        translator.generateExprCpp(s, address).append(";");
        ctx.addQuoted(s);

        OwnedHqlExpr length = translator.getBoundLength(boundDs);
        s.clear().append("end = cur+");
        translator.generateExprCpp(s, length).append(";");
        ctx.addQuoted(s);
        ctx.addQuoted("return (cur < end) ? cur : NULL;");
    }
}

void CompoundIteratorBuilder::createRawNextFunc(BuildCtx & ctx, IHqlExpression * expr, CursorArray & cursors)
{
    if (isArrayRowset(expr->queryType()))
    {
        ctx.addQuoted("cur++;");
        ctx.addQuoted("return (cur < end) ? *cur : NULL;");
    }
    else
    {
        bindParentCursors(ctx, cursors);
        translator.bindTableCursor(ctx, expr, "cur");

        CHqlBoundExpr bound;
        translator.getRecordSize(ctx, expr, bound);

        StringBuffer s;
        s.clear().append("cur+=");
        translator.generateExprCpp(s, bound.expr).append(";");
        ctx.addQuoted(s);
        ctx.addQuoted("return (cur < end) ? cur : NULL;");
    }
}

//------------------------------------------------------------------------------------------

