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
#ifndef __HQLITER_IPP_
#define __HQLITER_IPP_

class TransformSequenceBuilder
{
public:
    TransformSequenceBuilder(HqlCppTranslator & _translator, IHqlExpression * failValue) : translator(_translator) 
    {
        assertex(failValue);
        failedFilterValue.set(failValue);
    }

    void buildSequence(BuildCtx & ctx, BuildCtx * declarectx, IHqlExpression * expr);

protected:
    HqlCppTranslator & translator;
    HqlExprAttr failedFilterValue;
};


class CompoundIteratorBuilder
{
public:
    CompoundIteratorBuilder(HqlCppTranslator & _translator, BuildCtx & _nestedctx, BuildCtx & _declarectx) : translator(_translator), nestedctx(_nestedctx), declarectx(_declarectx) {}

    void buildCompoundIterator(BuildCtx & initctx, HqlExprArray & iterators, CursorArray & cursors);
    void createSingleLevelIterator(StringBuffer & iterName, StringBuffer & cursorName, IHqlExpression * expr, CursorArray & cursors);

protected:
    void bindParentCursors(BuildCtx & ctx, CursorArray & cursors);
    void createRawFirstFunc(BuildCtx & ctx, IHqlExpression * expr, CursorArray & cursors);
    void createRawNextFunc(BuildCtx & ctx, IHqlExpression * expr, CursorArray & cursors);
    void createSingleIterator(StringBuffer & iterName, IHqlExpression * expr, CursorArray & cursors);

protected:
    HqlCppTranslator & translator;
    BuildCtx & declarectx;
    BuildCtx & nestedctx;
};


bool isSequenceRoot(IHqlExpression * expr);
bool canBuildSequenceInline(IHqlExpression * expr);
IHqlExpression * gatherSelectorLevels(HqlExprArray & iterators, IHqlExpression * expr);

#endif
