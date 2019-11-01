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
    BuildCtx & nestedctx;
    BuildCtx & declarectx;
};


bool isSequenceRoot(IHqlExpression * expr);
bool canBuildSequenceInline(IHqlExpression * expr);
IHqlExpression * gatherSelectorLevels(HqlExprArray & iterators, IHqlExpression * expr);

#endif
