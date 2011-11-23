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
#ifndef __HQLCSET_IPP_
#define __HQLCSET_IPP_

class BaseDatasetCursor : public CInterface, implements IHqlCppDatasetCursor
{
public:
    BaseDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr * _boundDs);
    IMPLEMENT_IINTERFACE

    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual void buildIterateClass(BuildCtx & ctx, SharedHqlExpr & iter, SharedHqlExpr & row);
    virtual BoundRow * buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildIterateMembers(BuildCtx & declarectx, BuildCtx & initctx);

protected:
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx) = 0;
    IHqlExpression * createRow(BuildCtx & ctx, const char * prefix, StringBuffer & rowName);

protected:
    HqlCppTranslator & translator;
    OwnedHqlExpr ds;
    OwnedHqlExpr record;
    CHqlBoundExpr boundDs;
};

class BlockDatasetCursor : public BaseDatasetCursor
{
public:
    BlockDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);

protected:
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx);
};

class InlineBlockDatasetCursor : public BlockDatasetCursor
{
public:
    InlineBlockDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs);

    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual BoundRow * buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr);

protected:
    BoundRow * buildSelectFirst(BuildCtx & ctx, IHqlExpression * indexExpr, bool createDefaultRowIfNull);
};

class InlineLinkedDatasetCursor : public BaseDatasetCursor
{
public:
    InlineLinkedDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual BoundRow * buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx);
};

class MultiLevelDatasetCursor : public BaseDatasetCursor
{
public:
    MultiLevelDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual BoundRow * buildSelect(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx) { UNIMPLEMENTED; }

protected:
    BoundRow * doBuildIterateLoop(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * breakVar, bool topLevel);
};

//---------------------------------------------------------------------------

class BaseSetCursor : public CInterface, implements IHqlCppSetCursor
{
public:
    BaseSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr);
    IMPLEMENT_IINTERFACE

protected:
    HqlCppTranslator & translator;
    OwnedHqlExpr expr;
};

class ListSetCursor : public BaseSetCursor
{
public:
    ListSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & tgt, bool needToBreak);
    virtual void buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt);
    virtual void buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr);
    virtual bool isSingleValued() { return expr->numChildren() == 1; }

protected:
    void gatherSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & value, HqlExprAttr & cond);

protected:
    IHqlExpression * getCountExpr();
};

class AllSetCursor : public BaseSetCursor
{
public:
    AllSetCursor(HqlCppTranslator & _translator);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & tgt, bool needToBreak);
    virtual void buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt);
    virtual void buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr);
    virtual bool isSingleValued() { return false; }
};

class GeneralSetCursor : public BaseSetCursor
{
public:
    GeneralSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr, CHqlBoundExpr & boundSet);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & tgt, bool needToBreak);
    virtual void buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt);
    virtual void buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr);
    virtual bool isSingleValued();

protected:
    void checkNotAll(BuildCtx & ctx);
    IHqlExpression * createDatasetSelect(IHqlExpression * indexExpr);

protected:
    OwnedHqlExpr element;
    OwnedHqlExpr isAll;
    Owned<IHqlCppDatasetCursor> dsCursor;
    OwnedHqlExpr ds;
};

class CreateSetCursor : public BaseSetCursor
{
public:
    CreateSetCursor(HqlCppTranslator & _translator, IHqlExpression * _expr, IHqlCppDatasetCursor * _dsCursor);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & tgt, bool needToBreak);
    virtual void buildIterateClass(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt);
    virtual void buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr);
    virtual bool isSingleValued();

protected:
    IHqlExpression * createDatasetSelect(IHqlExpression * indexExpr);

protected:
    OwnedHqlExpr value;
    Owned<IHqlCppDatasetCursor> dsCursor;
    OwnedHqlExpr ds;
};

//---------------------------------------------------------------------------

class CHqlCppDatasetBuilder : public CInterface, implements IHqlCppDatasetBuilder
{
public:
    CHqlCppDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record);
    IMPLEMENT_IINTERFACE

    virtual bool isRestricted()                             { return false; }
    virtual bool buildLinkRow(BuildCtx & ctx, BoundRow * sourceRow) { return false; }
    virtual bool buildAppendRows(BuildCtx & ctx, IHqlExpression * expr) { return false; }

protected:
    HqlCppTranslator & translator;
    LinkedHqlExpr record;
};

class DatasetBuilderBase : public CHqlCppDatasetBuilder
{
public:
    DatasetBuilderBase(HqlCppTranslator & _translator, IHqlExpression * _record, bool _buildLinkedRows);
    IMPLEMENT_IINTERFACE

    virtual BoundRow * buildCreateRow(BuildCtx & ctx);
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);

protected:
    StringBuffer instanceName;
    StringBuffer builderName;
    OwnedHqlExpr dataset;
};

class BlockedDatasetBuilder : public DatasetBuilderBase
{
public:
    BlockedDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record);

    void setLimit(IHqlExpression * _count, bool _forceLength) { count.set(_count); forceLength = _forceLength; }

    virtual void buildDeclare(BuildCtx & ctx);
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound);

    virtual bool isRestricted() { return count != NULL; }

protected:
    OwnedHqlExpr count;
    bool forceLength;
};

class SingleRowTempDatasetBuilder : public CHqlCppDatasetBuilder
{
public:
    SingleRowTempDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, BoundRow * _row);
    IMPLEMENT_IINTERFACE

    virtual void buildDeclare(BuildCtx & ctx);
    virtual BoundRow * buildCreateRow(BuildCtx & ctx);
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput) { throwUnexpected(); }
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);

protected:
    OwnedHqlExpr record;
    Owned<BoundRow> row;
    Owned<BoundRow> cursor;
};

class LinkedDatasetBuilder : public DatasetBuilderBase
{
public:
    LinkedDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, IHqlExpression * _choosenLimit);

    virtual void buildDeclare(BuildCtx & ctx);
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual bool buildLinkRow(BuildCtx & ctx, BoundRow * sourceRow);
    virtual bool buildAppendRows(BuildCtx & ctx, IHqlExpression * expr);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);
    virtual bool isRestricted()                             { return choosenLimit != NULL; }

protected:
    LinkedHqlExpr choosenLimit;
};

//---------------------------------------------------------------------------

class SetBuilder : public CInterface, implements IHqlCppSetBuilder
{
public:
    SetBuilder(HqlCppTranslator & _translator, ITypeInfo * fieldType, IHqlExpression * _allVar);
    IMPLEMENT_IINTERFACE

    virtual void buildDeclare(BuildCtx & ctx);
    virtual IReferenceSelector * buildCreateElement(BuildCtx & ctx);
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void finishElement(BuildCtx & ctx);
    virtual void setAll(BuildCtx & ctx, IHqlExpression * isAll);

protected:
    HqlCppTranslator & translator;
    OwnedHqlExpr record;
    Owned<IHqlCppDatasetBuilder> datasetBuilder;
    OwnedHqlExpr allVar;
    BoundRow * activeRow;
};

//---------------------------------------------------------------------------

class TempSetBuilder : public SetBuilder
{
public:
    TempSetBuilder(HqlCppTranslator & _translator, ITypeInfo * fieldType, IHqlExpression * _allVar);
};

class InlineSetBuilder : public SetBuilder
{
public:
    InlineSetBuilder(HqlCppTranslator & _translator, ITypeInfo * fieldType, IHqlExpression * _allVar, IHqlExpression * _size, IHqlExpression * _address);
};

//---------------------------------------------------------------------------

class InlineDatasetBuilder : public CHqlCppDatasetBuilder
{
public:
    InlineDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, IHqlExpression * _size, IHqlExpression * _address);
    IMPLEMENT_IINTERFACE

    virtual void buildDeclare(BuildCtx & ctx);
    virtual BoundRow * buildCreateRow(BuildCtx & ctx);
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput) { throwUnexpected(); }
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);

protected:
    OwnedHqlExpr size;
    OwnedHqlExpr address;
    OwnedHqlExpr dataset;
    OwnedHqlExpr cursorVar;
};


#endif
