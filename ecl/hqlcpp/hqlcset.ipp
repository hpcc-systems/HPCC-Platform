/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#ifndef __HQLCSET_IPP_
#define __HQLCSET_IPP_

class BaseDatasetCursor : public CInterface, implements IHqlCppDatasetCursor
{
public:
    BaseDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr * _boundDs);
    IMPLEMENT_IINTERFACE

    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual void buildIterateClass(BuildCtx & ctx, SharedHqlExpr & iter, SharedHqlExpr & row);
    virtual BoundRow * buildSelectNth(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual BoundRow * buildSelectMap(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildInDataset(BuildCtx & ctx, IHqlExpression * inExpr, CHqlBoundExpr & tgt);
    virtual void buildIterateMembers(BuildCtx & declarectx, BuildCtx & initctx);
    virtual void buildCountDict(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExistsDict(BuildCtx & ctx, CHqlBoundExpr & tgt);

    using IHqlCppDatasetCursor::buildIterateClass;
protected:
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
    virtual BoundRow * buildSelectNth(BuildCtx & ctx, IHqlExpression * indexExpr);

protected:
    BoundRow * buildSelectFirst(BuildCtx & ctx, IHqlExpression * indexExpr, bool createDefaultRowIfNull);
};

class InlineLinkedDatasetCursor : public BaseDatasetCursor
{
public:
    InlineLinkedDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak) { return doBuildIterateLoop(ctx, needToBreak, false); }
    virtual BoundRow * buildSelectNth(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx);

protected:
    BoundRow * doBuildIterateLoop(BuildCtx & ctx, bool needToBreak, bool checkForNull);
};

class StreamedDatasetCursor : public BaseDatasetCursor
{
public:
    StreamedDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak) { return doBuildIterateLoop(ctx, needToBreak, false); }
    virtual BoundRow * buildSelectNth(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx);

protected:
    BoundRow * doBuildIterateLoop(BuildCtx & ctx, bool needToBreak, bool checkForNull);
};

class InlineLinkedDictionaryCursor : public InlineLinkedDatasetCursor
{
public:
    InlineLinkedDictionaryCursor(HqlCppTranslator & _translator, IHqlExpression * _ds, CHqlBoundExpr & _boundDs);

    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual BoundRow * buildSelectMap(BuildCtx & ctx, IHqlExpression * indexExpr);
    virtual void buildInDataset(BuildCtx & ctx, IHqlExpression * inExpr, CHqlBoundExpr & tgt);
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx) { throwUnexpected(); }
    virtual void buildCountDict(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExistsDict(BuildCtx & ctx, CHqlBoundExpr & tgt);

private:
    IHqlExpression * getFirstSearchValue(IHqlExpression * searchExpr, IHqlExpression * searchRecord);
};

class MultiLevelDatasetCursor : public BaseDatasetCursor
{
public:
    MultiLevelDatasetCursor(HqlCppTranslator & _translator, IHqlExpression * _ds);

    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt);
    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak);
    virtual BoundRow * buildSelectNth(BuildCtx & ctx, IHqlExpression * indexExpr);
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
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput, IAtom * serializeForm);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);

protected:
    void doFinishRow(BuildCtx & ctx, BoundRow * selfCursor, IHqlExpression *size);

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
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput, IAtom * serializeForm) { throwUnexpected(); }
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);

protected:
    OwnedHqlExpr record;
    Owned<BoundRow> row;
    Owned<BoundRow> cursor;
};

class LinkedDatasetBuilderBase : public DatasetBuilderBase
{
public:
    LinkedDatasetBuilderBase(HqlCppTranslator & _translator, IHqlExpression * _record);

    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual bool buildLinkRow(BuildCtx & ctx, BoundRow * sourceRow);
    virtual bool buildAppendRows(BuildCtx & ctx, IHqlExpression * expr);
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor);
};

class LinkedDatasetBuilder : public LinkedDatasetBuilderBase
{
public:
    LinkedDatasetBuilder(HqlCppTranslator & _translator, IHqlExpression * _record, IHqlExpression * _choosenLimit);

    virtual void buildDeclare(BuildCtx & ctx);
    virtual bool isRestricted() { return choosenLimit != NULL; }

protected:
    LinkedHqlExpr choosenLimit;
};

class LinkedDictionaryBuilder : public LinkedDatasetBuilderBase
{
public:
    LinkedDictionaryBuilder(HqlCppTranslator & _translator, IHqlExpression * _record);

    virtual void buildDeclare(BuildCtx & ctx);
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
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput, IAtom * serializeForm) { throwUnexpected(); }
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
