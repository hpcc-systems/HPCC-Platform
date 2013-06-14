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
#ifndef __HQLCPPC_HPP_
#define __HQLCPPC_HPP_

#include "eclhelper.hpp"

//---------------------------------------------------------------------------

class HqlCppTranslator;
class BoundRow;
class CHqlBoundExpr;
class CHqlBoundTarget;
class SizeStruct;
interface IReferenceSelector;
interface IHqlDelayedCodeGenerator;
struct ReadAheadState;

//external public interface into the members of a record...
class HQLCPP_API AColumnInfo : public MappingBase 
{
public:
    virtual void buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound) = 0;
    virtual void buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target) = 0;
    virtual void buildSizeOf(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound) = 0;
    virtual void buildOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound) = 0;
    virtual void buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction) = 0;
//  virtual bool buildCount(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound) = 0;
    virtual void buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound) = 0;
    virtual size32_t getContainerTrailingFixed() = 0;
    virtual bool hasFixedOffset() = 0;
    virtual bool isConditional() = 0;
    virtual bool isFixedSize() = 0;
    virtual AColumnInfo * lookupColumn(IHqlExpression * search) = 0;
    virtual ITypeInfo * queryType() const = 0;
    virtual bool requiresTemp() = 0;
    virtual bool modifyColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value, node_operator op) = 0;
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value) = 0;
    virtual void setRow(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IReferenceSelector * value) = 0;
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm) = 0;
    virtual void buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm) = 0;
    virtual bool prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state) = 0;
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state) = 0;
};

class HQLCPP_API ABoundActivity : public CInterface, public IInterface
{
public:
    ABoundActivity(IHqlExpression * _dataset, IHqlExpression * _bound, unsigned _activityid, unsigned _containerid, unsigned _graphId, ThorActivityKind _kind)
        : represents(_dataset), bound(_bound), activityId(_activityid), containerId(_containerid), graphId(_graphId)
    {
        outputCount = 0;
        kind = _kind;
    }
    IMPLEMENT_IINTERFACE

    inline  IHqlExpression * queryBound() const { return bound; }
    inline  IHqlExpression * queryDataset() const { return represents; }
    inline unsigned queryActivityId() const { return activityId; }
    inline ThorActivityKind queryActivityKind() const { return kind; }
    inline unsigned queryContainerId() const { return containerId; }
    inline unsigned queryGraphId() const { return graphId; }
    inline unsigned nextOutputCount() { return outputCount++; }
    IHqlDelayedCodeGenerator * createOutputCountCallback();

    void updateActivityKind(ThorActivityKind newKind) { kind = newKind; }

private:
    HqlExprAttr             represents;
    HqlExprAttr             bound;
    unsigned                activityId;
    unsigned                containerId;
    unsigned                graphId;
    unsigned                outputCount;
    ThorActivityKind        kind;
};


class HQLCPP_API ActivityAssociation : public HqlExprAssociation
{
public:
    ActivityAssociation(IHqlExpression * _dataset, ABoundActivity * _activity) : HqlExprAssociation(_dataset) { activity.set(_activity); }

    virtual AssocKind getKind()            { return AssocActivity; }

public:
    Linked<ABoundActivity>  activity;
};


interface IGenDatasetIterator : public IInterface
{
public:
    virtual void buildDeclare(HqlCppTranslator & translator, BuildCtx & ctx) = 0;
    virtual void buildFirst(HqlCppTranslator & translator, BuildCtx & ctx) = 0;
    virtual void buildIsValid(HqlCppTranslator & translator, BuildCtx & ctx, CHqlBoundExpr & bound) = 0;
    virtual void buildIterateLoop(HqlCppTranslator & translator, BuildCtx & ctx) = 0;
    virtual void buildNext(HqlCppTranslator & translator, BuildCtx & ctx) = 0;
};

extern HQLCPP_API IHqlExpression * convertAddressToValue(IHqlExpression * address, ITypeInfo * columnType);

#endif
