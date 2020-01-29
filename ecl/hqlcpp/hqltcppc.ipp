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
#ifndef __HQLCPPC_IPP_
#define __HQLCPPC_IPP_

#include "hqlutil.hpp"
#include "hqlhtcpp.ipp"
#include "hqltcppc.hpp"
#include "hqlcppc.hpp"

class HqlCppTranslator;

class SizeStruct
{
public:
    SizeStruct() { fixedSize = 0; varMinSize = 0; }
    SizeStruct(IHqlExpression * _self) { self.set(_self); fixedSize = 0; varMinSize = 0; }
    SizeStruct(const SizeStruct & other)                    { fixedSize = other.fixedSize; varMinSize = other.varMinSize; varSize.set(other.varSize); self.set(other.self); }

    void add(const SizeStruct & other);
    inline void addFixed(unsigned delta)                            
    { 
        fixedSize += delta; 
    }
    void addVariableExpr(unsigned _varMinSize, IHqlExpression * expr);
    void addVariable(unsigned _varMinSize, IHqlExpression * expr);
    void buildSizeExpr(HqlCppTranslator & translator, BuildCtx & ctx, BoundRow * row, CHqlBoundExpr & bound);
    void clear(IHqlExpression * _self)                      { fixedSize = 0; varSize.clear(); self.set(_self); }
    void forceToTemp(node_operator op, IHqlExpression * selector);
    unsigned getFixedSize() const                           { return fixedSize; }
    unsigned getMinimumSize()   const                       { return fixedSize+varMinSize; }
    IHqlExpression * getSizeExpr(BoundRow * cursor) const;
    bool isEmpty() const                                    { return fixedSize == 0 && varSize == NULL; }
    bool isFixedSize() const                                { return varSize == NULL; }
    bool isWorthCommoning() const;
    IHqlExpression * queryVarSize() const                   { return varSize; }
    IHqlExpression * querySelf() const                      { return self; }
    void set(const SizeStruct & other)                      { fixedSize = other.fixedSize; varSize.set(other.varSize); self.set(other.self); }
    void set(unsigned _fixedSize, IHqlExpression * _varSize) { fixedSize = _fixedSize; varSize.set(_varSize); }

protected:
    unsigned    fixedSize;
    unsigned    varMinSize;
    HqlExprAttr varSize;
    HqlExprAttr self;
};


//---------------------------------------------------------------------------

typedef MapOf<IHqlExpression *, AColumnInfo> ColumnToInfoMap;

class CContainerInfo;
class HQLCPP_API CMemberInfo : public AColumnInfo
{
public:
    CMemberInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//IMappingBase
    virtual const void * getKey() const { return &column; }

//AColumnInfo
    virtual void buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual bool hasFixedOffset();
    virtual bool isPayloadField() const override;
    virtual AColumnInfo * lookupColumn(IHqlExpression * search);
    virtual bool requiresTemp();

//null implementation
    virtual bool isConditional();

// implementation virtuals
    virtual void calcAllCachedOffsets();
            void calcCachedOffsets(const SizeStruct & offset, SizeStruct & sizeSelf);
    virtual void calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf);
    virtual IHqlExpression * createSelectorExpr();
    virtual void gatherSize(SizeStruct & target);
            void gatherOffset(SizeStruct & target, IHqlExpression * selector);
            void getSizeExpr(SizeStruct & target);
    virtual IReferenceSelector * getSelector(BuildCtx & ctx, IReferenceSelector * parentSelector);
    virtual void noteLastBitfield() {}
    virtual IHqlExpression * queryParentSelector(IHqlExpression * selector);
    virtual IHqlExpression * queryRootSelf();
    virtual ITypeInfo * queryType() const;
    virtual unsigned getTotalFixedSize();
    virtual unsigned getTotalMinimumSize();
    virtual unsigned getContainerTrailingFixed();
    virtual bool modifyColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value, node_operator op) { return false; }
    virtual bool checkCompatibleIfBlock(HqlExprCopyArray & conditions);
    virtual bool bindOffsetsFromClass(SizeStruct & accessorOffset, bool prevVariableSize);
    virtual void bindSizesFromOffsets(SizeStruct & thisOffset, const SizeStruct & nextOffset);
    
    void addVariableSize(size32_t varMinSize, SizeStruct & size);
    void getXPath(StringBuffer & out);
    StringBuffer & expandSelectPathText(StringBuffer & out, bool isLast) const;
    IHqlExpression * queryColumn() const { return column; }
    void getOffsets(SizeStruct & offset, SizeStruct & accessorOffset) const;

public:
    IHqlExpression * getCondition(BuildCtx & ctx);
    IHqlExpression * getConditionSelect(HqlCppTranslator & translator, BuildCtx & ctx, BoundRow * row);
    IHqlExpression * makeConditional(HqlCppTranslator & translator, BuildCtx & ctx, BoundRow * row, IHqlExpression * value);

    void setOffset(bool _hasVarOffset);     // to avoid loads of arguments to constructor
    void setPayload(bool _isPayload);       // to avoid loads of arguments to constructor

protected:
    virtual ITypeInfo * queryPhysicalType();
    void associateSizeOf(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * rawSize, size32_t extraSize);
    void buildConditionFilter(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    void doBuildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IHqlExpression * boundSize);
    void doBuildSkipInput(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, size32_t size);
    void checkAssignOk(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * newLength, unsigned fixedExtra);
    void checkConditionalAssignOk(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, size32_t fixedSize);
    void defaultSetColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);
    void ensureTargetAvailable(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, size32_t thisSize);
    IHqlExpression * createSelfPeekDeserializer(HqlCppTranslator & translator, IHqlExpression * helper);
    IHqlExpression * getColumnAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, ITypeInfo * columnType, size32_t delta = 0);
    void getColumnOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & tgt);
    IHqlExpression * getColumnRef(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    void gatherMaxRowSize(SizeStruct & totalSize, IHqlExpression * newSize, size32_t fixedExtra, IReferenceSelector * selector);

    IHqlExpression * addDatasetLimits(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * _value);
    bool hasDatasetLimits() const;

protected:
    CContainerInfo *    container;
    CMemberInfo *       prior;
    HqlExprAttr         column;
    SizeStruct          cachedOffset;       // Either fixed or sizeof(x)/offsetof(x) - rebound before building
    SizeStruct          cachedSize;         // Either fixed or sizeof(x) expressions - rebound before building
    SizeStruct          cachedAccessorOffset;// A translated expression containing the (fixed, field with the offset)
    unsigned            seq; // For fields the sequence number, for root container the maximum seq so far
    bool                hasVarOffset;
    bool                isOffsetCached;
    bool                isPayload = false;
};


typedef IArrayOf<CMemberInfo> CMemberInfoArray;

struct ReadAheadState
{
public:
    ReadAheadState(IReferenceSelector * _selector, IHqlExpression * _helper) : helper(_helper), selector(_selector) {};

    void addDummyMappings()
    {
        ForEachItemIn(i, requiredValues)
            mappedValues.append(OLINK(requiredValues.item(i)));
    }

    void setMapping(unsigned setIdx, IHqlExpression * expr)
    {
        mappedValues.replace(*LINK(expr), setIdx);
    }

    LinkedHqlExpr helper;
    HqlExprArray requiredValues;
    HqlExprArray mappedValues;
    IReferenceSelector * selector;
};

class HQLCPP_API CContainerInfo : public CMemberInfo
{
public:
    CContainerInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

// AColumnInfo
    virtual void buildSizeOf(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction);
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual void buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual bool prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state);
    virtual IHqlExpression * createSelectorExpr();
    virtual void setRow(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IReferenceSelector * value);

//  kind of interface....
    virtual void calcAllCachedOffsets();
    virtual void calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf);
    virtual IHqlExpression * getCondition(BuildCtx & ctx);
    virtual IHqlExpression * getRelativeSelf() = 0;
    virtual unsigned getTotalFixedSize();
    virtual unsigned getTotalMinimumSize();
    virtual bool isConditional();
    virtual bool isFixedSize()              { return fixedSize && !isDynamic; }

    virtual bool bindOffsetsFromClass(SizeStruct & accessorOffset, bool prevVariableSize);
    virtual void bindSizesFromOffsets(SizeStruct & thisOffset, const SizeStruct & nextOffset);
    virtual bool usesAccessClass() const    { return container->usesAccessClass(); }

    void addTrailingFixed(SizeStruct & size, CMemberInfo * cur);
    void subLeadingFixed(SizeStruct & size, CMemberInfo * cur);
    void getContainerXPath(StringBuffer & out);
    unsigned nextSeq();
    inline unsigned numChildren() const     { return children.ordinality(); }
            
public:
    void addChild(CMemberInfo * child);
    void setFixedSize(bool _fixed)          { fixedSize = _fixed; }
    void setDynamic()                       { isDynamic = true; }

protected:
    virtual void registerChild(CMemberInfo * child);
    void calcCachedChildrenOffsets(const SizeStruct & startOffset, SizeStruct & sizeSelf);

protected:
    CMemberInfoArray    children;
    SizeStruct          accessorSize;// A translated expression containing the (fixed, field with the offset)
    bool                fixedSize;
    bool                isDynamic;
};


class HQLCPP_API CRecordInfo : public CContainerInfo
{
public:
    CRecordInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual AColumnInfo * lookupColumn(IHqlExpression * search);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual IHqlExpression * getRelativeSelf();
    virtual IHqlExpression * queryRootSelf();
    virtual bool usesAccessClass() const    { return container ? container->usesAccessClass() : useAccessClass; }

    void setUseAccessClass()                { useAccessClass = true; }

protected:
    virtual void registerChild(CMemberInfo * child);

protected:
    ColumnToInfoMap map;
    OwnedHqlExpr cachedSelf;
    bool useAccessClass;
};


class HQLCPP_API CIfBlockInfo : public CContainerInfo
{
public:
    CIfBlockInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction);
    virtual void buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual void buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf);
    virtual bool checkCompatibleIfBlock(HqlExprCopyArray & conditions);
    virtual IHqlExpression * getCondition(BuildCtx & ctx);
    virtual bool isConditional();
    virtual bool isFixedSize()              { return false; }
    virtual IReferenceSelector * getSelector(BuildCtx & ctx, IReferenceSelector * parentSelector);
    virtual IHqlExpression * queryParentSelector(IHqlExpression * selector);
    virtual IHqlExpression * getRelativeSelf();
    virtual unsigned getTotalFixedSize();
    virtual unsigned getTotalMinimumSize();

private:
    OwnedHqlExpr condition;
    bool alwaysPresent;
};


class HQLCPP_API CColumnInfo : public CMemberInfo
{
public:
    CColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void buildSizeOf(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction);
    virtual void buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual void buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual bool prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);
    virtual void setRow(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IReferenceSelector * value) { throwUnexpected(); }

//Virtuals for column implementation
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool isFixedSize();
    virtual void gatherSize(SizeStruct & target);

protected:
    virtual ITypeInfo * queryLogicalType()          { return queryType(); }
    virtual void buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void calcCurrentOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);

    void buildDeserializeToBuilder(HqlCppTranslator & translator, BuildCtx & ctx, IHqlCppDatasetBuilder * builder, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual void buildDeserializeChildLoop(HqlCppTranslator & translator, BuildCtx & loopctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
};

class HQLCPP_API CSpecialIntColumnInfo : public CColumnInfo
{
public:
    CSpecialIntColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
};

class HQLCPP_API CPackedIntColumnInfo : public CColumnInfo
{
public:
    CPackedIntColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual ITypeInfo * queryLogicalType()          { return queryType()->queryPromotedType(); }
    virtual void gatherSize(SizeStruct & target);
    virtual bool isFixedSize()              { return false; }
};

class HQLCPP_API CSpecialStringColumnInfo : public CColumnInfo
{
public:
    CSpecialStringColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void gatherSize(SizeStruct & target);
};

class HQLCPP_API CSpecialVStringColumnInfo : public CColumnInfo
{
public:
    CSpecialVStringColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void gatherSize(SizeStruct & target);
};

class HQLCPP_API CAlienColumnInfo : public CColumnInfo
{
public:
    CAlienColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

//Virtuals for column implementation
    virtual void buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool isFixedSize();
    virtual void gatherSize(SizeStruct & target);

protected:
    virtual ITypeInfo * queryLogicalType()          { return queryType()->queryPromotedType(); }
    virtual ITypeInfo * queryPhysicalType();
    virtual void buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);

protected:
    IHqlExpression * doBuildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper);
    IHqlExpression * getAlienGetFunction(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    ITypeInfo * getPhysicalSourceType();
    unsigned getPhysicalSize();

protected:
    OwnedHqlExpr self;          // expression that represents self - different if nested.
};

class HQLCPP_API CBitfieldContainerInfo : public CContainerInfo
{
public:
    CBitfieldContainerInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void buildClear(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, int direction);
    virtual void buildExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void calcCachedSize(const SizeStruct & offset, SizeStruct & sizeSelf);
    virtual IHqlExpression * getRelativeSelf();
    virtual IReferenceSelector * getSelector(BuildCtx & ctx, IReferenceSelector * parentSelector);
    virtual void noteLastBitfield();
    virtual IHqlExpression * queryParentSelector(IHqlExpression * selector);

public:
    CMemberInfo * lastBitfield();
    ITypeInfo * queryStorageType() const        { return column->queryType(); }

protected:
    virtual void gatherSize(SizeStruct & target);
    virtual bool isFixedSize();
};


class HQLCPP_API CBitfieldInfo : public CColumnInfo
{
public:
    CBitfieldInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

//column implementation
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual void gatherSize(SizeStruct & target);
    virtual bool isFixedSize();

//public helper functions
    virtual ITypeInfo * queryLogicalType()          { return queryType()->queryPromotedType(); }
    ITypeInfo * queryStorageType()                  { return ((CBitfieldContainerInfo *)container)->queryStorageType(); }
    void noteLastBitfield()                         { isLastBitfield = true; }
    void setBitOffset(unsigned _bitOffset);

    inline bool queryIsLastBitfield() const { return isLastBitfield; }
    inline unsigned queryBitfieldOffset() const { return bitOffset; }
    unsigned queryBitfieldPackSize() const;

private:
    unsigned            bitOffset;
    bool                isLastBitfield;
};

class HQLCPP_API CRowReferenceColumnInfo : public CColumnInfo
{
public:
    CRowReferenceColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column)
    : CColumnInfo(_container, _prior, _column)
    {
    }

//AColumnInfo
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);

    virtual void gatherSize(SizeStruct & target);
    virtual bool isFixedSize()              { return true; }
};

class HQLCPP_API CVirtualColumnInfo : public CColumnInfo
{
public:
    CVirtualColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

//Virtuals for column implementation
    virtual void buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool isFixedSize();
    virtual void gatherSize(SizeStruct & target);

protected:
    virtual void calcCurrentOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector) {}
};

class HQLCPP_API CCsvColumnInfo : public CColumnInfo
{
public:
    CCsvColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, IAtom * _encoding);

//AColumnInfo
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

//Virtuals for column implementation
    virtual void buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool isFixedSize();
    virtual void gatherSize(SizeStruct & target);

protected:
    void getName(HqlCppTranslator & translator, BuildCtx & ctx, StringBuffer & out, const char * prefix, IReferenceSelector * selector);
    IHqlExpression * getColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);

protected:
    IAtom * encoding;
};

class HQLCPP_API CXmlColumnInfo : public CColumnInfo
{
public:
    CXmlColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

//Virtuals for column implementation
    virtual void buildAddress(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);
    virtual void buildColumnAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target);
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool isFixedSize();
    virtual void gatherSize(SizeStruct & target);

protected:
    virtual void calcCurrentOffset(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector) {}
    void buildFixedStringAssign(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, const CHqlBoundTarget & target, IHqlExpression * defaultValue, IIdAtom * func);
    IHqlExpression * getCallExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    IHqlExpression * getXmlDatasetExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    IHqlExpression * getXmlSetExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);

};

//---------------------------------------------------------------------------

enum MapFormat { MapFormatBinary, MapFormatCsv, MapFormatXml };

class HQLCPP_API ColumnToOffsetMap : public MappingBase
{
public:
    ColumnToOffsetMap(IHqlExpression * _key, IHqlExpression * record, unsigned _id, unsigned _packing, unsigned _maxRecordSize, bool _translateVirtuals, bool _useAccessClass);

    virtual const void * getKey() const { return &key; }

    void init(RecordOffsetMap & map);
    unsigned getFixedRecordSize();
    bool isFixedWidth() const                       { return fixedSizeRecord; }
    unsigned getMaxSize();
    bool isMaxSizeSpecified();
    unsigned numRootFields() const                  { return root.numChildren(); }
    size32_t getTotalFixedSize()                    { return root.getTotalFixedSize(); }
    size32_t getTotalMinimumSize()                  { return root.getTotalMinimumSize(); }
    virtual MapFormat getFormat()                   { return MapFormatBinary; }
    bool queryContainsIfBlock()                     { return containsIfBlock; }
    IHqlExpression * queryRecord() const            { return record; }
    unsigned queryId() const                        { return id; }
    bool usesAccessor() const                       { return root.usesAccessClass(); }

    AColumnInfo * queryRootColumn();
    bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper);
    void buildAccessor(StringBuffer & accessorName, HqlCppTranslator & translator, BuildCtx & declarectx, IHqlExpression * selector);

protected:
    virtual CMemberInfo * addColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map, bool isPayload);
    virtual CMemberInfo * createColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map, bool isPayload);
    CMemberInfo * expandRecord(IHqlExpression * record, CContainerInfo * container, RecordOffsetMap & map, unsigned payloadFields);
    void completeActiveBitfields();
    void ensureMaxSizeCached();

protected:
    unsigned id;
    LinkedHqlExpr key;
    IHqlExpression * record;
    CMemberInfo * prior;
    BitfieldPacker packer;
    unsigned maxAlign;
    unsigned packing;
    unsigned defaultMaxRecordSize;
    unsigned cachedMaxSize;
    unsigned payloadCount = 0;
    bool cachedDefaultMaxSizeUsed;
    bool fixedSizeRecord;
    bool translateVirtuals;
    bool containsIfBlock;
    CRecordInfo root;
};


class HQLCPP_API DynamicColumnToOffsetMap : public ColumnToOffsetMap
{
    using ColumnToOffsetMap::addColumn;
public:
    DynamicColumnToOffsetMap(unsigned _maxRecordSize);

    void addColumn(IHqlExpression * column, RecordOffsetMap & map);
};


class DelayedSizeGenerator : public IHqlDelayedCodeGenerator, public CInterface
{
public:
    DelayedSizeGenerator(ColumnToOffsetMap * _map)
    {
        map.set(_map);
    }
    IMPLEMENT_IINTERFACE

//IHqlDelayedCodeGenerator
    virtual void generateCpp(StringBuffer & out) { out.append(map->getTotalFixedSize()); }

public:
    Owned<ColumnToOffsetMap> map;
};


class DelayedUnsignedGenerator : public IHqlDelayedCodeGenerator, public CInterface
{
public:
    DelayedUnsignedGenerator(unsigned & _value)
    {
        value = &_value;
    }
    IMPLEMENT_IINTERFACE

//IHqlDelayedCodeGenerator
    virtual void generateCpp(StringBuffer & out) { out.append(*value); }

public:
    unsigned * value;
};


class HQLCPP_API CsvColumnToOffsetMap : public ColumnToOffsetMap
{
public:
    CsvColumnToOffsetMap(IHqlExpression * record, unsigned _maxRecordSize, bool _translateVirtuals, IAtom * _encoding);

    virtual MapFormat getFormat() override                  { return MapFormatCsv; }
    virtual CMemberInfo * createColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map, bool isPayload) override;

protected:
    IAtom * encoding;
};

class HQLCPP_API XmlColumnToOffsetMap : public ColumnToOffsetMap
{
public:
    XmlColumnToOffsetMap(IHqlExpression * record, unsigned _maxRecordSize, bool _translateVirtuals);

    virtual MapFormat getFormat()                   { return MapFormatXml; }
    virtual CMemberInfo * createColumn(CContainerInfo * container, IHqlExpression * column, RecordOffsetMap & map, bool isPayload) override;
};

//---------------------------------------------------------------------------

class HQLCPP_API CChildSetColumnInfo : public CColumnInfo
{
public:
    CChildSetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void gatherSize(SizeStruct & target);
};


class HQLCPP_API CChildDatasetColumnInfo : public CColumnInfo
{
public:
    CChildDatasetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, RecordOffsetMap & map, unsigned defaultMaxRecordSize);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual void buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void gatherSize(SizeStruct & target);
    virtual AColumnInfo * lookupColumn(IHqlExpression * search);

protected:
    unsigned maxChildSize;
};


class HQLCPP_API CChildLimitedDatasetColumnInfo : public CColumnInfo
{
public:
    CChildLimitedDatasetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, RecordOffsetMap & map, unsigned defaultMaxRecordSize);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool prepareReadAhead(HqlCppTranslator & translator, ReadAheadState & state);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void gatherSize(SizeStruct & target);
    virtual bool isFixedSize();
    virtual AColumnInfo * lookupColumn(IHqlExpression * search);

protected:
    void setColumnFromBuilder(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlCppDatasetBuilder * builder);
    virtual void buildDeserializeChildLoop(HqlCppTranslator & translator, BuildCtx & loopctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);

protected:
    OwnedHqlExpr countField;
    OwnedHqlExpr sizeField;
    unsigned fixedChildSize;
    unsigned maxChildSize;
};

class HQLCPP_API CChildLinkedDatasetColumnInfo : public CColumnInfo
{
public:
    CChildLinkedDatasetColumnInfo(CContainerInfo * _container, CMemberInfo * _prior, IHqlExpression * _column, RecordOffsetMap & map, unsigned defaultMaxRecordSize);

//AColumnInfo
    virtual void buildColumnExpr(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, CHqlBoundExpr & bound);  // get() after conditions.
    virtual void buildDeserialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual void buildSerialize(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * helper, IAtom * serializeForm);
    virtual bool buildReadAhead(HqlCppTranslator & translator, BuildCtx & ctx, ReadAheadState & state);
    virtual IHqlExpression * buildSizeOfUnbound(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool modifyColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value, node_operator op);
    virtual void setColumn(HqlCppTranslator & translator, BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * value);

    virtual void gatherSize(SizeStruct & target);
    virtual AColumnInfo * lookupColumn(IHqlExpression * search);
    virtual bool isFixedSize()              { return true; }

protected:
    unsigned maxChildSize;
};


void ensureSimpleLength(HqlCppTranslator & translator, BuildCtx & ctx, CHqlBoundExpr & bound);
void callDeserializeGetN(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, IHqlExpression * boundSize, IHqlExpression * address);
IHqlExpression * callDeserializerGetSize(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper);
void callDeserializerSkipInputSize(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, IHqlExpression * size);
void callDeserializerSkipInputTranslatedSize(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * helper, IHqlExpression * size);

struct HQLCPP_API HqlStmtExprAssociation : public HqlExprAssociation
{
public:
    HqlStmtExprAssociation(IHqlExpression * _represents, IHqlStmt * _stmt) :
    HqlExprAssociation(_represents), stmt(_stmt) {}

    virtual AssocKind getKind() { return AssocStmt; }

public:
    IHqlStmt * stmt;
};

#endif
