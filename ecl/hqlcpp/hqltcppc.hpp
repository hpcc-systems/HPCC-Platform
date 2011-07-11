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
#ifndef __HQLTCPPC_HPP_
#define __HQLTCPPC_HPP_

#include "hqlattr.hpp"

class ColumnToOffsetMap;
class HQLCPP_API BoundRow : public HqlExprAssociation, public IInterface
{
public:
    BoundRow(const BoundRow & other, IHqlExpression * _newBound);                                                       // other row
    BoundRow(BoundRow * row, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq);
    BoundRow(IHqlExpression * _dataset, IHqlExpression * _bound, ColumnToOffsetMap * _columnMap);                       // row
    BoundRow(IHqlExpression * _dataset, IHqlExpression * _bound, ColumnToOffsetMap * _columnMap, node_operator side, IHqlExpression * selSeq);  // cursor
    ~BoundRow();
    IMPLEMENT_IINTERFACE

    virtual AssocKind getKind()             { return (AssocKind)kind; }
    virtual bool isRowAssociation()         { return true; }
    virtual IHqlExpression * queryExpr() const  { return bound; }

    virtual BoundRow * clone(IHqlExpression * _newBound)    { return new BoundRow(*this, _newBound); }
    virtual IHqlExpression * getMappedSelector(BuildCtx & ctx, IReferenceSelector * selector);
    virtual IHqlExpression * getFinalFixedSizeExpr();
    
//meta information about the cursor...
    virtual bool isBinary();
    virtual bool isInherited() const        { return inherited; }           // inherited in any way.
    virtual bool beenSerialized() const     { return false; }           // serialized at some point in the parent row tree.
    virtual bool isSerialization() const    { return false; }           // a serialized row (not added to nonlocal children)
    virtual bool isNonLocal() const         { return false; }           // is this on demand? i.e. being serialized at this level.
    virtual IHqlExpression * queryAliasExpansion() { return NULL; }
    virtual ActivityInstance * queryActivity() { return NULL; }

    inline bool isConstant() const          { return false; }
    inline bool isModifyable() const        { return !isConstant() && !isInherited(); }

    inline bool isBuilder() const           { return hasModifier(bound->queryType(), typemod_builder); }
    inline bool isLinkCounted() const       { return hasLinkCountedModifier(bound->queryType()); }
    inline bool isConditional() const       { return conditional; }
    inline bool isResultAlias() const       { return resultAlias; }
    inline IHqlExpression * queryBound() const { return bound; }
    inline IHqlExpression * queryBuilder() const { return builder; }
    inline IHqlExpression * queryDataset() const { return dataset; }
    inline IHqlExpression * queryRecord() const { return dataset->queryRecord(); }
    inline node_operator querySide() const  { return side; }
    inline IHqlExpression * querySelector() const { return represents; }
    inline IHqlExpression * querySelSeq() const { return (side != no_none) ? represents->queryChild(1) : NULL; }

    inline void setBuilder(IHqlExpression * value)  { builder.set(value); }
    inline void setResultAlias()            { resultAlias = true; }

    AColumnInfo * queryRootColumn();
    inline void setConditional(bool _value)     { conditional = _value; }
    inline void setInherited(bool _value)       { inherited = _value; }
    unsigned getMaxSize();

    IHqlExpression * bindToRow(IHqlExpression * expr, IHqlExpression * exprSelector);
    void setAlias(IReferenceSelector * selector)                { alias.set(selector); }
    IReferenceSelector * queryAlias()                           { return alias; }
    IHqlExpression * queryBuilderEnsureMarker();

protected:
    HqlExprAttr                 dataset;
    HqlExprAttr                 bound;
    HqlExprAttr                 builder;
    HqlExprAttr                 builderEnsureMarker;
    ColumnToOffsetMap *         columnMap;
    Owned<IReferenceSelector>   alias;
    node_operator               side;
#ifdef _DEBUG
    AssocKind                   kind;
#else
    byte                        kind;
#endif
    bool                        conditional;
    bool                        resultAlias;
    bool                        inherited;
};


//---------------------------------------------------------------------------

//Inherited row, parent is a simple row, so only need to add a colocal-> prefix onto the bound cursor name.
class CoLocalSimpleRow : public BoundRow
{
public:
    CoLocalSimpleRow(const BoundRow & other, IHqlExpression * _newBound) : BoundRow(other, _newBound) { inherited = true; }

    virtual BoundRow * clone(IHqlExpression * _newBound)    { return new CoLocalSimpleRow(*this, _newBound); }
};

class DynamicColumnToOffsetMap;
class SerializationRow : public BoundRow
{
public:
    static SerializationRow * create(HqlCppTranslator & _translator, IHqlExpression * _bound, ActivityInstance * activity);

    virtual BoundRow * clone(IHqlExpression * _newBound);
    virtual IHqlExpression * getFinalFixedSizeExpr()        { return LINK(finalFixedSizeExpr); }
    virtual bool isSerialization() const                    { return true; }
    virtual ActivityInstance * queryActivity()              { return activity; }

    IHqlExpression * createField(_ATOM name, ITypeInfo * type);         // returns a no_select reference to the field
    IHqlExpression * addSerializedValue(IHqlExpression * path, ITypeInfo * type, IHqlExpression * colocal, bool isConditional);
    IHqlExpression * ensureSerialized(IHqlExpression * path, IHqlExpression * colocal, bool isConditional);
    IHqlExpression * ensureSerialized(BuildCtx & ctx, IHqlExpression * colocal, IReferenceSelector * selector);
    void finalize();
    unsigned numFields() const;
    IHqlExpression * queryRecord();
    void setBuilder(ParentExtract * _builder)       { extractBuilder = _builder; }


protected:
    SerializationRow(HqlCppTranslator & _translator, IHqlExpression * _dataset, IHqlExpression * _bound, DynamicColumnToOffsetMap * _columnMap, ActivityInstance * _activity);

protected:
    MapOwnedToOwned<IHqlExpression, IHqlExpression> mapping;
    ParentExtract * extractBuilder;
    HqlCppTranslator & translator;
    DynamicColumnToOffsetMap * serializedMap;
    OwnedHqlExpr finalFixedSizeExpr;
    ActivityInstance * activity;
    OwnedHqlExpr record;
};

class NonLocalIndirectRow : public BoundRow
{
public:
    NonLocalIndirectRow(const BoundRow & other, IHqlExpression * _newBound, SerializationRow * _serialization);

    virtual BoundRow * clone(IHqlExpression * _newBound)    { UNIMPLEMENTED; }
    virtual IHqlExpression * getMappedSelector(BuildCtx & ctx, IReferenceSelector * selector);
    virtual bool isInherited() const                        { return true; } 
    virtual bool isNonLocal() const                         { return true; }

protected:
    SerializationRow * serialization;
};


//Inherited row, parent is a simple row, so only need to add a colocal-> prefix onto the bound cursor name.
class BoundAliasRow : public BoundRow
{
public:
    BoundAliasRow(const BoundRow & other, IHqlExpression * _newBound, IHqlExpression * _expansion) : BoundRow(other, _newBound) { expansion.set(_expansion); }
    BoundAliasRow(const BoundAliasRow & other, IHqlExpression * _newBound) : BoundRow(other, _newBound) { expansion.set(other.expansion); }

    virtual BoundRow * clone(IHqlExpression * _newBound)    { return new BoundAliasRow(*this, _newBound); }
    virtual IHqlExpression * queryAliasExpansion() { return expansion; }

protected:
    HqlExprAttr expansion;
};


extern bool canReadFromCsv(IHqlExpression * record);
extern bool isSimpleLength(IHqlExpression * expr);


#endif
