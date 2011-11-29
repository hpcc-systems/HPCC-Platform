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
#ifndef __HQLHTCPP_IPP_
#define __HQLHTCPP_IPP_

#include "eclhelper.hpp"
#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

#include "hqlcpp.hpp"
#include "hqltrans.ipp"

//===========================================================================

class HQLCPP_API ThorBoundActivity : public ABoundActivity
{
public:
    ThorBoundActivity(IHqlExpression * _dataset, IHqlExpression * _bound, unsigned _activityid, unsigned _graphId, ThorActivityKind _kind) 
    : ABoundActivity(_dataset->queryBody(), _bound, _activityid, _graphId, _kind) {}
};

//===========================================================================

class MetaInstance
{
public:
    //Shouldn't really need to pass translator, but it provides a place to have a per-query unique id. Anything else seems even messier.
    MetaInstance()  { dataset = NULL; }
    MetaInstance(HqlCppTranslator & translator, IHqlExpression * _dataset);
    IHqlExpression * queryRecord();
    void setDataset(HqlCppTranslator & translator, IHqlExpression * _dataset);
    IHqlExpression * getMetaUniqueKey()     { return searchKey.getLink(); }
    const char * queryInstanceObject()      { return instanceObject ? instanceObject : instanceName; }

public:
    IHqlExpression * dataset;
    HqlExprAttr      searchKey;
    StringAttr       metaName;
    StringAttr       instanceName;
    StringAttr       metaFactoryName;
    StringAttr       instanceObject;
};

//===========================================================================

class SteppingFieldSelection
{
public:
    void clear() { ds.clear(); fields.clear(); }
    bool exists() { return ds != NULL; }
    void expandTransform(IHqlExpression * expr);
    void extractFields(SteppingFieldSelection & steppingFields);
    void generateSteppingMetaMember(HqlCppTranslator & translator, BuildCtx & ctx, const char * name);
    IHqlExpression * invertTransform(IHqlExpression * expr, IHqlExpression * select);
    void set(IHqlExpression * _ds, IHqlExpression * _fields);
    void setStepping(IHqlExpression * expr);

private:
    IHqlExpression * extractSelect(IHqlExpression * expr);
    void gatherFieldOffsetSizes(HqlCppTranslator & translator, UnsignedArray & result);
    IHqlExpression * generateSteppingMeta(HqlCppTranslator & translator);

public:
    LinkedHqlExpr ds;
    LinkedHqlExpr fields;
};


//===========================================================================

//MORE: I should derive the following and ActivityInstance from a common base class
class GlobalClassEvalContext;
class GlobalClassBuilder
{
public:
    GlobalClassBuilder(HqlCppTranslator & _translator, BuildCtx & ctx, const char * className, const char * baseName, const char * _accessorInterface);

    void buildClass(unsigned priority=0);
    void completeClass(unsigned priority=0);

    inline void setIncomplete(bool value) { classStmt->setIncomplete(value); }
    inline void setIncluded(bool value) { classStmt->setIncluded(value); }

public:
    HqlCppTranslator & translator;
    BuildCtx classctx;
    BuildCtx nestedctx;
    BuildCtx createctx;
    BuildCtx startctx;
    IHqlStmt * classStmt;
    Owned<ParentExtract> parentExtract;
    Owned<EvalContext> parentEvalContext;
    Owned<GlobalClassEvalContext> evalContext;
    IHqlStmt *  onCreateStmt;
    unsigned    onCreateMarker;

    StringAttr className;
    StringAttr baseName;
    StringAttr accessorInterface;
    StringAttr accessorName;
};


class JoinKeyInfo;
class ActivityInstance;
class NlpParseContext;
extern StringBuffer &expandLiteral(StringBuffer &s, const char *f);
class ActivityEvalContext;
class ActivityInstance : public HqlExprAssociation
{
public:
    ActivityInstance(HqlCppTranslator & _translator, BuildCtx & ctx, ThorActivityKind _kind, IHqlExpression * _dataset, const char * activityArgName);
    ~ActivityInstance();

// HqlExprAssociation
    virtual AssocKind getKind()                     { return AssocActivityInstance; }

    ABoundActivity  * queryBoundActivity()          { return table; }
    ABoundActivity  * getBoundActivity();
    bool isChildActivity()                          { return (containerActivity != NULL); }
    bool         isExternal();
    inline bool isAction() { return dataset->isAction(); }
    void         setLocal(bool value=true)          { isLocal = value; }
    void         setGrouped(bool value=true)        { isGrouped = value; }

    IHqlDelayedCodeGenerator * createOutputCountCallback() { return table->createOutputCountCallback(); }

    void buildPrefix();
    void buildSuffix();
    void buildMetaMember();

    void addAttribute(const char * name, const char * value);
    void addAttributeInt(const char * name, __int64 value);
    void addAttributeBool(const char * name, bool value);
    void addLocationAttribute(IHqlExpression * location);
    void addNameAttribute(IHqlExpression * location);
    void removeAttribute(const char * name);

    void createGraphNode(IPropertyTree * subGraph, bool isRoot);
    ParentExtract * createNestedExtract();
    void addBaseClass(const char * name, bool needLinkOverride);
    void addConstructorParameter(IHqlExpression * expr) { constructorArgs.append(*LINK(expr)); }
    void addConstructorMetaParameter();

    void processAnnotation(IHqlExpression * annotation);
    void processAnnotations(IHqlExpression * expr);
    void processHints(IHqlExpression * hintAttr);
    void processSection(IHqlExpression * hintAttr);

    BuildCtx &   onlyEvalOnceContext();
    inline IPropertyTree * querySubgraphNode() { return subgraph ? subgraph->tree.get() : NULL; }
    inline void setImplementationClass(_ATOM name) { implementationClassName = name; }
    void setInternalSink(bool value);

    void changeActivityKind(ThorActivityKind newKind);

protected:
    void noteChildActivityLocation(IHqlExpression * pass);
    void moveDefinitionToHeader();
    void processHint(IHqlExpression * attr);

public:
    HqlCppTranslator & translator;
    unsigned     activityId;
    ThorActivityKind kind;
    HqlExprAttr  dataset;
    LinkedHqlExpr sourceFileSequence;
    StringAttr   activityArgName;
    StringAttr   className;
    StringAttr   factoryName;
    StringAttr   instanceName;
    StringAttr   argsName;
    StringBuffer graphEclText;
    StringAttr   graphLabel;
    StringBuffer baseClassExtra;
    MetaInstance meta;
    _ATOM        implementationClassName;
    ABoundActivity* table;
    bool         isMember;
    bool         instanceIsLocal;
    bool         isCoLocal;
    bool         executedRemotely;
    bool         includedInHeader;
    bool         isLocal;
    bool         isGrouped;
    bool         hasChildActivity;
    GraphLocalisation activityLocalisation;
    ActivityInstance * containerActivity;
    Owned<ParentExtract> parentExtract;
    Owned<EvalContext> parentEvalContext;
    IHqlStmt *  onCreateStmt;
    unsigned    onCreateMarker;
    HqlExprArray constructorArgs;
    HqlExprCopyArray names;
    LocationArray locations;

    Linked<IPropertyTree> graphNode;
    IHqlStmt *   classStmt;
    IHqlStmt *   classGroupStmt;
    BuildCtx    classctx;
    BuildCtx    createctx;
    BuildCtx    startctx;
    BuildCtx    nestedctx;
    BuildCtx    onstartctx;
    Owned<ActivityEvalContext> evalContext;
    OwnedHqlExpr colocalMember;
    Owned<ParentExtract> nestedExtract;
    SubGraphInfo * subgraph;
};


unsigned getVirtualFieldSize(IHqlExpression * record);
IHqlExpression * getHozedKeyValue(IHqlExpression * _value);
IHqlExpression * getHozedBias(ITypeInfo * type);
IHqlExpression * convertIndexPhysical2LogicalValue(IHqlExpression * cur, IHqlExpression * physicalSelect, bool allowTranslate);
bool requiresHozedTransform(ITypeInfo * type);
bool isKeyableType(ITypeInfo * type);
IHqlExpression * getFilepos(IHqlExpression * dataset, bool isLocal);


class ReferenceSelector : public CInterface, implements IReferenceSelector
{
public:
    ReferenceSelector(HqlCppTranslator & _translator);
    IMPLEMENT_IINTERFACE


protected:
    HqlCppTranslator &      translator;
};

enum CDtype { CDTnone, CDTcount, CDTmarker, CDTterminator, CDThole, CDTrow, CDTlocal };
class DatasetSelector : public ReferenceSelector
{
public:
    DatasetSelector(HqlCppTranslator & _translator, BoundRow * _cursor, IHqlExpression * _path = NULL);
    ~DatasetSelector();

    virtual void assignTo(BuildCtx & ctx, const CHqlBoundTarget & target);
    virtual void buildAddress(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual void buildClear(BuildCtx & ctx, int direction);
    virtual void get(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual void getOffset(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual void getSize(BuildCtx & ctx, CHqlBoundExpr & bound);
    virtual size32_t getContainerTrailingFixed();
    virtual bool isBinary();
    virtual bool isConditional();
    virtual bool isRoot();
    virtual AColumnInfo * queryColumn();
    virtual IHqlExpression * queryExpr();
    virtual BoundRow * queryRootRow();
    virtual BoundRow * getRow(BuildCtx & ctx);
    virtual ITypeInfo * queryType();
    virtual void modifyOp(BuildCtx & ctx, IHqlExpression * expr, node_operator op);
    virtual void set(BuildCtx & ctx, IHqlExpression * expr);
    virtual void setRow(BuildCtx & ctx, IReferenceSelector * rhs);
    virtual IReferenceSelector * select(BuildCtx & ctx, IHqlExpression * selectExpr);

    virtual void buildDeserialize(BuildCtx & ctx, IHqlExpression * helper);
    virtual void buildSerialize(BuildCtx & ctx, IHqlExpression * helper);

private:
    DatasetSelector(DatasetSelector * _parent, BoundRow * _cursor, AColumnInfo * _column, IHqlExpression * _path);

    DatasetSelector * createChild(BoundRow * _cursor, AColumnInfo * newColumn, IHqlExpression * newPath);
    bool isDataset();
    IHqlExpression * resolveChildDataset(IHqlExpression * searchDataset) const;
    AColumnInfo * resolveField(IHqlExpression * search) const;

protected:
    IReferenceSelector * parent;
    BoundRow *      row;
    AColumnInfo *   column;
    HqlExprAttr     path;
    bool            matchedDataset;
};

void extractAtmostArgs(IHqlExpression * atmost, SharedHqlExpr & atmostCond, SharedHqlExpr & atmostLimit);

IHqlExpression * extractFilterConditions(HqlExprAttr & invariant, IHqlExpression * expr, IHqlExpression * dataset, bool spotCSE);
bool isLibraryScope(IHqlExpression * expr);
extern IHqlExpression * constantMemberMarkerExpr;

#endif
