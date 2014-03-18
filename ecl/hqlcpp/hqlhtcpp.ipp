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
    ThorBoundActivity(IHqlExpression * _dataset, IHqlExpression * _bound, unsigned _activityid, unsigned _containerid, unsigned _graphId, ThorActivityKind _kind)
    : ABoundActivity(_dataset->queryBody(), _bound, _activityid, _containerid, _graphId, _kind) {}
};

//===========================================================================

class MetaInstance
{
public:
    //Shouldn't really need to pass translator, but it provides a place to have a per-query unique id. Anything else seems even messier.
    MetaInstance()  { record = NULL; grouped = false; }
    MetaInstance(HqlCppTranslator & translator, IHqlExpression * _record, bool _isGrouped);
    bool isGrouped() const { return grouped; }
    IHqlExpression * queryRecord() const { return record; }
    void setMeta(HqlCppTranslator & translator, IHqlExpression * _record, bool _isGrouped);
    IHqlExpression * getMetaUniqueKey()     { return searchKey.getLink(); }
    const char * queryInstanceObject()      { return instanceObject ? instanceObject : instanceName; }

public:
    StringAttr       metaName;
    StringAttr       instanceName;
    StringAttr       metaFactoryName;
    StringAttr       instanceObject;
private:
    HqlExprAttr      searchKey;
    IHqlExpression * record;
    bool grouped;
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
    inline void setImplementationClass(IIdAtom * name) { implementationClassName = name; }
    inline bool requiresRemoteSerialize() const { return executedRemotely; }
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
    IIdAtom *        implementationClassName;
    ABoundActivity* table;
    bool         isMember;
    bool         instanceIsLocal;
    bool         isCoLocal;
    bool         isNoAccess;
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
    IHqlStmt * classGroup;
    unsigned    initialGroupMarker;
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

    virtual void buildDeserialize(BuildCtx & ctx, IHqlExpression * helper, IAtom * serializeForm);
    virtual void buildSerialize(BuildCtx & ctx, IHqlExpression * helper, IAtom * serializeForm);

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

IHqlExpression * extractFilterConditions(HqlExprAttr & invariant, IHqlExpression * expr, IHqlExpression * dataset, bool spotCSE, bool spotCseInIfDatasetConditions);
bool isLibraryScope(IHqlExpression * expr);
extern IHqlExpression * constantMemberMarkerExpr;

#endif
