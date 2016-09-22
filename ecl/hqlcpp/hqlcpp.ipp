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
#ifndef __HQLCPP_IPP_
#define __HQLCPP_IPP_

#include <stdio.h>

#include "jfile.hpp"
#include "hqlattr.hpp"
#include "hqlcpp.hpp"
#include "hqlstmt.ipp"
#include "hqlcppc.hpp"
#include "hqlres.hpp"
#include "hqlutil.hpp"
#include "hqlwcpp.hpp"
#include "hqltrans.ipp"
#include "hqlusage.hpp"
#include "eclrtl.hpp"

#ifdef _DEBUG
//#define SPOT_POTENTIAL_COMMON_ACTIVITIES
#endif

#define MAX_RECORD_SIZE     4096                // default value
#define OPTIMIZE_FUNCTION_ATTRIBUTE " OPTIMIZE"

enum GraphLocalisation {
    GraphNeverAccess,  // This variant of an activity never accesses the parent
    GraphNoAccess,  // This activity would normally access the parent, but doesn't here
    GraphCoLocal,  // activity runs on the same node as the
    GraphNonLocal,
    GraphCoNonLocal,
    GraphRemote
};

enum { 
    EclTextPrio = 1000,         // has no dependencies on anything else
    HashFunctionPrio = 1100,
    TypeInfoPrio = 1200,
    RowMetaPrio = 1500,         
    XmlTransformerPrio = 1700,
    SteppedPrio = 1800,
    SegMonitorPrio = 2000,
    RecordTranslatorPrio = BuildCtx::NormalPrio,
};

enum ExpressionFormat { 
    FormatNatural, 
    FormatBlockedDataset, 
    FormatLinkedDataset,
    FormatArrayDataset,
    FormatStreamedDataset,
    FormatMax,
};

ExpressionFormat queryNaturalFormat(ITypeInfo * type);

class HqlCppLibraryImplementation;
class HqlCppLibraryInstance;

//===========================================================================

class DatasetReference
{
public:
    DatasetReference(IHqlExpression * _ds) : ds(_ds)                        { side = no_none; }
    DatasetReference(IHqlExpression * _ds, node_operator _side, IHqlExpression * seq) : ds(_ds) 
    { 
        side = _side; 
        if (side)
            selector.setown(getSelector(side, seq)); 
    }

    inline node_operator querySide() const                                  { return side; }
    inline IHqlExpression * queryDataset() const                            { return ds; }
    IHqlExpression * getSelector(node_operator op, IHqlExpression * seq) const  { return createSelector(op, ds, seq); }
    IHqlExpression * querySelector() const;
    IHqlExpression * querySelSeq() const;
    IHqlExpression * mapCompound(IHqlExpression * expr, IHqlExpression * to) const;
    IHqlExpression * mapScalar(IHqlExpression * expr, IHqlExpression * to) const;

protected:
    LinkedHqlExpr ds;
    node_operator side;
    LinkedHqlExpr selector;
};


class LocationArray : public HqlExprArray
{
public:
    unsigned findLocation(IHqlExpression * location);
    bool queryNewLocation(IHqlExpression * location);
};


//===========================================================================

class HQLCPP_API HqlCppSection : public CInterface
{
public:
  HqlCppSection() : section(NULL),stmts(NULL) {}

public:
    IAtom *                       section;
    HqlStmts                    stmts;
};


class CppFileInfo : public CInterface
{
public:
    explicit CppFileInfo(unsigned activityId) : minActivityId(activityId), maxActivityId(activityId)
    {
    }

public:
    unsigned minActivityId;
    unsigned maxActivityId;
};

class HQLCPP_API HqlCppInstance : public IHqlCppInstance, public CInterface
{
public:
    HqlCppInstance(IWorkUnit * _workunit, const char * _wupathname);
    IMPLEMENT_IINTERFACE

    virtual HqlStmts * ensureSection(IAtom * section);
    virtual const char * queryLibrary(unsigned idx);
    virtual const char * queryObjectFile(unsigned idx);
    virtual const char * querySourceFile(unsigned idx);
    virtual HqlStmts * querySection(IAtom * section);
    virtual void flushHints();
    virtual void flushResources(const char *filename, ICodegenContextCallback * ctxCallback);
    virtual void addResource(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry=NULL, unsigned id=(unsigned)-1);
    virtual void addCompressResource(const char * type, unsigned len, const void * data, IPropertyTree *manifestEntry=NULL, unsigned id=(unsigned)-1);
    virtual void addManifest(const char *filename){resources.addManifest(filename);}
    virtual void addManifestFromArchive(IPropertyTree *archive){resources.addManifestFromArchive(archive);}
    virtual void addWebServiceInfo(IPropertyTree *wsinfo){resources.addWebServiceInfo(wsinfo);}
    virtual void getActivityRange(unsigned cppIndex, unsigned & minActivityId, unsigned & maxActivityId);
    
    bool useFunction(IHqlExpression * funcdef);
    void useInclude(const char * include);
    void useLibrary(const char * libname);
    void useObjectFile(const char * objname);
    void useSourceFile(const char * srcname);
    unsigned addStringResource(unsigned len, const char * body);
    void addHint(const char * hintXml, ICodegenContextCallback * ctxCallback);

    void processIncludes();
    void addPlugin(const char *plugin, const char *version);
        
private:
    void addPluginsAsResource();
    void appendHintText(const char * xml);

public:
    CIArray             sections;
    IArray               helpers;
    StringAttrArray     modules;
    StringAttrArray     objectFiles;
    StringAttrArray     sourceFiles;
    StringAttrArray     includes;
    CIArray             extra;
    ResourceManager     resources;
    Owned<IWorkUnit>    workunit;
    StringAttr          wupathname;
    Owned<IPropertyTree> plugins;
    Owned<IFileIOStream> hintFile;
    CIArrayOf<CppFileInfo> cppInfo;
};

//---------------------------------------------------------------------------

class HqlCppTranslator;
class BoundRow;
interface IHqlCppDatasetCursor : public IInterface
{
    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
    virtual BoundRow * buildIterateLoop(BuildCtx & ctx, bool needToBreak) = 0;
    virtual void buildIterateClass(BuildCtx & ctx, SharedHqlExpr & iter, SharedHqlExpr & row) = 0;
    virtual void buildIterateClass(BuildCtx & ctx, StringBuffer & cursorName, BuildCtx * initctx) = 0;
    virtual BoundRow * buildSelectNth(BuildCtx & ctx, IHqlExpression * indexExpr) = 0;
    virtual BoundRow * buildSelectMap(BuildCtx & ctx, IHqlExpression * indexExpr) = 0;
    virtual void buildInDataset(BuildCtx & ctx, IHqlExpression * inExpr, CHqlBoundExpr & tgt) = 0;
    virtual void buildIterateMembers(BuildCtx & declarectx, BuildCtx & initctx) = 0;
    virtual void buildCountDict(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
    virtual void buildExistsDict(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
};

interface IHqlCppSetCursor : public IInterface
{
    virtual void buildCount(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
    virtual void buildExists(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
    virtual void buildIsAll(BuildCtx & ctx, CHqlBoundExpr & tgt) = 0;
    virtual void buildIterateLoop(BuildCtx & ctx, CHqlBoundExpr & tgt, bool needToBreak) = 0;
    virtual void buildExprSelect(BuildCtx & ctx, IHqlExpression * indexExpr, CHqlBoundExpr & tgt) = 0;
    virtual void buildAssignSelect(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * indexExpr) = 0;
    virtual bool isSingleValued() = 0;
};


interface IHqlCppDatasetBuilder : public IInterface
{
public:
    virtual void buildDeclare(BuildCtx & ctx) = 0;
    virtual BoundRow * buildCreateRow(BuildCtx & ctx) = 0;                  //NB: Must be called within a addGroup(), or another un-ambiguous child context, may create a filter
    virtual BoundRow * buildDeserializeRow(BuildCtx & ctx, IHqlExpression * serializedInput, IAtom * serializeForm) = 0;
    virtual void finishRow(BuildCtx & ctx, BoundRow * selfCursor) = 0;
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target) = 0;
    virtual void buildFinish(BuildCtx & ctx, CHqlBoundExpr & bound) = 0;
    virtual bool buildLinkRow(BuildCtx & ctx, BoundRow * sourceRow) = 0;
    virtual bool buildAppendRows(BuildCtx & ctx, IHqlExpression * expr) = 0;
    virtual bool isRestricted() = 0;
};

interface IHqlCppSetBuilder : public IInterface
{
public:
    virtual void buildDeclare(BuildCtx & ctx) = 0;
    virtual IReferenceSelector * buildCreateElement(BuildCtx & ctx) = 0;        //NB: Must be called within a addGroup(), or another un-ambiguous child context, may create a filter
    virtual void buildFinish(BuildCtx & ctx, const CHqlBoundTarget & target) = 0;
    virtual void finishElement(BuildCtx & ctx) = 0;
    virtual void setAll(BuildCtx & ctx, IHqlExpression * isAll) = 0;
};


interface IHqlCodeCallback : public IInterface
{
    virtual void buildCode(HqlCppTranslator & translator, BuildCtx & ctx) = 0;
};

//---------------------------------------------------------------------------

class CheckedLengthExprAttr : public HqlExprAttr
{
public:
    inline void set(IHqlExpression * e)             { HqlExprAttr::set(e); check(); }
    inline void setown(IHqlExpression * e)              { HqlExprAttr::setown(e); check(); }
    inline void check()
    {
        if (get())
        {
            ITypeInfo * type = get()->queryType();
            assertex(type->getTypeCode() == type_int && type->getSize() == 4 && !type->isSigned());
        }
    }
};

class CHqlBoundTarget;
class HQLCPP_API CHqlBoundExpr
{
public:
    void clear()                                { expr.clear(); length.clear(); }
    bool exists() const                         { return (expr != NULL); }
    IHqlExpression * getIsAll() const;
    IHqlExpression * getComplexExpr() const;
    IHqlExpression * getTranslatedExpr() const;
    inline bool isStreamed() const              { return hasStreamedModifier(queryType()); }

    ITypeInfo * queryType() const               { return expr->queryType(); }
    void set(const CHqlBoundExpr & src)         { expr.set(src.expr); length.set(src.length); count.set(src.count); isAll.set(src.isAll); }
    void setFromTarget(const CHqlBoundTarget & target);
    void setFromTranslated(IHqlExpression * expr);

public:
    HqlExprAttr         expr;
    HqlExprAttr         count;
#ifdef _DEBUG
    CheckedLengthExprAttr length;
#else
    HqlExprAttr         length;
#endif
    HqlExprAttr         isAll;
};

class HQLCPP_API CHqlBoundTarget
{
public:
    CHqlBoundTarget() {}
    ~CHqlBoundTarget() { validate(); }

    bool extractFrom(const CHqlBoundExpr & bound);
    bool isFixedSize() const;
    IHqlExpression * getTranslatedExpr() const;
    ITypeInfo * queryType() const;
    ITypeInfo * getType() const                             { return LINK(queryType()); }
    void set(const CHqlBoundTarget& other)
    {
        length.set(other.length);
        count.set(other.count);
        expr.set(other.expr);
        isAll.set(other.isAll);
    }

    void validate() const;

public:
    HqlExprAttr         expr;
    HqlExprAttr         count;              // currently only for link counted rows
    HqlExprAttr         length;
    HqlExprAttr         isAll;              // if set location to store whether it is all or not
};


class BoundRow;
class BoundRow;
interface IReferenceSelector : public IInterface
{
public:
//code generation
//change these to inline functions 
    virtual void assignTo(BuildCtx & ctx, const CHqlBoundTarget & target) = 0;
    virtual void buildAddress(BuildCtx & ctx, CHqlBoundExpr & target) = 0;
    virtual void buildClear(BuildCtx & ctx, int direction) = 0;
    virtual void get(BuildCtx & ctx, CHqlBoundExpr & bound) = 0;
    virtual void getOffset(BuildCtx & ctx, CHqlBoundExpr & bound) = 0;
    virtual void getSize(BuildCtx & ctx, CHqlBoundExpr & bound) = 0;
    virtual size32_t getContainerTrailingFixed() = 0;
    virtual bool isBinary() = 0;
    virtual bool isConditional() = 0;
    virtual bool isRoot() = 0;
    virtual void modifyOp(BuildCtx & ctx, IHqlExpression * expr, node_operator op) = 0;
    virtual void set(BuildCtx & ctx, IHqlExpression * expr) = 0;
    virtual void setRow(BuildCtx & ctx, IReferenceSelector * rhs) = 0;

    virtual void buildDeserialize(BuildCtx & ctx, IHqlExpression * helper, IAtom * serializeForm) = 0;
    virtual void buildSerialize(BuildCtx & ctx, IHqlExpression * helper, IAtom * serializeForm) = 0;

//managing the selection
    virtual AColumnInfo * queryColumn() = 0;
    virtual BoundRow * queryRootRow() = 0;
    virtual IHqlExpression * queryExpr() = 0;
    virtual ITypeInfo * queryType() = 0;
    virtual IReferenceSelector * select(BuildCtx & ctx, IHqlExpression * selectExpr) = 0;
    virtual BoundRow * getRow(BuildCtx & ctx) = 0;
};

//---------------------------------------------------------------------------

class HqlCppCaseInfo
{
    friend class HqlCppTranslator;
public:
    HqlCppCaseInfo(HqlCppTranslator & _translator);

    void addPair(IHqlExpression * expr);
    void addPairs(HqlExprArray & pairs);
    bool buildAssign(BuildCtx & ctx, const CHqlBoundTarget & target);
    bool buildReturn(BuildCtx & ctx);
    void setCond(IHqlExpression * expr);
    void setDefault(IHqlExpression * expr);

protected:
    bool canBuildStaticList(ITypeInfo * type) { return isFixedSize(type); }

    void buildChop3Map(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & test, IHqlExpression * temp, unsigned start, unsigned end);
    void buildChop3Map(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & test);
    void buildChop2Map(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & test, unsigned start, unsigned end);
    IHqlExpression * buildIndexedMap(BuildCtx & ctx, IHqlExpression * test, unsigned lower, unsigned upper);
    void buildLoopChopMap(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & test);
    void buildIntegerSearchMap(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * est);
    void buildSwitchCondition(BuildCtx & ctx, CHqlBoundExpr & bound);
    void buildSwitchMap(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * test);
    void buildGeneralAssign(BuildCtx & ctx, const CHqlBoundTarget & target);
    void buildGeneralReturn(BuildCtx & ctx);

    bool canBuildArrayLookup(const CHqlBoundExpr & test);
    IHqlExpression * createCompareList();
    IHqlExpression * createComparisons();
    IHqlExpression * createResultsExpr(IHqlExpression * matchVar, bool canIncludeDefault, bool * includedDefault = NULL);
    void generateCompareVar(BuildCtx & ctx, IHqlExpression * target, CHqlBoundExpr & test, IHqlExpression * other);
    unsigned getNumPairs();
    bool hasLibraryChop();
    bool okToAlwaysEvaluateDefault();
    void processBranches();
    void promoteTypes();
    IHqlExpression * queryCreateSimpleResultAssign(IHqlExpression * search, IHqlExpression * resultExpr);
    bool queryBuildArrayLookup(BuildCtx & ctx, const CHqlBoundTarget & target, const CHqlBoundExpr & test);
    IHqlExpression * queryCompare(unsigned index);
    ITypeInfo * queryCompareType();
    IHqlExpression * queryReturn(unsigned index);
    void removeDuplicates();
    void sortPairs();
    void updateResultType(IHqlExpression * expr);

protected:
    HqlCppTranslator &translator;
    HqlExprAttr             cond;
    HqlExprAttr             defaultValue;
    HqlExprArray            pairs;
    HqlExprArray            originalPairs;
    bool complexCompare;
    bool constantCases;
    bool constantValues;
    OwnedITypeInfo resultType;
    OwnedITypeInfo indexType;
    OwnedITypeInfo promotedElementType;
};

//===========================================================================

enum
{
    MFremote = 1,
    MFsingle = 2,
};

class MemberFunction
{
public:
    MemberFunction(HqlCppTranslator & translator, BuildCtx & classctx, const char * text, unsigned _flags = 0);
    MemberFunction(HqlCppTranslator & translator, BuildCtx & classctx, StringBuffer & text, unsigned _flags = 0);
    ~MemberFunction();

    inline bool isExecutedOnce() const { return (flags & MFsingle) != 0; }

public:
    HqlCppTranslator & translator;
    BuildCtx ctx;
    unsigned flags;
};

//===========================================================================

class GlobalFileTracker : public IHqlDelayedCodeGenerator, public CInterface
{
public:
    GlobalFileTracker(IHqlExpression * _filename, IPropertyTree * _graphNode)
    {
        filename.set(_filename->queryBody());
        graphNode.set(_graphNode);
        usageCount = 0;
    }
    IMPLEMENT_IINTERFACE

//IHqlDelayedCodeGenerator
    virtual void generateCpp(StringBuffer & out) { out.append(usageCount); }

    bool checkMatch(IHqlExpression * searchFilename);
    void writeToGraph();

public:
    unsigned usageCount;
    OwnedHqlExpr filename;
    Owned<IPropertyTree> graphNode;
};

//===========================================================================

class WorkflowItem : public CInterface
{
    friend class WorkflowTransformer;
public:
    WorkflowItem(unsigned _wfid, node_operator _workflowOp) : wfid(_wfid), workflowOp(_workflowOp) { }
    WorkflowItem(IHqlExpression * _function);

    bool isFunction() const { return function != NULL; }
    IHqlExpression * getFunction() const;
    unsigned queryWfid() const { return wfid; }
    HqlExprArray & queryExprs() { return exprs; }

private:
    LinkedHqlExpr function;
    HqlExprArray exprs;
    UnsignedArray dependencies;
    unsigned wfid;
    node_operator workflowOp;
};

typedef CIArrayOf<WorkflowItem> WorkflowArray;

//---------------------------------------------------------------------------
typedef CIArrayOf<BoundRow> CursorArray;

enum AliasKind { NotFoundAlias, CreateTimeAlias, StartTimeAlias, RuntimeAlias };

class ActivityInstance;
class SerializationRow;
class EvalContext;
class InternalResultTracker;
//---------------------------------------------------------------------------

typedef CIArrayOf<BuildCtx> BuildCtxArray;

struct GeneratedGraphInfo : public CInterface
{
public:
    GeneratedGraphInfo(const char * _name, const char *_label) : name(_name), label(_label)
    {
        xgmml.setown(createPTree("graph"));
    }

public:
    StringAttr name, label;
    Owned<IPropertyTree> xgmml;
};

enum SubGraphType { SubGraphRoot, SubGraphRemote, SubGraphChild, SubGraphLoop };

struct SubGraphInfo : public HqlExprAssociation
{
public:
    SubGraphInfo(IPropertyTree * _tree, unsigned _id, unsigned _graphId, IHqlExpression * graphTag, SubGraphType _type);

    virtual AssocKind getKind() { return AssocSubGraph; }

public:
    Linked<IPropertyTree> tree;
    unsigned id;
    unsigned graphId;
    LinkedHqlExpr graphTag;
    SubGraphType type;
};
    
//MORE: This class is far too big and needs restructuring!!!!!


class ColumnToOffsetMap;
class RecordOffsetMap : public MapOf<IHqlExpression *, ColumnToOffsetMap>
{
public:
    ColumnToOffsetMap * queryMapping(IHqlExpression * record, unsigned maxRecordSize);
};

class ExprExprMap : public MapOwnedToOwned<IHqlExpression, IHqlExpression>
{
};

class SubStringInfo;
class MetaInstance;
class KeyedJoinInfo;
class HqlMapTransformer;

class TransformBuilder;

struct HqlCppOptions
{
    unsigned            defaultImplicitKeyedJoinLimit;
    unsigned            defaultImplicitIndexReadLimit;
    unsigned            optimizeDiskFlag;
    unsigned            activitiesPerCpp;
    unsigned            maxRecordSize;
    unsigned            inlineStringThreshold;
    unsigned            maxRootMaybeThorActions;
    unsigned            maxLocalRowSize;
    unsigned            insertProjectCostLevel;
    unsigned            dfaRepeatMax;
    unsigned            dfaRepeatMaxScore;
    unsigned            debugNlp;
    unsigned            regexVersion;
    unsigned            parseDfaComplexity;
    unsigned            resourceMaxMemory;
    unsigned            resourceMaxSockets;
    unsigned            resourceMaxActivities;
    unsigned            resourceMaxHeavy;
    unsigned            resourceMaxDistribute;
    unsigned            filteredReadSpillThreshold;
    unsigned            topnLimit;
    unsigned            specifiedClusterSize;
    unsigned            globalFoldOptions;
    unsigned            minimizeSpillSize;
    unsigned            applyInstantEclTransformationsLimit;
    unsigned            complexClassesThreshold;
    unsigned            complexClassesActivityFilter;
    unsigned            subgraphToRegeneate;
    unsigned            defaultPersistExpiry;
    unsigned            defaultExpiry;
    int                 defaultNumPersistInstances;
    CompilerType        targetCompiler;
    DBZaction           divideByZeroAction;
    bool                peephole;
    bool                foldConstantCast;
    bool                optimizeBoolReturn;
    bool                freezePersists;
    bool                checkRoxieRestrictions;
    bool                checkThorRestrictions;
    bool                allowCsvWorkunitRead;
    bool                evaluateCoLocalRowInvariantInExtract;
    bool                allowInlineSpill;
    bool                spanMultipleCpp;
    bool                optimizeGlobalProjects;
    bool                optimizeResourcedProjects;
    byte                notifyOptimizedProjects;
    bool                checkAsserts;
    bool                assertSortedDistributed;
    bool                optimizeLoopInvariant;
    bool                warnOnImplicitJoinLimit;
    bool                warnOnImplicitReadLimit;
    bool                commonUpChildGraphs;
    bool                detectAmbiguousSelector;
    bool                allowAmbiguousSelector;
    bool                regressionTest;
    bool                recreateMapFromIf;
    bool                addTimingToWorkunit;
    bool                reduceNetworkTraffic;
    bool                optimizeProjectsPreservePersists;
    bool                showMetaText;
    bool                resourceConditionalActions;
    bool                resourceSequential;
    bool                workunitTemporaries;
    bool                minimizeWorkunitTemporaries;
    bool                pickBestEngine;
    bool                groupedChildIterators;
    bool                convertJoinToLookup;
    bool                convertJoinToLookupIfSorted;
    bool                spotCSE;
    bool                spotCseInIfDatasetConditions;
    bool                noAllToLookupConversion;
    bool                optimizeNonEmpty;
    bool                allowVariableRoxieFilenames;
    bool                notifyWorkflowCse;
    bool                performWorkflowCse;
    bool                foldConstantDatasets;
    bool                hoistSimpleGlobal;
    bool                percolateConstants;
    bool                percolateFilters;
    bool                usePrefetchForAllProjects;
    bool                allFilenamesDynamic;
    bool                optimizeSteppingPostfilter;
    bool                moveUnconditionalActions;
    bool                paranoidCheckNormalized;
    bool                paranoidCheckDependencies;
    bool                preventKeyedSplit;
    bool                preventSteppedSplit;
    bool                canGenerateSimpleAction;
    bool                minimizeActivityClasses;
    bool                minimizeSkewBeforeSpill;
    bool                createSerializeForUnknownSize;
    bool                implicitLinkedChildRows;
    bool                mainRowsAreLinkCounted;
    bool                allowSections;
    bool                autoPackRecords;
    bool                commonUniqueNameAttributes;
    bool                sortIndexPayload;
    bool                foldFilter;
    bool                finalizeAllRows;
    bool                optimizeGraph;
    bool                orderDiskFunnel;
    bool                alwaysAllowAllNodes;
    bool                slidingJoins;
    bool                foldOptimized;
    bool                globalOptimize;
    bool                applyInstantEclTransformations;
    bool                calculateComplexity;
    bool                generateLogicalGraph;
    bool                generateLogicalGraphOnly;
    bool                globalAutoHoist;
    bool                expandRepeatAnyAsDfa;
    bool                unlimitedResources;
    bool                allowThroughSpill;
    bool                minimiseSpills;
    bool                spillMultiCondition;
    bool                spotThroughAggregate;
    bool                hoistResourced;
    bool                maximizeLexer;
    bool                foldStored;
    bool                spotTopN;
    bool                groupAllDistribute;
    bool                spotLocalMerge;
    bool                spotPotentialKeyedJoins;
    bool                combineTrivialStored;
    bool                combineAllStored;
    bool                allowStoredDuplicate;
    bool                allowScopeMigrate;
    bool                supportFilterProject;
    bool                normalizeExplicitCasts;
    bool                optimizeInlineSource;
    bool                optimizeDiskSource;
    bool                optimizeIndexSource;
    bool                optimizeChildSource;
    bool                reportLocations;
    bool                debugGeneratedCpp;
    bool                addFilesnamesToGraph;
    bool                normalizeLocations;
    bool                ensureRecordsHaveSymbols;
    bool                constantFoldNormalize;
    bool                constantFoldPostNormalize;
    bool                optimizeGrouping;
    bool                showMetaInGraph;
    bool                spotComplexClasses;
    bool                optimizeString1Compare;
    bool                optimizeSpillProject;
    bool                expressionPeephole;
    bool                optimizeIncrement;
    bool                supportsMergeDistribute;
    bool                debugNlpAsHint;
    bool                forceVariableWuid;
    bool                okToDeclareAndAssign;       // long time ago gcc had problems doing this for very complex functions
    bool                noteRecordSizeInGraph;
    bool                convertRealAssignToMemcpy;
    bool                allowActivityForKeyedJoin;
    bool                forceActivityForKeyedJoin;
    bool                addLibraryInputsToGraph;
    bool                showRecordCountInGraph;
    bool                serializeRowsetInExtract;
    bool                testIgnoreMaxLength;
    bool                trackDuplicateActivities;               // for diagnosing problems with code becoming duplicated
    bool                showActivitySizeInGraph;
    bool                addLocationToCpp;
    bool                alwaysCreateRowBuilder;                 // allow paranoid check to ensure builders are built everywhere
    bool                precalculateFieldOffsets;               // useful for some queries, can be expensive
    bool                generateStaticInlineTables;
    bool                staticRowsUseStringInitializer;
    bool                convertWhenExecutedToCompound;
    bool                standAloneExe;
    bool                enableCompoundCsvRead;
    bool                optimizeNestedConditional;
    bool                createImplicitAliases;
    bool                combineSiblingGraphs;
    bool                optimizeSharedGraphInputs;
    bool                supportsSubSortActivity;  // Does the target engine support SUBSORT?
    bool                implicitSubSort;  // convert sort when partially sorted to subsort (group,sort,ungroup)
    bool                implicitBuildIndexSubSort;  // use subsort when building indexes?
    bool                implicitJoinSubSort;  // use subsort for partially sorted join inputs when possible
    bool                implicitGroupSubSort;  // use subsort if some sort conditions match when grouping
    bool                implicitGroupHashAggregate;  // convert aggregate(sort(x,a),{..},a,d) to aggregate(group(sort(x,a),a_,{},d))
    bool                implicitGroupHashDedup;
    bool                reportFieldUsage;
    bool                reportFileUsage;
    bool                recordFieldUsage;
    bool                subsortLocalJoinConditions;
    bool                projectNestedTables;
    bool                showSeqInGraph;
    bool                normalizeSelectorSequence;
    bool                removeXpathFromOutput;
    bool                canLinkConstantRows;
    bool                checkAmbiguousRollupCondition;
    bool                paranoidCheckSelects;
    bool                matchExistingDistributionForJoin;
    bool                createImplicitKeyedDistributeForJoin;
    bool                expandHashJoin;
    bool                traceIR;
    bool                preserveCaseExternalParameter;
    bool                multiplePersistInstances;
    bool                optimizeParentAccess;
    bool                expandPersistInputDependencies;
    bool                expirePersists;
    bool                actionLinkInNewGraph;
    bool                optimizeMax;
    bool                useResultsForChildSpills;
    bool                alwaysUseGraphResults;
    bool                noConditionalLinks;
    bool                reportAssertFilenameTail;
    bool                newBalancedSpotter;
    bool                keyedJoinPreservesOrder;
    bool                expandSelectCreateRow;
    bool                obfuscateOutput;
    bool                showEclInGraph;
    bool                showChildCountInGraph;
    bool                optimizeSortAllFields;
    bool                optimizeSortAllFieldsStrict;
    bool                alwaysReuseGlobalSpills;
    bool                forceAllDatasetsParallel;
    bool                embeddedWarningsAsErrors;
    bool                optimizeCriticalFunctions;
    bool                addLikelihoodToGraph;
};

//Any information gathered while processing the query should be moved into here, rather than cluttering up the translator class
struct HqlCppDerived
{
    HqlCppDerived() 
    { 
    }
};

interface IDefRecordElement;
class NlpParseContext;

struct EvaluateCompareInfo
{
public:
    EvaluateCompareInfo(node_operator _op) { actionIfDiffer = null_stmt; op = _op; isBoolEquality = false; neverReturnMatch = true; alwaysReturns = false; }
    EvaluateCompareInfo(const EvaluateCompareInfo & info)
    {
        target.set(info.target); actionIfDiffer = info.actionIfDiffer; op = info.op; isBoolEquality = info.isBoolEquality;
        alwaysReturns = false;
        neverReturnMatch = true;
    }

    bool isEqualityCompare() const { return op == no_eq; }
    IHqlExpression * getEqualityReturnValue() { return isBoolEquality ? createConstant(false) : createIntConstant(1); }

    CHqlBoundTarget target;
    node_operator op;
    StmtKind actionIfDiffer;
    bool isBoolEquality;
    bool neverReturnMatch;
    bool alwaysReturns;
};

class AliasExpansionInfo;
class HashCodeCreator;
class ParentExtract;
class ConstantRowArray;
class SerializeKeyInfo;

enum PEtype {
    PETnone,
    PETchild,       // child query
    PETremote,      // allnodes
    PETloop,        // loop
    PETnested,      // nested class
    PETcallback,    // callback within a function
    PETlibrary,     // a library
    PETmax };

class HQLCPP_API HqlCppTranslator : implements IHqlCppTranslator, public CInterface
{
//MORE: This is in serious need of refactoring....

    friend class HqlCppCaseInfo;
    friend class ActivityInstance;
    friend class SourceBuilder;
    friend class DiskReadBuilder;
    friend class IndexReadBuilder;
    friend class FetchBuilder;
    friend class MonitorExtractor;
    friend class NlpParseContext;
    friend class KeyedJoinInfo;
    friend class ChildGraphBuilder;
public:
    HqlCppTranslator(IErrorReceiver * _errors, const char * _soName, IHqlCppInstance * _code, ClusterType _targetClusterType, ICodegenContextCallback *_logger);
    ~HqlCppTranslator();
    IMPLEMENT_IINTERFACE

//interface IHqlCppTranslator
    virtual bool buildCpp(IHqlCppInstance & _code, HqlQueryContext & query);
    virtual double getComplexity(IHqlCppInstance & _code, IHqlExpression * expr);
    virtual bool spanMultipleCppFiles()         { return options.spanMultipleCpp; }
    virtual unsigned getNumExtraCppFiles()      { return activitiesThisCpp ? curCppFile : 0; }

//Statements.
    void buildStmt(BuildCtx & ctx, IHqlExpression * expr);

//General
    IReferenceSelector * buildReference(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * buildActiveReference(BuildCtx & ctx, IHqlExpression * expr);

//Scalar processing
    void buildExpr(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildSimpleExpr(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildTempExpr(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format=FormatNatural);
    void buildTempExpr(BuildCtx & ctx, BuildCtx & declareCtx, CHqlBoundTarget & target, IHqlExpression * expr, ExpressionFormat format, bool ignoreSetAll);
    IHqlExpression * buildSimplifyExpr(BuildCtx & ctx, IHqlExpression * expr);
    void buildExprEnsureType(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ITypeInfo * type);
    void buildExprViaTypedTemp(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ITypeInfo * type);

    void buildAssign(BuildCtx & ctx, IHqlExpression * target, IHqlExpression * expr);
    void buildExprAssign(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void assign(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & expr);
    void buildIncrementAssign(BuildCtx & ctx, IHqlExpression * target, IHqlExpression * value);
    void buildIncrementAssign(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * value);
    void buildIncrementAssign(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * value);

    void buildExprOrAssign(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * tgt);

//Set processing
    void buildSetAssign(BuildCtx & ctx, IHqlCppSetBuilder * builder, IHqlExpression * expr);
    void buildSetAssignViaBuilder(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * value);

//Row Processing
    IReferenceSelector * buildActiveRow(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * buildNewRow(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * buildNewOrActiveRow(BuildCtx & ctx, IHqlExpression * expr, bool isNew);
    void buildRowAssign(BuildCtx & ctx, BoundRow * target, IHqlExpression * expr);
    void buildRowAssign(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void buildRowAssign(BuildCtx & ctx, IReferenceSelector * target, IReferenceSelector * source);
    BoundRow * ensureLinkCountedRow(BuildCtx & ctx, BoundRow * row);
    IReferenceSelector * ensureLinkCountedRow(BuildCtx & ctx, IReferenceSelector * source);

//Dataset processing.
    void buildAnyExpr(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);
    void doBuildDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);
    void buildDatasetAssign(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void buildDatasetAssign(BuildCtx & ctx, IHqlCppDatasetBuilder * builder, IHqlExpression * expr);

    void ensureDatasetFormat(BuildCtx & ctx, ITypeInfo * type, CHqlBoundExpr & tgt, ExpressionFormat format);

    BoundRow * buildDatasetIterate(BuildCtx & ctx, IHqlExpression * _expr, bool needToBreak);
    IReferenceSelector * buildDatasetIndex(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * buildDatasetIndexViaIterator(BuildCtx & ctx, IHqlExpression * expr);
    IHqlExpression * ensureIteratedRowIsLive(BuildCtx & initctx, BuildCtx & searchctx, BuildCtx & iterctx, BoundRow * row, IHqlExpression * dataset, IHqlExpression * rowExpr);
    BoundRow * buildOptimizeSelectFirstRow(BuildCtx & ctx, IHqlExpression * expr);

    IReferenceSelector * buildDatasetSelectMap(BuildCtx & ctx, IHqlExpression * expr);

// Helper functions

    __declspec(noreturn) void ThrowStringException(int code,const char *format, ...) __attribute__((format(printf, 3, 4), noreturn));            // override the global function to try and add more context information

    void buildAddress(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildBlockCopy(BuildCtx & ctx, IHqlExpression * tgt, CHqlBoundExpr & src);
    void buildClear(BuildCtx & ctx, IHqlExpression * expr);
    void buildClear(BuildCtx & ctx, const CHqlBoundTarget & target);
    void buildFilter(BuildCtx & ctx, IHqlExpression * expr);
    void buildFilteredReturn(BuildCtx & ctx, IHqlExpression * filter, IHqlExpression * value);
    void buildCachedExpr(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildReturn(BuildCtx & ctx, IHqlExpression * expr, ITypeInfo * type=NULL);
    ABoundActivity * buildActivity(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * buildCachedActivity(BuildCtx & ctx, IHqlExpression * expr, bool isRoot = false);
    ABoundActivity * getConditionalActivity(BuildCtx & ctx, IHqlExpression * expr, bool isChild);
    void buildRootActivity(BuildCtx & ctx, IHqlExpression * expr);
    bool specialCaseBoolReturn(BuildCtx & ctx, IHqlExpression * expr);

    void buildCompoundAssign(BuildCtx & ctx, IHqlExpression * left, IReferenceSelector * leftSelector, IHqlExpression * rightScope, IHqlExpression * rightSelector);
    void buildCompoundAssign(BuildCtx & ctx, IHqlExpression * left, IHqlExpression * right);

    void associateCounter(BuildCtx & ctx, IHqlExpression * counterExpr, const char * name);

// child dataset processing.
    IHqlExpression * buildSpillChildDataset(BuildCtx & ctx, IHqlExpression * expr);
    IHqlExpression * forceInlineAssignDataset(BuildCtx & ctx, IHqlExpression * expr);

    bool canProcessInline(BuildCtx * ctx, IHqlExpression * expr);
    bool canIterateInline(BuildCtx * ctx, IHqlExpression * expr);
    bool canAssignInline(BuildCtx * ctx, IHqlExpression * expr);
    bool canEvaluateInline(BuildCtx * ctx, IHqlExpression * expr);

    void buildAssignChildDataset(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void buildChildDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);

    IHqlExpression * bindFunctionCall(IIdAtom * name, HqlExprArray & args);
    IHqlExpression * bindFunctionCall(IIdAtom * name, IHqlExpression * arg1);
    IHqlExpression * bindFunctionCall(IIdAtom * name, HqlExprArray & args, ITypeInfo * newType);
    IHqlExpression * bindFunctionCall(IHqlExpression * function, HqlExprArray & args);
    IHqlExpression * bindTranslatedFunctionCall(IIdAtom * name, HqlExprArray & args);
    IHqlExpression * bindTranslatedFunctionCall(IHqlExpression * function, HqlExprArray & args);

    void buildFunctionCall(BuildCtx & ctx, IIdAtom * name, HqlExprArray & args);
    void buildTranslatedFunctionCall(BuildCtx & ctx, IIdAtom * name, HqlExprArray & args);
    void callProcedure(BuildCtx & ctx, IIdAtom * name, HqlExprArray & args);
    
    void expandFunctions(bool expandInline);
    IHqlExpression * needFunction(IIdAtom * name);
    bool registerGlobalUsage(IHqlExpression * filename);
    IHqlExpression * queryActiveNamedActivity();
    IHqlExpression * queryActiveActivityLocation() const;
    void reportWarning(WarnErrorCategory category, unsigned id, const char * msg, ...) __attribute__((format(printf, 4, 5)));
    void reportWarning(WarnErrorCategory category, ErrorSeverity explicitSeverity, IHqlExpression * location, unsigned id, const char * msg, ...) __attribute__((format(printf, 6, 7)));
    void reportError(IHqlExpression * location, int code, const char *format, ...) __attribute__((format(printf, 4, 5)));
    void reportErrorDirect(IHqlExpression * location, int code,const char *msg, bool alwaysAbort);
    void addWorkunitException(ErrorSeverity severity, unsigned code, const char * msg, IHqlExpression * location);
    void useFunction(IHqlExpression * funcdef);
    void useLibrary(const char * libname);
    void finalizeResources();
    void generateStatistics(const char * targetDir, const char * variant);

    inline bool queryEvaluateCoLocalRowInvariantInExtract() const { return options.evaluateCoLocalRowInvariantInExtract; }
    inline byte notifyOptimizedProjectsLevel()              { return options.notifyOptimizedProjects; }
    inline bool generateAsserts() const                     { return options.checkAsserts; }
    inline bool getCheckRoxieRestrictions() const           { return options.checkRoxieRestrictions; }
    inline bool queryFreezePersists() const                 { return options.freezePersists; }
    inline bool checkIndexReadLimit() const                 { return options.warnOnImplicitReadLimit; }
    inline unsigned getDefaultImplicitIndexReadLimit() const { return options.defaultImplicitIndexReadLimit; }
    inline bool queryCommonUpChildGraphs() const            { return options.commonUpChildGraphs; }
    inline bool insideLibrary() const                       { return outputLibraryId != NULL; }
    inline bool hasDynamicFilename(IHqlExpression * expr) const { return options.allFilenamesDynamic || hasDynamic(expr); }
    inline bool canGenerateStringInline(unsigned len)       { return ((options.inlineStringThreshold == 0) || (len <= options.inlineStringThreshold)); }

    unsigned getOptimizeFlags() const;
    unsigned getSourceAggregateOptimizeFlags() const;
    void addGlobalOnWarning(IHqlExpression * setMetaExpr);

    ClusterType getTargetClusterType() const { return targetClusterType; }
    inline bool targetRoxie() const { return targetClusterType == RoxieCluster; }
    inline bool targetHThor() const { return targetClusterType == HThorCluster; }
    inline bool targetThor() const { return isThorCluster(targetClusterType); }
    inline IErrorReceiver & queryErrorProcessor() { return *errorProcessor; }
    inline ErrorSeverityMapper & queryLocalOnWarningMapper() { return *localOnWarnings; }

    void pushMemberFunction(MemberFunction & func);
    void popMemberFunction();
    unsigned getConsistentUID(IHqlExpression * ptr);
    bool insideOnCreate(BuildCtx & ctx);
    bool insideOnStart(BuildCtx & ctx);
    bool tempRowRequiresFinalize(IHqlExpression * record) const;
    void convertBoundDatasetToFirstRow(IHqlExpression * expr, CHqlBoundExpr & bound);
    void convertBoundRowToDataset(BuildCtx & ctx, CHqlBoundExpr & bound, const BoundRow * row, ExpressionFormat preferredFormat);

    //Be very careful before calling this.......  
    //Either isIndependentMaybeShared is set - which case the item inserted into the initctx can have no dependencies
    //or isIndependentMaybeShared is false, and the code that is inserted is never implicitly shared.
    bool getInvariantMemberContext(BuildCtx & ctx, BuildCtx * * declarectx, BuildCtx * * initctx, bool isIndependentMaybeShared, bool invariantEachStart);

    IPropertyTree * gatherFieldUsage(const char * variant, const IPropertyTree * exclude);
    void writeFieldUsage(const char * targetDir, IPropertyTree * xml, const char * variant);

public:
    BoundRow * bindSelf(BuildCtx & ctx, IHqlExpression * dataset, const char * builder);
    BoundRow * bindSelf(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * expr, IHqlExpression * builder);
    BoundRow * bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * bound, bool isLinkCounted, node_operator no_side, IHqlExpression * selSeq);
    BoundRow * bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator no_side, IHqlExpression * selSeq);
    BoundRow * bindCsvTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, node_operator side, IHqlExpression * selSeq, bool translateVirtuals, IAtom * encoding);
    BoundRow * bindCsvTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq, bool translateVirtuals, IAtom * encoding);
    BoundRow * bindXmlTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * name, node_operator side, IHqlExpression * selSeq, bool translateVirtuals);
    BoundRow * bindXmlTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq, bool translateVirtuals);
    BoundRow * createTableCursor(IHqlExpression * dataset, IHqlExpression * bound, node_operator side, IHqlExpression * selSeq);
    BoundRow * createTableCursor(IHqlExpression * dataset, const char * name, bool isLinkCounted, node_operator side, IHqlExpression * selSeq);
    BoundRow * bindRow(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * bound);
    BoundRow * bindRow(BuildCtx & ctx, IHqlExpression * expr, const char * name);
    BoundRow * bindConstantRow(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound);
    BoundRow * bindSelectorAsSelf(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * expr);
    BoundRow * bindSelectorAsRootRow(BuildCtx & ctx, IReferenceSelector * selector, IHqlExpression * expr);
    void bindRows(BuildCtx & ctx, node_operator side, IHqlExpression * selSeq, IHqlExpression * rowsid, IHqlExpression * dataset, const char * numText, const char * rowsText, bool mainRowsAreLinkCounted);
    BoundRow * bindTableCursorOrRow(BuildCtx & ctx, IHqlExpression * expr, const char * name);
    BoundRow * recreateTableCursor(IHqlExpression * dataset, BoundRow * row, node_operator side, IHqlExpression * selSeq);
    BoundRow * rebindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, BoundRow * row, node_operator no_side, IHqlExpression * selSeq);
    void finishSelf(BuildCtx & ctx, BoundRow * self, BoundRow * target);
    void ensureRowAllocated(BuildCtx & ctx, const char * builder);
    void ensureRowAllocated(BuildCtx & ctx, BoundRow * row);

    inline BoundRow * bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * bound, node_operator side, IHqlExpression * selSeq)
           { return bindTableCursor(ctx, dataset, bound, false, side, selSeq); }
    inline BoundRow * bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, const char * bound)
           { return bindTableCursor(ctx, dataset, bound, false, no_none, NULL); }
    inline BoundRow * bindTableCursor(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * bound)
           { return bindTableCursor(ctx, dataset, bound, no_none, NULL); }


    IHqlExpression * getRtlFieldKey(IHqlExpression * expr, IHqlExpression * ownerRecord);
    unsigned buildRtlField(StringBuffer * instanceName, IHqlExpression * fieldKey);
    unsigned buildRtlType(StringBuffer & instanceName, ITypeInfo * type, unsigned typeFlags);
    unsigned buildRtlRecordFields(StringBuffer & instanceName, IHqlExpression * record, IHqlExpression * rowRecord);
    unsigned expandRtlRecordFields(StringBuffer & fieldListText, IHqlExpression * record, IHqlExpression * rowRecord);
    unsigned buildRtlIfBlockField(StringBuffer & instanceName, IHqlExpression * ifblock, IHqlExpression * rowRecord);
    unsigned getRtlFieldInfo(StringBuffer & fieldInfoName, IHqlExpression * field, IHqlExpression * rowRecord);

    void buildMetaInfo(MetaInstance & instance);
    IHqlExpression * buildMetaParameter(IHqlExpression * arg);
    void buildMetaForRecord(StringBuffer & name, IHqlExpression * record);
    void buildMetaForSerializedRecord(StringBuffer & name, IHqlExpression * record, bool isGrouped);
    BoundRow * createBoundRow(IHqlExpression * dataset, IHqlExpression * bound);
    void getRecordSize(BuildCtx & ctx, IHqlExpression * dataset, CHqlBoundExpr & bound);
    BoundRow * resolveSelectorDataset(BuildCtx & ctx, IHqlExpression * dataset);
    BoundRow * resolveDatasetRequired(BuildCtx & ctx, IHqlExpression * expr);
    ColumnToOffsetMap * queryRecordOffsetMap(IHqlExpression * record);
    IHqlExpression * queryRecord(BuildCtx & ctx, IHqlExpression * expr);
    RecordOffsetMap & queryRecordMap()              { return recordMap; }
    unsigned getDefaultMaxRecordSize()              { return options.maxRecordSize; }
    void buildReturnRecordSize(BuildCtx & ctx, BoundRow * cursor);
    bool isFixedRecordSize(IHqlExpression * record);
    bool recordContainsIfBlock(IHqlExpression * record);
    unsigned getFixedRecordSize(IHqlExpression * record);
    unsigned getMaxRecordSize(IHqlExpression * record);
    IHqlExpression * getRecordSize(IHqlExpression * dataset);
    unsigned getCsvMaxLength(IHqlExpression * csvAttr);
    void ensureRowSerializer(StringBuffer & serializerName, BuildCtx & ctx, IHqlExpression * record, IAtom * format, IAtom * kind);
    void ensureRowPrefetcher(StringBuffer & prefetcherName, BuildCtx & ctx, IHqlExpression * record);
    IHqlExpression * createSerializer(BuildCtx & ctx, IHqlExpression * record, IAtom * format, IAtom * kind);

    AliasKind buildExprInCorrectContext(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, bool evaluateLocally);
    ParentExtract * createExtractBuilder(BuildCtx & ctx, PEtype type, IHqlExpression * graphId, IHqlExpression * expr, bool doDeclare);
    ParentExtract * createExtractBuilder(BuildCtx & ctx, PEtype type, IHqlExpression * graphId, GraphLocalisation localisation, bool doDeclare);
        
    void buildDefaultRow(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound);
    void buildNullRow(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound);

    void buildTransformBody(BuildCtx & ctx, IHqlExpression * transform, IHqlExpression * left, IHqlExpression * right, IHqlExpression * self, IHqlExpression * selSeq);
    BoundRow * buildTransformCursors(BuildCtx & ctx, IHqlExpression * transform, IHqlExpression * left, IHqlExpression * right, IHqlExpression * self, IHqlExpression * selSeq);
    void doBuildTransformBody(BuildCtx & ctx, IHqlExpression * transform, BoundRow * selfCursor);

    void noteXpathUsed(const char * xpath);
    void noteXpathUsed(IHqlExpression * expr);

    HqlCppOptions const & queryOptions() const { return options; }
    bool needToSerializeToSlave(IHqlExpression * expr) const;
    ITimeReporter * queryTimeReporter() const { return timeReporter; }
    void noteFinishedTiming(const char * name, cycle_t startCycles)
    {
        if (options.addTimingToWorkunit)
            timeReporter->addTiming(name, get_cycles_now()-startCycles);
    }

    void updateClusterType();
    bool buildCode(HqlQueryContext & query, const char * embeddedLibraryName, const char * embeddedGraphName);

    inline StringBuffer & generateExprCpp(StringBuffer & out, IHqlExpression * expr)
    {
        return ::generateExprCpp(out, expr, options.targetCompiler);
    }

    inline StringBuffer & generateTypeCpp(StringBuffer & out, ITypeInfo * type, const char * name)
    {
        return ::generateTypeCpp(out, type, name, options.targetCompiler);
    }

    void setTargetClusterType(ClusterType clusterType);
    void checkAbort();

public:
    //various helper functions.
    IHqlExpression * addBigLiteral(const char *lit, unsigned litLen);
    IHqlExpression * addLiteral(const char * text);
    IHqlExpression * addDataLiteral(const char *lit, unsigned litLen);
    IHqlExpression * addStringLiteral(const char *lit);
    IHqlExpression * associateLocalFailure(BuildCtx & ctx, const char * exceptionName);
    IHqlExpression * convertBoundStringToChar(const CHqlBoundExpr & bound);
    void createTempFor(BuildCtx & ctx, ITypeInfo * exprType, CHqlBoundTarget & target, typemod_t modifier, ExpressionFormat format);
    void createTempFor(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundTarget & target);

    BoundRow * declareTempRow(BuildCtx & ctx, BuildCtx & codectx, IHqlExpression * expr);
    BoundRow * createRowBuilder(BuildCtx & ctx, BoundRow * targetRow);
    void finalizeTempRow(BuildCtx & ctx, BoundRow * targetRow, BoundRow * rowBuilder);
    BoundRow * declareTempAnonRow(BuildCtx & ctx, BuildCtx & codectx, IHqlExpression * record);

    IHqlExpression * declareLinkedRowExpr(BuildCtx & ctx, IHqlExpression * record, bool isMember);
    BoundRow * declareLinkedRow(BuildCtx & ctx, IHqlExpression * expr, bool isMember);
    BoundRow * declareStaticRow(BuildCtx & ctx, IHqlExpression * expr);

    void expandTranslated(IHqlExpression * expr, CHqlBoundExpr & tgt);
    IHqlExpression * getBoundCount(const CHqlBoundExpr & bound);
    IHqlExpression * getBoundLength(const CHqlBoundExpr & bound);
    IHqlExpression * getBoundSize(const CHqlBoundExpr & bound);
    IHqlExpression * getBoundSize(ITypeInfo * type, IHqlExpression * length, IHqlExpression * data);
    IHqlExpression * getElementPointer(IHqlExpression * source);
    IHqlExpression * getIndexedElementPointer(IHqlExpression * source, IHqlExpression * index);
    IHqlExpression * getIndexedElementPointer(IHqlExpression * source, unsigned index);
    IHqlExpression * getListLength(BuildCtx & ctx, IHqlExpression * expr);
    void getRecordECL(IHqlExpression * record, StringBuffer & eclText);
    void ensureHasAddress(BuildCtx & ctx, CHqlBoundExpr & tgt);
    void normalizeBoundExpr(BuildCtx & ctx, CHqlBoundExpr & bound);

    IWorkUnit * wu()           { return code->workunit; }
    void useInclude(const char * name)                      { code->useInclude(name); }
    HqlCppInstance * queryCode() const                      { return code; }
    unsigned curSubGraphId(BuildCtx & ctx);
    unsigned beginFunctionGetCppIndex(unsigned activityId, bool isChildActivity);

    void buildAssignToTemp(BuildCtx & ctx, IHqlExpression * variable, IHqlExpression * expr);       // create a bound target for the variable and assign
    void queryAddResultDependancy(ABoundActivity & whoAmIActivity, IHqlExpression * seq, IHqlExpression * name);
    void associateRemoteResult(ActivityInstance & instance, IHqlExpression * seq, IHqlExpression * name);

    IHqlCppSetCursor * createSetSelector(BuildCtx & ctx, IHqlExpression * expr);
    IHqlCppSetBuilder * createTempSetBuilder(ITypeInfo * type, IHqlExpression * allVar);
    IHqlCppSetBuilder * createInlineSetBuilder(ITypeInfo * type, IHqlExpression * allVar, IHqlExpression * size, IHqlExpression * address);

    IHqlCppDatasetCursor * createDatasetSelector(BuildCtx & ctx, IHqlExpression * expr, ExpressionFormat format = FormatNatural);
    IHqlCppDatasetBuilder * createBlockedDatasetBuilder(IHqlExpression * record);
    IHqlCppDatasetBuilder * createSingleRowTempDatasetBuilder(IHqlExpression * record, BoundRow * row);
    IHqlCppDatasetBuilder * createInlineDatasetBuilder(IHqlExpression * record, IHqlExpression * size, IHqlExpression * address);
    IHqlCppDatasetBuilder * createChoosenDatasetBuilder(IHqlExpression * record, IHqlExpression * maxCount);
    IHqlCppDatasetBuilder * createLimitedDatasetBuilder(IHqlExpression * record, IHqlExpression * maxCount);
    IHqlCppDatasetBuilder * createLinkedDatasetBuilder(IHqlExpression * record, IHqlExpression * choosenLimit = NULL);
    IHqlCppDatasetBuilder * createLinkedDictionaryBuilder(IHqlExpression * record);
    IReferenceSelector * createSelfSelect(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr, IHqlExpression * rootSelector);
    IReferenceSelector * createReferenceSelector(BoundRow * cursor, IHqlExpression * path);
    IReferenceSelector * createReferenceSelector(BoundRow * cursor);

    IHqlExpression * convertBetweenCountAndSize(const CHqlBoundExpr & bound, bool getLength);
    
    void assignBound(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * rhs);             // assign rhs to 
    void assignBoundToTemp(BuildCtx & ctx, IHqlExpression * lhs, IHqlExpression * rhs);

    bool expandFunctionPrototype(StringBuffer & s, IHqlExpression * funcdef);
    void expandFunctionPrototype(BuildCtx & ctx, IHqlExpression * funcdef);
    void buildCppFunctionDefinition(BuildCtx &funcctx, IHqlExpression * bodycode, const char *proto);
    void buildScriptFunctionDefinition(BuildCtx &funcctx, IHqlExpression * bodycode, const char *proto);
    void buildFunctionDefinition(IHqlExpression * funcdef);
    void assignAndCast(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & expr);
    void assignCastUnknownLength(BuildCtx & ctx, const CHqlBoundTarget & target, CHqlBoundExpr & pure);
    void assignSwapInt(BuildCtx & ctx, ITypeInfo * to, const CHqlBoundTarget & target, CHqlBoundExpr & pure);
    void buildAssignViaTemp(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);

    void buildRecordSerializeExtract(BuildCtx & ctx, IHqlExpression * memoryRecord);

    void doBuildSetAssignAndCast(BuildCtx & ctx, IHqlCppSetBuilder * builder, IHqlExpression * value);

    IHqlExpression * createWrapperTemp(BuildCtx & ctx, ITypeInfo * type, typemod_t modifier);

    void doBuildExprAssign(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildBoolAssign(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);

    void doBuildChoose(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr);

    void buildExprAssignViaType(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, ITypeInfo * type);
    void buildExprAssignViaString(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, unsigned len);
    void doBuildDivideByZero(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * zero, CHqlBoundExpr * bound);

    void doBuildAssignAddSets(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * value);
    void doBuildAssignAggregate(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignAll(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignAnd(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, bool invert);
    void doBuildAssignCall(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * src);
    void doBuildAssignCast(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * src);
    void doBuildAssignCatch(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignChoose(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignCompare(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignConcat(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignCount(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignDivide(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignExecuteWhen(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignEventExtra(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignEventName(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignFailMessage(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignFormat(IIdAtom * func, BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignGetResult(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignGetGraphResult(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignHashCrc(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignHashElement(BuildCtx & ctx, HashCodeCreator & creator, IHqlExpression * elem);
    void doBuildAssignHashElement(BuildCtx & ctx, HashCodeCreator & creator, IHqlExpression * elem, IHqlExpression * record);
    void doBuildAssignHashMd5(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignIdToBlob(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignIf(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignIn(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignInCreateSet(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignInStored(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignIndex(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignList(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignLoopCounter(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignOr(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignOrder(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignUnicodeOrder(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignRegexFindReplace(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignSubString(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignToXmlorJson(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignTrim(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignWhich(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildAssignWuid(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void doBuildCaseInfo(IHqlExpression * expr, HqlCppCaseInfo & info);
    void doBuildInCaseInfo(IHqlExpression * expr, HqlCppCaseInfo & info, IHqlExpression * normalizedValues = NULL);
    void doBuildAssignToFromUnicode(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);

    void buildAssignDeserializedDataset(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IAtom * serializeForm);
    void buildAssignSerializedDataset(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IAtom * serializeForm);
    void buildDeserializedDataset(BuildCtx & ctx, ITypeInfo * type, IHqlExpression * expr, CHqlBoundExpr & tgt, IAtom * serializeForm);
    void buildSerializedDataset(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, IAtom * serializeForm);

    void buildDatasetAssignAggregate(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignChoose(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignCombine(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignInlineTable(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignDatasetFromTransform(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignJoin(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignProject(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignTempTable(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);
    void buildDatasetAssignXmlProject(BuildCtx & ctx, IHqlCppDatasetBuilder * target, IHqlExpression * expr);

    void buildDatasetAssignChoose(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);
    void buildDatasetAssignIf(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr);

    BoundRow * buildDatasetIterateSelectN(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateChoosen(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateFromDictionary(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateLimit(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateProject(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateUserTable(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateSpecialTempTable(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);
    BoundRow * buildDatasetIterateStreamedCall(BuildCtx & ctx, IHqlExpression * expr, bool needToBreak);

    void createInlineDictionaryRows(HqlExprArray & args, ConstantRowArray & boundRows, IHqlExpression * keyRecord, IHqlExpression * nullRow);
    bool buildConstantRows(ConstantRowArray & boundRows, IHqlExpression * transforms);
    void doBuildDatasetLimit(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);
    bool doBuildConstantDatasetInlineTable(IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);
    bool doBuildDictionaryInlineTable(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);
    void doBuildDatasetNull(IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);

    void doBuildCheckDatasetLimit(BuildCtx & ctx, IHqlExpression * expr, const CHqlBoundExpr & bound);

    void doBuildRowAssignAggregate(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignAggregateClear(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignAggregateNext(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr, bool isSingleExists, IHqlExpression * guard);
    void doBuildRowAssignCombine(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignNullRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignProject(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignUserTable(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignProjectRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignCreateRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);
    void doBuildRowAssignSerializeRow(BuildCtx & ctx, IReferenceSelector * target, IHqlExpression * expr);

    IReferenceSelector * doBuildRowDeserializeRow(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowFromXMLorJSON(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowIdToBlob(BuildCtx & ctx, IHqlExpression * expr, bool isNew);
    IReferenceSelector * doBuildRowIf(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowMatchAttr(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowMatchRow(BuildCtx & ctx, IHqlExpression * expr, bool isNew);
    IReferenceSelector * doBuildRowSelectTop(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowViaTemp(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowCreateRow(BuildCtx & ctx, IHqlExpression * expr);
    IReferenceSelector * doBuildRowNull(BuildCtx & ctx, IHqlExpression * expr);

    void buildConstRow(IHqlExpression * record, IHqlExpression * rowData, CHqlBoundExpr & bound);
    bool doBuildRowConstantNull(IHqlExpression * expr, CHqlBoundExpr & bound);
    bool doBuildRowConstantTransform(IHqlExpression * transform, CHqlBoundExpr & bound);

    void doBuildRowIfBranch(BuildCtx & initctx, BuildCtx & ctx, BoundRow * targetRow, IHqlExpression * branchExpr);

    void doBuildReturnCompare(BuildCtx & ctx, IHqlExpression * expr, node_operator op, bool isBoolEquality, bool neverReturnTrue);
    void buildReturnOrder(BuildCtx & ctx, IHqlExpression *sortList, const DatasetReference & dataset);

    IHqlExpression * createLoopSubquery(IHqlExpression * dataset, IHqlExpression * selSeq, IHqlExpression * rowsid, IHqlExpression * body, IHqlExpression * filter, IHqlExpression * again, IHqlExpression * counter, bool multiInstance, unsigned & loopAgainResult);
    unique_id_t buildGraphLoopSubgraph(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * selSeq, IHqlExpression * rowsid, IHqlExpression * body, IHqlExpression * counter, bool multiInstance);
    unique_id_t buildRemoteSubgraph(BuildCtx & ctx, IHqlExpression * dataset);
        
    void doBuildCall(BuildCtx & ctx, const CHqlBoundTarget * tgt, IHqlExpression * expr, CHqlBoundExpr * result);
    IHqlExpression * doBuildInternalFunction(IHqlExpression * funcdef);
    
    IHqlExpression * doBuildCharLength(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildHashMd5Element(BuildCtx & ctx, IHqlExpression * elem, CHqlBoundExpr & state);
    AliasKind doBuildAliasValue(BuildCtx & ctx, IHqlExpression * value, CHqlBoundExpr & tgt, AliasExpansionInfo * parentInfo);

    void pushCluster(BuildCtx & ctx, IHqlExpression * cluster);
    void popCluster(BuildCtx & ctx);

    void noteResultAccessed(BuildCtx & ctx, IHqlExpression * seq, IHqlExpression * name);
    void noteResultDefined(BuildCtx & ctx, ActivityInstance * activityInstance, IHqlExpression * seq, IHqlExpression * name, bool alwaysExecuted);

//Expressions:
    void doBuildExprAbs(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprAdd(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprAggregate(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprAlias(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr * tgt, AliasExpansionInfo * parentInfo);
    void doBuildExprAll(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprBlobToId(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprArith(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprCall(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprCast(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprCast(BuildCtx & ctx, ITypeInfo * type, CHqlBoundExpr & pure, CHqlBoundExpr & tgt);
    void doBuildExprCompare(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprCompareElement(BuildCtx & ctx, node_operator comp_op, IHqlExpression * lhs, IHqlExpression * rhs, CHqlBoundExpr & tgt);
    void doBuildExprCountDict(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprCount(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprCounter(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprDivide(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprEmbedBody(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr * tgt);
    void doBuildExprEvaluate(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprExists(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprExistsDict(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprFailCode(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprField(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprFileLogicalName(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprFilepos(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprFormat(IIdAtom * func, BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprGetGraphResult(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, ExpressionFormat format);
    void doBuildExprGetResult(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprIdToBlob(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprIf(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprIndex(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprInDict(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprIsValid(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprList(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprConstList(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprDynList(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprNegate(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprNot(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprOffsetOf(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprOrdered(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprRank(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprRanked(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprRegexFindReplace(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound);
    void doBuildExprRegexFindSet(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & bound);
    void doBuildExprRound(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprSelect(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprSizeof(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprSubString(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprSysFunc(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, IIdAtom * funcName);
    void doBuildExprTransfer(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprTrim(BuildCtx & ctx, IHqlExpression * target, CHqlBoundExpr & tgt);
    void doBuildExprTrunc(BuildCtx & ctx, IHqlExpression * target, CHqlBoundExpr & tgt);
    void doBuildExprToFromUnicode(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprKeyUnicode(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprWuid(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprXmlText(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprXmlUnicode(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);

    bool doBuildExprInfiniteSubString(BuildCtx & ctx, SubStringInfo & info, CHqlBoundExpr & tgt);
    bool doBuildExprSpecialSubString(BuildCtx & ctx, SubStringInfo & info, CHqlBoundExpr & tgt);
    void doBuildExprAnySubString(BuildCtx & ctx, SubStringInfo & info, CHqlBoundExpr & tgt);
    bool doBuildExprSetCompare(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildExprSetCompareAll(BuildCtx & ctx, IHqlExpression * set, CHqlBoundExpr & tgt, bool invert);
    void doBuildExprSetCompareNone(BuildCtx & ctx, IHqlExpression * set, CHqlBoundExpr & tgt, bool invert);

    void doBuildAggregateList(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * tgt);
    bool doBuildAggregateMinMaxList(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, IHqlExpression * list, CHqlBoundExpr * tgt, node_operator compareOp);

    void buildWorkflow(WorkflowArray & workflow);
    void buildWorkflowItem(BuildCtx & ctx, IHqlStmt * switchStmt, unsigned wfid, IHqlExpression * expr);
    void buildWorkflowPersistCheck(BuildCtx & ctx, IHqlExpression * expr);

    IHqlExpression * cvtGetEnvToCall(IHqlExpression * expr);

//Statements
    void doBuildStmtApply(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtAssert(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtAssign(BuildCtx & ctx, IHqlExpression * target, IHqlExpression * expr);
    void doBuildStmtAssignModify(BuildCtx & ctx, IHqlExpression * target, IHqlExpression * expr, node_operator op);
    void doBuildStmtCall(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtCluster(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtEnsureResult(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtFail(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtIf(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtNotify(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtOutput(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtSetResult(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtSkip(BuildCtx & ctx, IHqlExpression * expr, bool * canReachFollowing);
    void doBuildStmtUpdate(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildStmtWait(BuildCtx & ctx, IHqlExpression * expr);

    void optimizeBuildActionList(BuildCtx & ctx, IHqlExpression * exprs);

    bool buildNWayInputs(CIArrayOf<ABoundActivity> & inputs, BuildCtx & ctx, IHqlExpression * input);

//Activities.   
    ABoundActivity * doBuildActivityAction(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityApply(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityAssert(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCacheAlias(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCallSideEffect(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCase(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityCatch(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityChildAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityChildDataset(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityChildGroupAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityChildNormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityChoose(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * cond, CIArrayOf<ABoundActivity> & inputs, bool isRoot);
    ABoundActivity * doBuildActivityChoose(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityChooseSets(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityChooseSetsEx(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCloned(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCombine(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCombineGroup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCompoundSelectNew(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityConcat(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCountTransform(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityCreateRow(BuildCtx & ctx, IHqlExpression * expr, bool isDataset);
    ABoundActivity * doBuildActivityXmlRead(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDedup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDefineSideEffect(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDenormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDictionaryWorkunitWrite(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityDiskAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDiskGroupAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDiskNormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDiskRead(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDistribute(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityDistribution(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivitySectionInput(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityEnth(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityExecuteWhen(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityForceLocal(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityFetch(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityFilter(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityFilterGroup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityGetGraphResult(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityGetGraphLoopResult(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityGraphLoop(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityGroup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityIf(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityIndexAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityIndexGroupAggregate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityIndexNormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityIndexRead(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityInlineTable(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityIterate(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityJoin(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityJoinOrDenormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityKeyDiff(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityKeyedJoinOrDenormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityKeyedDistribute(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityKeyPatch(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityLibraryInstance(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityLibrarySelect(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityLimit(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityLinkedRawChildDataset(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityLoop(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityMerge(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNonEmpty(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNWayMerge(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNWayMergeJoin(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNormalize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNormalizeChild(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNormalizeGroup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNormalizeLinkedChild(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityNull(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityOutput(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityOutputIndex(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityOutputWorkunit(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityParse(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityPipeThrough(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityPrefetchProject(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityProject(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityProcess(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityPullActivity(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityQuantile(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityRegroup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityRemote(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityReturnResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivityRollup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityRollupGroup(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityRowsetIndex(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityRowsetRange(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityRowsetRange(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * rowset, IHqlExpression * inputSelection);
    ABoundActivity * doBuildActivitySample(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySection(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySelectNew(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySelectNth(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySequentialParallel(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivitySerialize(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySetGraphDictionaryResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivitySetGraphResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivitySetGraphLoopResult(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySetResult(BuildCtx & ctx, IHqlExpression * expr, bool isRoot);
    ABoundActivity * doBuildActivitySideEffect(BuildCtx & ctx, IHqlExpression * expr, bool isRoot, bool expandChildren);
    ABoundActivity * doBuildActivitySpill(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySplit(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityHTTP(BuildCtx & ctx, IHqlExpression * expr, bool isSink, bool isRoot);
    ABoundActivity * doBuildActivitySOAP(BuildCtx & ctx, IHqlExpression * expr, bool isSink, bool isRoot);
    ABoundActivity * doBuildActivitySort(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityStreamedCall(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivitySub(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityTable(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityFirstN(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityTempTable(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityTraceActivity(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityUngroup(BuildCtx & ctx, IHqlExpression * expr, ABoundActivity * boundDataset);
    ABoundActivity * doBuildActivityWorkunitRead(BuildCtx & ctx, IHqlExpression * expr);
    ABoundActivity * doBuildActivityXmlParse(BuildCtx & ctx, IHqlExpression * expr);

    void doBuildHttpHeaderStringFunction(BuildCtx & ctx, IHqlExpression * expr);

    void doBuildTempTableFlags(BuildCtx & ctx, IHqlExpression * expr, bool isConstant, bool canFilter);

    void doBuildXmlEncode(BuildCtx & ctx, const CHqlBoundTarget * tgt, IHqlExpression * expr, CHqlBoundExpr * result);

    IHqlExpression * doBuildOrderElement(BuildCtx & ctx, IHqlExpression * left, IHqlExpression * right);

    void doBuildFilterAnd(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildFilterAndRange(BuildCtx & ctx, unsigned first, unsigned last, HqlExprArray & conds);
    void doBuildFilterToTarget(BuildCtx & ctx, const CHqlBoundTarget & isOk, HqlExprArray & conds, bool invert);
    void doBuildFilterNextAndRange(BuildCtx & ctx, unsigned & curIndex, unsigned maxIterations, HqlExprArray & conds);
    bool canBuildOptimizedCount(BuildCtx & ctx, IHqlExpression * dataset, CHqlBoundExpr & tgt, node_operator aggOp);
    void setBoundCount(CHqlBoundExpr & tgt, const CHqlBoundExpr & src, node_operator aggOp);
    bool canEvaluateInContext(BuildCtx & ctx, IHqlExpression * expr);
    void gatherActiveCursors(BuildCtx & ctx, HqlExprCopyArray & activeRows);

    IHqlStmt * buildFilterViaExpr(BuildCtx & ctx, IHqlExpression * expr);

    void doBuildPureSubExpr(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void doBuildTempExprConcat(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildConcatFArgs(HqlExprArray & args, BuildCtx & ctx, const HqlExprArray & values, ITypeInfo * argType);
    void doBuildVarLengthConcatF(BuildCtx & ctx, const CHqlBoundTarget & target, const HqlExprArray & values);
    bool doBuildFixedLengthConcatF(BuildCtx & ctx, const CHqlBoundTarget & target, const HqlExprArray & values);

    void doBuildCastViaTemp(BuildCtx & ctx, ITypeInfo * to, CHqlBoundExpr & pure, CHqlBoundExpr & tgt);
    void doBuildCastViaString(BuildCtx & ctx, ITypeInfo * to, const CHqlBoundExpr & pure, CHqlBoundExpr & tgt);
    void bindAndPush(BuildCtx & ctx, IHqlExpression * value);
    IHqlExpression * createFormatCall(IIdAtom * func, IHqlExpression * expr);
    void createOrderList(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * ascdesc, CHqlBoundExpr & tgt);
    bool ensurePushed(BuildCtx & ctx, const CHqlBoundExpr & pure);
    void ensureSimpleExpr(BuildCtx & ctx, CHqlBoundExpr & tgt);
    IHqlExpression * ensureSimpleTranslatedExpr(BuildCtx & ctx, IHqlExpression * expr);
    void ensureContextAvailable(BuildCtx & ctx);

    IHqlExpression * getConstWuid(IHqlExpression * expr);
    IHqlExpression * getFirstCharacter(IHqlExpression * source);
    bool hasAddress(BuildCtx & ctx, IHqlExpression * expr);

    void buildStartTimer(BuildCtx & ctx, CHqlBoundExpr & boundTimer, CHqlBoundExpr & boundStart, const char * name);
    void buildStopTimer(BuildCtx & ctx, const CHqlBoundExpr & boundTimer, const CHqlBoundExpr & boundStart);

    IHqlExpression * convertOrToAnd(IHqlExpression * expr);
    bool childrenRequireTemp(BuildCtx & ctx, IHqlExpression * expr, bool includeChildren);
    bool requiresTemp(BuildCtx & ctx, IHqlExpression * expr, bool includeChildren);
    bool requiresTempAfterFirst(BuildCtx & ctx, IHqlExpression * expr);

    void tidyupExpr(BuildCtx & ctx, CHqlBoundExpr & bound);

    void doStringTranslation(BuildCtx & ctx, ICharsetInfo * tgtset, ICharsetInfo * srcset, unsigned tgtlen, IHqlExpression * srclen, IHqlExpression * target, IHqlExpression * src);

    void cacheOptions();
    void overrideOptionsForLibrary();
    void overrideOptionsForQuery();

    void doExpandAliases(BuildCtx & ctx, IHqlExpression * expr, AliasExpansionInfo & info);
    void expandAliases(BuildCtx & ctx, IHqlExpression * expr, AliasExpansionInfo * parentInfo);
    void expandAliasScope(BuildCtx & ctx, IHqlExpression * expr);
    IHqlExpression * queryExpandAliasScope(BuildCtx & ctx, IHqlExpression * expr);

    void addDependency(BuildCtx & ctx, ABoundActivity * element, ABoundActivity * dependent, IAtom * kind, const char * label=NULL);
    void addDependency(BuildCtx & ctx, ABoundActivity * element, ActivityInstance * instance, IAtom * kind, const char * label=NULL);
    void addDependency(BuildCtx & ctx, ABoundActivity * sourceActivity, IPropertyTree * sinkGraph, ABoundActivity * sinkActivity, IAtom * kind, const char * label, unsigned inputIndex, int whenId);
    void addActionConnection(BuildCtx & ctx, ABoundActivity * element, ActivityInstance * instance, IAtom * kind, const char * label, unsigned inputIndex, int whenId);
    void addFileDependency(IHqlExpression * name, ABoundActivity * whoAmI);

    void doBuildClearAggregateRecord(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * self, IHqlExpression * transform);
    void doBuildAggregateClearFunc(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildAggregateFirstFunc(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildAggregateNextFunc(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildAggregateMergeFunc(BuildCtx & ctx, IHqlExpression * expr, bool & requiresOrderedMerge);
    void doBuildAggregateProcessTransform(BuildCtx & ctx, BoundRow * selfRow, IHqlExpression * expr, IHqlExpression * alreadyDoneExpr);

    void doBuildFuncIsSameGroup(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * sortlist);

    void processUserAggregateTransform(IHqlExpression * expr, IHqlExpression * transform, SharedHqlExpr & firstTransform, SharedHqlExpr & nextTransform);
    void doBuildUserAggregateFuncs(BuildCtx & ctx, IHqlExpression * expr, bool & requiresOrderedMerge);
    void doBuildUserAggregateProcessTransform(BuildCtx & ctx, BoundRow * selfRow, IHqlExpression * expr, IHqlExpression * transform, IHqlExpression * alreadyDoneExpr);
    void doBuildUserMergeAggregateFunc(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * mergeTransform);
    IHqlExpression * getUserAggregateMergeTransform(IHqlExpression * expr, bool & requiresOrderedMerge);

    void doBuildDistributionClearFunc(BuildCtx & ctx, IHqlExpression * dataset, HqlExprArray & fields);
    void doBuildDistributionNextFunc(BuildCtx & ctx, IHqlExpression * dataset, HqlExprArray & fields);
    void doBuildDistributionFunc(BuildCtx & funcctx, unsigned numFields, const char * action);
    void doBuildDistributionDestructFunc(BuildCtx & funcctx, unsigned numFields);
    void doBuildDistributionSerializeFunc(BuildCtx & funcctx, unsigned numFields);
    void doBuildDistributionMergeFunc(BuildCtx & funcctx, unsigned numFields);
    void doBuildDistributionGatherFunc(BuildCtx & funcctx, unsigned numFields);

    void doBuildParseTransform(BuildCtx & classctx, IHqlExpression * expr);
    void doBuildParseValidators(BuildCtx & classctx, IHqlExpression * expr);
    void doBuildMatched(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound);
    void doBuildMatchAttr(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound);
    void doBuildParseSearchText(BuildCtx & classctx, IHqlExpression * dataset, IHqlExpression * search, type_t searchType, ITypeInfo * transferType);
    void doBuildParseSearchText(BuildCtx & classctx, IHqlExpression * expr);
    void doBuildParseCompiled(BuildCtx & classctx, MemoryBuffer & buffer);
    void doBuildParseExtra(BuildCtx & classctx, IHqlExpression * expr);
    void compileParseSearchPattern(IHqlExpression * expr);
    void gatherExplicitMatched(IHqlExpression * expr);

    void doBuildNewRegexFindReplace(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound);
    
    IHqlExpression * doBuildRegexCompileInstance(BuildCtx & ctx, IHqlExpression * pattern, bool unicode, bool caseSensitive);
    IHqlExpression * doBuildRegexFindInstance(BuildCtx & ctx, IHqlExpression * compiled, IHqlExpression * search, bool cloneSearch);
    
    IHqlExpression * doCreateGraphLookup(BuildCtx & declarectx, BuildCtx & resolvectx, unique_id_t id, const char * activity, bool isChild);
    IHqlExpression * buildGetLocalResult(BuildCtx & ctx, IHqlExpression * expr);

    IHqlExpression * queryOptimizedExists(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * dataset);
    void doBuildAssignAggregateLoop(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IHqlExpression * dataset, IHqlExpression * doneFirstVar, bool multiPath);

    void validateExprScope(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * expr, const char * opName, const char * argName);

    void buildActivityFramework(ActivityInstance * instance);
    void buildActivityFramework(ActivityInstance * instance, bool alwaysExecuted);      // called for all actions
    void buildCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * sortList, const DatasetReference & dataset);
    void buildCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq);
    void buildCompareMemberLR(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq);
    void buildCompareMember(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset);
    void buildOrderedCompare(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * sorts, CHqlBoundExpr & bound, IHqlExpression * leftDataset, IHqlExpression * rightDataset);

    void buildCompareEqClass(BuildCtx & ctx, const char * name, IHqlExpression * sortList, const DatasetReference & dataset);
    void buildCompareEqClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq);
    void buildCompareEqMemberLR(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, IHqlExpression * datasetLeft, IHqlExpression * datasetRight, IHqlExpression * selSeq);
    void buildCompareEqMember(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset);
    void buildNaryCompareClass(BuildCtx & ctx, const char * name, IHqlExpression * expr, IHqlExpression * datasetLeft, IHqlExpression * selSeq, IHqlExpression * rowsid);
    void buildNaryCompareMember(BuildCtx & ctx, const char * name, IHqlExpression * expr, IHqlExpression * datasetLeft, IHqlExpression * selSeq, IHqlExpression * rowsid);

    void buildConnectInputOutput(BuildCtx & ctx, ActivityInstance * instance, ABoundActivity * table, unsigned outputIndex, unsigned inputIndex, const char * label = NULL, bool nWay = false);
    void buildConnectOrders(BuildCtx & ctx, ABoundActivity * slaveActivity, ABoundActivity * masterActivity);
    void buildDedupFilterFunction(BuildCtx & ctx, HqlExprArray & equalities, HqlExprArray & conds, IHqlExpression * dataset, IHqlExpression * selSeq);
    void buildDedupSerializeFunction(BuildCtx & ctx, const char * funcName, IHqlExpression * srcDataset, IHqlExpression * tgtDataset, HqlExprArray & srcValues, HqlExprArray & tgtValues, IHqlExpression * selSeq);
    void buildDictionaryHashClass(IHqlExpression *record, StringBuffer &lookupHelperName);
    void buildDictionaryHashMember(BuildCtx & ctx, IHqlExpression *dictionary, const char * memberName);
    void buildHashClass(BuildCtx & ctx, const char * name, IHqlExpression * orderExpr, const DatasetReference & dataset);
    void buildHashOfExprsClass(BuildCtx & ctx, const char * name, IHqlExpression * cond, const DatasetReference & dataset, bool compareToSelf);
    void buildInstancePrefix(ActivityInstance * instance);
    void buildInstanceSuffix(ActivityInstance * instance);
    void buildIterateTransformFunction(BuildCtx & ctx, IHqlExpression * boundDataset, IHqlExpression * transform, IHqlExpression * counter, IHqlExpression * selSeq);
    void buildProcessTransformFunction(BuildCtx & ctx, IHqlExpression * expr);
    void buildRollupTransformFunction(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * selSeq );
    void buildClearRecord(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * record, int direction);
    void buildClearRecordMember(BuildCtx & ctx, const char * name, IHqlExpression * dataset);
    IHqlExpression * getSerializedLayoutFunction(IHqlExpression * record, unsigned numKeyedFields);
    void buildSerializedLayoutMember(BuildCtx & ctx, IHqlExpression * record, const char * name, unsigned numKeyedFields);

    IHqlExpression * getClearRecordFunction(IHqlExpression * record, int direction=0);
    void doBuildSequenceFunc(BuildCtx & ctx, IHqlExpression * expr, bool ignoreInternal);
    void buildSetResultInfo(BuildCtx & ctx, IHqlExpression * originalExpr, IHqlExpression * value, ITypeInfo * type, bool isPersist, bool associateResult);

    void buildTransformXml(BuildCtx & ctx, IHqlExpression * expr, HqlExprArray & assigns, IHqlExpression * parentSelector);
    void buildHTTPtoXml(BuildCtx & ctx);
    void buildSOAPtoXml(BuildCtx & ctx, IHqlExpression * dataset, IHqlExpression * transform, IHqlExpression * selSeq);

    void buildRecordEcl(BuildCtx & subctx, IHqlExpression * dataset, const char * methodName, bool removeXpath);
    void doTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self);
    void doUpdateTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self, BoundRow * previous, bool alwaysNextRow);
    void doInlineTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * targetRow);
    void doUserTransform(BuildCtx & ctx, IHqlExpression * transform, BoundRow * self);
    IHqlExpression * createOrderFromSortList(const DatasetReference & dataset, IHqlExpression * sortList, IHqlExpression * leftSelect, IHqlExpression * rightSelect);

    void buildSkewThresholdMembers(BuildCtx & ctx, IHqlExpression * expr);
    void doCompareLeftRight(BuildCtx & ctx, const char * funcname, const DatasetReference & datasetLeft, const DatasetReference & datasetRight, const HqlExprArray & left, const HqlExprArray & right);
    void buildSlidingMatchFunction(BuildCtx & ctx, const HqlExprArray & leftEq, const HqlExprArray & rightEq, const HqlExprArray & slidingMatches, const char * funcname, unsigned childIndex, const DatasetReference & datasetL, const DatasetReference & datasetR);
    void doBuildIndexOutputTransform(BuildCtx & ctx, IHqlExpression * record, SharedHqlExpr & rawRecord, bool hasFileposition, IHqlExpression * maxlength);

    void buildKeyedJoinExtra(ActivityInstance & instance, IHqlExpression * expr, KeyedJoinInfo * joinKey);
    void buildKeyJoinIndexReadHelper(ActivityInstance & instance, IHqlExpression * expr, KeyedJoinInfo * joinKey);
    void buildKeyJoinFetchHelper(ActivityInstance & instance, IHqlExpression * expr, KeyedJoinInfo * joinKey);
    void doBuildJoinRowLimitHelper(ActivityInstance & instance, IHqlExpression * rowlimit, IHqlExpression * filename, bool generateImplicitLimit);
    IHqlExpression * createFailMessage(const char * prefix, IHqlExpression * limit, IHqlExpression * filename, unique_id_t id);
    IHqlExpression * createFailAction(const char * prefix, IHqlExpression * limit, IHqlExpression * filename, unique_id_t id);

    void buildJoinMatchFunction(BuildCtx & ctx, const char * name, IHqlExpression * left, IHqlExpression * right, IHqlExpression * match, IHqlExpression * selSeq);

    void associateBlobHelper(BuildCtx & ctx, IHqlExpression * ds, const char * name);
    IHqlExpression * getBlobRowSelector(BuildCtx & ctx, IHqlExpression * expr);

    void buildXmlSerialize(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, HqlExprArray * assigns, unsigned pass, unsigned & expectedIndex);
    void buildXmlSerialize(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, HqlExprArray * assigns);
    void buildXmlSerialize(BuildCtx & ctx, IHqlExpression * dataset, const char * funcname, bool isMeta);
    void buildXmlSerializeScalar(BuildCtx & ctx, IHqlExpression * selected, IHqlExpression * name);
    void buildXmlSerializeSet(BuildCtx & ctx, IHqlExpression * field, IHqlExpression * value);
    void buildXmlSerializeSetValues(BuildCtx & ctx, IHqlExpression * value, IHqlExpression * itemName, bool includeAll);
    void buildXmlSerializeDataset(BuildCtx & ctx, IHqlExpression * field, IHqlExpression * value, HqlExprArray * assigns);
    void buildXmlSerializeBeginArray(BuildCtx & ctx, IHqlExpression * name);
    void buildXmlSerializeEndArray(BuildCtx & ctx, IHqlExpression * name);
    void buildXmlSerializeBeginNested(BuildCtx & ctx, IHqlExpression * name, bool doIndent);
    void buildXmlSerializeEndNested(BuildCtx & ctx, IHqlExpression * name);
    void buildXmlSerializeUsingMeta(BuildCtx & ctx, IHqlExpression * dataset, const char * self);

    void buildSetXmlSerializer(StringBuffer & helper, ITypeInfo * valueType);

    void buildMetaMember(BuildCtx & ctx, IHqlExpression * datasetOrRecord, bool isGrouped, const char * name);
    void buildMetaSerializerClass(BuildCtx & ctx, IHqlExpression * record, const char * serializerName, IAtom * serializeForm);
    void buildMetaDeserializerClass(BuildCtx & ctx, IHqlExpression * record, const char * deserializerName, IAtom * serializeForm);
    bool buildMetaPrefetcherClass(BuildCtx & ctx, IHqlExpression * record, const char * prefetcherName);

    void buildLibraryInstanceExtract(BuildCtx & ctx, HqlCppLibraryInstance * libraryInstance);
    void buildLibraryGraph(BuildCtx & graphctx, IHqlExpression * expr, const char * graphName);

    ActivityInstance * queryCurrentActivity(BuildCtx & ctx);
    unique_id_t queryCurrentActivityId(BuildCtx & ctx);
    IHqlExpression * getCurrentActivityId(BuildCtx & ctx);          // can be variable
    void associateSkipReturnMarker(BuildCtx & ctx, IHqlExpression * value, BoundRow * self);
    IHqlExpression * createClearRowCall(BuildCtx & ctx, BoundRow * self);
    bool insideActivityRemoteSerialize(BuildCtx & ctx);

    EvalContext * queryEvalContext(BuildCtx & ctx)          { return (EvalContext *)ctx.queryFirstAssociation(AssocExtractContext); }
    inline unsigned nextActivityId()                        { return ++curActivityId; }
    bool insideChildOrLoopGraph(BuildCtx & ctx);
    bool insideChildQuery(BuildCtx & ctx);
    bool insideRemoteGraph(BuildCtx & ctx);
    bool isCurrentActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag);
    void buildChildGraph(BuildCtx & ctx, IHqlExpression * expr);

    void buildXmlReadChildrenIterator(BuildCtx & subctx, const char * iterTag, IHqlExpression * rowName, SharedHqlExpr & subRowExpr);
    void buildXmlReadTransform(IHqlExpression * dataset, StringBuffer & className, bool & usesContents);
    void doBuildXmlReadMember(ActivityInstance & instance, IHqlExpression * expr, const char * functionName, bool & usesContents);

    void ensureSerialized(const CHqlBoundTarget & variable, BuildCtx & serializectx, BuildCtx & deserializectx, const char * inBufferName, const char * outBufferName, IAtom * serializeForm);
    void ensureRowAllocator(StringBuffer & allocatorName, BuildCtx & ctx, IHqlExpression * record, IHqlExpression * activityId);
    IHqlExpression * createRowAllocator(BuildCtx & ctx, IHqlExpression * record);

    void beginExtract(BuildCtx & ctx, ParentExtract * extractBuilder);
    void endExtract(BuildCtx & ctx, ParentExtract * extractBuilder);

    IDefRecordElement * createMetaRecord(IHqlExpression * record);

public:
    void doBuildBoolFunction(BuildCtx & ctx, const char * name, bool value);
    void doBuildBoolFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildSignedFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildSizetFunction(BuildCtx & ctx, const char * name, size32_t value);
    void doBuildUnsignedFunction(BuildCtx & ctx, const char * name, unsigned value);
    void doBuildUnsignedFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildUnsignedFunction(BuildCtx & ctx, const char * name, const char * value);
    void doBuildUnsigned64Function(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildVarStringFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildDataFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildStringFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildDoubleFunction(BuildCtx & ctx, const char * name, IHqlExpression * value);
    void doBuildFunction(BuildCtx & ctx, ITypeInfo * type, const char * name, IHqlExpression * value);
    void doBuildFunctionReturn(BuildCtx & ctx, ITypeInfo * type, IHqlExpression * value);
    void doBuildUserFunctionReturn(BuildCtx & ctx, ITypeInfo * type, IHqlExpression * value);

    void addFilenameConstructorParameter(ActivityInstance & instance, const char * name, IHqlExpression * expr);
    void buildFilenameFunction(ActivityInstance & instance, BuildCtx & classctx, const char * name, IHqlExpression * expr, bool isDynamic);
    void buildRefFilenameFunction(ActivityInstance & instance, BuildCtx & classctx, const char * name, IHqlExpression * dataset);
    void createAccessFunctions(StringBuffer & helperFunc, BuildCtx & declarectx, unsigned prio, const char * interfaceName, const char * object);

    void beginNestedClass(BuildCtx & classctx, const char * member, const char * bases, const char * memberExtra = NULL, ParentExtract * extract = NULL);
    void endNestedClass();

    void buildEncryptHelper(BuildCtx & ctx, IHqlExpression * encryptAttr, const char * funcname = NULL);
    void buildFormatCrcFunction(BuildCtx & ctx, const char * name, IHqlExpression * dataset, IHqlExpression * expr, unsigned payloadDelta);
    void buildLimitHelpers(BuildCtx & ctx, IHqlExpression * expr, IHqlExpression * filename, unique_id_t id);
    void buildLimitHelpers(BuildCtx & ctx, IHqlExpression * rowLimit, IHqlExpression * failAction, bool isSkip, IHqlExpression * filename, unique_id_t id);

    void doGenerateMetaDestruct(BuildCtx & ctx, IHqlExpression * selector, IHqlExpression * record);
    void generateMetaRecordSerialize(BuildCtx & ctx, IHqlExpression * record, const char * diskSerializerName, const char * diskDeserializerName, const char * internalSerializerName, const char * internalDeserializerName, const char * prefetcherName);

    void filterExpandAssignments(BuildCtx & ctx, TransformBuilder * builder, HqlExprArray & assigns, IHqlExpression * expr);

protected:
    void buildIteratorFirst(BuildCtx & ctx, IHqlExpression * iter, IHqlExpression * row);
    void buildIteratorNext(BuildCtx & ctx, IHqlExpression * iter, IHqlExpression * row);
    bool shouldEvaluateSelectAsAlias(BuildCtx & ctx, IHqlExpression * expr);
    IWUResult * createWorkunitResult(int sequence, IHqlExpression * nameExpr);
    void noteFilename(ActivityInstance & instance, const char * name, IHqlExpression * expr, bool isDynamic);
    bool checkGetResultContext(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);
    void buildGetResultInfo(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr * boundTarget, const CHqlBoundTarget * targetAssign);
    void buildGetResultSetInfo(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr * boundTarget, const CHqlBoundTarget * targetAssign);
    void buildExpiryHelper(BuildCtx & ctx, IHqlExpression * expireAttr);
    void buildUpdateHelper(BuildCtx & ctx, ActivityInstance & instance, IHqlExpression * input, IHqlExpression * updateAttr);
    void buildClusterHelper(BuildCtx & ctx, IHqlExpression * expr);

    void precalculateFieldOffsets(BuildCtx & ctx, IHqlExpression * expr, BoundRow * cursor);
    IHqlExpression * optimizeIncrementAssign(BuildCtx & ctx, IHqlExpression * value);

    void doBuildEvalOnce(BuildCtx & ctx, const CHqlBoundTarget * target, IHqlExpression * expr, CHqlBoundExpr * bound);

    void doBuildAssignCompareRow(BuildCtx & ctx, EvaluateCompareInfo & info, IHqlExpression * left, IHqlExpression * right);
    void doBuildAssignCompareTable(BuildCtx & ctx, EvaluateCompareInfo & info, IHqlExpression * left, IHqlExpression * right);
    void doBuildAssignCompareElement(BuildCtx & ctx, EvaluateCompareInfo & info, IHqlExpression * left, IHqlExpression * right, bool isFirst, bool isLast);
    void doBuildAssignCompare(BuildCtx & ctx, EvaluateCompareInfo & target, HqlExprArray & leftValues, HqlExprArray & rightValues, bool isFirst, bool isOuter);
    void expandRowOrder(IHqlExpression * selector, IHqlExpression * record, HqlExprArray & values, bool isRow);
    void expandSimpleOrder(IHqlExpression * left, IHqlExpression * right, HqlExprArray & leftValues, HqlExprArray & rightValues);
    void expandOrder(IHqlExpression * expr, HqlExprArray & leftValues, HqlExprArray & rightValues, SharedHqlExpr & defaultValue);
    void optimizeOrderValues(HqlExprArray & leftValues, HqlExprArray & rightValues, bool isEqualityCompare);
    IHqlExpression * querySimpleOrderSelector(IHqlExpression * expr);

    unsigned doBuildThorChildSubGraph(BuildCtx & ctx, IHqlExpression * expr, SubGraphType kind, unsigned thisId=0, IHqlExpression * represents=NULL);
    unsigned doBuildThorSubGraph(BuildCtx & ctx, IHqlExpression * expr, SubGraphType kind, unsigned thisId=0, IHqlExpression * represents=NULL);
    void doBuildThorGraph(BuildCtx & ctx, IHqlExpression * expr);

    IHqlExpression * createResultName(IHqlExpression * name, bool isPersist);
    bool isFixedWidthDataset(IHqlExpression * dataset);
    IHqlExpression * normalizeGlobalIfCondition(BuildCtx & ctx, IHqlExpression * expr);
    void substituteClusterSize(HqlExprArray & exprs);
    void throwCannotCast(ITypeInfo * from, ITypeInfo * to);

    void ensureSerialized(BuildCtx & ctx, const CHqlBoundTarget & variable);

    void doBuildExprRowDiff(BuildCtx & ctx, const CHqlBoundTarget & target, IHqlExpression * expr, IHqlExpression * leftSelector, IHqlExpression * rightRecord, IHqlExpression * rightSelector, StringBuffer & selectorText, bool isCount);
    void doBuildExprRowDiff(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt);

    void doFilterAssignment(BuildCtx & ctx, TransformBuilder * builder, HqlExprArray & assigns, IHqlExpression * expr);
    void doFilterAssignments(BuildCtx & ctx, TransformBuilder * builder, HqlExprArray & assigns, IHqlExpression * expr);
    bool extractSerializeKey(SerializeKeyInfo & info, const DatasetReference & dataset, const HqlExprArray & sorts, bool isGlobal);
    void generateSerializeAssigns(BuildCtx & ctx, IHqlExpression * record, IHqlExpression * selector, IHqlExpression * selfSelect, IHqlExpression * leftSelect, const DatasetReference & srcDataset, const DatasetReference & tgtDataset, HqlExprArray & srcSelects, HqlExprArray & tgtSelects, bool needToClear, node_operator serializeOp, IAtom * serialForm);
    void generateSerializeFunction(BuildCtx & ctx, const char * funcName, const DatasetReference & srcDataset, const DatasetReference & tgtDataset, HqlExprArray & srcSelects, HqlExprArray & tgtSelects, node_operator serializeOp, IAtom * serialForm);
    void generateSerializeKey(BuildCtx & nestedctx, node_operator side, SerializeKeyInfo & keyInfo, const DatasetReference & dataset, bool generateCompares);
    void generateSerializeKey(BuildCtx & ctx, node_operator side, const DatasetReference & dataset, const HqlExprArray & sorts, bool isGlobal, bool generateCompares);             //NB: sorts are ats.xyz
    void generateSortCompare(BuildCtx & nestedctx, BuildCtx & ctx, node_operator index, const DatasetReference & dataset, const HqlExprArray & sorts, IHqlExpression * noSortAttr, bool canReuseLeft, bool isLightweight, bool isLocal);
    void addSchemaField(IHqlExpression *field, MemoryBuffer &schema, IHqlExpression *selector);
    void addSchemaFields(IHqlExpression * record, MemoryBuffer &schema, IHqlExpression *selector);
    void addSchemaResource(int seq, const char * name, IHqlExpression * record);
    void addSchemaResource(int seq, const char * name, unsigned len, const char * schemaXml);
    void doAddSchemaFields(IHqlExpression * record, MemoryBuffer &schema, IHqlExpression *selector);
    IWUResult * createDatasetResultSchema(IHqlExpression * sequenceExpr, IHqlExpression * name, IHqlExpression * record, HqlExprArray &xmlnsAttrs, bool createTransformer, bool isFile);

    void buildReturnCsvValue(BuildCtx & ctx, IHqlExpression * _expr);
    void buildCsvListFunc(BuildCtx & classctx, const char * func, IHqlExpression * value, const char * defaultValue);
    void buildCsvParameters(BuildCtx & subctx, IHqlExpression * csvAttr, IHqlExpression * record, bool isReading);
    unsigned buildCsvReadTransform(BuildCtx & subctx, IHqlExpression * dataset, bool newInterface, IHqlExpression * csvAttr);
    void buildCsvReadTransform(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, unsigned & numFields);
    void buildCsvReadTransformer(IHqlExpression * dataset, StringBuffer & instanceName, IHqlExpression * optCsvAttr);

    void buildCsvWriteScalar(BuildCtx & ctx, IHqlExpression * expr, IAtom * encoding);
    void buildCsvWriteTransform(BuildCtx & subctx, IHqlExpression * dataset, IAtom * encoding);
    void buildCsvWriteTransform(BuildCtx & subctx, IHqlExpression * expr, IHqlExpression * selector, IAtom * encoding);

    void buildCsvWriteMembers(ActivityInstance * instance, IHqlExpression * dataset, IHqlExpression * csvAttr);
    void buildXmlWriteMembers(ActivityInstance * instance, IHqlExpression * dataset, IHqlExpression * xmlAttr);

//ThorHole helper functions...
    IHqlExpression * doBuildDatabaseLoader(BuildCtx & ctx, IHqlExpression * expr);
    void doReportWarning(WarnErrorCategory category, ErrorSeverity explicitSeverity, IHqlExpression * location, unsigned id, const char * msg);

    void optimizePersists(HqlExprArray & exprs);
    IHqlExpression * convertSetResultToExtract(IHqlExpression * expr);
    void allocateSequenceNumbers(HqlExprArray & exprs);
    void convertLogicalToActivities(WorkflowItem & curWorkflow);
    void flattenDatasets(WorkflowArray & array);

    void spotGlobalCSE(WorkflowItem & curWorkflow);
    IHqlExpression * spotGlobalCSE(IHqlExpression * _expr);
    void spotGlobalCSE(HqlExprArray & exprs);
    IHqlExpression * extractGlobalCSE(IHqlExpression * expr);
    void processCppBodyDirectives(IHqlExpression * expr);


    void markThorBoundaries(WorkflowItem & curWorkflow);
    void normalizeGraphForGeneration(HqlExprArray & exprs, HqlQueryContext & query);
    void applyGlobalOptimizations(HqlExprArray & exprs);
    void transformWorkflowItem(WorkflowItem & curWorkflow);
    bool transformGraphForGeneration(HqlQueryContext & query, WorkflowArray & exprs);
    void processEmbeddedLibraries(HqlExprArray & exprs, HqlExprArray & internalLibraries, bool isLibrary);
    void pickBestEngine(WorkflowArray & array);
    void pickBestEngine(HqlExprArray & exprs);
    IHqlExpression * separateLibraries(IHqlExpression * query, HqlExprArray & internalLibraries);

    void doBuildSerialize(BuildCtx & ctx, IIdAtom * name, IHqlExpression * length, CHqlBoundExpr & bound, const char * bufferName);
    void modifyOutputLocations(HqlExprArray & exprs);
    IHqlExpression * getDefaultOutputAttr(IHqlExpression * expr);
    IHqlExpression * calculatePersistInputCrc(BuildCtx & ctx, IHqlExpression * expr);
    IHqlExpression * calculatePersistInputCrc(BuildCtx & ctx, DependenciesUsed & dependencies);
    bool ifRequiresAssignment(BuildCtx & ctx, IHqlExpression * expr);

    void logGraphEdge(IPropertyTree * subGraph, unsigned __int64 source, unsigned __int64 target, unsigned sourceIndex, unsigned targetIndex, const char * label, bool nWay);

    void beginGraph(const char * graphName = NULL);
    void clearGraph();
    void endGraph();

    double getComplexity(WorkflowArray & exprs);
    double getComplexity(HqlExprArray & exprs);
    double getComplexity(IHqlExpression * expr, ClusterType cluster);
    bool prepareToGenerate(HqlQueryContext & query, WorkflowArray & exprs, bool isEmbeddedLibrary);
    IHqlExpression * getResourcedGraph(IHqlExpression * expr, IHqlExpression * graphIdExpr);
    IHqlExpression * getResourcedChildGraph(BuildCtx & ctx, IHqlExpression * childQuery, unsigned numResults, node_operator graphKind);
    IHqlExpression * optimizeCompoundSource(IHqlExpression * expr, unsigned flags);
    IHqlExpression * optimizeGraphPostResource(IHqlExpression * expr, unsigned csfFlags, bool projectBeforeSpill);
    bool isInlineOk();
    GraphLocalisation getGraphLocalisation(IHqlExpression * expr, bool isInsideChildQuery);
    bool isAlwaysCoLocal();
    bool isNeverDistributed(IHqlExpression * expr);

    void ensureWorkUnitUpdated();
    bool getDebugFlag(const char * name, bool defValue);
    void initOptions();
    void postProcessOptions();
    SourceFieldUsage * querySourceFieldUsage(IHqlExpression * expr);
    void noteAllFieldsUsed(IHqlExpression * expr);
    IHqlExpression * translateGetGraphResult(BuildCtx & ctx, IHqlExpression * expr);

public:
    IHqlExpression * convertToPhysicalIndex(IHqlExpression * tableExpr);
    IHqlExpression * buildIndexFromPhysical(IHqlExpression * expr);
    //MORE: At some point the global getUniqueId() should be killed so there are only local references.
    inline unsigned __int64 getUniqueId() { return ::getUniqueId(); } //{ return ++nextUid; }
    inline StringBuffer & getUniqueId(StringBuffer & target) { return appendUniqueId(target, getUniqueId()); }
    inline unsigned curGraphSequence() const { return activeGraph ? graphSeqNumber : 0; }
    UniqueSequenceCounter & querySpillSequence() { return spillSequence; }

public:
    void traceExpression(const char * title, IHqlExpression * expr, unsigned level=500);
    void traceExpressions(const char * title, HqlExprArray & exprs, unsigned level=500);
    void traceExpressions(const char * title, WorkflowItem & workflow, unsigned level=500) { traceExpressions(title, workflow.queryExprs(), level); };
    void traceExpressions(const char * title, WorkflowArray & exprs);

    void checkNormalized(IHqlExpression * expr);
    void checkNormalized(WorkflowArray & array);
    void checkNormalized(WorkflowItem & workflow) { checkNormalized(workflow.queryExprs()); }
    void checkNormalized(HqlExprArray & exprs);
    void checkNormalized(BuildCtx & ctx, IHqlExpression * expr);

    void checkAmbiguousRollupCondition(IHqlExpression * expr);
    void exportWarningMappings();

protected:
    HqlCppInstance *    code;
    IHqlScope *         internalScope;
    RecordOffsetMap     recordMap;      // no_record -> offset information
    ExprExprMap         physicalIndexCache;
    unsigned            litno;
    StringAttr          soName;
    Owned<ErrorSeverityMapper> globalOnWarnings;
    Owned<ErrorSeverityMapper> localOnWarnings;
    Linked<IErrorReceiver> errorProcessor;
    HqlCppOptions       options;
    HqlCppDerived       derived;
    unsigned            activitiesThisCpp;
    unsigned            curCppFile;
    Linked<ICodegenContextCallback> ctxCallback;
    ClusterType         targetClusterType;
    bool contextAvailable;
    unsigned maxSequence;
    unsigned            startCursorSet;
    bool                requireTable;
    BuildCtx *          activeGraphCtx;
    HqlExprArray        metas;
    Owned<GeneratedGraphInfo> activeGraph;
    unsigned            graphSeqNumber;
    StringAttr          graphLabel;
    NlpParseContext *   nlpParse;               // Not linked so it can try and stay opaque.
    bool                xmlUsesContents;
    CIArrayOf<GlobalFileTracker> globalFiles;
    CIArrayOf<InternalResultTracker> internalResults;
    HqlCppLibraryImplementation *       outputLibrary;          // not linked to opaque
    OwnedHqlExpr         outputLibraryId;
    unsigned            curActivityId;
    unsigned            holeUniqueSequence;
    unsigned            nextUid;
    unsigned            nextTypeId;
    unsigned            nextFieldId;
    unsigned            curWfid;
    unsigned            implicitFunctionId = 0;
    HqlExprArray        internalFunctions;
    HqlExprArray        internalFunctionExternals;
    UniqueSequenceCounter spillSequence;
    
#ifdef SPOT_POTENTIAL_COMMON_ACTIVITIES
    LocationArray       savedActivityLocations;
    HqlExprArray        savedActivities;
#endif
    struct
    {
        HqlExprArray    activityExprs;
        HqlExprArray    activityNorms;
        UnsignedArray   activityIds;
        UnsignedArray   activityCrcs;
    } tracking;

    //The following are only ok to access the top value - but not to build a parent tree.
    //the activities cannot be assumed to be generated in a stackwise manner.
    //e.g, could generate a a.b a.c (while a.b is still active) (because of strength reduction).
    CIArrayOf<ABoundActivity> activeActivities;
    CIArrayOf<GeneratedGraphInfo> graphs;
    HqlExprArray activityExprStack;             //only used for improving the error reporting
    PointerArray recordIndexCache;
    Owned<ITimeReporter> timeReporter;
    CIArrayOf<SourceFieldUsage> trackedSources;
};


//===========================================================================

class HQLCPP_API HqlQueryInstance : implements IHqlQueryInstance, public CInterface
{
public:
    HqlQueryInstance();
    IMPLEMENT_IINTERFACE

    StringBuffer &  queryDllName(StringBuffer & out);

protected:
    unique_id_t         instance;
};

//---------------------------------------------------------------------------------------------------------------------

class CompoundBuilder
{
public:
    CompoundBuilder(node_operator _op);

    void addOperand(IHqlExpression * arg);
    IHqlExpression * getCompound();

protected:
    node_operator op;
    HqlExprAttr compound;
    HqlExprAttr first;
};


class LoopInvariantHelper
{
public:
    LoopInvariantHelper() { active = NULL; }
    ~LoopInvariantHelper() { finished(); }

    bool getBestContext(BuildCtx & ctx, IHqlExpression * expr);

protected:
    void finished();

protected:
    IHqlStmt * active;
};


//===========================================================================

void cvtChooseListToPairs(HqlExprArray & target, IHqlExpression * from, unsigned base);
void cvtIndexListToPairs(HqlExprArray & target, IHqlExpression * from);
void cvtInListToPairs(HqlExprArray & target, IHqlExpression * from, bool valueIfMatch);
HQLCPP_API IHqlExpression * splitLongString(IHqlExpression * expr);
void createTempFor(ITypeInfo * exprType, CHqlBoundTarget & target, const char * prefix);
void checkSelectFlags(IHqlExpression * expr);

extern HQLCPP_API IHqlExpression * getPointer(IHqlExpression * source);
extern IHqlExpression * adjustIndexBaseToZero(IHqlExpression * index);
extern IHqlExpression * adjustIndexBaseToOne(IHqlExpression * index);
extern IHqlExpression * multiplyValue(IHqlExpression * expr, unsigned __int64 value);
extern bool isComplexSet(ITypeInfo * type, bool isConstant);
extern bool isComplexSet(IHqlExpression * expr);
extern bool isConstantSet(IHqlExpression * expr);

extern bool canProcessInline(BuildCtx * ctx, IHqlExpression * expr);
extern bool canIterateInline(BuildCtx * ctx, IHqlExpression * expr);
extern bool canAssignInline(BuildCtx * ctx, IHqlExpression * expr);
extern bool canEvaluateInline(BuildCtx * ctx, IHqlExpression * expr);
extern bool canAssignNotEvaluateInline(BuildCtx * ctx, IHqlExpression * expr);
extern bool isNonLocal(IHqlExpression * expr, bool optimizeParentAccess);
extern bool alwaysEvaluatesToBound(IHqlExpression * expr);

extern void buildClearPointer(BuildCtx & ctx, IHqlExpression * expr);
extern IHqlExpression * getVirtualReplacement(IHqlExpression * expr, IHqlExpression * virtualDef, IHqlExpression * dataset);
extern IHqlExpression * convertWrapperToPointer(IHqlExpression * expr);
extern IHqlExpression * createVariable(ITypeInfo * type);
extern void getMappedFields(HqlExprArray & aggregateFields, IHqlExpression * transform, HqlExprArray & recordFields, IHqlExpression * newSelector);
extern IHqlExpression * queryBlobHelper(BuildCtx & ctx, IHqlExpression * select);

inline bool isInPayload() { return true; }          // placeholder - otherwise records in non payload can't be keyed sometimes.

extern bool filterIsTableInvariant(IHqlExpression * expr);

//NB: Watch out about calling this - if expression can be evaluated in a context; may not mean it is the correct context to evaluate it in
// e.g., if same LEFT selector is in current context and parent.
extern bool mustEvaluateInContext(BuildCtx & ctx, IHqlExpression * expr);
extern const char * boolToText(bool value);
extern bool activityNeedsParent(IHqlExpression * expr);
extern GraphLocalisation queryActivityLocalisation(IHqlExpression * expr, bool optimizeParentAccess);
extern bool isGraphIndependent(IHqlExpression * expr, IHqlExpression * graph);
extern IHqlExpression * adjustBoundIntegerValues(IHqlExpression * left, IHqlExpression * right, bool subtract);
extern bool isNullAssign(const CHqlBoundTarget & target, IHqlExpression * expr);

SubGraphInfo * matchActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag);
bool isActiveGraph(BuildCtx & ctx, IHqlExpression * graphTag);
inline SubGraphInfo * queryActiveSubGraph(BuildCtx & ctx) 
{ 
    return static_cast<SubGraphInfo *>(ctx.queryFirstAssociation(AssocSubGraph));
}

#endif
