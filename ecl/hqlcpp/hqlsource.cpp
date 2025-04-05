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
#include "jliball.hpp"
#include "hql.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "jdebug.hpp"
#include "eclrtl_imp.hpp"
#include "rtlkey.hpp"

#include "hql.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqlttcpp.ipp"
#include "hqlutil.hpp"
#include "hqlthql.hpp"

#include "hqlwcpp.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlopt.hpp"
#include "hqlfold.hpp"
#include "hqlcerrors.hpp"
#include "hqlcatom.hpp"
#include "hqlccommon.hpp"
#include "hqltrans.ipp"
#include "hqlpmap.hpp"
#include "hqlttcpp.ipp"
#include "hqlsource.ipp"
#include "hqlcse.ipp"
#include "hqliter.ipp"
#include "thorcommon.hpp"
#include "hqlinline.hpp"
#include "hqliproj.hpp"

//#define FLATTEN_DATASETS
//#define HACK_TO_IGNORE_TABLE

//#define TraceExprPrintLog(x, expr)                DBGLOG(x ": %s", expr->toString(StringBuffer()).str());
#define TraceExprPrintLog(x, expr)              
//#define TraceTableFields

inline bool needToSerializeRecord(node_operator mode)
{
    return (mode == no_thor || mode == no_flat);
}

inline bool needToSerializeRecord(IHqlExpression * mode)
{
    return needToSerializeRecord(mode->getOperator());
}

//---------------------------------------------------------------------------

void HqlCppTranslator::addGlobalOnWarning(IHqlExpression * setMetaExpr)
{
    globalOnWarnings->addOnWarning(setMetaExpr);
}

unsigned HqlCppTranslator::getSourceAggregateOptimizeFlags() const
{
    const bool insideChildQuery = false; // value does not currently matter
    return getOptimizeFlags(insideChildQuery)|HOOfold|HOOinsidecompound;
}

void HqlCppTranslator::doBuildExprFilepos(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (buildExprInCorrectContext(ctx, expr, tgt, false))
        return;

    throwError(HQLERR_CouldNotResolveFileposition);     // internal error: fileposition should have been available.
}

void HqlCppTranslator::doBuildExprFileLogicalName(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt)
{
    if (buildExprInCorrectContext(ctx, expr, tgt, false))
        return;

    throwError(HQLERR_CouldNotResolveFileposition);     // internal error: fileposition should have been available.
}


//---------------------------------------------------------------------------

node_operator getDatasetKind(IHqlExpression * dataset)
{
    dataset = queryPhysicalRootTable(dataset);
    if (dataset)
    {
        IHqlExpression * mode = dataset->queryChild(2);
        if (mode)
            return mode->getOperator();
    }
    return no_none;
}



unsigned getProjectCount(IHqlExpression * expr)
{
    unsigned projectCount = 0;
    while (expr->getOperator() != no_table)
    {
        switch (expr->getOperator())
        {
        case no_hqlproject:
        case no_usertable:
        case no_newusertable:
        case no_selectfields:
            projectCount++;
            break;
        }
        expr = expr->queryChild(0);
    }
    return projectCount;
}


IHqlExpression * queryFetch(IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_fetch:
            return expr;
        case no_filter:
        case no_compound_fetch:
        case no_limit:
        case no_keyedlimit:
        case no_choosen:
            break;
        default:
            UNIMPLEMENTED;
        }
        expr = expr->queryChild(0);
    }
}


bool isSimpleSource(IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_keyindex:
        case no_newkeyindex:
        case no_table:
        case no_temptable:
        case no_inlinetable:
        case no_workunit_dataset:
        case no_xmlproject:
        case no_null:
        case no_datasetfromrow:
        case no_datasetfromdictionary:
        case no_getgraphresult:
        case no_getgraphloopresult:
        case no_rows:
            return true;
        case no_choosen:
        case no_limit:
        case no_keyedlimit:
        case no_sorted:
        case no_stepped:
        case no_distributed:
        case no_preservemeta:
        case no_unordered:
        case no_grouped:
        case no_compound_diskread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexread:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_selectnew:
        case no_compound_inline:
        case no_section:
        case no_sectioninput:
        case no_nofold:
        case no_nohoist:
        case no_nocombine:
        case no_dataset_alias:
            break;
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}

static bool isSimpleProjectingDiskRead(IHqlExpression * expr)
{
    bool projected = false;
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_table:
            return true;
        case no_hqlproject:
        case no_newusertable:
            if (projected)
                return false;
            //MORE: HPCC-18469 Check if the transform only assigns fields with the same name from source to the target
            if (!isSimpleProject(expr))
                return false;
            projected = true;
            break;
        default:
            return false;
        }
        expr = expr->queryChild(0);
    }
}

IHqlExpression * getVirtualSelector(IHqlExpression * dataset)
{
    IHqlExpression * table = queryPhysicalRootTable(dataset);
    if (!table)
        table = dataset;
    return LINK(table->queryNormalizedSelector());
}

IHqlExpression * getFilepos(IHqlExpression * dataset, bool isLocal)
{
    IHqlExpression * attr = isLocal ? createLocalAttribute() : NULL;
    return createValue(no_filepos, LINK(fposType), getVirtualSelector(dataset), attr);
}

IHqlExpression * getFileLogicalName(IHqlExpression * dataset)
{
    return createValue(no_file_logicalname, makeVarStringType(UNKNOWN_LENGTH, NULL, NULL), getVirtualSelector(dataset));
}

IHqlExpression * getVirtualReplacement(IHqlExpression * field, IHqlExpression * virtualDef, IHqlExpression * dataset)
{
    IAtom * virtualKind = virtualDef->queryName();

    if (virtualKind == filepositionAtom)
        return getFilepos(dataset, false);
    else if (virtualKind == localFilePositionAtom)
        return getFilepos(dataset, true);
    else if (virtualKind == sizeofAtom)
        return createValue(no_sizeof, LINK(sizetType), getVirtualSelector(dataset));
    else if (virtualKind == logicalFilenameAtom)
        return createValue(no_implicitcast, field->getType(), getFileLogicalName(dataset));
    throwError1(HQLERR_UnknownVirtualAttr, str(virtualKind));
    return NULL;
}

static IHqlExpression * createFileposCall(HqlCppTranslator & translator, IIdAtom * name, const char * provider, const char * rowname)
{
    HqlExprArray args;
    args.append(*createVariable(provider, makeBoolType()));
    args.append(*createVariable(rowname, makeBoolType()));  // really a row
    return translator.bindFunctionCall(name, args);
}

//---------------------------------------------------------------------------

void VirtualFieldsInfo::gatherVirtualFields(IHqlExpression * _record, bool ignoreVirtuals, bool ensureSerialized)
{
    OwnedHqlExpr record = ensureSerialized ? getSerializedForm(_record, diskAtom) : LINK(_record);
    if (record != _record)
        requiresDeserialize = true;

    //MORE: This should really recurse through records to check for nested virtual fields.
    // e.g., inside ifblocks, or even records....
    ForEachChild(idx, record)
    {
        IHqlExpression * cur = record->queryChild(idx);
        IHqlExpression * virtualAttr = NULL;
        if (!ignoreVirtuals)
            virtualAttr = cur->queryAttribute(virtualAtom);

        if (virtualAttr)
        {
            selects.append(*LINK(cur));
            if (virtuals.find(*virtualAttr) == NotFound)
                virtuals.append(*LINK(virtualAttr));
        }
        else
        {
            //Also adds attributes...
            physicalFields.append(*LINK(cur));
            if (virtuals.ordinality())
                simpleVirtualsAtEnd = false;
        }
    }
}

IHqlExpression * VirtualFieldsInfo::createPhysicalRecord()
{
    if (physicalFields.ordinality() == 1)
        if (physicalFields.item(0).getOperator() == no_record)
            return LINK(&physicalFields.item(0));
    return createRecord(physicalFields);
}
//---------------------------------------------------------------------------

class VirtualRecordTransformCreator : public RecordTransformCreator
{
public:
    VirtualRecordTransformCreator(IHqlExpression * _dataset) { dataset = _dataset; }

    virtual IHqlExpression * getMissingAssignValue(IHqlExpression * expr)
    {
        IHqlExpression * virtualAttr = expr->queryAttribute(virtualAtom);
        assertex(virtualAttr);
        return getVirtualReplacement(expr, virtualAttr->queryChild(0), dataset);
    }

protected:
    IHqlExpression * dataset;
};

IHqlExpression * createTableWithoutVirtuals(VirtualFieldsInfo & info, IHqlExpression * tableExpr)
{
    IHqlExpression * record = tableExpr->queryChild(1);
    OwnedHqlExpr diskRecord = info.createPhysicalRecord();
    //Clone the annotations to improve the regenerated text in the graph
    OwnedHqlExpr diskRecordWithMeta = record->cloneAllAnnotations(diskRecord);

    HqlExprArray args;
    unwindChildren(args, tableExpr);
    args.replace(*LINK(diskRecordWithMeta), 1);
    IHqlExpression * newDataset = tableExpr->queryBody()->clone(args);

    VirtualRecordTransformCreator mapper(newDataset);
    IHqlExpression * newTransform = mapper.createMappingTransform(no_newtransform, record, newDataset);
    OwnedHqlExpr projected = createDataset(no_newusertable, { newDataset, LINK(record), newTransform, createAttribute(_internal_Atom) });
    return tableExpr->cloneAllAnnotations(projected);
}

IHqlExpression * buildTableWithoutVirtuals(VirtualFieldsInfo & info, IHqlExpression * expr)
{
    IHqlExpression * tableExpr = queryPhysicalRootTable(expr);
    OwnedHqlExpr projected = createTableWithoutVirtuals(info, tableExpr);
    return replaceExpression(expr, tableExpr, projected);
}


static IHqlExpression * createTableFromSerialized(IHqlExpression * tableExpr)
{
    IHqlExpression * record = tableExpr->queryChild(1);
    OwnedHqlExpr diskRecord = getSerializedForm(record, diskAtom);
    OwnedHqlExpr diskRecordWithMeta = record->cloneAllAnnotations(diskRecord);

    OwnedHqlExpr newTable = replaceChild(tableExpr->queryBody(), 1, diskRecordWithMeta);

    OwnedHqlExpr transform = createRecordMappingTransform(no_newtransform, record, newTable->queryNormalizedSelector());
    OwnedHqlExpr projected = createDataset(no_newusertable, { LINK(newTable), LINK(record), LINK(transform), createAttribute(_internal_Atom) });
    return tableExpr->cloneAllAnnotations(projected);
}

static IHqlExpression * buildTableFromSerialized(IHqlExpression * expr)
{
    IHqlExpression * tableExpr = queryPhysicalRootTable(expr);
    OwnedHqlExpr projected = createTableFromSerialized(tableExpr);
    return replaceExpression(expr, tableExpr, projected);
}


static IHqlExpression * nextDiskField(IHqlExpression * diskRecord, unsigned & diskIndex)
{
    for (;;)
    {
        IHqlExpression * cur = diskRecord->queryChild(diskIndex++);
        if (!cur || !cur->isAttribute())
            return cur;
    }
}

static IHqlExpression * queryOriginalKey(IHqlExpression * expr)
{
    IHqlExpression * original = queryAttributeChild(expr, _original_Atom, 0);
    if (original)
        return original;
    else
        return expr;
}



static void createPhysicalLogicalAssigns(HqlExprArray & assigns, IHqlExpression * self, IHqlExpression * diskRecord, IHqlExpression * record, IHqlExpression * diskDataset, bool allowTranslate, unsigned fileposIndex)
{
    unsigned numFields = record->numChildren();
    unsigned diskIndex = 0;
    for (unsigned idx2=0; idx2 < numFields; idx2++)
    {
        IHqlExpression * cur = record->queryChild(idx2);
        switch (cur->getOperator())
        {
        case no_ifblock:
            {
                IHqlExpression * ifblock = nextDiskField(diskRecord, diskIndex);
                assertex(ifblock && ifblock->getOperator() == no_ifblock);
                createPhysicalLogicalAssigns(assigns, self, ifblock->queryChild(1), cur->queryChild(1), diskDataset, false, NotFound);
                break;
            }
        case no_record:
            {
                IHqlExpression * srcRecord = nextDiskField(diskRecord, diskIndex);
                assertex(srcRecord && srcRecord->getOperator() == no_record);
                createPhysicalLogicalAssigns(assigns, self, srcRecord, cur, diskDataset, allowTranslate, NotFound);
                break;
            }
        case no_field:
            {
                OwnedHqlExpr target = createSelectExpr(LINK(self), LINK(cur));
                OwnedHqlExpr newValue;
                IHqlExpression * curPhysical = nextDiskField(diskRecord, diskIndex);
                OwnedHqlExpr physicalSelect = createSelectExpr(LINK(diskDataset), LINK(curPhysical));
                if (cur->isDatarow() && !cur->hasAttribute(blobAtom) && (!isInPayload() || (physicalSelect->queryType() != target->queryType())))
                {
                    HqlExprArray subassigns;
                    OwnedHqlExpr childSelf = createSelector(no_self, cur, NULL);
                    createPhysicalLogicalAssigns(subassigns, childSelf, curPhysical->queryRecord(), cur->queryRecord(), physicalSelect, false, NotFound);
                    OwnedHqlExpr transform = createValue(no_transform, makeTransformType(cur->queryRecord()->getType()), subassigns);
                    newValue.setown(createRow(no_createrow, transform.getClear()));
                }
                else
                    newValue.setown(convertIndexPhysical2LogicalValue(cur, physicalSelect, allowTranslate && (idx2 != fileposIndex)));

                if (newValue)
                    assigns.append(*createAssign(target.getClear(), newValue.getClear()));
                break;
            }
        }

    }
}


static void createPhysicalLogicalAssigns(HqlExprArray & assigns, IHqlExpression * diskDataset, IHqlExpression * tableExpr, bool hasFilePosition)
{
    IHqlExpression * record = tableExpr->queryRecord();
    unsigned fileposIndex = (hasFilePosition ? record->numChildren() - 1 : NotFound);
    OwnedHqlExpr self = getSelf(record);
    createPhysicalLogicalAssigns(assigns, self, diskDataset->queryRecord(), record, diskDataset, true, fileposIndex);
}


IHqlExpression * HqlCppTranslator::convertToPhysicalIndex(IHqlExpression * tableExpr)
{
    LinkedHqlExpr * match = physicalIndexCache.getValue(tableExpr);
    if (match)
        return LINK(*match);

    if (tableExpr->hasAttribute(_original_Atom))
        return LINK(tableExpr);

    assertex(tableExpr->getOperator() == no_newkeyindex);
    IHqlExpression * record = tableExpr->queryRecord();

    HqlMapTransformer mapper;
    bool hasFilePosition = getBoolAttribute(tableExpr, filepositionAtom, true);
    IHqlExpression * diskRecord = createPhysicalIndexRecord(mapper, record, hasFilePosition, false);

    unsigned payload = numPayloadFields(tableExpr);
    assertex(payload || !hasFilePosition);
    HqlExprArray args;
    unwindChildren(args, tableExpr);
    args.replace(*diskRecord, 1);
    removeAttribute(args, _payload_Atom);
    args.append(*createAttribute(_payload_Atom, getSizetConstant(payload)));
    args.append(*createAttribute(_original_Atom, LINK(tableExpr)));

    //remove the preload attribute and replace with correct value
    IHqlExpression * newDataset = createDataset(tableExpr->getOperator(), args);

    HqlExprArray assigns;
    createPhysicalLogicalAssigns(assigns, newDataset, tableExpr, hasFilePosition);
    OwnedHqlExpr projectedTable = createDataset(no_newusertable, newDataset, createComma(LINK(record), createValue(no_newtransform, makeTransformType(record->getType()), assigns)));
    physicalIndexCache.setValue(tableExpr, projectedTable);
    return projectedTable.getClear();
}


IHqlExpression * convertToPhysicalTable(IHqlExpression * tableExpr, bool ensureSerialized)
{
    VirtualFieldsInfo fieldInfo;
    fieldInfo.gatherVirtualFields(tableExpr->queryRecord(), tableExpr->hasAttribute(_noVirtual_Atom), ensureSerialized);
    if (fieldInfo.hasVirtualsOrDeserialize())
        return createTableWithoutVirtuals(fieldInfo, tableExpr);
    return LINK(tableExpr);
}


IHqlExpression * HqlCppTranslator::buildIndexFromPhysical(IHqlExpression * expr)
{
    IHqlExpression * tableExpr = queryPhysicalRootTable(expr);
    OwnedHqlExpr newProject;
    if (queryOptions().newIndexReadMapping && !recordContainsBlobs(tableExpr->queryRecord()))
    {
        //once it is legal for the input to a transform to be non-serialized then following should be enabled
        //return LINK(expr);

        IHqlExpression * record = tableExpr->queryChild(1);
        OwnedHqlExpr diskRecord = getSerializedForm(record, diskAtom);
        if (record == diskRecord)
            return LINK(expr);

        OwnedHqlExpr newDataset = replaceChild(tableExpr, 1, diskRecord);

        VirtualRecordTransformCreator mapper(newDataset);
        IHqlExpression * newTransform = mapper.createMappingTransform(no_newtransform, record, newDataset);
        newProject.setown(createDataset(no_newusertable, { LINK(newDataset), LINK(record), newTransform, createAttribute(_internal_Atom) }));
        newProject.setown(tableExpr->cloneAllAnnotations(newProject));
    }
    else
        newProject.setown(convertToPhysicalIndex(tableExpr));
    return replaceExpression(expr, tableExpr, newProject);
}

//---------------------------------------------------------------------------

class SourceSteppingInfo
{
public:
    inline bool exists() { return rawStepping.exists(); }

    IHqlExpression * firstStepped()
    {
        if (rawStepping.exists())
            return rawStepping.fields->queryChild(0);
        return NULL;
    }

    void extractRaw()
    {
        rawSteppingProject.extractFields(rawStepping);
    }

    void checkKeyable(CppFilterExtractor & monitors)
    {
        if (!rawStepping.exists())
            return;
        unsigned prev = NotFound;
        ForEachChild(i, rawStepping.fields)
        {
            IHqlExpression * cur = rawStepping.fields->queryChild(i);
            unsigned thisMatch = monitors.queryKeySelectIndex(cur);
            if (thisMatch == NotFound)
                throwError1(HQLERR_StepFieldNotKeyed, str(cur->queryChild(1)->queryName()));
            if ((prev != NotFound) && (thisMatch != prev+1))
                throwError1(HQLERR_StepFieldNotContiguous, str(cur->queryChild(1)->queryName()));
            prev = thisMatch;
        }
    }

    void generateMembers(HqlCppTranslator & translator, BuildCtx & ctx)
    {
        rawStepping.generateSteppingMetaMember(translator, ctx, "RawSteppingMeta");

        if (outputStepping.exists())
        {
            if (outputStepping.exists() && outputStepping.ds != rawStepping.ds)
            {
                outputStepping.generateSteppingMetaMember(translator, ctx, "ProjectedSteppingMeta");

                MemberFunction func(translator, ctx, "virtual void mapOutputToInput(ARowBuilder & crSelf, const void * _projected, unsigned numFields) override");
                translator.ensureRowAllocated(func.ctx, "crSelf");
                func.ctx.addQuotedLiteral("const byte * pr = (const byte *)_projected;");

                translator.bindTableCursor(func.ctx, rawStepping.ds, "crSelf.row()");
                translator.bindTableCursor(func.ctx, outputStepping.ds, "pr");
                StringBuffer s;
                ForEachChild(i, outputStepping.fields)
                {
                    IHqlExpression * curOutput = outputStepping.fields->queryChild(i);
                    IHqlExpression * curRawExpr = rawSteppingProject.fields->queryChild(i);
                    IHqlExpression * curRawSelect = rawStepping.fields->queryChild(i);
                    OwnedHqlExpr original = outputStepping.invertTransform(curRawExpr, curOutput);
                    func.ctx.addQuoted(s.clear().append("if (numFields < ").append(i+1).append(") return;"));
                    translator.buildAssign(func.ctx, curRawSelect, original);
                }
            }
        }
        else
        {
            OwnedHqlExpr fail = createValue(no_fail, makeVoidType(), createConstant("Cannot step output of index read"));
            MemberFunction func(translator, ctx, "virtual void mapOutputToInput(ARowBuilder & crSelf, const void * _projected, unsigned numFields) override");
            translator.buildStmt(func.ctx, fail);
        }
    }

public:
    SteppingFieldSelection outputStepping;
    SteppingFieldSelection rawSteppingProject;
    SteppingFieldSelection rawStepping;
};

//---------------------------------------------------------------------------

static bool forceLegacyMapping(IHqlExpression * expr)
{
    //Use __OPTION__(LEGACY(TRUE)) to force legacy mapping code
    IHqlExpression * options = expr->queryAttribute(__option__Atom);
    return getBoolAttribute(options, legacyAtom, false);
}

class SourceBuilder
{
public:
    SourceBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr, bool canReadWriteGenerically, bool forceReadWriteGenerically)
        : tableExpr(_tableExpr), newInputMapping(false), translator(_translator)
    { 
        nameExpr.setown(foldHqlExpression(_nameExpr));
        needDefaultTransform = true; 
        needToCallTransform = false; 
        isPreloaded = false;
        isCompoundCount = false;
        transformCanFilter = false;
        IHqlExpression * preload = tableExpr ? tableExpr->queryAttribute(preloadAtom) : NULL;
        if (preload)
        {
            isPreloaded = true;
            preloadSize.set(preload->queryChild(0));
        }
        failedFilterValue.set(queryZero());
        isNormalize = false;
        aggregation = false;
        instance = NULL;
        returnIfFilterFails = true;
        useFilterMappings = true;
        generateUnfilteredTransform = false;
        allowDynamicFormatChange = tableExpr && !tableExpr->hasAttribute(fixedAtom);
        onlyExistsAggreate = false;
        monitorsForGrouping = false;
        useImplementationClass = false;
        isUnfilteredCount = false;
        requiresOrderedMerge = false;
        useGenericDiskReadWrite = (translator.queryOptions().genericDiskReadWrites && canReadWriteGenerically) || forceReadWriteGenerically;
        rootSelfRow = NULL;
        activityKind = TAKnone;

        if (tableExpr)
        {
            if (isKey(tableExpr))
                newInputMapping = translator.queryOptions().newIndexReadMapping;
            else
                newInputMapping = translator.queryOptions().newDiskReadMapping;

            if (!useGenericDiskReadWrite && forceLegacyMapping(tableExpr))
                newInputMapping = false;

            //If this index has been translated using the legacy method then ensure we continue to use that method
            if (isKey(tableExpr) && queryAttributeChild(tableExpr, _original_Atom, 0))
                newInputMapping = false;
            switch (tableExpr->getOperator())
            {
            case no_fetch:
            case no_compound_fetch:
                newInputMapping = false;
                break;
            }
        }
        else
            newInputMapping = false;
    }
    virtual ~SourceBuilder() {}

    virtual void buildMembers(IHqlExpression * expr) = 0;
    virtual void buildTransformFpos(BuildCtx & transformCtx) = 0;
    virtual void extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds);
    virtual void buildTransformElements(BuildCtx & ctx, IHqlExpression * expr, bool ignoreFilters);
    virtual void buildTransform(IHqlExpression * expr) = 0;
    virtual void analyse(IHqlExpression * expr);

    void buildCanMatch(IHqlExpression * expr);
    void buildMatchFilter(BuildCtx & ctx, IHqlExpression * expr);

    void buildFilenameMember();
    void appendFilter(SharedHqlExpr & unkeyedFilter, IHqlExpression * expr);
    void buildKeyedLimitHelper(IHqlExpression * expr);
    void buildLimits(BuildCtx & classctx, IHqlExpression * expr, unique_id_t id);
    void buildReadMembers( IHqlExpression * expr);
    void buildSteppingMeta(IHqlExpression * expr, CppFilterExtractor * monitors);
    void buildTransformBody(BuildCtx & transformCtx, IHqlExpression * expr, bool returnSize, bool ignoreFilters, bool bindInputRow);
    void checkDependencies(BuildCtx & ctx, IHqlExpression * expr);
    bool containsStepping(IHqlExpression * expr);
    ABoundActivity * buildActivity(BuildCtx & ctx, IHqlExpression * expr, ThorActivityKind activityKind, const char *kind, ABoundActivity *input);
    void gatherVirtualFields(bool ignoreVirtuals, bool ensureSerialized);
    void deduceDiskRecords();
    void deduceIndexRecords();
    bool recordHasVirtuals()                                { return fieldInfo.hasVirtuals(); }
    bool recordHasVirtualsOrDeserialize()                               { return fieldInfo.hasVirtualsOrDeserialize(); }
    bool isSourceInvariant(IHqlExpression * dataset, IHqlExpression * expr);
    bool hasExistChoosenLimit() { return (choosenValue && getIntValue(choosenValue) == 1); }
    bool isRootSelector(IHqlExpression * expr) const;

protected:
    void assignLocalExtract(BuildCtx & ctx, ParentExtract * extractBuilder, IHqlExpression * dataset, const char * argName);
    void associateFilePositions(BuildCtx & ctx, const char * provider, const char * rowname);
    void buildSteppedHelpers();
    void doBuildAggregateSelectIterator(BuildCtx & ctx, IHqlExpression * expr);
    void doBuildNormalizeIterators(BuildCtx & ctx, IHqlExpression * expr, bool isChildIterator);
    void buildAggregateHelpers(IHqlExpression * expr);
    void buildCountHelpers(IHqlExpression * expr, bool allowMultiple);
    virtual void buildFlagsMember(IHqlExpression * expr) {}
    void buildGlobalGroupAggregateHelpers(IHqlExpression * expr);
    void buildGroupAggregateHelpers(ParentExtract * extractBuilder, IHqlExpression * aggregate);
    void buildGroupAggregateCompareHelper(ParentExtract * extractBuilder, IHqlExpression * aggregate, HqlExprArray & recordFields, HqlExprArray & agrgegateFields);
    void buildGroupAggregateHashHelper(ParentExtract * extractBuilder, IHqlExpression * dataset, IHqlExpression * fields);
    void buildGroupAggregateProcessHelper(ParentExtract * extractBuilder, IHqlExpression * aggregate, const char * name, bool doneAny);
    void buildGroupingMonitors(IHqlExpression * expr, CppFilterExtractor & monitors);
    void buildGroupAggregateTransformBody(BuildCtx & transformctx, IHqlExpression * expr, bool useExtract, bool bindInputRow);
    void buildNormalizeHelpers(IHqlExpression * expr);
    void buildTargetCursor(Shared<BoundRow> & tempRow, Shared<BoundRow> & rowBuilder, BuildCtx & ctx, IHqlExpression * expr);
    void associateTargetCursor(BuildCtx & subctx, BuildCtx & ctx, BoundRow * tempRow, BoundRow * rowBuilder, IHqlExpression * expr);
    IHqlExpression * ensureAggregateGroupingAliased(IHqlExpression * aggregate);
    void gatherSteppingMeta(IHqlExpression * expr, SourceSteppingInfo & info);
    void gatherSteppingMeta(IHqlExpression * expr, SteppingFieldSelection & outputStepping, SteppingFieldSelection & rawStepping);
    void gatherFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr);
    void rebindFilepositons(BuildCtx & ctx, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq, bool isLocal);
    void rebindFilepositons(BuildCtx & ctx, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq);

    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr);
    virtual void analyseGraph(IHqlExpression * expr);
    virtual bool isExplicitExists() { return false; }

public:
    VirtualFieldsInfo fieldInfo;
    ActivityInstance * instance;
    BoundRow *      rootSelfRow;
    HqlExprAttr     tableExpr;
    HqlExprAttr     nameExpr;
    HqlExprAttr     fpos;
    HqlExprAttr     lfpos;
    HqlExprAttr     limitExpr;
    HqlExprAttr     keyedLimitExpr;
    HqlExprAttr     choosenValue;
    HqlExprAttr     preloadSize;
    HqlExprAttr     firstTransformer;
    HqlExprAttr     lastTransformer;
    HqlExprAttr     alreadyDoneFlag;
    HqlExprArray    originalFilters;
    HqlExprArray    mappedFilters;
    HqlExprArray    removedFilters;
    HqlExprAttr     failedFilterValue;
    HqlExprAttr     compoundCountVar;
    HqlExprAttr     physicalRecord;
    HqlExprAttr     inputRecord;        // The format of the row that is passed to the transform
    LinkedHqlExpr   expectedRecord;
    LinkedHqlExpr   projectedRecord;
    LinkedHqlExpr   tableSelector;
    LinkedHqlExpr   projectedSelector;
    HqlExprAttr     steppedExpr;
    Linked<BuildCtx> globaliterctx;
    HqlExprCopyArray parentCursors;
    HqlExprAttr     logicalFilenameMarker;
    ThorActivityKind activityKind;
    bool            useFilterMappings;
    bool            needDefaultTransform;
    bool            needToCallTransform;
    bool            isPreloaded;
    bool            transformCanFilter;
    bool            isCompoundCount;
    bool            isNormalize;
    bool            aggregation;
    bool            returnIfFilterFails;
    bool            allowDynamicFormatChange;
    bool            onlyExistsAggreate;
    bool            monitorsForGrouping;
    bool            generateUnfilteredTransform;
    bool            useImplementationClass;
    bool            isUnfilteredCount;
    bool            isVirtualLogicalFilenameUsed = false;
    bool            isVirtualLogicalFileposUsed = false;
    bool            transformUsesVirtualLogicalFilename = false;
    bool            transformUsesVirtualFilePosition = false;
    bool            requiresOrderedMerge;
    bool            newInputMapping;
    bool            extractCanMatch = false;
    bool            useGenericDiskReadWrite = false;
    bool            hasDynamicOptions = false;

protected:
    HqlCppTranslator & translator;
};


struct HQLCPP_API MonitoredDefinedValue : public HqlSimpleDefinedValue
{
public:
    MonitoredDefinedValue(bool & _usedFlag, IHqlExpression * _original, IHqlExpression * _expr)
    : HqlSimpleDefinedValue(_original, _expr), usedFlag(_usedFlag)
    { }

    virtual IHqlExpression * queryExpr() const
    {
        usedFlag = true;
        return HqlSimpleDefinedValue::queryExpr();
    }

public:
    bool & usedFlag;
};



bool SourceBuilder::isSourceInvariant(IHqlExpression * dataset, IHqlExpression * expr)
{
    if (containsAssertKeyed(expr))
        return false;
    if (!containsActiveDataset(expr))
        return true;

    HqlExprCopyArray inScope;
    expr->gatherTablesUsed(inScope);
    if (!canEvaluateInScope(parentCursors, inScope))
        return false;

    //Carefull....  It looks ok, but it is possible that the same dataset occurs at multiple levels (sqfilt.hql)
    //So need to be careful that the datasets being referenced aren't newly in scope for this activity...
    for (;;)
    {
        if (inScope.contains(*dataset->queryNormalizedSelector()))
            return false;

        //To be strictly correct we need to walk no_select chain of root datasets!
        dataset = queryNextMultiLevelDataset(dataset, true);
        if (!dataset)
            return true;
    }
}

bool SourceBuilder::isRootSelector(IHqlExpression * expr) const
{
    if (!tableExpr)
        return false;
    return (expr->queryNormalizedSelector() == tableExpr->queryNormalizedSelector());
}

void SourceBuilder::analyse(IHqlExpression * expr)
{
    IHqlExpression * body = expr->queryBody(true);
    if (expr != body)
    {
        switch (expr->getAnnotationKind())
        {
        case annotate_meta:
            //the onwarning state will be restored by the scope held in HqlCppTranslator::buildActivity
            translator.localOnWarnings->processMetaAnnotation(expr);
            break;
        case annotate_location:
            instance->addLocationAttribute(expr);
            break;
        case annotate_symbol:
            //don't clear onWarnings when we hit a symbol because the warnings within a compound activity aren't generated at the correct point
            instance->addNameAttribute(expr);
            break;
        }
        analyse(body);
        return;
    }

    node_operator op = expr->getOperator();

    switch (op)
    {
    case no_cachealias:
        analyse(expr->queryChild(1));
        return;
    case no_newkeyindex:
    case no_table:
    case no_fetch:
    case no_select:     // handled below
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_workunit_dataset:
    case no_call:
    case no_externalcall:
    case no_rows:
    case no_libraryinput:
        break;
    default:
        analyse(expr->queryChild(0));
        break;
    }

    switch (op)
    {
    case no_table:
    case no_newkeyindex:
        if (!newInputMapping)
        {
            assertex(!fieldInfo.hasVirtuals());
        }
        if (newInputMapping)
        {
            if (!tableExpr->hasAttribute(_noVirtual_Atom) && (tableExpr->queryChild(2)->getOperator() != no_pipe))
            {
                if (containsVirtualField(tableExpr->queryRecord(), logicalFilenameAtom))
                    isVirtualLogicalFilenameUsed = true;
                if (containsVirtualField(tableExpr->queryRecord(), filepositionAtom) || containsVirtualField(tableExpr->queryRecord(), localFilePositionAtom))
                    isVirtualLogicalFileposUsed = true;
            }
        }
        break;
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_call:
    case no_externalcall:
    case no_rows:
    case no_libraryinput:
        break;
    case no_filter:
        {
            //LIMIT(ds(filter1))(filter2) cannot be implemented as a compound activity - because it is impossible to count the rows being
            //filtered by filter1.
            if (limitExpr)
                throwError(HQLERR_CannotFilterLimitInsideActivity);
            OwnedHqlExpr unkeyedFilter;
            HqlExprArray conds;
            unwindFilterConditions(conds, expr);
            extractMonitors(expr, unkeyedFilter, conds);

            if (unkeyedFilter)
            {
                if (!extractCanMatch || !isRootSelector(expr))
                    transformCanFilter = true;
                originalFilters.append(*LINK(expr));
                mappedFilters.append(*unkeyedFilter.getClear());
            }
            else
                removedFilters.append(*LINK(expr));
            break;
        }
    case no_select:
        {
            bool isNew;
            IHqlExpression * ds = querySelectorDataset(expr, isNew);
            if (isNew && isMultiLevelDatasetSelector(expr, false))
            {
                if (!translator.resolveSelectorDataset(instance->startctx, ds))
                {
                    analyse(ds);
                    isNormalize = true;
                }
            }
            break;
        }
    case no_stepped:
        if (steppedExpr)
            throwError(HQLERR_MultipleStepped);
        steppedExpr.set(expr);
        break;
    case no_sorted:
    case no_distributed:
    case no_preservemeta:
    case no_unordered:
    case no_grouped:
    case no_alias_scope:
    case no_section:
    case no_sectioninput:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
    case no_dataset_alias:
        break;
    case no_preload:
        isPreloaded = true;
        preloadSize.set(queryRealChild(expr, 1));
        break;
    case no_limit:
        limitExpr.set(expr);
        break;
    case no_keyedlimit:
        keyedLimitExpr.set(expr);
        break;
    case no_choosen:
        {
            choosenValue.set(expr->queryChild(1));
            IHqlExpression * first = queryRealChild(expr, 2);
            if (first)
            {
                Owned<ITypeInfo> type = makeIntType(8, true);
                choosenValue.setown(createValue(no_sub, LINK(type), createValue(no_add, LINK(type), ensureExprType(choosenValue, type), ensureExprType(first, type)), createConstant(I64C(1))));
            }
            choosenValue.setown(foldHqlExpression(choosenValue));
        }
        break;
    case no_hqlproject:
    case no_newusertable:
        needToCallTransform = true;
        needDefaultTransform = false;
        if (transformContainsSkip(queryNewColumnProvider(expr)))
            transformCanFilter = true;
        if (!firstTransformer)
            firstTransformer.set(expr);
        lastTransformer.set(expr);
        break;
    case no_aggregate:
        {
            needToCallTransform = true;
            needDefaultTransform = false;
            aggregation = true;
            if (!firstTransformer)
                firstTransformer.set(expr);
            lastTransformer.set(expr);
            break;
        }
    case no_newaggregate:
        {
            needToCallTransform = true;
            needDefaultTransform = false;
            aggregation = true;
            if (!firstTransformer)
                firstTransformer.set(expr);
            lastTransformer.set(expr);

            IHqlExpression * transform = expr->queryChild(2);
            node_operator aggOp = queryTransformSingleAggregate(transform);
            onlyExistsAggreate = ((aggOp == no_existsgroup) || (aggOp == no_none));     // The implicit project code can remove all the aggregate() operators....
            if (isCompoundCount)
            {
                IHqlExpression * rhs = transform->queryChild(0)->queryChild(1);
                IHqlExpression * filter = queryRealChild(rhs, 0);
                if (filter)
                    transformCanFilter = true;
            }
            break;
        }
    case no_usertable:
    case no_selectfields:
        UNIMPLEMENTED;
        break;
    case no_fetch:
        needToCallTransform = true;
        needDefaultTransform = false;
        if (!firstTransformer)
            firstTransformer.set(expr);
        lastTransformer.set(expr);
        break;
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_fetch:
    case no_compound_selectnew:
        break;
    default:
        throwUnexpectedOp(op);
    }
}

void SourceBuilder::appendFilter(SharedHqlExpr & unkeyedFilter, IHqlExpression * expr)
{
    if (expr)
    {
        if (expr->queryValue())
        {
            if (!expr->queryValue()->getBoolValue())
                unkeyedFilter.set(expr);
        }
        else
        {
            if (unkeyedFilter)
                unkeyedFilter.setown(createValue(no_and, makeBoolType(), unkeyedFilter.getClear(), LINK(expr)));
            else
                unkeyedFilter.set(expr);
        }
    }
}


void SourceBuilder::associateFilePositions(BuildCtx & ctx, const char * provider, const char * rowname)
{
    if (fpos)
    {
        Owned<IHqlExpression> fposExpr = createFileposCall(translator, getFilePositionId, provider, rowname);
        ctx.associateOwn(*new MonitoredDefinedValue(transformUsesVirtualFilePosition, fpos, fposExpr));
    }

    if (lfpos)
    {
        Owned<IHqlExpression> fposExpr = createFileposCall(translator, getLocalFilePositionId, provider, rowname);
        ctx.associateOwn(*new MonitoredDefinedValue(transformUsesVirtualFilePosition, lfpos, fposExpr));
    }

    if (logicalFilenameMarker)
    {
        Owned<IHqlExpression> nameExpr = createFileposCall(translator, queryLogicalFilenameId, provider, rowname);
        ctx.associateOwn(*new MonitoredDefinedValue(transformUsesVirtualLogicalFilename, logicalFilenameMarker, nameExpr));
    }
}


void SourceBuilder::rebindFilepositons(BuildCtx & ctx, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq, bool isLocal)
{
    if (!tableExpr)
        return;
    OwnedHqlExpr searchPos = getFilepos(tableExpr, isLocal);
    HqlExprAssociation * match = ctx.queryMatchExpr(searchPos);
    if (match)
    {
        OwnedHqlExpr selector = createSelector(side, dataset, selSeq);
        OwnedHqlExpr selectorFpos = getFilepos(selector, isLocal);
        ctx.associateOwn(*new MonitoredDefinedValue(transformUsesVirtualFilePosition, selectorFpos, match->queryExpr()));
    }
}


void SourceBuilder::rebindFilepositons(BuildCtx & ctx, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq)
{
    // don't allow the rebinding to modify these flags.
    bool savedVirtualLogicalFilenameUsed = transformUsesVirtualLogicalFilename;
    bool savedVirtualFilePositionUsed = transformUsesVirtualFilePosition;

    rebindFilepositons(ctx, dataset, side, selSeq, true);
    rebindFilepositons(ctx, dataset, side, selSeq, false);
    OwnedHqlExpr searchLogicalFilename = getFileLogicalName(dataset);
    HqlExprAssociation * match = ctx.queryMatchExpr(searchLogicalFilename);
    if (match)
    {
        OwnedHqlExpr selector = createSelector(side, dataset, selSeq);
        OwnedHqlExpr selectorLogicalFilename = getFileLogicalName(dataset);
        ctx.associateOwn(*new MonitoredDefinedValue(transformUsesVirtualLogicalFilename, selectorLogicalFilename, match->queryExpr()));
    }
    transformUsesVirtualLogicalFilename = savedVirtualLogicalFilenameUsed;
    transformUsesVirtualFilePosition = savedVirtualFilePositionUsed;
}



void SourceBuilder::buildFilenameMember()
{
    //---- virtual const char * getFileName() { return "x.d00"; } ----
    SummaryType summaryType = SummaryType::ReadFile;
    switch (activityKind)
    {
        case TAKindexread:
        case TAKindexnormalize:
        case TAKindexaggregate:
        case TAKindexcount:
        case TAKindexgroupaggregate:
            summaryType = SummaryType::ReadIndex;
            break;
        case TAKspillread:
            summaryType = SummaryType::SpillFile;
            break;
    }
    if (tableExpr->hasAttribute(_spill_Atom))
        summaryType = SummaryType::SpillFile;
    else if (tableExpr->hasAttribute(jobTempAtom))
        summaryType = SummaryType::JobTemp;
    else if (tableExpr->hasAttribute(_workflowPersist_Atom))
        summaryType = SummaryType::PersistFile;
    translator.buildFilenameFunction(*instance, instance->startctx, WaFilename, "getFileName", nameExpr, translator.hasDynamicFilename(tableExpr), summaryType, tableExpr->hasAttribute(optAtom), tableExpr->hasAttribute(_signed_Atom));
}

void SourceBuilder::buildReadMembers(IHqlExpression * expr)
{
    buildFilenameMember();

    //---- virtual bool needTransform() { return <bool>; } ----
    if (needToCallTransform || transformCanFilter)
        translator.doBuildBoolFunction(instance->classctx, "needTransform", true);

    //---- virtual bool transformMayFilter() { return <bool>; } ----
    if (transformCanFilter)
        translator.doBuildBoolFunction(instance->classctx, "transformMayFilter", true);

    if (translator.queryOptions().generateDiskFormats)
        translator.addFormatAttribute(*instance, WaDiskFormat, tableExpr->queryRecord());
}

void SourceBuilder::buildLimits(BuildCtx & classctx, IHqlExpression * expr, unique_id_t id)
{
    if (limitExpr)
        translator.buildLimitHelpers(classctx, limitExpr, nameExpr, id);

    if (choosenValue)
    {
        MemberFunction func(translator, classctx, "virtual unsigned __int64 getChooseNLimit() override");
        OwnedHqlExpr newLimit = ensurePositiveOrZeroInt64(choosenValue);
        translator.buildReturn(func.ctx, newLimit);
    }
}

void SourceBuilder::buildTransformBody(BuildCtx & transformCtx, IHqlExpression * expr, bool returnSize, bool ignoreFilters, bool bindInputRow)
{
    if (tableExpr && bindInputRow)
    {
        IHqlExpression * mode = (tableExpr->getOperator() == no_table) ? tableExpr->queryChild(2) : NULL;
        if (mode && mode->getOperator() == no_csv && !useGenericDiskReadWrite)
        {
            translator.bindCsvTableCursor(transformCtx, tableExpr, "Src", no_none, NULL, true, queryCsvEncoding(mode));
        }
        else
        {
            translator.bindTableCursor(transformCtx, projectedSelector, "left");
        }
        transformCtx.addGroup();
    }

    rootSelfRow = translator.bindSelf(transformCtx, expr, "crSelf");
    buildTransformFpos(transformCtx);
    buildTransformElements(transformCtx, expr, ignoreFilters);

    if (returnSize)
    {
        CHqlBoundExpr boundTargetSize;
        if (needDefaultTransform)
        {
            IHqlExpression * left = expr->queryNormalizedSelector();
            OwnedHqlExpr source = ensureActiveRow(left);
            translator.buildAssign(transformCtx, rootSelfRow->querySelector(), source);
        }
        translator.getRecordSize(transformCtx, rootSelfRow->querySelector(), boundTargetSize);

        transformCtx.setNextDestructor();
        transformCtx.addReturn(boundTargetSize.expr);
    }
    rootSelfRow = NULL;
}

void SourceBuilder::buildTargetCursor(Shared<BoundRow> & tempRow, Shared<BoundRow> & rowBuilder, BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(lastTransformer != NULL);
    if (expr == lastTransformer)
    {
        rowBuilder.set(rootSelfRow);
    }
    else
    {
        tempRow.setown(translator.declareTempAnonRow(ctx, ctx, expr));
        ctx.addGroup();
        // group is important, otherwise sizeof(self.x) gets cached incorrectly
        // not so sure, but references to LEFT(x) may be misresolved
        rowBuilder.setown(translator.createRowBuilder(ctx, tempRow));
    }
}

void SourceBuilder::associateTargetCursor(BuildCtx & subctx, BuildCtx & ctx, BoundRow * tempRow, BoundRow * rowBuilder, IHqlExpression * expr)
{
    //First remove the old active dataset
    //This is not strictly necessary, but it avoids the redundant row being serialized to any child queries
    BoundRow * oldCursor = translator.resolveSelectorDataset(ctx, expr->queryChild(0));
    ctx.removeAssociation(oldCursor);

    //And add an association for expr
    if (tempRow)
    {
        translator.finalizeTempRow(subctx, tempRow, rowBuilder);
        translator.bindTableCursor(ctx, expr, tempRow->queryBound());
    }
    else
        translator.bindTableCursor(ctx, expr, rowBuilder->queryBound());
}


void SourceBuilder::buildTransformElements(BuildCtx & ctx, IHqlExpression * expr, bool ignoreFilters)
{
    //This function can be called again for the unfiltered tranform.  Don't process annotations again.
    if ((expr != instance->activityExpr) && !ignoreFilters)
        instance->processAnnotations(expr);

    expr = expr->queryBody();
    node_operator op = expr->getOperator();

    switch (op)
    {
    case no_cachealias:
        buildTransformElements(ctx, expr->queryChild(1), ignoreFilters);
        return;
    case no_newkeyindex:
    case no_table:
    case no_fetch:
    case no_select:     // handled below
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_call:
    case no_externalcall:
    case no_rows:
    case no_libraryinput:
        break;
    default:
        buildTransformElements(ctx, expr->queryChild(0), ignoreFilters);
        break;
    }

    switch (op)
    {
    case no_newkeyindex:
    case no_table:
        if (newInputMapping)
        {
        }
        else
        {
            assertex(!fieldInfo.hasVirtuals());
        }
        break;
    case no_null:
    case no_anon:
    case no_pseudods:
        break;
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_call:
    case no_externalcall:
    case no_rows:
    case no_libraryinput:
        throwUnexpectedOp(op);
        break;
    case no_select:
        processTransformSelect(ctx, expr);
        break;
    case no_sorted:
    case no_stepped:
    case no_distributed:
    case no_preservemeta:
    case no_unordered:
    case no_grouped:
    case no_preload:
    case no_limit:
    case no_keyedlimit:
    case no_choosen:
    case no_alias_scope:
    case no_section:
    case no_sectioninput:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
        break;
    case no_filter:
        {
            if (!ignoreFilters && (!extractCanMatch || !isRootSelector(expr)))
            {
                LinkedHqlExpr cond;
                if (useFilterMappings)
                {
                    unsigned match = originalFilters.find(*expr);
                    if (match != NotFound)
                        cond.set(&mappedFilters.item(match));
                }
                else
                {
                    HqlExprArray args;
                    unwindRealChildren(args, expr, 1);
                    cond.setown(createBalanced(no_and, queryBoolType(), args));
                }

                if (cond)
                {
                    IHqlExpression * ds = expr->queryChild(0);
                    OwnedHqlExpr test = returnIfFilterFails ? getInverse(cond) : LINK(cond);
                    if (translator.queryOptions().foldFilter)
                        test.setown(foldScopedHqlExpression(translator.queryErrorProcessor(), ds->queryNormalizedSelector(), test));

                    if (translator.options.spotCSE)
                        test.setown(spotScalarCSE(test, ds, translator.queryOptions().spotCseInIfDatasetConditions));

                    if (!returnIfFilterFails)
                        translator.buildFilter(ctx, test);
                    else
                    {
                        LinkedHqlExpr mismatchReturnValue = failedFilterValue;
                        //If the output row has already been generated, then returning at this point will leak any
                        //child datasets. To avoid that we explicitly call the destructor on the output row.
                        if (recordRequiresDestructor(expr->queryRecord()))
                        {
                            if (lastTransformer && lastTransformer->queryNormalizedSelector() == expr->queryNormalizedSelector())
                            {
                                StringBuffer s;
                                translator.buildMetaForRecord(s, expr->queryRecord());
                                s.append(".destruct(crSelf.row())");
                                OwnedHqlExpr cleanupAction = createQuoted(s.str(), makeVoidType());
                                //Create a compound expression (destroy-old, return-value)
                                mismatchReturnValue.setown(createCompound(LINK(cleanupAction), LINK(mismatchReturnValue)));
                            }
                        }
                        translator.buildFilteredReturn(ctx, test, mismatchReturnValue);
                    }
                }
            }
        }
        break;
    case no_hqlproject:
        {
            IHqlExpression * dataset = expr->queryChild(0);
            IHqlExpression * datasetSelector = dataset->queryNormalizedSelector();
            IHqlExpression * selSeq = querySelSeq(expr);
            OwnedHqlExpr leftSelect = createSelector(no_left, dataset, querySelSeq(expr));

            if ((projectedSelector != tableSelector) && (expr == firstTransformer))
                datasetSelector = projectedSelector;

            //Following is a bit nasty....
            //Converting the no_hqlproject to a no_newusertable means that some of the expressions
            //are commoned up with expressions calculated by a previous filter, reducing the code.
            //However, it isn't valid if transform contains an instance of newSelect - 
            //e.g. project(i(x), transform(exists(i....))) - see jholt25.xhql
            //And unfortunately it fails silently.
            //So we use queryReplaceSelector which fails if an ambiguity is introduced by the replacement
            OwnedHqlExpr newSelect = ensureActiveRow(datasetSelector);
            OwnedHqlExpr transform = queryNewReplaceSelector(expr->queryChild(1), leftSelect, newSelect);

            BuildCtx subctx(ctx);       // buildTargetCursor adds group if necessary
            Linked<BoundRow> tempRow;
            Linked<BoundRow> rowBuilder;
            buildTargetCursor(tempRow, rowBuilder, subctx, expr);
            if (!transform)
            {
                //The replace introduced an ambiguity => need to use the unmapped expression.
                BoundRow * prevCursor = translator.resolveSelectorDataset(ctx, datasetSelector);
                transform.set(expr->queryChild(1));

                translator.rebindTableCursor(subctx, dataset, prevCursor, no_left, selSeq);
                rebindFilepositons(subctx, dataset, no_left, selSeq);
            }
            if (returnIfFilterFails)
            {
                translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);      // failedFilterValue already handles clearing the result
            }
            else
            {
                //MORE: Probably not implementable, should try and prevent this happening!
                //translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);
            }
            translator.doTransform(subctx, transform, rowBuilder);
            ctx.addGroup();
            associateTargetCursor(subctx, ctx, tempRow, rowBuilder, expr);
        }
        break;
    case no_newusertable:
        {
            BuildCtx subctx(ctx);
            Linked<BoundRow> tempRow;
            Linked<BoundRow> rowBuilder;
            buildTargetCursor(tempRow, rowBuilder, subctx, expr);
            LinkedHqlExpr transform = expr->queryChild(2);
            if (tableExpr && (expr == firstTransformer))
            {
                if (tableSelector != projectedSelector)
                    transform.setown(newReplaceSelector(transform, tableSelector, projectedSelector));
            }

            if (returnIfFilterFails)
            {
                translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);      // failedFilterValue already handles clearing the result
            }
            else
            {
                //MORE: Probably not implementable
                //translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);
            }
            translator.doTransform(subctx, transform, rowBuilder);
            ctx.addGroup();
            associateTargetCursor(subctx, ctx, tempRow, rowBuilder, expr);
        }
        break;
    case no_aggregate:
        {
            if (alreadyDoneFlag)
            {
                IHqlExpression * dataset = expr->queryChild(0);
                IHqlExpression * selSeq = querySelSeq(expr);
                IHqlExpression * transform = expr->queryChild(2);
                BuildCtx subctx(ctx);

                //Similar code to no_hqlproject.  Should possibly try and do a similar mapping to the transforms
                //(but complicated by having merge/first)
                BoundRow * prevCursor = translator.resolveSelectorDataset(ctx, dataset);
                translator.rebindTableCursor(subctx, dataset, prevCursor, no_left, selSeq);
                rebindFilepositons(subctx, dataset, no_left, selSeq);

                Linked<BoundRow> tempRow;
                Linked<BoundRow> rowBuilder;
                buildTargetCursor(tempRow, rowBuilder, subctx, expr);
                translator.doBuildUserAggregateProcessTransform(subctx, rowBuilder, expr, transform, alreadyDoneFlag);
                subctx.addAssign(alreadyDoneFlag, queryBoolExpr(true));
                associateTargetCursor(subctx, ctx, tempRow, rowBuilder, expr);
            }
        }
        break;
    case no_newaggregate:
        {
            IHqlExpression * transform = expr->queryChild(2);
            if (isCompoundCount)
            {
                //This should really be special cased in a count() baseclass, but can't be bothered at the moment.
                IHqlExpression * rhs = transform->queryChild(0)->queryChild(1);
                IHqlExpression * filter = queryRealChild(rhs, 0);
                if (filter)
                {
                    if (!returnIfFilterFails)
                        translator.buildFilter(ctx, filter);
                    else
                    {
                        OwnedHqlExpr test = getInverse(filter);
                        translator.buildFilteredReturn(ctx, test, failedFilterValue);
                    }
                }
                if (compoundCountVar)
                {
                    OwnedHqlExpr inc = adjustValue(compoundCountVar, 1);
                    ctx.addAssign(compoundCountVar, inc);
                }
            }
            else if (alreadyDoneFlag)
            {
                BuildCtx subctx(ctx);
                Linked<BoundRow> tempRow;
                Linked<BoundRow> rowBuilder;
                buildTargetCursor(tempRow, rowBuilder, subctx, expr);
                translator.doBuildAggregateProcessTransform(subctx, rowBuilder, expr, alreadyDoneFlag);
                subctx.addAssign(alreadyDoneFlag, queryBoolExpr(true));
                associateTargetCursor(subctx, ctx, tempRow, rowBuilder, expr);
            }
        }
        break;
    case no_usertable:
    case no_selectfields:
        UNIMPLEMENTED;
        break;

    case no_fetch:
        {
            BuildCtx subctx(ctx);
            Linked<BoundRow> tempRow;
            Linked<BoundRow> rowBuilder;
            buildTargetCursor(tempRow, rowBuilder, subctx, expr);
            // MORE - don't understand why this is required here but not in hqlproject above
            IHqlExpression * dataset = expr->queryChild(0);
            BoundRow * leftCursor = nullptr;
            switch (getDatasetKind(tableExpr))
            {
            case no_csv:
                leftCursor = translator.bindCsvTableCursor(subctx, dataset, "Left", no_left, querySelSeq(expr), true, queryCsvTableEncoding(tableExpr));
                break;
            case no_xml:
            case no_json:
                leftCursor = translator.bindXmlTableCursor(subctx, dataset, "xmlLeft", no_left, querySelSeq(expr), true);
                break;
            }
            if (!leftCursor)
                leftCursor = translator.bindTableCursor(subctx, dataset, "left", no_left, querySelSeq(expr));

            BoundRow * rightCursor = NULL;
            LinkedHqlExpr transform = expr->queryChild(3);
            if (!containsOnlyLeft(transform, true)) 
            {
                IHqlExpression * rhs = expr->queryChild(1);
                IHqlExpression * memoryRhsRecord = rhs->queryRecord();

                //RIGHT is the input data row.  
                //a) In hthor the remote file is read locally, so RIGHT is a deserialized unmodified row.
                //b) in roxie it is serialized, and accessed in its serialized form.
                //c) in thor it is serialized, but also (inefficiently) deserialized, so use the deserialized form.
                //NOTE: Currently extractJoinFields either does nothing, or sends the entire row.  Needless to say that could be improved - 
                //and would mean the roxie/thor code required changing
                if (translator.targetRoxie())
                {
                    OwnedHqlExpr serializedRhsRecord = getSerializedForm(memoryRhsRecord, diskAtom);
                    OwnedHqlExpr serializedRhs = createDataset(no_null, LINK(serializedRhsRecord));
                    rightCursor = translator.bindTableCursor(subctx, serializedRhs, "right", no_right, querySelSeq(expr));
                    transform.setown(replaceMemorySelectorWithSerializedSelector(transform, memoryRhsRecord, no_right, querySelSeq(expr), diskAtom));
                }
                else
                {
                    rightCursor = translator.bindTableCursor(subctx, rhs, "right", no_right, querySelSeq(expr));
                }
            }

            buildTransformFpos(subctx);     // unusual, but this must occur after the row cursor is bound
            translator.associateSkipReturnMarker(subctx, failedFilterValue, NULL);      // failedFilterValue already handles clearing the result
            translator.doTransform(subctx, transform, rowBuilder);
            subctx.removeAssociation(leftCursor);
            subctx.removeAssociation(rightCursor);
            associateTargetCursor(subctx, ctx, tempRow, rowBuilder, expr);
        }
        break;

    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_childcount:
    case no_compound_childgroupaggregate:
    case no_compound_fetch:
    case no_compound_selectnew:
        break;
    default:
        throwUnexpectedOp(op);
    }
}


void SourceBuilder::buildMatchFilter(BuildCtx & ctx, IHqlExpression * expr)
{
    expr = expr->queryBody();
    node_operator op = expr->getOperator();

    switch (op)
    {
    case no_cachealias:
        buildMatchFilter(ctx, expr->queryChild(1));
        return;
    case no_newkeyindex:
    case no_table:
    case no_fetch:
    case no_select:     // handled below
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_call:
    case no_externalcall:
    case no_rows:
    case no_libraryinput:
        return;
    default:
        buildMatchFilter(ctx, expr->queryChild(0));
        break;
    }

    switch (op)
    {
    case no_filter:
        {
            LinkedHqlExpr cond;
            if (useFilterMappings)
            {
                unsigned match = originalFilters.find(*expr);
                if (match != NotFound)
                    cond.set(&mappedFilters.item(match));
            }
            else
            {
                HqlExprArray args;
                unwindRealChildren(args, expr, 1);
                cond.setown(createBalanced(no_and, queryBoolType(), args));
            }

            if (cond)
            {
                IHqlExpression * ds = expr->queryChild(0);
                OwnedHqlExpr test = getInverse(cond);
                if (translator.queryOptions().foldFilter)
                    test.setown(foldScopedHqlExpression(translator.queryErrorProcessor(), ds->queryNormalizedSelector(), test));

                if (translator.options.spotCSE)
                    test.setown(spotScalarCSE(test, ds, translator.queryOptions().spotCseInIfDatasetConditions));

                if (tableSelector != projectedSelector)
                    test.setown(newReplaceSelector(test, tableSelector, projectedSelector));
                translator.buildFilteredReturn(ctx, test, queryBoolExpr(false));
            }
        }
        break;
    }
}


void SourceBuilder::doBuildAggregateSelectIterator(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    if (isNewSelector(expr))
        buildTransformElements(ctx, ds, false);
    Owned<IHqlCppDatasetCursor> cursor = translator.createDatasetSelector(ctx, expr->queryNormalizedSelector());
    cursor->buildIterateLoop(ctx, false);
}

void SourceBuilder::processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
{
    throwUnexpected();
}

void SourceBuilder::doBuildNormalizeIterators(BuildCtx & ctx, IHqlExpression * expr, bool isChildIterator)
{
    HqlExprArray iterators;
    IHqlExpression * root = gatherSelectorLevels(iterators, expr);

    //MORE: transform also needs to be inside this iterctx
    BuildCtx iterctx(*globaliterctx);

    MemberFunction firstFunc(translator, instance->startctx);
    CursorArray cursors;
    if (isChildIterator)
    {
        assertex(!root);
        firstFunc.start("virtual bool first() override");
    }
    else
    {
        assertex(root);

        firstFunc.start("virtual bool first(const void * _src) override");
        bool isProjected = (root->queryNormalizedSelector() != tableExpr->queryNormalizedSelector());
        if (!isProjected)
        {
            iterctx.addQuotedLiteral("byte * src;");
            associateFilePositions(iterctx, "activity->fpp", "activity->src");      // in case no projection in first()
            firstFunc.ctx.addQuotedLiteral("src = (byte *)_src;");
        }
        else
        {
            firstFunc.ctx.addQuotedLiteral("byte * src = (byte *)_src;");
        }

        translator.associateBlobHelper(firstFunc.ctx, tableExpr, "fpp");
        BoundRow * tableCursor = translator.bindTableCursor(firstFunc.ctx, tableExpr, "src");
        associateFilePositions(firstFunc.ctx, "fpp", "src");

        TransformSequenceBuilder builder(translator, queryBoolExpr(false));
        builder.buildSequence(firstFunc.ctx, &iterctx, root);

        if (!isProjected)
            cursors.append(*LINK(tableCursor));
        else
        {
            BoundRow * match = translator.resolveSelectorDataset(firstFunc.ctx, root);
            assertex(match);
            cursors.append(*LINK(match));
        }
    }

    CompoundIteratorBuilder iterBuilder(translator, instance->nestedctx, iterctx);
    if (iterators.ordinality() == 1)
    {
        StringBuffer s, iterName, cursorName;
        iterBuilder.createSingleLevelIterator(iterName, cursorName, &iterators.item(0), cursors);
        firstFunc.ctx.addQuoted(s.clear().append("return (").append(cursorName).append(" = (byte *)").append(iterName).append(".first()) != 0;"));

        {
            BuildCtx nextctx(instance->startctx);
            nextctx.addQuotedFunction("virtual bool next() override");
            nextctx.addQuoted(s.clear().append("return (").append(cursorName).append(" = (byte *)").append(iterName).append(".next()) != 0;"));
        }
    }
    else
    {
        iterBuilder.buildCompoundIterator(instance->onlyEvalOnceContext(), iterators, cursors);

        firstFunc.ctx.addQuotedLiteral("return iter.first();");

        {
            BuildCtx nextctx(instance->startctx);
            nextctx.addQuotedFunction("virtual bool next() override");
            nextctx.addQuotedLiteral("return iter.next();");
        }
    }

    ForEachItemIn(i, cursors)
    {
        BoundRow & cur = cursors.item(i);
        //Rebind the cursors into the local context - so that accessor helper classes will be generated if required.
        translator.bindTableCursor(ctx, cur.queryDataset(), cur.queryBound(), cur.querySide(), cur.querySelSeq());
    }
}

void SourceBuilder::checkDependencies(BuildCtx & ctx, IHqlExpression * expr)
{
    //Add dependency information
    Owned<ABoundActivity> bound = instance->getBoundActivity();
    translator.addFileDependency(nameExpr, bound);
}

void SourceBuilder::extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds)
{
    ForEachItemIn(i, conds)
    {
        IHqlExpression * filter = &conds.item(i);
#ifdef ALLOW_CHILD_ITERATORS_TO_CALL_CANMATCHANY
        if (isSourceInvariant(ds, filter))                  // more actually isSourceInvariant.
            extendAndCondition(globalGuard, filter);
        else
#endif
            appendFilter(unkeyedFilter, filter);
    }
}

void SourceBuilder::analyseGraph(IHqlExpression * expr)
{
    analyse(expr);
    SourceFieldUsage * fieldUsage = translator.querySourceFieldUsage(tableExpr);
    if (fieldUsage && !fieldUsage->isComplete())
    {
        if (expr->queryNormalizedSelector() == tableExpr->queryNormalizedSelector())
            fieldUsage->noteAll();
        else
            gatherFieldUsage(fieldUsage, expr);
    }
}

void SourceBuilder::gatherFieldUsage(SourceFieldUsage * fieldUsage, IHqlExpression * expr)
{
    for (;;)
    {
        if (expr->queryBody() == tableExpr->queryBody())
            return;
        if (fieldUsage->isComplete())
            return;

        IHqlExpression * ds = expr->queryChild(0);
        switch (expr->getOperator())
        {
        case no_fetch:
            {
                assertex(ds->queryBody() == tableExpr->queryBody());
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
                ::gatherFieldUsage(fieldUsage, expr, left, false);
                return;
            }
        }

        assertex(getNumChildTables(expr) == 1);
        if (ds->queryNormalizedSelector() == tableExpr->queryNormalizedSelector())
            gatherParentFieldUsage(fieldUsage, expr);

        expr = ds;
    }
}

inline bool useDescriptiveGraphLabel(ThorActivityKind kind)
{
    switch (kind)
    {
    case TAKcsvfetch:
    case TAKxmlfetch:
    case TAKjsonfetch:
    case TAKfetch:
        return false;
    }
    return true;
}


static bool expandGraphLabel(ThorActivityKind kind)
{
    switch (kind)
    {
    case TAKdiskread:
    case TAKnewdiskread:
    case TAKcsvread:
    case TAKxmlread:
    case TAKjsonread:
    case TAKdiskcount:
    case TAKdiskexists:
    case TAKspillread:
        return true;
    default:
        return false;
    }
}

ABoundActivity * SourceBuilder::buildActivity(BuildCtx & ctx, IHqlExpression * expr, ThorActivityKind _activityKind, const char *kind, ABoundActivity *input)
{
    activityKind = _activityKind;

    translator.gatherActiveCursors(ctx, parentCursors);

    bool isSpill = tableExpr && tableExpr->hasAttribute(_spill_Atom);
    //If genericDiskReads are supported, this will no longer generate spill activities.
    //Good for testing, but will change once all disk reads go through that interface
    if (isSpill && (activityKind == TAKdiskread))
        activityKind = TAKspillread;
    useImplementationClass = translator.queryOptions().minimizeActivityClasses && translator.targetRoxie() && (activityKind == TAKspillread);

    Owned<ActivityInstance> localInstance = new ActivityInstance(translator, ctx, activityKind, expr, kind);
    if (useImplementationClass)
        localInstance->setImplementationClass(newMemorySpillReadArgId);

    if (((activityKind >= TAKdiskread) && (activityKind <= TAKdiskgroupaggregate)) || (activityKind == TAKspillread) || (activityKind == TAKnewdiskread))
    {
        IHqlExpression * seq = querySequence(tableExpr);
        translator.noteResultAccessed(ctx, seq, nameExpr);
    }

    instance = localInstance;

    StringBuffer graphLabel;
    graphLabel.append(getActivityText(activityKind));

    bool isFiltered = false;
    double filterLikelihood = 1.0;
    if (expandGraphLabel(activityKind))
    {
        graphLabel.clear();
        if (expr != tableExpr)
        {
            if (isGrouped(expr))
                graphLabel.append("Grouped\n");

            IHqlExpression * cur = expr;
            bool isProjected = false;
            for (;;)
            {
                switch (cur->getOperator())
                {
                case no_filter:
                    if (isKnownLikelihood(filterLikelihood))
                    {
                        double likelihood = queryActivityLikelihood(cur);
                        if (isKnownLikelihood(likelihood))
                            // Combine the likelihood of the 2 filter conditions
                            // N.B. this only works if the filter probability are independent
                            filterLikelihood *= likelihood;
                        else
                            // One of the filter probability is unknown, so the overall probability is unknown
                            setUnknownLikelihood(filterLikelihood);
                    }
                    isFiltered = true;
                    break;
                case no_hqlproject:
                case no_newusertable:
                case no_transformascii:
                case no_transformebcdic:
                    if (!cur->hasAttribute(_internal_Atom))
                        isProjected = true;
                    break;
                case no_table:
                    cur = NULL;
                    break;
                }
                if (!cur)
                    break;
                cur = cur->queryChild(0);
            }

            if (isFiltered)
            {
                graphLabel.append("Filtered\n");
            }
            if (isProjected)
                graphLabel.append("Projected\n");
        }

        if ((translator.getTargetClusterType() == RoxieCluster) && isSpill)
            graphLabel.append("Read");
        else if (isExplicitExists() && (activityKind == TAKdiskcount))
            graphLabel.append("Disk Exists");
        else
            graphLabel.append(getActivityText(activityKind));
    }
    if (isExplicitExists())
    {
        if (activityKind == TAKindexcount)
            graphLabel.clear().append("Index Exists");
    }
    if (useDescriptiveGraphLabel(activityKind))
    {
        StringBuffer eclChunk;

        bool isLimited = false;
        bool isStepped = false;
        IHqlExpression * cur = expr;
        instance->graphEclText.clear();
        for (;;)
        {
            eclChunk.clear();
            switch (cur->getOperator())
            {
            case no_hqlproject:
            case no_newusertable:
            case no_transformascii:
            case no_transformebcdic:
                break;
            case no_filter:
                toECL(cur->queryBody(), eclChunk, false, true);
                break;
            case no_table:
            case no_newkeyindex:
            case no_select:
                toECL(cur->queryBody(), eclChunk, false, true);
                cur = NULL;
                break;
            case no_stepped:
                isStepped = true;
                break;
            case no_limit:
            case no_keyedlimit:
                isLimited = true;
                break;
            default:
                if (getNumChildTables(cur) == 0)
                {
                    toECL(cur->queryBody(), eclChunk, false, true);
                    cur = NULL;
                }
                break;
            }
            instance->graphEclText.insert(0, eclChunk);
            if (!cur)
                break;
            cur = cur->queryChild(0);
        }
        if (isLimited)
            graphLabel.insert(0, "Limited\n");
        if (isStepped)
            graphLabel.insert(0, "Stepped\n");
        if (localInstance->isLocal && !isSpill)
            graphLabel.insert(0, "Local ");
    }

    if (nameExpr && nameExpr->queryValue())
    {
        if (isSpill)
        {
            if (activityKind != TAKspillread)
                graphLabel.append("\nSpill");
        }
        else
        {
            graphLabel.newline();
            if (tableExpr->hasAttribute(_workflowPersist_Atom))
                graphLabel.append("Persist ");

            StringBuffer filename;
            //Call getStringValue() rather than generateECL() to avoid 't quote \ etc. in the string
            getStringValue(filename.append("'"), nameExpr).append("'");

            const char * coloncolon = strstr(filename, "::");
            if (coloncolon)
            {
                for (;;)
                {
                    const char * next = strstr(coloncolon+2, "::");
                    if (!next)
                        break;
                    coloncolon = next;
                }
                graphLabel.append("'...").append(coloncolon);
            }
            else
                graphLabel.append(filename);
        }
    }
    instance->graphLabel.set(graphLabel.str());

    translator.buildActivityFramework(localInstance);
    translator.buildInstancePrefix(localInstance);

    analyseGraph(expr);

    if (!useImplementationClass)
    {
        buildMembers(expr);
        buildTransform(expr);
        buildCanMatch(expr);
        buildFlagsMember(expr);

        if (tableExpr && (activityKind < TAKchildread || activityKind > TAKchildthroughnormalize))
        {
            switch (activityKind)
            {
            case TAKindexread:
            case TAKindexnormalize:
            case TAKindexaggregate:
            case TAKindexcount:
            case TAKindexgroupaggregate:
            case TAKindexexists:
            {
                translator.buildMetaMember(instance->classctx, expectedRecord, false, "queryDiskRecordSize");
                translator.buildMetaMember(instance->classctx, projectedRecord, false, "queryProjectedDiskRecordSize");
                break;
            }
            }
        }
    }
    else
    {
        assertex(!hasDynamic(tableExpr));
        bool matched = translator.registerGlobalUsage(nameExpr);
        if (!matched)
        {
            StringBuffer spillName;
            getExprECL(nameExpr, spillName);
            if (translator.queryOptions().allowThroughSpill)
                throwError1(HQLERR_ReadSpillBeforeWriteFix, spillName.str());
            else
                throwError1(HQLERR_ReadSpillBeforeWrite, spillName.str());
        }
        translator.addFilenameConstructorParameter(*instance, WaFilename, nameExpr, SummaryType::SpillFile);
    }

    if (steppedExpr)
        buildSteppedHelpers();

    if (translator.targetRoxie())
        instance->addAttributeBool(WaIsSpill, isSpill);
    else if (needToCallTransform || transformCanFilter)
        instance->addAttributeBool(WaIsTransformSpill, isSpill);
    else
        instance->addAttributeBool(WaIsSpill, isSpill);
    if (isFiltered)
    {
        if (isKnownLikelihood(filterLikelihood))
        {
            StringBuffer text;
            filterLikelihood *= 100;
            text.setf("%3.2f%%", filterLikelihood);
            instance->addAttribute(WaMatchLikelihood, text);
        }
    }
    IHqlExpression * spillReason = tableExpr ? queryAttributeChild(tableExpr, _spillReason_Atom, 0) : NULL;

    if (spillReason && !translator.queryOptions().obfuscateOutput)
    {
        StringBuffer text;
        getStringValue(text, spillReason);
        instance->addAttribute(WaSpillReason, text.str());
    }

    if (tableExpr)
        instance->addSignedAttribute(tableExpr->queryAttribute(_signed_Atom));

    checkDependencies(ctx, expr);
    translator.buildInstanceSuffix(localInstance);
    if (input)
        translator.buildConnectInputOutput(ctx, localInstance, input, 0, 0);

    instance = NULL;
    return localInstance->getBoundActivity();
}


void SourceBuilder::buildKeyedLimitHelper(IHqlExpression * self)
{
    if (keyedLimitExpr)
    {
        IHqlExpression * limitValue = keyedLimitExpr->queryChild(1);
        {
            MemberFunction func(translator, instance->startctx, "virtual unsigned __int64 getKeyedLimit() override");
            translator.buildReturn(func.ctx, limitValue);
            if (isZero(limitValue))
                translator.WARNING(CategoryUnusual, HQLWRN_KeyedLimitIsZero);
        }

        LinkedHqlExpr fail = keyedLimitExpr->queryChild(2);
        if (!fail || fail->isAttribute())
            fail.setown(translator.createFailAction("Keyed limit exceeded", limitValue, NULL, instance->activityId));

        {
            MemberFunction func(translator, instance->startctx, "virtual void onKeyedLimitExceeded() override");
            translator.buildStmt(func.ctx, fail);
        }

        IHqlExpression * transform = queryAttributeChild(keyedLimitExpr, onFailAtom, 0);
        if (transform)
        {
            MemberFunction func(translator, instance->startctx, "virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & crSelf) override");
            translator.ensureRowAllocated(func.ctx, "crSelf");
            translator.buildTransformBody(func.ctx, transform, NULL, NULL, self, NULL);
        }
    }
}


void SourceBuilder::buildSteppedHelpers()
{
    StringBuffer steppedFlags;

    IHqlExpression * priority = steppedExpr->queryAttribute(priorityAtom);
    if (priority)
    {
        steppedFlags.append("|SSFhaspriority");
        translator.doBuildFunction(instance->startctx, doubleType, "getPriority", priority->queryChild(0));
    }

    IHqlExpression * prefetch = steppedExpr->queryAttribute(prefetchAtom);
    if (prefetch)
    {
        steppedFlags.append("|SSFhasprefetch");
        translator.doBuildUnsignedFunction(instance->startctx, "getPrefetchSize", prefetch->queryChild(0));
    }

    IHqlExpression * filtered = steppedExpr->queryAttribute(filteredAtom);
    if (filtered)
        steppedFlags.append("|SSFalwaysfilter");

    if (steppedFlags.length())
        translator.doBuildUnsignedFunction(instance->classctx, "getSteppedFlags", steppedFlags.str()+1);
}



void SourceBuilder::assignLocalExtract(BuildCtx & ctx, ParentExtract * extractBuilder, IHqlExpression * dataset, const char * argName)
{
    if (extractBuilder)
    {
        StringBuffer s;
        s.append("byte * ");
        translator.generateExprCpp(s, extractBuilder->queryExtractName());
        s.append(" = (byte *) ").append(argName).append(";");
        ctx.addQuoted(s);
    }
    else
    {
        StringBuffer s;
        ctx.addQuoted(s.append("unsigned char * left = (unsigned char *) ").append(argName).append(";"));
        translator.bindTableCursor(ctx, dataset, "left");
    }
}


void SourceBuilder::buildGroupAggregateHashHelper(ParentExtract * extractBuilder, IHqlExpression * dataset, IHqlExpression * fields)
{
    instance->classctx.addQuotedLiteral("virtual IHash * queryHash() override { return &hash; }");

    BuildCtx classctx(instance->nestedctx);
    IHqlStmt * classStmt = translator.beginNestedClass(classctx, "hash", "IHash", NULL, extractBuilder);

    {
        MemberFunction func(translator, classctx, "virtual unsigned hash(const void * _self) override");
        assignLocalExtract(func.ctx, extractBuilder, dataset, "_self");

        OwnedHqlExpr hash = createValue(no_hash32, LINK(unsignedType), LINK(fields), createAttribute(internalAtom));
        translator.buildReturn(func.ctx, hash);
    }

    translator.endNestedClass(classStmt);
}

void SourceBuilder::buildGroupAggregateCompareHelper(ParentExtract * extractBuilder, IHqlExpression * aggregate, HqlExprArray & recordFields, HqlExprArray & aggregateFields)
{
    //Special case comparison of IF(<global-bool>, value, some-constant), so that the order expression only compares if <global-bool> is true
    //specifically to improve david's BIRPs code.
    HqlExprArray optimizedLeft, optimizedRight;
    ForEachItemIn(i, recordFields)
    {
        IHqlExpression & curLeft = recordFields.item(i);
        IHqlExpression & curRight = aggregateFields.item(i);
        IHqlExpression * newLeft = NULL;
        IHqlExpression * newRight = NULL;

        if (curLeft.getOperator() == no_if)
        {
            IHqlExpression * cond = curLeft.queryChild(0);
            if (isIndependentOfScope(cond))
            {
                IHqlExpression * elseValue = curLeft.queryChild(2);
                if (elseValue->getOperator() == no_constant)
                {
                    newLeft = LINK(&curLeft);
                    newRight = createValue(no_if, curRight.getType(), LINK(cond), LINK(&curRight), LINK(elseValue));
                }
            }
        }

        if (newLeft)
        {
            optimizedLeft.append(*newLeft);
            optimizedRight.append(*newRight);
        }
        else
        {
            optimizedLeft.append(OLINK(curLeft));
            optimizedRight.append(OLINK(curRight));
        }
    }

    OwnedHqlExpr leftList = createSortList(optimizedLeft);
    DatasetReference datasetRight(aggregate, no_activetable, NULL);
    OwnedHqlExpr selSeq = createDummySelectorSequence();
    OwnedHqlExpr rightList = createSortList(optimizedRight);
    OwnedHqlExpr rightSelect = datasetRight.getSelector(no_right, selSeq);
    OwnedHqlExpr rightResolved = datasetRight.mapCompound(rightList, rightSelect);

    OwnedHqlExpr order = createValue(no_order, makeIntType(sizeof(signed), true), LINK(leftList), LINK(rightResolved));

    //Now generate the nested class
    instance->classctx.addQuotedLiteral("virtual ICompare * queryCompareRowElement() override { return &compareRowElement; }");

    BuildCtx classctx(instance->nestedctx);
    IHqlStmt * classStmt = translator.beginNestedClass(classctx, "compareRowElement", "ICompare", NULL, extractBuilder);

    {
        MemberFunction func(translator, classctx, "virtual int docompare(const void * _left, const void * _right) const override");
        assignLocalExtract(func.ctx, extractBuilder, aggregate->queryChild(0), "_left");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
        func.ctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);

        translator.bindTableCursor(func.ctx, aggregate, "right", no_right, selSeq);
        translator.doBuildReturnCompare(func.ctx, order, no_eq, false, false);
    }

    translator.endNestedClass(classStmt);
}


void SourceBuilder::buildGroupAggregateProcessHelper(ParentExtract * extractBuilder, IHqlExpression * aggregate, const char * name, bool doneAny)
{
    StringBuffer proto;
    IHqlExpression * dataset = aggregate->queryChild(0);
    IHqlExpression * tgtRecord = aggregate->queryChild(1);
    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));

    proto.append("virtual size32_t ").append(name).append("(ARowBuilder & crSelf, const void * _src) override");
    MemberFunction validateFunc(translator, instance->nestedctx, proto, MFdynamicproto);

    translator.ensureRowAllocated(validateFunc.ctx, "crSelf");
    assignLocalExtract(validateFunc.ctx, extractBuilder, dataset, "_src");

    BoundRow * selfRow = translator.bindSelf(validateFunc.ctx, resultDataset, "crSelf");

    if (extractBuilder)
    {
        MemberEvalContext * evalContext = new MemberEvalContext(translator, extractBuilder, translator.queryEvalContext(validateFunc.ctx), validateFunc.ctx);
        validateFunc.ctx.associateOwn(*evalContext);
        evalContext->initContext();
        validateFunc.ctx.associateExpr(codeContextMarkerExpr, codeContextMarkerExpr);
    }

    if (aggregate->getOperator() == no_aggregate)
    {
        //It is inefficient to call processUserAggregateTransform() twice, but this is an unusual construct, so leave as it 
        //is for the moment.
        OwnedHqlExpr firstTransform;
        OwnedHqlExpr nextTransform;
        translator.processUserAggregateTransform(aggregate, aggregate->queryChild(2), firstTransform, nextTransform);
        IHqlExpression * transform = doneAny ? nextTransform : firstTransform;

        OwnedHqlExpr left = createSelector(no_left, dataset, querySelSeq(aggregate));
        OwnedHqlExpr mappedTransform = replaceSelector(transform, left, dataset);
        translator.doBuildUserAggregateProcessTransform(validateFunc.ctx, selfRow, aggregate, mappedTransform, queryBoolExpr(doneAny));
    }
    else
        translator.doBuildAggregateProcessTransform(validateFunc.ctx, selfRow, aggregate, queryBoolExpr(doneAny));
    translator.buildReturnRecordSize(validateFunc.ctx, selfRow);
}

void SourceBuilder::buildGroupAggregateHelpers(ParentExtract * extractBuilder, IHqlExpression * aggregate)
{
    IHqlExpression * dataset = aggregate->queryChild(0);
    LinkedHqlExpr transform = aggregate->queryChild(2);
    LinkedHqlExpr grouping = aggregate->queryChild(3);
    if (aggregate->getOperator() == no_aggregate)
    {
        OwnedHqlExpr left = createSelector(no_left, dataset, querySelSeq(aggregate));
        grouping.setown(replaceSelector(grouping, left, dataset));
        transform.setown(replaceSelector(transform, left, dataset));
    }

    HqlExprArray recordFields, aggregateFields;
    grouping->unwindList(recordFields, no_sortlist);
    getMappedFields(aggregateFields, transform, recordFields, queryActiveTableSelector());

    OwnedHqlExpr allRecordFields = createValueSafe(no_sortlist, makeSortListType(NULL), recordFields);
    OwnedHqlExpr allAggregateFields = createValueSafe(no_sortlist, makeSortListType(NULL), aggregateFields);

    //virtual size32_t processFirst(void * target, const void * src)
    buildGroupAggregateProcessHelper(extractBuilder, aggregate, "processFirst", false);

    //virtual size32_t processNext(void * target, const void * src)
    buildGroupAggregateProcessHelper(extractBuilder, aggregate, "processNext", true);

    //virtual IHash * queryHash()
    buildGroupAggregateHashHelper(extractBuilder, dataset, allRecordFields);

    //virtual ICompare * queryCompareElements()
    DatasetReference outRef(aggregate, no_activetable, NULL);
    translator.buildCompareMember(instance->nestedctx, "CompareElements", allAggregateFields, outRef);      // compare transformed elements

    //virtual ICompare * queryCompareRowElement()
    buildGroupAggregateCompareHelper(extractBuilder, aggregate, recordFields, aggregateFields);

    //virtual IHash * queryHashElement()
    translator.buildHashOfExprsClass(instance->nestedctx, "HashElement", allAggregateFields, outRef, true);

}


IHqlExpression * SourceBuilder::ensureAggregateGroupingAliased(IHqlExpression * aggregate)
{
    IHqlExpression * dataset = aggregate->queryChild(0);
    IHqlExpression * grouping = aggregate->queryChild(3);

    //Force complex grouping fields into aliases to reduce processing...
    HqlMapTransformer transformer;
    transformer.setMapping(dataset, dataset);
    ForEachChild(i, grouping)
    {
        IHqlExpression * cur = grouping->queryChild(i);
        if (translator.requiresTemp(instance->nestedctx, cur, true) && (cur->getOperator() != no_alias))
        {
            OwnedHqlExpr alias = createAliasOwn(LINK(cur), createAttribute(internalAtom));
            transformer.setMapping(cur, alias);
        }
    }
    return transformer.transformRoot(aggregate);
}


void SourceBuilder::buildGlobalGroupAggregateHelpers(IHqlExpression * expr)
{
    IHqlExpression * aggregate = expr->queryChild(0);
    node_operator op = aggregate->getOperator();
    assertex(op == no_newaggregate || op == no_aggregate);

    StringBuffer s;
    //virtual size32_t clearAggregate(void * self) = 0;
    translator.doBuildAggregateClearFunc(instance->startctx, aggregate);

    //virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * src) = 0;      //only call if transform called at least once on src.
    translator.doBuildAggregateMergeFunc(instance->startctx, aggregate, requiresOrderedMerge);

    //virtual void processRow(void * self, const void * src) = 0;
    {
        BuildCtx rowctx(instance->startctx);
        rowctx.addQuotedFunction("virtual void processRow(const void * src, IHThorGroupAggregateCallback * callback) override");
        rowctx.addQuotedLiteral("doProcessRow((byte *)src, callback);");
    }

    //virtual void processRows(void * self, size32_t srcLen, const void * src) = 0;
    //Only meaningful for a dataset, and even then I'm not sure it is ever used.
    if (!isKey(tableExpr))
    {
        OwnedHqlExpr newTableExpr = LINK(tableExpr);

        MemberFunction func(translator, instance->startctx, "virtual void processRows(size32_t srcLen, const void * _left, IHThorGroupAggregateCallback * callback) override");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        OwnedHqlExpr ds = createVariable("left", makeReferenceModifier(newTableExpr->getType()));
        OwnedHqlExpr len = createVariable("srcLen", LINK(sizetType));
        OwnedHqlExpr fullDs = createTranslated(ds, len);

        BoundRow * curRow = translator.buildDatasetIterate(func.ctx, fullDs, false);
        s.clear().append("doProcessRow(");
        translator.generateExprCpp(s, curRow->queryBound());
        s.append(", callback);");
        func.ctx.addQuoted(s);
    }
}


void SourceBuilder::buildGroupingMonitors(IHqlExpression * expr, CppFilterExtractor & monitors)
{
    IHqlExpression * aggregate = expr->queryChild(0);
    node_operator op = aggregate->getOperator();
    assertex(op == no_newaggregate || op == no_aggregate);

    IHqlExpression * dataset = aggregate->queryChild(0);
    IHqlExpression * grouping = aggregate->queryChild(3);

    //virtual void createGroupSegmentMonitors(IIndexReadContext *ctx) = 0;
    MemberFunction func(translator, instance->startctx, "virtual bool createGroupSegmentMonitors(IIndexReadContext * irc) override");

    monitorsForGrouping = true;
    if (op == no_newaggregate)
        translator.bindTableCursor(func.ctx, dataset, "_dummy");
    else
        translator.bindTableCursor(func.ctx, dataset, "_dummy", no_left, querySelSeq(aggregate));
    unsigned maxField = 0;
    ForEachChild(i, grouping)
    {
        unsigned nextField = 0;
        if (!monitors.createGroupingMonitor(func.ctx, "irc", grouping->queryChild(i), nextField))
        {
            monitorsForGrouping = false;
            func.setIncluded(false);
            break;
        }
        if (maxField < nextField)
            maxField = nextField;
    }
    if (monitorsForGrouping)
        func.ctx.addReturn(queryBoolExpr(true));

    if (monitorsForGrouping)
    {
        translator.doBuildUnsignedFunction(instance->classctx, "getGroupingMaxField", maxField);
    }
}



void SourceBuilder::buildAggregateHelpers(IHqlExpression * expr)
{
    IHqlExpression * aggregate = expr->queryChild(0);
    node_operator op = aggregate->getOperator();
    assertex(op == no_newaggregate || op == no_aggregate);

    StringBuffer s;
    alreadyDoneFlag.setown(instance->startctx.getTempDeclare(queryBoolType(), NULL));
    instance->onstartctx.addAssign(alreadyDoneFlag, queryBoolExpr(false));

    //virtual bool   processedAnyRows() = 0;
    translator.doBuildBoolFunction(instance->startctx, "processedAnyRows", alreadyDoneFlag);

    //virtual size32_t clearAggregate(ARowBuilder & crSelf) = 0;
    translator.doBuildAggregateClearFunc(instance->startctx, aggregate);

    //virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * src) = 0;      //only call if transform called at least once on src.
    translator.doBuildAggregateMergeFunc(instance->startctx, aggregate, requiresOrderedMerge);
}


void SourceBuilder::buildCountHelpers(IHqlExpression * expr, bool allowMultiple)
{
    StringBuffer s;

    //---- virtual bool hasFilter() { return <bool>; } ----
    if (transformCanFilter||isNormalize)
        translator.doBuildBoolFunction(instance->classctx, "hasFilter", true);

    if (allowMultiple)
    {
        bool isExists = hasExistChoosenLimit();
        OwnedHqlExpr one = getSizetConstant(1);

        if (transformCanFilter||isNormalize)
        {
            //virtual size32_t numValid(const void * src) = 0;
            {
                BuildCtx rowctx(instance->startctx);
                rowctx.addQuotedFunction("virtual size32_t numValid(const void * src) override");
                rowctx.addQuotedLiteral("return valid((byte *)src);");
            }

            //virtual size32_t numValid(size32_t srcLen, const void * src);
            {
                MemberFunction func(translator, instance->startctx, "virtual size32_t numValid(size32_t srcLen, const void * _src) override");
                func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *)_src;");
                OwnedHqlExpr ds = createVariable("src", makeReferenceModifier(tableExpr->getType()));
                OwnedHqlExpr len = createVariable("srcLen", LINK(sizetType));
                OwnedHqlExpr fullDs = createTranslated(ds, len);

                if (isExists)
                {
                    BuildCtx iterctx(func.ctx);
                    BoundRow * curRow = translator.buildDatasetIterate(iterctx, fullDs, false);
                    s.clear().append("if (valid(");
                    translator.generateExprCpp(s, curRow->queryBound());
                    s.append("))");
                    iterctx.addQuotedCompound(s, nullptr);
                    iterctx.addReturn(one);
                    func.ctx.addQuotedLiteral("return 0;");
                }
                else
                {
                    func.ctx.addQuotedLiteral("size32_t cnt = 0;");
                    BuildCtx iterctx(func.ctx);
                    BoundRow * curRow = translator.buildDatasetIterate(iterctx, fullDs, false);
                    s.clear().append("cnt += valid(");
                    translator.generateExprCpp(s, curRow->queryBound());
                    s.append(");");
                    iterctx.addQuoted(s);
                    func.ctx.addQuotedLiteral("return cnt;");
                }
            }
        }
        else
        {
            //virtual size32_t numValid(size32_t srcLen, const void * src);
            MemberFunction func(translator, instance->startctx, "virtual size32_t numValid(size32_t srcLen, const void * _src) override");
            if (isExists)
                func.ctx.addReturn(one);
            else
            {
                func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *)_src;");
                CHqlBoundExpr bound;
                bound.length.setown(createVariable("srcLen", LINK(sizetType)));
                bound.expr.setown(createVariable("src", makeReferenceModifier(tableExpr->getType())));
                OwnedHqlExpr count = translator.getBoundCount(bound);
                func.ctx.addReturn(count);
            }
        }
    }
}


void SourceBuilder::buildNormalizeHelpers(IHqlExpression * expr)
{
}


void SourceBuilder::buildCanMatch(IHqlExpression * expr)
{
    if (extractCanMatch)
    {
        MemberFunction func(translator, instance->startctx);
        func.start("virtual bool canMatch(const void * _left) override");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        if (newInputMapping)
            translator.bindTableCursor(func.ctx, projectedSelector, "left");
        else
            translator.bindTableCursor(func.ctx, tableExpr, "left");

        //This will have no ill effect for disk read, and is used for blob lookup
        translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
        buildTransformFpos(func.ctx);

        unsigned mark = func.numStmts();
        buildMatchFilter(func.ctx, firstTransformer ? firstTransformer->queryChild(0) : expr);
        if (func.numStmts() != mark)
        {
            func.ctx.addReturn(queryBoolExpr(true));
            translator.doBuildBoolFunction(instance->classctx, "hasMatchFilter", true);
        }
        else
            func.setIncluded(false);
    }
}


void SourceBuilder::buildGroupAggregateTransformBody(BuildCtx & transformCtx, IHqlExpression * expr, bool useExtract, bool bindInputRow)
{
    buildTransformBody(transformCtx, expr, false, false, bindInputRow);

    IHqlExpression * aggregate = expr->queryChild(0);
    OwnedHqlExpr mappedAggregate = ensureAggregateGroupingAliased(aggregate);
    Owned<ParentExtract> extractBuilder;
    if (useExtract || (aggregate != mappedAggregate))
    {
        extractBuilder.setown(translator.createExtractBuilder(transformCtx, PETcallback, NULL, GraphCoLocal, true));
        if (!translator.queryOptions().serializeRowsetInExtract)
            extractBuilder->setAllowDestructor();
        translator.beginExtract(transformCtx, extractBuilder);
        buildGroupAggregateHelpers(extractBuilder, mappedAggregate);
        translator.endExtract(transformCtx, extractBuilder);
    }
    else
        buildGroupAggregateHelpers(NULL, mappedAggregate);

    HqlExprArray args;
    args.append(*createVariable("callback", makeBoolType()));
    if (extractBuilder)
    {
        CHqlBoundExpr boundExtract;
        extractBuilder->endCreateExtract(boundExtract);
        args.append(*boundExtract.getTranslatedExpr());
    }
    else
    {
        BoundRow * match = translator.resolveDatasetRequired(transformCtx, aggregate->queryChild(0));
        Owned<ITypeInfo> rowType = makeReferenceModifier(makeRowType(queryNullRecord()->getType()));
        OwnedHqlExpr rowAddr = getPointer(match->queryBound());
        OwnedHqlExpr castBound = createValue(no_typetransfer, LINK(rowType), LINK(rowAddr));
        args.append(*createTranslated(castBound));
    }
    OwnedHqlExpr call = translator.bindFunctionCall(addAggregateRowId, args);
    translator.buildStmt(transformCtx, call);
}


void SourceBuilder::gatherVirtualFields(bool ignoreVirtuals, bool ensureSerialized)
{
    IHqlExpression * record = tableExpr->queryRecord();
    fieldInfo.gatherVirtualFields(record, ignoreVirtuals, ensureSerialized);
    if (fieldInfo.hasVirtuals())
        physicalRecord.setown(fieldInfo.createPhysicalRecord());
    else
        physicalRecord.set(record);

    expectedRecord.set(physicalRecord);
    projectedRecord.set(physicalRecord);

    tableSelector.set(tableExpr->queryNormalizedSelector());
    projectedSelector.set(tableSelector);
}

void SourceBuilder::deduceDiskRecords()
{
    HqlExprAttr mode = tableExpr->queryChild(2);
    node_operator modeOp = mode->getOperator();
    bool isPiped = modeOp==no_pipe;

    gatherVirtualFields(tableExpr->hasAttribute(_noVirtual_Atom) || isPiped, needToSerializeRecord(modeOp));

    if (newInputMapping)
    {
        projectedRecord.set(tableExpr->queryRecord());
        expectedRecord.setown(getSerializedForm(physicalRecord, diskAtom));

        //If the record translator can proccess the record then virtual fields can be anywhere, otherwise they need
        //to be at the end so they can be appended - and the projected format will match the serialized disk format
        if (!canDefinitelyProcessWithTranslator(projectedRecord) && !fieldInfo.canAppendVirtuals())
        {
            StringBuffer typeName;
            unsigned recordTypeFlags = translator.buildRtlType(typeName, projectedRecord->queryType());
            if (recordTypeFlags & RFTMcannotinterpret)
                throwError(HQLERR_CannotInterpretRecord);
        }
    }
    else
    {
        projectedRecord.set(tableExpr->queryRecord());
        expectedRecord.setown(getSerializedForm(physicalRecord, diskAtom));
    }
}


void SourceBuilder::deduceIndexRecords()
{
    gatherVirtualFields(true, true);

    //A slightly round about way to get the meta including keyed/blob information for the physical file.
    IHqlExpression * indexExpr = queryOriginalKey(tableExpr);
    OwnedHqlExpr serializedRecord;
    unsigned numPayload = numPayloadFields(indexExpr);
    if (numPayload)
        serializedRecord.setown(notePayloadFields(indexExpr->queryRecord(), numPayload));
    else
        serializedRecord.set(indexExpr->queryRecord());
    serializedRecord.setown(getSerializedForm(serializedRecord, diskAtom));

    bool hasFilePosition = getBoolAttribute(indexExpr, filepositionAtom, true);
    expectedRecord.setown(createMetadataIndexRecord(serializedRecord, hasFilePosition));

    if (newInputMapping)
    {
        //We are expecting the translator to map the field values, this uses the expected ecl structure
        projectedRecord.set(tableExpr->queryRecord());
        //physical?
    }
    else
    {
        projectedRecord.set(expectedRecord);    // This needs to match expectedRecord so that no translation occurs on keyed fields etc.
    }
}


/*
interface ICompoundSourceSteppingMeta : extends ISteppingMeta
{
    virtual ISteppingMeta * queryRawSteppingMeta() = 0;
    virtual ISteppingMeta * queryProjectedSteppingMeta() = 0;       // if null no projection takes place
    virtual void mapOutputToIInput(void * originalRow, const void * projectedRow, unsigned firstField, unsigned numFields) = 0;
};
*/


bool SourceBuilder::containsStepping(IHqlExpression * expr)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_stepped:
            return true;
        default:
            {
                if (expr->queryBody() == tableExpr)
                    return false;
                unsigned numChildren = getNumChildTables(expr);
                if (numChildren == 0)
                    return false;
                assertex(numChildren == 1);
            }
        }
        expr = expr->queryChild(0);
    }
}


void SourceBuilder::gatherSteppingMeta(IHqlExpression * expr, SteppingFieldSelection & outputStepping, SteppingFieldSelection & rawStepping)
{
    for (;;)
    {
        switch (expr->getOperator())
        {
        case no_newusertable:
        case no_hqlproject:
            if (rawStepping.exists())
            {
                rawStepping.expandTransform(expr);
            }
            else
            {
                gatherSteppingMeta(expr->queryChild(0), outputStepping, rawStepping);
                outputStepping.clear();
                return;
                //throwError(HQLERR_CantProjectStepping);
                //gatherSteppingMeta(expr->queryChild(0), outputStepping, rawStepping);
                //apply projection to the output fields and inverse to the raw fields.
            }
            break;
        case no_stepped:
            {
                outputStepping.setStepping(expr);
                rawStepping.setStepping(expr);
                break;
            }
        default:
            {
                if (expr->queryBody() == tableExpr)
                    return;
                unsigned numChildren = getNumChildTables(expr);
                if (numChildren == 0)
                    return;
                assertex(numChildren == 1);
            }
        }
        expr = expr->queryChild(0);
    }
}


void SourceBuilder::gatherSteppingMeta(IHqlExpression * expr, SourceSteppingInfo & info)
{
    if (!steppedExpr)
        return;

    gatherSteppingMeta(expr, info.outputStepping, info.rawSteppingProject);
    if (info.rawSteppingProject.exists())
        info.extractRaw();
}

//-----------------------------------------------------------------------------------------------
//-- Disk file processing
//-----------------------------------------------------------------------------------------------

class DiskReadBuilderBase : public SourceBuilder
{
public:
    DiskReadBuilderBase(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr, bool canReadWriteGenerically, bool forceReadWriteGenerically)
        : SourceBuilder(_translator, _tableExpr, _nameExpr, canReadWriteGenerically, forceReadWriteGenerically), monitors(_tableExpr, _translator, 0, true, true)
    {
        fpos.setown(getFilepos(tableExpr, false));
        lfpos.setown(getFilepos(tableExpr, true));
        logicalFilenameMarker.setown(getFileLogicalName(tableExpr));
        mode = tableExpr->queryChild(2);
        modeOp = mode->getOperator();
        includeFormatCrc = ((modeOp != no_csv) || useGenericDiskReadWrite) && (modeOp != no_pipe);
    }

    virtual void buildMembers(IHqlExpression * expr);
    virtual void buildTransformFpos(BuildCtx & transformCtx);
    virtual void extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds);

protected:
    virtual void buildFlagsMember(IHqlExpression * expr);

protected:
    CppFilterExtractor monitors;
    OwnedHqlExpr globalGuard;
    IHqlExpression * mode;
    node_operator modeOp;
    bool includeFormatCrc;
};


void DiskReadBuilderBase::buildMembers(IHqlExpression * expr)
{
    StringBuffer s;

    //Process any KEYED() information
    if (monitors.isKeyed())
    {
        MemberFunction func(translator, instance->startctx, "virtual void createSegmentMonitors(IIndexReadContext *irc) override");
        monitors.buildSegments(func.ctx, "irc", true);
    }
    instance->addAttributeBool(WaIsKeyed, monitors.isKeyed());

    //---- virtual unsigned getFlags()
    instance->addAttributeBool(WaIsPreload, isPreloaded);

    bool matched = translator.registerGlobalUsage(tableExpr->queryChild(0));
    if (translator.getTargetClusterType() == RoxieCluster)
    {
        instance->addAttributeBool(WaIsFileOpt, tableExpr->hasAttribute(optAtom));
        instance->addAttributeBool(WaIsGlobalSpill, tableExpr->hasAttribute(jobTempAtom));
        if (tableExpr->hasAttribute(jobTempAtom) && !matched)
            throwUnexpected();
    }
    if (!matched && expr->hasAttribute(_spill_Atom))
    {
        StringBuffer spillName;
        getExprECL(tableExpr->queryChild(0), spillName);
        if (translator.queryOptions().allowThroughSpill)
            throwError1(HQLERR_ReadSpillBeforeWriteFix, spillName.str());
        else
            throwError1(HQLERR_ReadSpillBeforeWrite, spillName.str());
    }

    if (useGenericDiskReadWrite)
    {
        if ((modeOp != no_thor) && (modeOp != no_flat))
        {
            StringBuffer format;
            if (modeOp != no_filetype)
            {
                format.append(getOpString(modeOp)).toLowerCase();
            }
            else
            {
                // Pluggable file type; cite the file type name
                IHqlExpression * fileType = queryAttributeChild(mode, fileTypeAtom, 0);
                getStringValue(format, fileType);
                format.toLowerCase();
            }
            instance->startctx.addQuotedF("virtual const char * queryFormat() { return \"%s\"; }", format.str());
        }
    }

    //---- virtual bool canMatchAny() { return <value>; } ----
    LinkedHqlExpr guard = globalGuard.get();
    extendConditionOwn(guard, no_and, LINK(monitors.queryGlobalGuard()));
    if (guard)
        translator.doBuildBoolFunction(instance->startctx, "canMatchAny", guard);

    translator.buildEncryptHelper(instance->startctx, tableExpr->queryAttribute(encryptAtom));

    //---- preloadSize is passed via the xgmml, not via a member
    if (preloadSize)
    {
        instance->addAttributeInt(WaSizePreload, preloadSize->queryValue()->getIntValue());
    }

    if (includeFormatCrc)
    {
        //Spill files can still have virtual attributes in their physical records => remove them.
        OwnedHqlExpr noVirtualRecord = removeVirtualAttributes(expectedRecord);
        translator.buildFormatCrcFunction(instance->classctx, "getDiskFormatCrc", noVirtualRecord);

        if (newInputMapping)
            translator.buildFormatCrcFunction(instance->classctx, "getProjectedFormatCrc", projectedRecord);
        else
            translator.buildFormatCrcFunction(instance->classctx, "getProjectedFormatCrc", noVirtualRecord);
    }

    translator.buildMetaMember(instance->classctx, expectedRecord, isGrouped(tableExpr), "queryDiskRecordSize");
    if (activityKind != TAKpiperead)
        translator.buildMetaMember(instance->classctx, projectedRecord, isGrouped(tableExpr), "queryProjectedDiskRecordSize");

    buildLimits(instance->startctx, expr, instance->activityId); 
    buildKeyedLimitHelper(expr);

    //Note the helper base class contains code like the following
    //IThorDiskCallback * fpp;");
    //virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }");
}

void DiskReadBuilderBase::buildFlagsMember(IHqlExpression * expr)
{
    StringBuffer flags;
    if (tableExpr->hasAttribute(_spill_Atom)) flags.append("|TDXtemporary");
    if (tableExpr->hasAttribute(jobTempAtom)) flags.append("|TDXjobtemp");
    if (tableExpr->hasAttribute(groupedAtom)) flags.append("|TDXgrouped");
    if (tableExpr->hasAttribute(__compressed__Atom)) flags.append("|TDXcompress");
    if (tableExpr->hasAttribute(unsortedAtom)) flags.append("|TDRunsorted");
    if (tableExpr->hasAttribute(optAtom)) flags.append("|TDRoptional");
    if (tableExpr->hasAttribute(_workflowPersist_Atom)) flags.append("|TDXupdateaccessed");
    if (useGenericDiskReadWrite) flags.append("|TDXgeneric");

    if (isPreloaded) flags.append("|TDRpreload");
    if (monitors.isKeyed()) flags.append("|TDRkeyed");
    if (limitExpr)
    {
        if (limitExpr->hasAttribute(onFailAtom))
            flags.append("|TDRlimitcreates");
        else if (limitExpr->hasAttribute(skipAtom))
            flags.append("|TDRlimitskips");
    }
    if (keyedLimitExpr)
    {
        if (keyedLimitExpr->hasAttribute(onFailAtom))
            flags.append("|TDRkeyedlimitcreates|TDRcountkeyedlimit");           // is count correct?
        else if (keyedLimitExpr->hasAttribute(skipAtom))
            flags.append("|TDRkeyedlimitskips|TDRcountkeyedlimit");
        else if (keyedLimitExpr->hasAttribute(countAtom))
            flags.append("|TDRcountkeyedlimit");
    }
    if (onlyExistsAggreate) flags.append("|TDRaggregateexists");
    if (monitorsForGrouping) flags.append("|TDRgroupmonitors");
    if (!nameExpr->isConstant()) flags.append("|TDXvarfilename");
    if (isNonConstantAndQueryInvariant(nameExpr)) flags.append("|TDRinvariantfilename");
    if (translator.hasDynamicFilename(tableExpr)) flags.append("|TDXdynamicfilename");
    if (isUnfilteredCount) flags.append("|TDRunfilteredcount");
    if (isVirtualLogicalFilenameUsed || transformUsesVirtualLogicalFilename)
        flags.append("|TDRfilenamecallback");
    if (isVirtualLogicalFileposUsed || transformUsesVirtualFilePosition)
        flags.append("|TDRfileposcallback");
    if (transformUsesVirtualFilePosition || transformUsesVirtualLogicalFilename)
        flags.append("|TDRtransformvirtual");
    if (requiresOrderedMerge) flags.append("|TDRorderedmerge");
    if (hasDynamicOptions) flags.append("|TDRdynformatoptions");
    if (fieldInfo.hasVirtuals() && fieldInfo.canAppendVirtuals())
    {
        if (!canDefinitelyProcessWithTranslator(projectedRecord))
            flags.append("|TDRcloneappendvirtual");
    }

    if (flags.length())
        translator.doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
}


void DiskReadBuilderBase::buildTransformFpos(BuildCtx & transformCtx)
{
    if ((modeOp == no_csv) && !useGenericDiskReadWrite)
        associateFilePositions(transformCtx, "fpp", "dataSrc[0]");
    else
        associateFilePositions(transformCtx, "fpp", "left");
}


void DiskReadBuilderBase::extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds)
{
    HqlExprAttr mode = tableExpr->queryChild(2);
    //KEYED filters can only currently be implemented for binary files - not csv, xml or pipe....
    if (queryTableMode(tableExpr) == no_flat)
    {
        if (translator.queryOptions().implicitKeyedDiskFilter)
        {
            HqlExprArray newconds;
            ForEachItemIn(i, conds)
            {
                IHqlExpression * filter = &conds.item(i);
                if (isSourceInvariant(ds, filter))                  // more actually isSourceInvariant.
                    extendConditionOwn(globalGuard, no_and, LINK(filter));
                else
                    newconds.append(OLINK(*filter));
            }

            OwnedHqlExpr extraFilter;
            monitors.extractFilters(newconds, extraFilter);
            appendFilter(unkeyedFilter, extraFilter);
        }
        else
        {
            OwnedHqlExpr implicitFilter;
            ForEachItemIn(i, conds)
            {
                IHqlExpression * filter = &conds.item(i);
                if (isSourceInvariant(ds, filter))                  // more actually isSourceInvariant.
                    extendConditionOwn(globalGuard, no_and, LINK(filter));
                else
                {
                    node_operator op = filter->getOperator();
                    switch (op)
                    {
                    case no_assertkeyed:
                    case no_assertwild:
                        {
                            //MORE: This needs to test that the fields are at fixed offsets, fixed length, and collatable.
                            OwnedHqlExpr extraFilter;
                            monitors.extractFilters(filter, extraFilter);

                            //NB: Even if it is keyed then (part of) the test condition might be duplicated.
                            appendFilter(unkeyedFilter, extraFilter);
                            break;
                        }
                    default:
                        // Add this condition to the catchall filter
                        appendFilter(implicitFilter, filter);
                        break;
                    }
                }
            }

            if (implicitFilter)
            {
                if (translator.queryOptions().implicitKeyedDiskFilter && !monitors.isKeyed())
                {
                    OwnedHqlExpr extraFilter;
                    monitors.extractFilters(implicitFilter.get(), extraFilter);
                    appendFilter(unkeyedFilter, extraFilter);
                }
                else
                    appendFilter(unkeyedFilter, implicitFilter);
            }
        }
    }
    else
        SourceBuilder::extractMonitors(ds, unkeyedFilter, conds);
}

//---------------------------------------------------------------------------

class DiskReadBuilder : public DiskReadBuilderBase
{
public:
    DiskReadBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr, (_tableExpr->queryChild(2)->getOperator() != no_pipe), (_tableExpr->queryChild(2)->getOperator() == no_filetype))
    {
        extractCanMatch = (modeOp == no_thor) || (modeOp == no_flat) ||
                          (modeOp == no_filetype) ||
                          ((modeOp == no_csv) && useGenericDiskReadWrite);
    }

protected:
    virtual void buildTransform(IHqlExpression * expr) override;
    virtual void buildMembers(IHqlExpression * expr) override;
    virtual void analyseGraph(IHqlExpression * expr) override;

    void buildFormatOption(BuildCtx & ctx, IHqlExpression * name, IHqlExpression * value);
    void buildFormatOptions(BuildCtx & fixedCtx, IHqlExpression * expr);
    void buildFormatOptionsFunction(IHqlExpression * expr);
};


void DiskReadBuilder::analyseGraph(IHqlExpression * expr)
{
    DiskReadBuilderBase::analyseGraph(expr);
    if (newInputMapping && extractCanMatch && firstTransformer)
    {
        //If the record cannot be read using the serialized meta information, do not reduce the fields because the translator
        //cannot perform the mapping.
        if (!canDefinitelyProcessWithTranslator(projectedRecord))
            return;

        //Calculate the minimum set of fields required by any post-filters and projects.
        projectedRecord.setown(getMinimumInputRecord(translator, firstTransformer));
        if (projectedRecord != firstTransformer->queryChild(0)->queryRecord())
        {
            OwnedHqlExpr selSeq = createUniqueSelectorSequence();
            projectedSelector.setown(createSelector(no_left, projectedRecord, selSeq));

            //Check if projecting the input fields to the minimum now means the transform can be removed.
            if ((firstTransformer == lastTransformer) && (projectedRecord == firstTransformer->queryRecord()))
            {
                if (isSimpleProject(firstTransformer))
                    needToCallTransform = false;
            }
        }
    }
}


void DiskReadBuilder::buildMembers(IHqlExpression * expr)
{
    if ((modeOp == no_csv) && !useGenericDiskReadWrite)
        buildFilenameMember();
    else if (modeOp != no_pipe)
        buildReadMembers(expr);
    DiskReadBuilderBase::buildMembers(expr);

    //---- virtual const char * getPipeProgram() { return "grep"; } ----
    if (modeOp==no_pipe)
    {
        if (expr->hasAttribute(_disallowed_Atom))
            throwError(HQLERR_PipeNotAllowed);

        {
            MemberFunction func(translator, instance->startctx, "virtual const char * getPipeProgram() override");
            translator.buildReturn(func.ctx, mode->queryChild(0), unknownVarStringType);
        }

        IHqlExpression * csvFromPipe = tableExpr->queryAttribute(csvAtom);
        IHqlExpression * xmlFromPipe = tableExpr->queryAttribute(xmlAtom);
        bool usesContents = false;
        if (csvFromPipe)
        {
            if (isValidCsvRecord(tableExpr->queryRecord()))
            {
                StringBuffer csvInstanceName;
                translator.buildCsvReadTransformer(tableExpr, csvInstanceName, csvFromPipe);

                StringBuffer s;
                s.append("virtual ICsvToRowTransformer * queryCsvTransformer() override { return &").append(csvInstanceName).append("; }");
                instance->classctx.addQuoted(s);
            }
            else
            {
                throwUnexpected();  // should be caught earlier
            }
        }
        else if (xmlFromPipe)
        {
            translator.doBuildXmlReadMember(*instance, expr, "queryXmlTransformer", usesContents);
            translator.doBuildVarStringFunction(instance->classctx, "getXmlIteratorPath", queryAttributeChild(xmlFromPipe, rowAtom, 0));
        }
        
        StringBuffer flags;
        if (tableExpr->hasAttribute(groupAtom))      // not supported in parser?
            flags.append("|TPFgroupeachrow");
        if (tableExpr->hasAttribute(optAtom))        // not supported in parser?
            flags.append("|TPFnofail");

        if (csvFromPipe)
            flags.append("|TPFreadcsvfrompipe");
        if (xmlFromPipe)
            flags.append("|TPFreadxmlfrompipe");
        if (usesContents)
            flags.append("|TPFreadusexmlcontents");

        if (flags.length())
            translator.doBuildUnsignedFunction(instance->classctx, "getPipeFlags", flags.str()+1);
    }
}


void DiskReadBuilder::buildFormatOption(BuildCtx & ctx, IHqlExpression * name, IHqlExpression * value)
{
    if (value->isAttribute())
    {
    }
    else if (value->isList())
    {
        node_operator op = value->getOperator();
        if ((op == no_list) && value->numChildren())
        {
            ForEachChild(i, value)
                buildFormatOption(ctx, name, value->queryChild(i));
        }
        else if ((op == no_list) || (op == no_null))
        {
            //MORE: There should be a better way of doing this!
            translator.buildXmlSerializeBeginNested(ctx, name, false);
            translator.buildXmlSerializeEndNested(ctx, name);
        }
    }
    else
    {
        translator.buildXmlSerializeScalar(ctx, value, name);
    }
}

void DiskReadBuilder::buildFormatOptions(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * pluggableFileTypeAtom = expr->queryAttribute(fileTypeAtom); // null if pluggable file type not used
    
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);

        // Skip if expression is a pluggable file type (we don't want it appearing as an option)
        // or if it is not an attribute
        if (cur != pluggableFileTypeAtom && cur->isAttribute())
        {
            OwnedHqlExpr name = createConstant(str(cur->queryName()));
            if (cur->numChildren())
            {
                ForEachChild(c, cur)
                    buildFormatOption(ctx, name, cur->queryChild(c));
            }
            else
                translator.buildXmlSerializeScalar(ctx, queryBoolExpr(true), name);
        }
    }
}

void DiskReadBuilder::buildFormatOptionsFunction(IHqlExpression * expr)
{
    MemberFunction formatFunc(translator, instance->createctx, "virtual void getFormatOptions(IXmlWriter & out) override", MFopt);

    buildFormatOptions(formatFunc.ctx, expr);
    if (!expr->isConstant())
        hasDynamicOptions = true;
}

void DiskReadBuilder::buildTransform(IHqlExpression * expr)
{
    if (modeOp == no_pipe)
    {
        assertex(!(needToCallTransform || transformCanFilter));
        return;
    }

    if (recordRequiresSerialization(tableExpr->queryRecord(), diskAtom))
    {
        //Sanity check to ensure that the projected row is only in the in memory format if no transform needs to be called.
        if (needToCallTransform || transformCanFilter)
            throwUnexpectedX("Projected dataset should have been serialized");

        //Base implementation for a disk read throws an exception if it is called.
        return;
    }

    if ((modeOp == no_csv) && !useGenericDiskReadWrite)
    {
        translator.buildCsvParameters(instance->nestedctx, mode, NULL, true);

        {
            MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenSrc, const char * * dataSrc) override");
            translator.ensureRowAllocated(func.ctx, "crSelf");

            //associateVirtualCallbacks(*this, func.ctx, tableExpr);

            buildTransformBody(func.ctx, expr, true, false, true);
        }

        rootSelfRow = NULL;

        unsigned maxColumns = countTotalFields(tableExpr->queryRecord(), false);
        translator.doBuildUnsignedFunction(instance->classctx, "getMaxColumns", maxColumns);
        return;
    }

    if (useGenericDiskReadWrite)
        buildFormatOptionsFunction(mode);

    MemberFunction func(translator, instance->startctx);
    if ((instance->kind == TAKdiskread) || (instance->kind == TAKspillread) || (instance->kind == TAKnewdiskread))
        func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left) override");
    else
        func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IFilePositionProvider * fpp) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
    buildTransformBody(func.ctx, expr, true, false, true);
}


//---------------------------------------------------------------------------
ABoundActivity * HqlCppTranslator::doBuildActivityDiskRead(BuildCtx & ctx, IHqlExpression * expr)
{
//  assertex(!isGroupedActivity(expr));
    IHqlExpression *tableExpr = queryPhysicalRootTable(expr);
    if (!tableExpr)
        return buildCachedActivity(ctx, expr->queryChild(0));           // Somehow a null appeared.
    HqlExprAttr mode = tableExpr->queryChild(2);
    node_operator modeOp = mode->getOperator();
    bool isPiped = modeOp==no_pipe;

    DiskReadBuilder info(*this, tableExpr, tableExpr->queryChild(0));
    info.deduceDiskRecords();

    unsigned optFlags = (options.foldOptimized ? HOOfold : 0);
    if (info.newInputMapping && ((modeOp != no_csv) || options.genericDiskReadWrites) && (modeOp != no_xml) && (modeOp != no_pipe))
    {
        //The projected disk information (which is passed to the transform) uses the in memory format IFF
        // - The disk read is a trivial slimming transform (so no transform needs calling on the projected disk format.
        // - It is used for all disk reads since directly transforming is always at least as efficient as going via
        //   the serialized form.
        // Otherwise the table is converted to the serialized format.

        const bool forceAllProjectedSerialized = options.forceAllProjectedDiskSerialized;
        //Reading from a spill file uses the in-memory format to optimize on-demand spilling.
        bool optimizeInMemorySpill = targetThor();
        IHqlExpression * record = tableExpr->queryRecord();
        bool useInMemoryFormat = optimizeInMemorySpill && isSimpleProjectingDiskRead(expr) && !canDefinitelyProcessWithTranslator(record);
        if (forceAllProjectedSerialized || !useInMemoryFormat)
        {
            //else if the the table isn't serialized, then map to a serialized table, and then project to the real format
            if (recordRequiresSerialization(record, diskAtom))
            {
                OwnedHqlExpr transformed = buildTableFromSerialized(expr);
                //Need to wrap a possible no_usertable, otherwise the localisation can go wrong.
                if (expr->getOperator() == no_table)
                    transformed.setown(createDataset(no_compound_diskread, LINK(transformed)));
                OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, optFlags);
                traceExpression("afterDiskOptimize", optimized);
                return doBuildActivityDiskRead(ctx, optimized);
            }
        }

        //Otherwise the dataset is in the correct format

    }
    else
    {
        if (info.recordHasVirtuals() || info.fieldInfo.requiresDeserialize)
        {
            OwnedHqlExpr transformed = buildTableWithoutVirtuals(info.fieldInfo, expr);
            //Need to wrap a possible no_usertable, otherwise the localisation can go wrong.
            if (expr->getOperator() == no_table)
                transformed.setown(createDataset(no_compound_diskread, LINK(transformed)));
            OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, optFlags);
            traceExpression("afterDiskOptimize", optimized);
            return doBuildActivityDiskRead(ctx, optimized);
        }
    }

    OwnedHqlExpr optimized;
    if (expr->getOperator() == no_table)
        optimized.set(expr);
    else
        optimized.setown(optimizeHqlExpression(queryErrorProcessor(), expr, optFlags));

    if (optimized != expr)
        return buildActivity(ctx, optimized, false);

    if (isPiped)
        return info.buildActivity(ctx, expr, TAKpiperead, "PipeRead", NULL);
    ensureDiskAccessAllowed(tableExpr);
    if (info.useGenericDiskReadWrite)
        return info.buildActivity(ctx, expr, TAKnewdiskread, "NewDiskRead", NULL);
    if (modeOp == no_csv)
        return info.buildActivity(ctx, expr, TAKcsvread, "CsvRead", NULL);
    return info.buildActivity(ctx, expr, TAKdiskread, "DiskRead", NULL);
}

//---------------------------------------------------------------------------

class DiskNormalizeBuilder : public DiskReadBuilderBase
{
public:
    DiskNormalizeBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr, false, false)
    { 
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void analyseGraph(IHqlExpression * expr);
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildNormalizeIterators(ctx, expr, false);
    }

};


void DiskNormalizeBuilder::analyseGraph(IHqlExpression * expr)
{
    DiskReadBuilderBase::analyseGraph(expr);
    needDefaultTransform = (expr->queryNormalizedSelector()->getOperator() == no_select);
}

void DiskNormalizeBuilder::buildMembers(IHqlExpression * expr)
{
    buildFilenameMember();
    DiskReadBuilderBase::buildMembers(expr);
    buildNormalizeHelpers(expr);
}

void DiskNormalizeBuilder::buildTransform(IHqlExpression * expr)
{
    globaliterctx.setown(new BuildCtx(instance->startctx));
    globaliterctx->addGroup();

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, true, false, false);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityDiskNormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(!isGroupedActivity(expr));
    IHqlExpression *tableExpr = queryPhysicalRootTable(expr);
    ensureDiskAccessAllowed(tableExpr);
    HqlExprAttr mode = tableExpr->queryChild(2);
    assertex(mode->getOperator()!=no_pipe);

    DiskNormalizeBuilder info(*this, tableExpr, tableExpr->queryChild(0));
    info.deduceDiskRecords();

    LinkedHqlExpr transformed = expr;
    if (info.recordHasVirtualsOrDeserialize())
        transformed.setown(buildTableWithoutVirtuals(info.fieldInfo, expr));

    unsigned optFlags = (options.foldOptimized ? HOOfold : 0);
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, optFlags);
    if (optimized != expr)
        return buildActivity(ctx, optimized, false);

    return info.buildActivity(ctx, expr, TAKdisknormalize, "DiskNormalize", NULL);
}

//---------------------------------------------------------------------------

class DiskAggregateBuilder : public DiskReadBuilderBase
{
public:
    DiskAggregateBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr, false, false)
    { 
        failedFilterValue.clear();
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        DiskReadBuilderBase::analyseGraph(expr);
        returnIfFilterFails = !isNormalize;
    }
};


void DiskAggregateBuilder::buildMembers(IHqlExpression * expr)
{
    StringBuffer s;

    buildFilenameMember();
    DiskReadBuilderBase::buildMembers(expr);
    buildAggregateHelpers(expr);

    //virtual void processRow(void * self, const void * src) = 0;
    {
        BuildCtx rowctx(instance->startctx);
        rowctx.addQuotedFunction("virtual void processRow(ARowBuilder & crSelf, const void * src) override");
        rowctx.addQuotedLiteral("doProcessRow(crSelf, (const byte *)src);");
    }

    //virtual void processRows(void * self, size32_t srcLen, const void * src) = 0;
    {
        MemberFunction func(translator, instance->startctx, "virtual void processRows(ARowBuilder & crSelf, size32_t srcLen, const void * _left) override");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        OwnedHqlExpr ds = createVariable("left", makeReferenceModifier(tableExpr->getType()));
        OwnedHqlExpr len = createVariable("srcLen", LINK(sizetType));
        OwnedHqlExpr fullDs = createTranslated(ds, len);

        Owned<IHqlCppDatasetCursor> iter = translator.createDatasetSelector(func.ctx, fullDs);
        BoundRow * curRow = iter->buildIterateLoop(func.ctx, false);
        s.clear().append("doProcessRow(crSelf, ");
        translator.generateExprCpp(s, curRow->queryBound());
        s.append(");");
        func.ctx.addQuoted(s);
    }
}


void DiskAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "void doProcessRow(ARowBuilder & crSelf, const byte * left)");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, false, false, true);
}


//---------------------------------------------------------------------------

class DiskCountBuilder : public DiskReadBuilderBase
{
public:
    DiskCountBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr, node_operator _aggOp)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr, false, false)
    { 
        aggOp = _aggOp;
        isCompoundCount = true;
        failedFilterValue.set(queryZero());
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);
    virtual bool isExplicitExists() { return (aggOp == no_existsgroup); }

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        DiskReadBuilderBase::analyseGraph(expr);
        returnIfFilterFails = !isNormalize;
        if (aggOp == no_existsgroup)
            choosenValue.setown(getSizetConstant(1));
    }

protected:
    node_operator aggOp;
};


void DiskCountBuilder::buildMembers(IHqlExpression * expr)
{
    isUnfilteredCount = !(transformCanFilter||isNormalize);
    buildFilenameMember();
    DiskReadBuilderBase::buildMembers(expr);
    buildCountHelpers(expr, true);
}


void DiskCountBuilder::buildTransform(IHqlExpression * expr)
{
    if (transformCanFilter||isNormalize)
    {
        MemberFunction func(translator, instance->startctx, "size32_t valid(const byte * left)");
        OwnedHqlExpr cnt;
        if (isNormalize)
        {
            compoundCountVar.setown(func.ctx.getTempDeclare(sizetType, queryZero()));
            cnt.set(compoundCountVar);
        }
        else
            cnt.setown(getSizetConstant(1));

        BuildCtx subctx(func.ctx);
        buildTransformBody(subctx, expr, false, false, true);
        func.ctx.addReturn(cnt);
    }
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityDiskAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    assertex(!isGroupedActivity(expr));
    IHqlExpression *tableExpr = queryPhysicalRootTable(expr);
    ensureDiskAccessAllowed(tableExpr);

    HqlExprAttr mode = tableExpr->queryChild(2);
    assertex(mode->getOperator()!=no_pipe);

    DiskAggregateBuilder info(*this, tableExpr, tableExpr->queryChild(0));
    info.deduceDiskRecords();

    LinkedHqlExpr transformed = expr;
    if (info.recordHasVirtualsOrDeserialize())
        transformed.setown(buildTableWithoutVirtuals(info.fieldInfo, expr));
    transformed.setown(optimizeHqlExpression(queryErrorProcessor(), transformed, getSourceAggregateOptimizeFlags()));

    if (transformed != expr)
        return buildActivity(ctx, transformed, false);

    node_operator aggOp = querySimpleAggregate(expr, true, false);
    if (aggOp == no_countgroup || aggOp == no_existsgroup)
    {
        DiskCountBuilder info(*this, tableExpr, tableExpr->queryChild(0), aggOp);
        info.deduceDiskRecords();

        return info.buildActivity(ctx, expr, TAKdiskcount, "DiskCount", NULL);
    }
    else
        return info.buildActivity(ctx, expr, TAKdiskaggregate, "DiskAggregate", NULL);
}

//---------------------------------------------------------------------------

class DiskGroupAggregateBuilder : public DiskReadBuilderBase
{
public:
    DiskGroupAggregateBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr, false, false)
    { 
        failedFilterValue.clear();
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        DiskReadBuilderBase::analyseGraph(expr);
        returnIfFilterFails = !isNormalize;
    }
};


void DiskGroupAggregateBuilder::buildMembers(IHqlExpression * expr)
{
    buildFilenameMember();

    buildGroupingMonitors(expr, monitors);
    DiskReadBuilderBase::buildMembers(expr);

    buildGlobalGroupAggregateHelpers(expr);
}

void DiskGroupAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "void doProcessRow(const byte * left, IHThorGroupAggregateCallback * callback)");
    bool accessesCallback = containsOperator(expr, no_filepos) || containsOperator(expr, no_file_logicalname); 
    buildGroupAggregateTransformBody(func.ctx, expr, isNormalize || accessesCallback, true);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityDiskGroupAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression *tableExpr = queryPhysicalRootTable(expr);
    ensureDiskAccessAllowed(tableExpr);

    HqlExprAttr mode = tableExpr->queryChild(2);
    assertex(mode->getOperator()!=no_pipe);

    DiskGroupAggregateBuilder info(*this, tableExpr, tableExpr->queryChild(0));
    info.deduceDiskRecords();

    LinkedHqlExpr transformed = expr;
    if (info.recordHasVirtualsOrDeserialize())
        transformed.setown(buildTableWithoutVirtuals(info.fieldInfo, expr));
    transformed.setown(optimizeHqlExpression(queryErrorProcessor(), transformed, getSourceAggregateOptimizeFlags()));

    if (transformed != expr)
        return buildActivity(ctx, transformed, false);

    return info.buildActivity(ctx, expr, TAKdiskgroupaggregate, "DiskGroupAggregate", NULL);
}

//-----------------------------------------------------------------------------------------------
//-- Child dataset processing
//-----------------------------------------------------------------------------------------------

class ChildBuilderBase : public SourceBuilder
{
public:
    ChildBuilderBase(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : SourceBuilder(_translator, _tableExpr, _nameExpr, false, false)
    { 
    }

    virtual void buildMembers(IHqlExpression * expr) {}
    virtual void buildTransformFpos(BuildCtx & transformCtx)            {}
};



class ChildNormalizeBuilder : public ChildBuilderBase
{
public:
    ChildNormalizeBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : ChildBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void analyseGraph(IHqlExpression * expr);
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildNormalizeIterators(ctx, expr, true);
    }

};


void ChildNormalizeBuilder::analyseGraph(IHqlExpression * expr)
{
    ChildBuilderBase::analyseGraph(expr);
    needDefaultTransform = (expr->queryNormalizedSelector()->getOperator() == no_select);
}

void ChildNormalizeBuilder::buildMembers(IHqlExpression * expr)
{
    ChildBuilderBase::buildMembers(expr);
    buildNormalizeHelpers(expr);
}

void ChildNormalizeBuilder::buildTransform(IHqlExpression * expr)
{
    globaliterctx.setown(new BuildCtx(instance->startctx));
    globaliterctx->addGroup();

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, true, false, false);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityChildNormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    ChildNormalizeBuilder info(*this, NULL, NULL);
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), expr, HOOfold);

    if (optimized != expr)
        return buildActivity(ctx, optimized, false);

    return info.buildActivity(ctx, expr, TAKchildnormalize, "ChildNormalize", NULL);
}

//---------------------------------------------------------------------------

class ChildAggregateBuilder : public ChildBuilderBase
{
public:
    ChildAggregateBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : ChildBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
        failedFilterValue.clear();
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        ChildBuilderBase::analyseGraph(expr);
        returnIfFilterFails = false;
    }
};


void ChildAggregateBuilder::buildMembers(IHqlExpression * expr)
{
    ChildBuilderBase::buildMembers(expr);
    buildAggregateHelpers(expr);
}


void ChildAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "virtual void processRows(ARowBuilder & crSelf) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, false, false, false);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityChildAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    ChildAggregateBuilder info(*this, NULL, NULL);

    OwnedHqlExpr transformed = optimizeHqlExpression(queryErrorProcessor(), expr, getSourceAggregateOptimizeFlags());
    if (transformed != expr)
        return buildActivity(ctx, transformed, false);

    return info.buildActivity(ctx, expr, TAKchildaggregate, "ChildAggregate", NULL);
}

//---------------------------------------------------------------------------

class ChildGroupAggregateBuilder : public ChildBuilderBase
{
public:
    ChildGroupAggregateBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : ChildBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
        failedFilterValue.clear();
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        ChildBuilderBase::analyseGraph(expr);
        returnIfFilterFails = false;
    }
};


void ChildGroupAggregateBuilder::buildMembers(IHqlExpression * expr)
{
    ChildBuilderBase::buildMembers(expr);

    IHqlExpression * aggregate = expr->queryChild(0);
    assertex(aggregate->getOperator() == no_newaggregate);

    StringBuffer s;
    //virtual size32_t clearAggregate(void * self) = 0;
    translator.doBuildAggregateClearFunc(instance->startctx, aggregate);

    //virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * src) - never actually called.
    instance->startctx.addQuotedLiteral("virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * src) override { return 0; }");
}

void ChildGroupAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "void processRows(IHThorGroupAggregateCallback * callback)");
    buildGroupAggregateTransformBody(func.ctx, expr, true, false);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityChildGroupAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    ChildGroupAggregateBuilder info(*this, NULL, NULL);

    OwnedHqlExpr transformed = optimizeHqlExpression(queryErrorProcessor(), expr, getSourceAggregateOptimizeFlags());
    if (transformed != expr)
        return buildActivity(ctx, transformed, false);

    return info.buildActivity(ctx, expr, TAKchildgroupaggregate, "ChildGroupAggregate", NULL);
}


//---------------------------------------------------------------------------

class ChildThroughNormalizeBuilder : public ChildBuilderBase
{
public:
    ChildThroughNormalizeBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : ChildBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void analyseGraph(IHqlExpression * expr);
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        translator.bindTableCursor(ctx, expr, "left");
        doBuildNormalizeIterators(ctx, expr, false);
    }

};


void ChildThroughNormalizeBuilder::analyseGraph(IHqlExpression * expr)
{
    ChildBuilderBase::analyseGraph(expr);
    needDefaultTransform = (expr->queryNormalizedSelector()->getOperator() == no_select);
}

void ChildThroughNormalizeBuilder::buildMembers(IHqlExpression * expr)
{
    ChildBuilderBase::buildMembers(expr);
    buildNormalizeHelpers(expr);
}

void ChildThroughNormalizeBuilder::buildTransform(IHqlExpression * expr)
{
    globaliterctx.setown(new BuildCtx(instance->startctx));
    globaliterctx->addGroup();

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, true, false, false);
}


ABoundActivity * HqlCppTranslator::doBuildActivityCompoundSelectNew(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), expr, HOOfold);
    if (optimized->getOperator() == no_null)
        return buildCachedActivity(ctx, optimized);


    IHqlExpression * select = queryRoot(optimized);
    if (!(select && select->getOperator() == no_select && select->hasAttribute(newAtom)))
    {
        if (optimized->getOperator() == no_compound_selectnew)
            return buildCachedActivity(ctx, optimized->queryChild(0));
        return buildCachedActivity(ctx, optimized);
    }

    IHqlExpression * ds = select->queryChild(0);
    Owned<ABoundActivity> childActivity = buildCachedActivity(ctx, ds);

    OwnedHqlExpr fakeDataset = createDataset(no_anon, LINK(ds->queryRecord()));
    OwnedHqlExpr fakeSelect = createNewSelectExpr(LINK(fakeDataset), LINK(select->queryChild(1)));
    OwnedHqlExpr activityExpr = replaceExpression(optimized, select, fakeSelect);

    ChildThroughNormalizeBuilder info(*this, fakeDataset, NULL);
    info.gatherVirtualFields(true, true);   // ,false?
    return info.buildActivity(ctx, activityExpr, TAKchildthroughnormalize, "ChildThroughNormalize", childActivity);
}

//---------------------------------------------------------------------------


static ABoundActivity * buildNullIndexActivity(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * expr)
{
    while (isCompoundSource(expr))
        expr = expr->queryChild(0);
    return translator.buildCachedActivity(ctx, expr);
}


class IndexReadBuilderBase : public SourceBuilder
{
    friend class MonitorRemovalTransformer;
public:
    IndexReadBuilderBase(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : SourceBuilder(_translator, _tableExpr, _nameExpr, false, false),
          monitors(_tableExpr, _translator, -(int)numPayloadFields(_tableExpr), false, getHintBool(_tableExpr, createValueSetsAtom, _translator.queryOptions().createValueSets))
    {
    }

    virtual void buildMembers(IHqlExpression * expr);
    virtual void extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds);

protected:
    virtual void buildFlagsMember(IHqlExpression * expr);
    virtual void buildTransformFpos(BuildCtx & transformCtx)
    {
        associateFilePositions(transformCtx, "fpp", "left");
    }
    IHqlExpression * removeMonitors(IHqlExpression * expr);

protected:
    CppFilterExtractor monitors;
    SourceSteppingInfo steppingInfo;
};

void IndexReadBuilderBase::buildMembers(IHqlExpression * expr)
{
    //---- virtual void createSegmentMonitors(struct IIndexReadContext *) { ... } ----
    {
        MemberFunction func(translator, instance->startctx, "virtual void createSegmentMonitors(IIndexReadContext *irc) override");
        monitors.buildSegments(func.ctx, "irc", false);
    }

    SourceFieldUsage * fieldUsage = translator.querySourceFieldUsage(tableExpr);
    if (fieldUsage)
        monitors.noteKeyedFieldUsage(fieldUsage);

    buildLimits(instance->startctx, expr, instance->activityId);
    if (!limitExpr && !keyedLimitExpr && !choosenValue && (instance->kind == TAKindexread || instance->kind == TAKindexnormalize) && !steppedExpr)
    {
        unsigned implicitLimit = translator.getDefaultImplicitIndexReadLimit();
        if (translator.checkIndexReadLimit())
        {
            StringBuffer keyname;
            if (implicitLimit)
                translator.WARNINGAT2(CategoryLimit, queryLocation(expr), HQLWRN_ImplicitReadAddLimit, implicitLimit, monitors.queryKeyName(keyname));
            else
                translator.WARNINGAT1(CategoryLimit, queryLocation(expr), HQLWRN_ImplicitReadLimit, monitors.queryKeyName(keyname));
        }

        if (implicitLimit)
        {
            OwnedHqlExpr limit = getSizetConstant(implicitLimit);
            translator.buildLimitHelpers(instance->startctx, limit, NULL, false, nameExpr, instance->activityId);
        }
    }

    instance->addAttributeBool(WaIsPreload, isPreloaded);
    if (translator.getTargetClusterType() == RoxieCluster)
        instance->addAttributeBool(WaIsIndexOpt, tableExpr->hasAttribute(optAtom));

    if (monitors.queryGlobalGuard())
        translator.doBuildBoolFunction(instance->startctx, "canMatchAny", monitors.queryGlobalGuard());

    buildKeyedLimitHelper(expr);

    LinkedHqlExpr diskRecord = tableExpr->queryRecord();
    if (newInputMapping)
    {
        HqlMapTransformer mapper;
        bool hasFilePosition = getBoolAttribute(tableExpr, filepositionAtom, true);
        diskRecord.setown(createPhysicalIndexRecord(mapper, diskRecord, hasFilePosition, false));
    }

    translator.buildFormatCrcFunction(instance->classctx, "getDiskFormatCrc", true, diskRecord, tableExpr, 1);
    if (newInputMapping || (!tableExpr || !isKey(tableExpr)))
        translator.buildFormatCrcFunction(instance->classctx, "getProjectedFormatCrc", projectedRecord);
    else
        translator.buildFormatCrcFunction(instance->classctx, "getProjectedFormatCrc", true, diskRecord, tableExpr, 1); // backward compatibility for indexes

    IHqlExpression * originalKey = queryOriginalKey(tableExpr);
    translator.buildSerializedLayoutMember(instance->classctx, originalKey->queryRecord(), "getIndexLayout", numKeyedFields(originalKey));

    //Note the helper base class contains code like the following
    //IThorIndexCallback * fpp;");
    //virtual void setCallback(IThorIndexCallback * _tc) { fpp = _tc; }");
}

void IndexReadBuilderBase::buildFlagsMember(IHqlExpression * expr)
{
    StringBuffer flags;
    if (tableExpr->hasAttribute(sortedAtom))
        flags.append("|TIRsorted");
    else if (!isOrdered(tableExpr))
        flags.append("|TIRunordered");
    if (!monitors.isFiltered())
        flags.append("|TIRnofilter");
    if (isPreloaded)
        flags.append("|TIRpreload");
    if (tableExpr->hasAttribute(optAtom))
        flags.append("|TIRoptional");
    if (limitExpr && limitExpr->hasAttribute(skipAtom))
        flags.append("|TIRlimitskips");
    if (limitExpr && limitExpr->hasAttribute(onFailAtom))
        flags.append("|TIRlimitcreates");
    if (generateUnfilteredTransform)
        flags.append("|TIRunfilteredtransform");
    if (keyedLimitExpr)
    {
        if (keyedLimitExpr->hasAttribute(onFailAtom))
            flags.append("|TIRkeyedlimitcreates|TIRcountkeyedlimit");
        else if (keyedLimitExpr->hasAttribute(skipAtom))
            flags.append("|TIRkeyedlimitskips|TIRcountkeyedlimit");
        else if (keyedLimitExpr->hasAttribute(countAtom))
            flags.append("|TIRcountkeyedlimit");
    }
    IHqlExpression * firstStepped = steppingInfo.firstStepped();
    if (firstStepped && monitors.isEqualityFilterBefore(firstStepped))
        flags.append("|TIRstepleadequality");
    if (onlyExistsAggreate) flags.append("|TIRaggregateexists");
    if (monitorsForGrouping) flags.append("|TIRgroupmonitors");
    if (!nameExpr->isConstant()) flags.append("|TIRvarfilename");
    if (isNonConstantAndQueryInvariant(nameExpr)) flags.append("|TIRinvariantfilename");
    if (translator.hasDynamicFilename(tableExpr)) flags.append("|TIRdynamicfilename");
    if (requiresOrderedMerge) flags.append("|TIRorderedmerge");
    if (monitors.useValueSets())
        flags.append("|TIRnewfilters");
    if (containsOperator(expr, no_id2blob))
        flags.append("|TIRusesblob");

    if (flags.length())
        translator.doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
}

void IndexReadBuilderBase::extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds)
{
    OwnedHqlExpr extraFilter;
    monitors.extractFilters(conds, extraFilter);
    appendFilter(unkeyedFilter, extraFilter);
}

class MonitorRemovalTransformer : public HqlMapTransformer
{
public:
    MonitorRemovalTransformer(IndexReadBuilderBase & _builder) : builder(_builder) {}

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    IndexReadBuilderBase & builder;
};


IHqlExpression * MonitorRemovalTransformer::createTransformed(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_filter:
        {
            IHqlExpression * ds = expr->queryChild(0);
            IHqlExpression * body = expr->queryBody();
            unsigned match = builder.originalFilters.find(*body);
            if (match == NotFound)
            {
                if (builder.removedFilters.contains(*body))
                    return transform(ds);
                return NewHqlTransformer::createTransformed(expr);
            }

            IHqlExpression & newFilter = builder.mappedFilters.item(match);
            HqlExprArray args;
            args.append(*transform(ds));
            args.append(*transform(&newFilter));
            return expr->clone(args);
        }
    }
    return NewHqlTransformer::createTransformed(expr);
}

IHqlExpression * IndexReadBuilderBase::removeMonitors(IHqlExpression * expr)
{
    MonitorRemovalTransformer mapper(*this);
    mapper.setMapping(tableExpr, tableExpr);

    return mapper.transformRoot(expr);
}

//---------------------------------------------------------------------------


class NewIndexReadBuilder : public IndexReadBuilderBase
{
public:
    NewIndexReadBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : IndexReadBuilderBase(_translator, _tableExpr, _nameExpr)
    {
        extractCanMatch = true;
    }

    virtual void analyseGraph(IHqlExpression * expr)
    {
        IndexReadBuilderBase::analyseGraph(expr);
        gatherSteppingMeta(expr, steppingInfo);
        if (steppedExpr && transformCanFilter && translator.queryOptions().optimizeSteppingPostfilter)
        {
            //If sorted, but can't step output no point in duplicating the transform
            if (steppingInfo.outputStepping.exists())
                generateUnfilteredTransform = true;
        }

        returnIfFilterFails = !isNormalize;
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);
};

void NewIndexReadBuilder::buildMembers(IHqlExpression * expr)
{
    if (steppingInfo.exists())
    {
        steppingInfo.checkKeyable(monitors);
        monitors.preventMerge(steppingInfo.firstStepped());
    }

    buildReadMembers(expr);
    if (steppingInfo.exists())
        steppingInfo.generateMembers(translator, instance->classctx);
    IndexReadBuilderBase::buildMembers(expr);
}

void NewIndexReadBuilder::buildTransform(IHqlExpression * expr)
{
    if (true)
    {
        MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left) override");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
        buildTransformBody(func.ctx, expr, true, false, true);
    }

    if (generateUnfilteredTransform)
    {
        MemberFunction func(translator, instance->startctx, "virtual size32_t unfilteredTransform(ARowBuilder & crSelf, const void * _left) override");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
        buildTransformBody(func.ctx, expr, true, true, true);
    }
}

//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityIndexRead(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr transformed = buildIndexFromPhysical(expr);
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, HOOfold);

    IHqlExpression *tableExpr = queryPhysicalRootTable(optimized);
    //If the filter is false, then it may get reduced to a NULL operation!
    if (!tableExpr)
        return buildNullIndexActivity(*this, ctx, optimized);
    ensureDiskAccessAllowed(tableExpr);
    if (optimized->getOperator() != no_compound_indexread)
        optimized.setown(createDataset(no_compound_indexread, LINK(optimized)));

    traceExpression("beforeIndexRead", optimized);
    assertex(tableExpr->getOperator() == no_newkeyindex);
    NewIndexReadBuilder info(*this, tableExpr, tableExpr->queryChild(3));
    info.deduceIndexRecords();
    if (info.containsStepping(optimized))
        return info.buildActivity(ctx, optimized, TAKindexread, "SteppedIndexRead", NULL);
    return info.buildActivity(ctx, optimized, TAKindexread, "IndexRead", NULL);
}

//---------------------------------------------------------------------------

class IndexNormalizeBuilder : public IndexReadBuilderBase
{
public:
    IndexNormalizeBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : IndexReadBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void analyseGraph(IHqlExpression * expr);
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildNormalizeIterators(ctx, expr, false);
    }

};


void IndexNormalizeBuilder::analyseGraph(IHqlExpression * expr)
{
    IndexReadBuilderBase::analyseGraph(expr);
    needDefaultTransform = (expr->queryNormalizedSelector()->getOperator() == no_select);
}

void IndexNormalizeBuilder::buildMembers(IHqlExpression * expr)
{
    buildFilenameMember();
    IndexReadBuilderBase::buildMembers(expr);
    buildNormalizeHelpers(expr);
}

void IndexNormalizeBuilder::buildTransform(IHqlExpression * expr)
{
    globaliterctx.setown(new BuildCtx(instance->startctx));
    globaliterctx->addGroup();

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf) override");
    translator.ensureRowAllocated(func.ctx, "crSelf");

    //Because this transform creates iterator classes for the child iterators the expression tree needs to be modified
    //instead of using an inline tests.  We could switch to using this all the time for indexes once I trust it!
    OwnedHqlExpr simplified = removeMonitors(expr);
    lastTransformer.set(queryExpression(simplified->queryDataset()->queryTable()));
    useFilterMappings=false;
    buildTransformBody(func.ctx, simplified, true, false, false);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityIndexNormalize(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr transformed = buildIndexFromPhysical(expr);
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, HOOfold);
    traceExpression("afterOptimizeIndexNormalize", optimized);

    IHqlExpression *tableExpr = queryPhysicalRootTable(optimized);
    if (!tableExpr)
        return buildNullIndexActivity(*this, ctx, optimized);
    ensureDiskAccessAllowed(tableExpr);

    assertex(tableExpr->getOperator() == no_newkeyindex);
    IndexNormalizeBuilder info(*this, tableExpr, tableExpr->queryChild(3));
    info.deduceIndexRecords();
    return info.buildActivity(ctx, optimized, TAKindexnormalize, "IndexNormalize", NULL);
}

//---------------------------------------------------------------------------

class IndexAggregateBuilder : public IndexReadBuilderBase
{
public:
    IndexAggregateBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : IndexReadBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
        failedFilterValue.clear();
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        IndexReadBuilderBase::analyseGraph(expr);
        returnIfFilterFails = !isNormalize;
    }
};


void IndexAggregateBuilder::buildMembers(IHqlExpression * expr)
{
    StringBuffer s;

    buildFilenameMember();
    IndexReadBuilderBase::buildMembers(expr);
    buildAggregateHelpers(expr);

    //virtual void processRow(void * self, const void * src) = 0;
    {
        BuildCtx rowctx(instance->startctx);
        rowctx.addQuotedFunction("virtual void processRow(ARowBuilder & crSelf, const void * src) override");
        rowctx.addQuotedLiteral("doProcessRow(crSelf, (const byte *)src);");
    }

    //virtual void processRows(ARowBuilder & crSelf, size32_t srcLen, const void * _left)
    //is meaningless for an index - uses the default error implementation in the base class
}


void IndexAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "void doProcessRow(ARowBuilder & crSelf, const byte * left)");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
    buildTransformBody(func.ctx, expr, false, false, true);
}


//---------------------------------------------------------------------------

class IndexCountBuilder : public IndexReadBuilderBase
{
public:
    IndexCountBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr, node_operator _aggOp)
        : IndexReadBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
        aggOp = _aggOp;
        isCompoundCount = true;
        failedFilterValue.set(queryZero());
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);
    virtual bool isExplicitExists() { return (aggOp == no_existsgroup); }

protected:
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        IndexReadBuilderBase::analyseGraph(expr);
        returnIfFilterFails = !isNormalize;
        IHqlExpression * aggregate = expr->queryChild(0);
        if (isKeyedCountAggregate(aggregate))
        {
            if (isNormalize)
                translator.throwError(HQLERR_KeyedCountCantNormalize);
            if (!monitors.isKeyedExplicitly())
                translator.throwError(HQLERR_KeyedCountNotKeyed);
            if (transformCanFilter)
            {
                ForEachItemIn(i, originalFilters)
                    removedFilters.append(OLINK(originalFilters.item(i)));
                originalFilters.kill();
                mappedFilters.kill();
                transformCanFilter = false;
            }
        }
        if (aggOp == no_existsgroup)
            choosenValue.setown(getSizetConstant(1));
    }

protected:
    node_operator aggOp;
};


void IndexCountBuilder::buildMembers(IHqlExpression * expr)
{
    buildFilenameMember();
    IndexReadBuilderBase::buildMembers(expr);
    buildCountHelpers(expr, false);
}


void IndexCountBuilder::buildTransform(IHqlExpression * expr)
{
    if (transformCanFilter||isNormalize)
    {
        MemberFunction func(translator, instance->startctx, "virtual size32_t numValid(const void * _left) override");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
        OwnedHqlExpr cnt;
        if (isNormalize)
        {
            compoundCountVar.setown(func.ctx.getTempDeclare(sizetType, queryZero()));
            cnt.set(compoundCountVar);
        }
        else
            cnt.setown(getSizetConstant(1));

        BuildCtx subctx(func.ctx);
        buildTransformBody(subctx, expr, false, false, true);
        func.ctx.addReturn(cnt);
    }
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityIndexAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr transformed = buildIndexFromPhysical(expr);
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, getSourceAggregateOptimizeFlags());

    IHqlExpression *tableExpr = queryPhysicalRootTable(optimized);
    if (!tableExpr)
        return buildNullIndexActivity(*this, ctx, optimized);
    ensureDiskAccessAllowed(tableExpr);

    assertex(tableExpr->getOperator() == no_newkeyindex);
    node_operator aggOp = querySimpleAggregate(expr, true, false);
    if (aggOp == no_countgroup || aggOp == no_existsgroup)
    {
        IndexCountBuilder info(*this, tableExpr, tableExpr->queryChild(3), aggOp);
        info.deduceIndexRecords();
        return info.buildActivity(ctx, optimized, TAKindexcount, "IndexCount", NULL);
    }
    else
    {
        IndexAggregateBuilder info(*this, tableExpr, tableExpr->queryChild(3));
        info.deduceIndexRecords();
        return info.buildActivity(ctx, optimized, TAKindexaggregate, "IndexAggregate", NULL);
    }
}

//---------------------------------------------------------------------------

class IndexGroupAggregateBuilder : public IndexReadBuilderBase
{
public:
    IndexGroupAggregateBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : IndexReadBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
        failedFilterValue.clear();
        transformAccessesCallback = false;
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);

protected:
    void doBuildProcessCountMembers(BuildCtx & ctx, IHqlExpression * aggregate);
    virtual void processTransformSelect(BuildCtx & ctx, IHqlExpression * expr)
    {
        doBuildAggregateSelectIterator(ctx, expr);
    }
    virtual void analyseGraph(IHqlExpression * expr)
    {
        IndexReadBuilderBase::analyseGraph(expr);
        returnIfFilterFails = !isNormalize;
    }

protected:
    bool transformAccessesCallback;
};


void IndexGroupAggregateBuilder::buildMembers(IHqlExpression * expr)
{
    transformAccessesCallback = containsOperator(expr, no_filepos) || containsOperator(expr, no_id2blob); 

    buildFilenameMember();
    buildGroupingMonitors(expr, monitors);
    IndexReadBuilderBase::buildMembers(expr);
    buildGlobalGroupAggregateHelpers(expr);

    if (!isNormalize && !transformCanFilter && monitorsForGrouping && !transformAccessesCallback)
    {
        IHqlExpression * aggregate = expr->queryChild(0);
        ThorActivityKind newKind = TAKnone;
        switch (querySingleAggregate(aggregate, false, true, true))
        {
        case no_countgroup:
            newKind = TAKindexgroupcount;
            break;
        case no_existsgroup:
            newKind = TAKindexgroupexists;
            break;
        }
        if (newKind)
        {
            instance->changeActivityKind(newKind);
            doBuildProcessCountMembers(instance->startctx, aggregate);
        }
    }
}

void IndexGroupAggregateBuilder::doBuildProcessCountMembers(BuildCtx & ctx, IHqlExpression * aggregate)
{
    IHqlExpression * dataset = aggregate->queryChild(0);
    IHqlExpression * tgtRecord = aggregate->queryChild(1);
    IHqlExpression * transform = aggregate->queryChild(2);
    OwnedHqlExpr resultDataset = createDataset(no_anon, LINK(tgtRecord));

    {
        MemberFunction func(translator, ctx, "virtual size32_t initialiseCountGrouping(ARowBuilder & crSelf, const void * _src) override");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("unsigned char * src = (unsigned char *) _src;");
        translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
        BoundRow * selfCursor = translator.bindSelf(func.ctx, resultDataset, "crSelf");
        translator.bindTableCursor(func.ctx, dataset, "src");

        //Replace count() with 0, exists() with true and call as a transform - which will error if the replacement fails.
        OwnedHqlExpr count = createValue(no_countgroup, LINK(defaultIntegralType));
        OwnedHqlExpr exists = createValue(no_existsgroup, makeBoolType());
        OwnedHqlExpr newCount = createNullExpr(count);
        OwnedHqlExpr newTransform = replaceExpression(transform, count, newCount);
        newTransform.setown(replaceExpression(newTransform, exists, queryBoolExpr(true)));

        translator.doTransform(func.ctx, newTransform, selfCursor);
        translator.buildReturnRecordSize(func.ctx, selfCursor);
    }

    {
        MemberFunction func(translator, ctx, "virtual size32_t processCountGrouping(ARowBuilder & crSelf, unsigned __int64 count) override");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        BoundRow * selfCursor = translator.bindSelf(func.ctx, resultDataset, "crSelf");

        OwnedHqlExpr newCount = createTranslatedOwned(createVariable("count", LINK(defaultIntegralType)));

        OwnedHqlExpr self = getSelf(aggregate);
        ForEachChild(idx, transform)
        {
            IHqlExpression * cur = transform->queryChild(idx);
            if (cur->isAttribute())
                continue;

            OwnedHqlExpr target = selfCursor->bindToRow(cur->queryChild(0), self);
            IHqlExpression * src = cur->queryChild(1);
            IHqlExpression * arg = queryRealChild(src, 0);

            BuildCtx condctx(func.ctx);
            node_operator srcOp = src->getOperator();
            switch (srcOp)
            {
            case no_countgroup:
                {
                    if (arg)
                        translator.buildFilter(condctx, arg);
                    OwnedHqlExpr newValue = createValue(no_add, target->getType(), LINK(target), ensureExprType(newCount, target->queryType()));
                    translator.buildAssign(condctx, target, newValue);
                }
                break;
            }
        }
        translator.buildReturnRecordSize(func.ctx, selfCursor);
    }
}

void IndexGroupAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "void doProcessRow(const byte * left, IHThorGroupAggregateCallback * callback)");
    translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
    buildGroupAggregateTransformBody(func.ctx, expr, isNormalize || transformAccessesCallback, true);
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityIndexGroupAggregate(BuildCtx & ctx, IHqlExpression * expr)
{
    OwnedHqlExpr transformed = buildIndexFromPhysical(expr);
    OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, getSourceAggregateOptimizeFlags());

    IHqlExpression *tableExpr = queryPhysicalRootTable(optimized);
    if (!tableExpr)
        return buildNullIndexActivity(*this, ctx, optimized);
    ensureDiskAccessAllowed(tableExpr);

    IHqlExpression * aggregate = expr->queryChild(0);
    assertex(aggregate->getOperator() == no_newaggregate || aggregate->getOperator() == no_aggregate);
    ThorActivityKind tak = TAKindexgroupaggregate;
    assertex(tableExpr->getOperator() == no_newkeyindex);
    IndexGroupAggregateBuilder info(*this, tableExpr, tableExpr->queryChild(3));
    info.deduceIndexRecords();
    return info.buildActivity(ctx, optimized, tak, "IndexGroupAggregate", NULL);
}

//---------------------------------------------------------------------------

void associateVirtualCallbacks(HqlCppTranslator & translator, BuildCtx & ctx, IHqlExpression * dataset)
{
    OwnedHqlExpr fpos = getFilepos(dataset, false);
    OwnedHqlExpr lfpos = getFilepos(dataset, true);
    Owned<IHqlExpression> fposExpr = createFileposCall(translator, getFilePositionId, "fpp", "crSelf.row()");
    Owned<IHqlExpression> lfposExpr = createFileposCall(translator, getLocalFilePositionId, "fpp", "crSelf.row()");
    ctx.associateExpr(fpos, fposExpr);
    ctx.associateExpr(lfpos, lfposExpr);
}

void HqlCppTranslator::buildXmlReadTransform(IHqlExpression * dataset, StringBuffer & factoryName, bool & usesContents)
{
    OwnedHqlExpr xmlMarker = createAttribute(xmlReadMarkerAtom, LINK(dataset->queryRecord()));
    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(xmlMarker);

    if (match)
    {
        IHqlExpression * matchExpr = match->queryExpr();
        matchExpr->queryChild(0)->queryValue()->getStringValue(factoryName);
        usesContents = matchExpr->queryChild(1)->queryValue()->getBoolValue();
        return;
    }

    StringBuffer s, id, className;
    getUniqueId(id);
    className.append("cx2r").append(id);

    const char * interfaceName = "IXmlToRowTransformer";


    StringBuffer prolog, epilog;
    prolog.append("struct ").append(className).append(" : public RtlCInterface, implements ").append(interfaceName);
    epilog.append(";");

    GlobalClassBuilder builder(*this, declarectx, className, "CXmlToRowTransformer", interfaceName, true, false);
    builder.buildClass(XmlTransformerPrio);
    builder.setIncomplete(true);

    BuildCtx & classctx = builder.classctx;
    s.clear().append("inline ").append(className).append("(unsigned _activityId) : CXmlToRowTransformer(_activityId) {}");
    classctx.addQuoted(s);

    {
        MemberFunction func(*this, classctx, "virtual size32_t transform(ARowBuilder & crSelf, IColumnProvider * row, IThorDiskCallback * fpp) override");
        ensureRowAllocated(func.ctx, "crSelf");

        xmlUsesContents = false;
        //MORE: If this becomes a compound activity
        BoundRow * rootSelfRow = bindSelf(func.ctx, dataset, "crSelf");
        bindXmlTableCursor(func.ctx, dataset, "row", no_none, NULL, true);
        OwnedHqlExpr activityId = createVariable("activityId", LINK(sizetType));
        func.ctx.associateExpr(queryActivityIdMarker(), activityId);

        associateVirtualCallbacks(*this, func.ctx, dataset);

        OwnedHqlExpr active = ensureActiveRow(dataset);
        buildAssign(func.ctx, rootSelfRow->querySelector(), active);
        buildReturnRecordSize(func.ctx, rootSelfRow);
        usesContents = xmlUsesContents;
        rootSelfRow = NULL;
    }

    buildMetaMember(classctx, dataset, false, "queryRecordSize");

    builder.setIncomplete(false);
    builder.completeClass(XmlTransformerPrio);

    factoryName.append(builder.accessorName);

    OwnedHqlExpr matchedValue = createAttribute(internalAtom, createConstant(factoryName.str()), createConstant(usesContents));
    declarectx.associateExpr(xmlMarker, matchedValue);
}

//---------------------------------------------------------------------------

unsigned HqlCppTranslator::buildCsvReadTransform(BuildCtx & subctx, IHqlExpression * dataset, bool newInterface, IHqlExpression * csvAttr)
{
    MemberFunction func(*this, subctx);

    if (newInterface)
        func.start("virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenSrc, const char * * dataSrc) override");
    else
        func.start("virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenSrc, const char * * dataSrc, unsigned __int64 _fpos) override");

    //MORE: If this becomes a compound activity
    BoundRow * rootSelfRow = bindSelf(func.ctx, dataset, "crSelf");
    bindCsvTableCursor(func.ctx, dataset, "Src", no_none, NULL, true, queryCsvEncoding(csvAttr));
    ensureRowAllocated(func.ctx, rootSelfRow);

    if (newInterface)
    {
        associateVirtualCallbacks(*this, func.ctx, dataset);
    }
    else
    {
        OwnedHqlExpr fpos = getFilepos(dataset, false);
        OwnedHqlExpr fposVar = createVariable("_fpos", fpos->getType());
        func.ctx.associateExpr(fpos, fposVar);
    }

    OwnedHqlExpr active = ensureActiveRow(dataset);
    buildAssign(func.ctx, rootSelfRow->querySelector(), active);
    buildReturnRecordSize(func.ctx, rootSelfRow);
    rootSelfRow = NULL;

    return countTotalFields(dataset->queryRecord(), false);
}

void HqlCppTranslator::buildCsvReadTransformer(IHqlExpression * dataset, StringBuffer & instanceName, IHqlExpression * optCsvAttr)
{
    OwnedHqlExpr csvMarker = createAttribute(csvReadMarkerAtom, LINK(dataset->queryRecord()), LINK(optCsvAttr));
    BuildCtx declarectx(*code, declareAtom);
    HqlExprAssociation * match = declarectx.queryMatchExpr(csvMarker);

    if (match)
    {
        IHqlExpression * matchExpr = match->queryExpr();
        matchExpr->queryChild(0)->queryValue()->getStringValue(instanceName);
        return;
    }

    StringBuffer id, className;
    getUniqueId(id);
    instanceName.append("c2r").append(id);
    className.append("cc2r").append(id);

    StringBuffer prolog, epilog;
    prolog.append("struct ").append(className).append(" : public RtlCInterface, implements ICsvToRowTransformer");
    epilog.append(" ").append(instanceName).append(";");

    BuildCtx classctx(declarectx);
    classctx.setNextPriority(XmlTransformerPrio);
    IHqlStmt * transformClass = classctx.addQuotedCompound(prolog, epilog);
    transformClass->setIncomplete(true);
    transformClass->setIncluded(false);         // if can't generate csv for this record, then don't generate an invalid class.

    classctx.addQuotedLiteral("virtual void Link() const override { RtlCInterface::Link(); }");
    classctx.addQuotedLiteral("virtual bool Release() const override { return RtlCInterface::Release(); }");

    unsigned maxColumns = buildCsvReadTransform(classctx, dataset, false, optCsvAttr);
    doBuildUnsignedFunction(classctx, "getMaxColumns", maxColumns);

    buildMetaMember(classctx, dataset, false, "queryRecordSize");
    buildCsvParameters(classctx, optCsvAttr, NULL, true);

    transformClass->setIncomplete(false);
    transformClass->setIncluded(true);

    if (options.spanMultipleCpp)
    {
        StringBuffer helperFunc;
        createAccessFunctions(helperFunc, declarectx, XmlTransformerPrio, "ICsvToRowTransformer", instanceName.str());
        instanceName.clear().append(helperFunc).append("()");
    }

    OwnedHqlExpr matchedValue = createAttribute(internalAtom, createConstant(instanceName.str()));
    declarectx.associateExpr(csvMarker, matchedValue);
}


ABoundActivity * HqlCppTranslator::doBuildActivityXmlRead(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression * tableExpr = expr;
    ensureDiskAccessAllowed(tableExpr);
    IHqlExpression * filename = tableExpr->queryChild(0);
    IHqlExpression * mode = tableExpr->queryChild(2);
    node_operator modeType = mode->getOperator();
    StringBuffer s;

    ThorActivityKind kind = (modeType == no_json) ? TAKjsonread : TAKxmlread;
    Owned<ActivityInstance> instance = new ActivityInstance(*this, ctx, kind, expr, "XmlRead");

    buildActivityFramework(instance);

    buildInstancePrefix(instance);

    //MORE: Improve when we support projecting xml instead of reading all
    SourceFieldUsage * fieldUsage = querySourceFieldUsage(tableExpr);
    if (fieldUsage && !fieldUsage->seenAll())
        fieldUsage->noteAll();

    //---- virtual const char * getFileName() { return "x.d00"; } ----
    buildFilenameFunction(*instance, instance->startctx, WaFilename, "getFileName", filename, hasDynamicFilename(tableExpr), SummaryType::ReadIndex, tableExpr->hasAttribute(optAtom), tableExpr->hasAttribute(_signed_Atom));
    buildEncryptHelper(instance->startctx, tableExpr->queryAttribute(encryptAtom));

    bool usesContents = false;
    doBuildXmlReadMember(*instance, tableExpr, "queryTransformer", usesContents);

    doBuildVarStringFunction(instance->classctx, "getXmlIteratorPath", queryRealChild(mode, 0));

    buildMetaMember(instance->classctx, tableExpr, false, "queryDiskRecordSize");  // A lie, but I don't care....
    buildMetaMember(instance->classctx, tableExpr, false, "queryProjectedDiskRecordSize");  // A lie, but I don't care....

    //virtual unsigned getFlags() = 0;
    StringBuffer flags;
    if (expr->hasAttribute(_spill_Atom)) flags.append("|TDXtemporary");
    if (expr->hasAttribute(unsortedAtom)) flags.append("|TDRunsorted");
    if (expr->hasAttribute(optAtom)) flags.append("|TDRoptional");
    if (usesContents) flags.append("|TDRusexmlcontents");
    if (mode->hasAttribute(noRootAtom)) flags.append("|TDRxmlnoroot");
    if (!filename->isConstant()) flags.append("|TDXvarfilename");
    if (hasDynamicFilename(expr)) flags.append("|TDXdynamicfilename");

    if (flags.length())
        doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);

    //Note the helper base class contains code like the following
    //IThorDiskCallback * fpp;");
    //virtual void setCallback(IThorDiskCallback * _tc) { fpp = _tc; }");

    buildInstanceSuffix(instance);

    addFileDependency(filename, instance->queryBoundActivity());

    return instance->getBoundActivity();
}


//---------------------------------------------------------------------------

ABoundActivity * HqlCppTranslator::doBuildActivityTable(BuildCtx & ctx, IHqlExpression * expr)
{
    node_operator mode = expr->queryChild(2)->getOperator();
    switch (mode)
    {
    case no_thor:
    case no_flat:
    case no_pipe:
    case no_csv:
    case no_filetype:
        return doBuildActivityDiskRead(ctx, expr);
    case no_xml:
    case no_json:
        return doBuildActivityXmlRead(ctx, expr);
    default:
        UNIMPLEMENTED;
    }
}

//---------------------------------------------------------------------------

class FetchBuilder : public SourceBuilder
{
public:
    FetchBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr, IHqlExpression * _fetchExpr)
        : SourceBuilder(_translator, _tableExpr, _nameExpr, false, false)
    {
        compoundExpr.set(_fetchExpr);
        fetchExpr.set(queryFetch(_fetchExpr));
        selSeq.set(querySelSeq(fetchExpr));
        fetchRhs = fetchExpr->queryChild(1);
        memoryRhsRecord = fetchRhs->queryRecord();
        serializedRhsRecord.setown(getSerializedForm(memoryRhsRecord, diskAtom));
    }

    virtual void buildMembers(IHqlExpression * expr);
    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildTransformFpos(BuildCtx & transformCtx);

protected:
    HqlExprAttr compoundExpr;
    HqlExprAttr fetchExpr;
    HqlExprAttr selSeq;
    HqlExprAttr serializedRhsRecord;
    IHqlExpression * fetchRhs;
    IHqlExpression * memoryRhsRecord;
};


void FetchBuilder::buildMembers(IHqlExpression * expr)
{
    buildFilenameMember();

    IHqlExpression * fetch = queryFetch(expr);
    {
        MemberFunction func(translator, instance->startctx, "virtual unsigned __int64 extractPosition(const void * _right) override");
        func.ctx.addQuotedLiteral("const unsigned char * right = (const unsigned char *) _right;");
        translator.bindTableCursor(func.ctx, fetch->queryChild(1), "right", no_right, selSeq);
        translator.buildReturn(func.ctx, fetch->queryChild(2));
    }
    
    translator.buildEncryptHelper(instance->startctx, tableExpr->queryAttribute(encryptAtom), "getFileEncryptKey");

    //Fetch flags
    StringBuffer flags;
    if (tableExpr->hasAttribute(optAtom))
        flags.append("|FFdatafileoptional");
    if (!nameExpr->isConstant())
        flags.append("|FFvarfilename");
    if (translator.hasDynamicFilename(tableExpr))     
        flags.append("|FFdynamicfilename");
    if (isNonConstantAndQueryInvariant(nameExpr))
        flags.append("|FFinvariantfilename");

    if (flags.length())
        translator.doBuildUnsignedFunction(instance->classctx, "getFetchFlags", flags.str()+1);

    if (tableExpr->hasAttribute(optAtom) && translator.targetRoxie())
        instance->addAttributeBool(WaIsFileOpt, true);

    buildLimits(instance->startctx, expr, instance->activityId);

    switch (getDatasetKind(tableExpr))
    {
    case no_csv:
        {
            translator.buildCsvParameters(instance->nestedctx, tableExpr->queryChild(2), NULL, true);
            unsigned maxColumns = getFieldCount(tableExpr->queryRecord());
            StringBuffer s;
            s.clear().append("virtual unsigned getMaxColumns() override { return ").append(maxColumns).append("; }");
            instance->classctx.addQuoted(s);
            break;
        }
    case no_xml:
    case no_json:
    case no_filetype:
        break;
    default:
        translator.buildFormatCrcFunction(instance->classctx, "getDiskFormatCrc", physicalRecord);
        break;
    }

    if (!containsOnlyLeft(fetch->queryChild(3), true))
    {
        //MORE: Need to change following if we optimize it to only extract the relevant fields.
        instance->classctx.addQuotedLiteral("virtual bool extractAllJoinFields() override { return true; }");

        {
            MemberFunction func(translator, instance->startctx, "virtual size32_t extractJoinFields(ARowBuilder & crSelf, const void * _left) override");
            translator.ensureRowAllocated(func.ctx, "crSelf");
            translator.buildRecordSerializeExtract(func.ctx, memoryRhsRecord);
        }

        StringBuffer s;
        MetaInstance meta(translator, serializedRhsRecord, false);
        translator.buildMetaInfo(meta);
        instance->classctx.addQuoted(s.clear().append("virtual IOutputMetaData * queryExtractedSize() override { return &").append(meta.queryInstanceObject()).append("; }"));
    }

    translator.buildMetaMember(instance->classctx, expectedRecord, isGrouped(tableExpr), "queryDiskRecordSize");
    translator.buildMetaMember(instance->classctx, projectedRecord, isGrouped(tableExpr), "queryProjectedDiskRecordSize");
}


void FetchBuilder::buildTransform(IHqlExpression * expr)
{
    translator.xmlUsesContents = false;

    MemberFunction func(translator, instance->startctx);
    switch (getDatasetKind(tableExpr))
    {
    case no_csv:
        func.start("virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenLeft, const char * * dataLeft, const void * _right, unsigned __int64 _fpos) override");
        func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *)_right;");
        break;
    case no_xml:
    case no_json:
        func.start("virtual size32_t transform(ARowBuilder & crSelf, IColumnProvider * xmlLeft, const void * _right, unsigned __int64 _fpos) override");
        func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *)_right;");
        break;
    default:
        func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _fpos) override");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *)_right;");
        break;
    }

    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, true, false, true);

    if (translator.xmlUsesContents)
        instance->classctx.addQuotedLiteral("virtual bool requiresContents() override { return true; }");
}


void FetchBuilder::buildTransformFpos(BuildCtx & transformCtx)
{
    fpos.setown(createVariable("_fpos", LINK(fposType)));

    //NB: Because the fetch gets merged with the usertable used to project the dataset, the
    //transform contains filepos(LEFT) not filepos(tableExpr)
    OwnedHqlExpr leftSelect = createSelector(no_left, fetchExpr->queryChild(0), selSeq);
    OwnedHqlExpr fposField = getFilepos(leftSelect, false);
    transformCtx.associateExpr(fposField, fpos);
    //MORE: Could possibly support virtual(filename) here
}


static HqlTransformerInfo fetchInputReplacerInfo("FetchInputReplacer");
class FetchInputReplacer : public NewHqlTransformer
{
public:
    FetchInputReplacer(IHqlExpression * _newDataset, node_operator side) 
        : NewHqlTransformer(fetchInputReplacerInfo)
    { 
        newDataset = _newDataset; 
        child = (side == no_left) ? 0 : 1; 
    }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        if (expr->getOperator() == no_fetch)
            return replaceChild(expr, child, newDataset);
        return NewHqlTransformer::createTransformed(expr);
    }

protected:
    IHqlExpression * newDataset;
    unsigned child;
};

IHqlExpression * replaceFetchInput(IHqlExpression * expr, IHqlExpression * newDataset, node_operator side)
{
    FetchInputReplacer replacer(newDataset, side);
    return replacer.transformRoot(expr);
}

ABoundActivity * HqlCppTranslator::doBuildActivityFetch(BuildCtx & ctx, IHqlExpression * expr)
{
    IHqlExpression *fetch = queryFetch(expr);
    IHqlExpression *tableExpr = queryPhysicalRootTable(fetch->queryChild(0));
    if (!tableExpr)
        throwError(HQLERR_FetchNonDiskfile);
    FetchBuilder info(*this, tableExpr, tableExpr->queryChild(0), expr);
    info.deduceDiskRecords();//?needToSerializeRecord(mode)

    unsigned optFlags = (options.foldOptimized ? HOOfold : 0);
    if (info.recordHasVirtualsOrDeserialize())
    {
        OwnedHqlExpr projected = createTableWithoutVirtuals(info.fieldInfo, tableExpr);
        //Nasty: We don't want to optimize the rhs, otherwise references get changed!
        //so optimize everything except the rhs, and then add the rhs back in again.
        IHqlExpression * fetchRhs = fetch->queryChild(1);
        OwnedHqlExpr null = createDataset(no_anon, LINK(fetchRhs->queryRecord()));
        OwnedHqlExpr simple = replaceFetchInput(expr, null, no_right);
        OwnedHqlExpr transformed = replaceExpression(simple, tableExpr, projected);
        OwnedHqlExpr optSimple = optimizeHqlExpression(queryErrorProcessor(), transformed, optFlags);
        IHqlExpression * newFetch = queryFetch(optSimple);
        assertex(newFetch);
        IHqlExpression * lhs = newFetch->queryChild(0);
        if (lhs->getOperator() != no_table)
            throwError1(HQLERR_ExpectedFileLhsFetch, getOpString(lhs->getOperator()));

        OwnedHqlExpr optimized = replaceFetchInput(optSimple, fetchRhs, no_right);
        return doBuildActivityFetch(ctx, optimized);
    }
    if (getProjectCount(expr) > 1)
    {
        OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), expr, optFlags);
        return doBuildActivityFetch(ctx, optimized);
    }

    Owned<ABoundActivity> childActivity = buildCachedActivity(ctx, fetch->queryChild(1));
    node_operator kind = getDatasetKind(tableExpr);
    switch (kind)
    {
    case no_csv:
        return info.buildActivity(ctx, expr, TAKcsvfetch, "CsvFetch", childActivity);
    case no_xml:
        return info.buildActivity(ctx, expr, TAKxmlfetch, "XmlFetch", childActivity);
    case no_json:
        //Note use of "XmlFetch" because we want the code generator to leverage existing xml classes
        return info.buildActivity(ctx, expr, TAKjsonfetch, "XmlFetch", childActivity);
    case no_flat:
    case no_thor:
        return info.buildActivity(ctx, expr, TAKfetch, "Fetch", childActivity);
    }
    throwError1(HQLERR_FetchNotSupportMode, getOpString(kind));
    return NULL;
}
