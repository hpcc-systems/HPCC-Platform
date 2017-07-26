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
#include "hqltrans.ipp"
#include "hqlpmap.hpp"
#include "hqlttcpp.ipp"
#include "hqlsource.ipp"
#include "hqlcse.ipp"
#include "hqliter.ipp"
#include "thorcommon.hpp"
#include "hqlinline.hpp"

//#define FLATTEN_DATASETS
//#define HACK_TO_IGNORE_TABLE

//#define TraceExprPrintLog(x, expr)                PrintLog(x ": %s", expr->toString(StringBuffer()).str());
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

static IHqlExpression * getExplicitlyPromotedCompare(IHqlExpression * filter)
{
    switch (filter->getOperator())
    {
    case no_in:
    case no_notin:
        return LINK(filter);
    }
    IHqlExpression * l = filter->queryChild(0);
    IHqlExpression * r = filter->queryChild(1);
    ITypeInfo * lType = queryUnqualifiedType(l->queryType());
    ITypeInfo * rType = queryUnqualifiedType(r->queryType());
    if (lType == rType)
        return LINK(filter);

    //Add explicit casts to the type.  ensureExprType won't add a (string) to a string2 field.
    Owned<ITypeInfo> promotedType = getPromotedECLType(lType, rType);
    HqlExprArray args;
    if (lType == promotedType)
        args.append(*LINK(l));
    else
        args.append(*createValue(no_implicitcast, LINK(promotedType), LINK(l)));
    args.append(*ensureExprType(r, promotedType));
    return filter->clone(args);
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
            if (isUnknownSize(cur->queryType()))
                simpleVirtualsAtEnd = false;
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
    OwnedHqlExpr projected = createDatasetF(no_newusertable, newDataset, LINK(record), newTransform, createAttribute(_internal_Atom), NULL);
    return tableExpr->cloneAllAnnotations(projected);
}

IHqlExpression * buildTableWithoutVirtuals(VirtualFieldsInfo & info, IHqlExpression * expr)
{
    IHqlExpression * tableExpr = queryPhysicalRootTable(expr);
    OwnedHqlExpr projected = createTableWithoutVirtuals(info, tableExpr);
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
                if (idx2  == fileposIndex)
                    newValue.setown(getFilepos(diskDataset, false));
                else
                {
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
                        newValue.setown(convertIndexPhysical2LogicalValue(cur, physicalSelect, allowTranslate));
                }

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


static IHqlExpression * mapIfBlock(HqlMapTransformer & mapper, IHqlExpression * cur);
static IHqlExpression * mapIfBlockRecord(HqlMapTransformer & mapper, IHqlExpression * record)
{
    HqlExprArray mapped;
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        if (cur->getOperator() == no_ifblock)
            mapped.append(*mapIfBlock(mapper, cur));
        else
            mapped.append(*LINK(cur));
    }
    return record->clone(mapped);
}


static IHqlExpression * mapIfBlock(HqlMapTransformer & mapper, IHqlExpression * cur)
{
    HqlExprArray args;
    unwindChildren(args, cur);
    args.replace(*mapper.transformRoot(&args.item(0)), 0);
    args.replace(*mapIfBlockRecord(mapper, &args.item(1)), 1);
    return cur->clone(args);
}


static IHqlExpression * createPhysicalIndexRecord(HqlMapTransformer & mapper, IHqlExpression * tableExpr, IHqlExpression * record, bool hasInternalFileposition, bool allowTranslate)
{
    HqlExprArray physicalFields;
    unsigned max = record->numChildren() - (hasInternalFileposition ? 1 : 0);
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * cur = record->queryChild(idx);
        IHqlExpression * newField = NULL;

        if (cur->isAttribute())
            physicalFields.append(*LINK(cur));
        else if (cur->getOperator() == no_ifblock)
            physicalFields.append(*mapIfBlock(mapper, cur));
        else if (cur->getOperator() == no_record)
            physicalFields.append(*createPhysicalIndexRecord(mapper, tableExpr, cur, false, allowTranslate));
        else if (cur->hasAttribute(blobAtom))
        {
            newField = createField(cur->queryId(), makeIntType(8, false), NULL, NULL);
        }
        else
        {
            OwnedHqlExpr select = createSelectExpr(LINK(tableExpr), LINK(cur));
            if (!allowTranslate)
                newField = LINK(cur);
            else if (cur->isDatarow() && !isInPayload())
            {
                //MORE: Mappings for ifblocks using self.a.b (!)
                HqlMapTransformer childMapper;
                OwnedHqlExpr newRecord = createPhysicalIndexRecord(childMapper, select, cur->queryRecord(), false, allowTranslate);
                HqlExprArray args;
                unwindChildren(args, cur);
                newField = createField(cur->queryId(), newRecord->getType(), args);
            }
            else
            {
                //This should support other non serialized formats.  E.g., link counted strings. 
                //Simplest would be to move getSerializedForm code + call that first.
                if (cur->hasAttribute(_linkCounted_Atom) || cur->isDatarow())
                {
                    newField = getSerializedForm(cur, diskAtom);
                    assertex(newField != cur || cur->isDatarow());
                }
                else
                {
                    OwnedHqlExpr hozed = getHozedKeyValue(select);
                    if (hozed->queryType() == select->queryType())
                        newField = LINK(cur);
                    else
                        newField = createField(cur->queryId(), hozed->getType(), extractFieldAttrs(cur));
                }
            }
        }

        if (newField)
        {
            physicalFields.append(*newField);
            if (cur != newField)
            {
                IHqlExpression * self = querySelfReference();
                OwnedHqlExpr select = createSelectExpr(LINK(self), LINK(cur));
                OwnedHqlExpr physicalSelect = createSelectExpr(LINK(self), LINK(newField));
                OwnedHqlExpr newValue = convertIndexPhysical2LogicalValue(cur, physicalSelect, true);
                mapper.setMapping(select, newValue);
            }
        }
    }

    return createRecord(physicalFields);
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
    bool hasFileposition = getBoolAttribute(tableExpr, filepositionAtom, true);
    IHqlExpression * diskRecord = createPhysicalIndexRecord(mapper, tableExpr, record, hasFileposition, true);

    unsigned payload = numPayloadFields(tableExpr);
    assertex(payload || !hasFileposition);
    unsigned newPayload = hasFileposition ? payload-1 : payload;
    HqlExprArray args;
    unwindChildren(args, tableExpr);
    args.replace(*diskRecord, 1);
    removeAttribute(args, _payload_Atom);
    args.append(*createAttribute(_payload_Atom, getSizetConstant(newPayload)));
    args.append(*createAttribute(_original_Atom, LINK(tableExpr)));

    //remove the preload attribute and replace with correct value
    IHqlExpression * newDataset = createDataset(tableExpr->getOperator(), args);

    HqlExprArray assigns;
    createPhysicalLogicalAssigns(assigns, newDataset, tableExpr, hasFileposition);
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

    OwnedHqlExpr newProject = convertToPhysicalIndex(tableExpr);
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

    void checkKeyable(MonitorExtractor & monitors)
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

                MemberFunction func(translator, ctx, "virtual void mapOutputToInput(ARowBuilder & crSelf, const void * _projected, unsigned numFields)");
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
            MemberFunction func(translator, ctx, "virtual void mapOutputToInput(void * _original, const void * _projected, unsigned numFields)");
            translator.buildStmt(func.ctx, fail);
        }
    }

public:
    SteppingFieldSelection outputStepping;
    SteppingFieldSelection rawSteppingProject;
    SteppingFieldSelection rawStepping;
};

//---------------------------------------------------------------------------

class SourceBuilder
{
public:
    SourceBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : tableExpr(_tableExpr), translator(_translator)
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
        isVirtualLogicalFilenameUsed = false;
        requiresOrderedMerge = false;
        rootSelfRow = NULL;
        activityKind = TAKnone;
    }
    virtual ~SourceBuilder() {}

    virtual void buildMembers(IHqlExpression * expr) = 0;
    virtual void buildTransformFpos(BuildCtx & transformCtx) = 0;
    virtual void extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds);
    virtual void buildTransformElements(BuildCtx & ctx, IHqlExpression * expr, bool ignoreFilters);
    virtual void buildTransform(IHqlExpression * expr) = 0;
    virtual void analyse(IHqlExpression * expr);

    void buildFilenameMember();
    void appendFilter(SharedHqlExpr & unkeyedFilter, IHqlExpression * expr);
    void buildKeyedLimitHelper(IHqlExpression * expr);
    void buildLimits(BuildCtx & classctx, IHqlExpression * expr, unique_id_t id);
    void buildReadMembers( IHqlExpression * expr);
    void buildSteppingMeta(IHqlExpression * expr, MonitorExtractor * monitors);
    void buildTransformBody(BuildCtx & transformCtx, IHqlExpression * expr, bool returnSize, bool ignoreFilters, bool bindInputRow);
    void checkDependencies(BuildCtx & ctx, IHqlExpression * expr);
    bool containsStepping(IHqlExpression * expr);
    ABoundActivity * buildActivity(BuildCtx & ctx, IHqlExpression * expr, ThorActivityKind activityKind, const char *kind, ABoundActivity *input);
    void gatherVirtualFields(bool ignoreVirtuals, bool ensureSerialized);
    bool recordHasVirtuals()                                { return fieldInfo.hasVirtuals(); }
    bool recordHasVirtualsOrDeserialize()                               { return fieldInfo.hasVirtualsOrDeserialize(); }
    bool isSourceInvariant(IHqlExpression * dataset, IHqlExpression * expr);
    bool hasExistChoosenLimit() { return (choosenValue && getIntValue(choosenValue) == 1); }


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
    void buildGroupingMonitors(IHqlExpression * expr, MonitorExtractor & monitors);
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
    HqlExprAttr     lastTransformer;
    HqlExprAttr     alreadyDoneFlag;
    HqlExprArray    originalFilters;
    HqlExprArray    mappedFilters;
    HqlExprArray    removedFilters;
    HqlExprAttr     failedFilterValue;
    HqlExprAttr     compoundCountVar;
    HqlExprAttr     physicalRecord;
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
    bool            isVirtualLogicalFilenameUsed;
    bool            requiresOrderedMerge;
protected:
    HqlCppTranslator & translator;
};


struct HQLCPP_API HqlFilePositionDefinedValue : public HqlSimpleDefinedValue
{
public:
    HqlFilePositionDefinedValue(SourceBuilder & _builder, IHqlExpression * _original, IHqlExpression * _expr) 
    : HqlSimpleDefinedValue(_original, _expr), builder(_builder)
    { }

    virtual IHqlExpression * queryExpr() const              
    { 
        builder.isVirtualLogicalFilenameUsed = true;
        return HqlSimpleDefinedValue::queryExpr();
    }

public:
    SourceBuilder & builder;
};


bool SourceBuilder::isSourceInvariant(IHqlExpression * dataset, IHqlExpression * expr)
{
    if (containsAssertKeyed(expr))
        return false;
    if (!containsActiveDataset(expr))
        return true;

    HqlExprCopyArray inScope;
    expr->gatherTablesUsed(NULL, &inScope);
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
        if (fieldInfo.hasVirtuals())
        {
            assertex(fieldInfo.simpleVirtualsAtEnd);
            needToCallTransform = true;
            needDefaultTransform = false;
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
            OwnedHqlExpr unkeyedFilter;
            HqlExprArray conds;
            unwindFilterConditions(conds, expr);
            extractMonitors(expr, unkeyedFilter, conds);

            if (unkeyedFilter)
            {
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
        needToCallTransform = true;
        needDefaultTransform = false;
        lastTransformer.set(expr);
        break;
    case no_newusertable:
        needToCallTransform = true;
        needDefaultTransform = false;
        lastTransformer.set(expr);
        break;
    case no_aggregate:
        {
            needToCallTransform = true;
            needDefaultTransform = false;
            aggregation = true;
            lastTransformer.set(expr);
            break;
        }
    case no_newaggregate:
        {
            needToCallTransform = true;
            needDefaultTransform = false;
            aggregation = true;
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
                unkeyedFilter.setown(createValue(no_and, unkeyedFilter.getClear(), LINK(expr)));
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
        ctx.associateExpr(fpos, fposExpr);
    }

    if (lfpos)
    {
        Owned<IHqlExpression> fposExpr = createFileposCall(translator, getLocalFilePositionId, provider, rowname);
        ctx.associateExpr(lfpos, fposExpr);
    }

    if (logicalFilenameMarker)
    {
        Owned<IHqlExpression> nameExpr = createFileposCall(translator, queryLogicalFilenameId, provider, rowname);
        ctx.associateOwn(*new HqlFilePositionDefinedValue(*this, logicalFilenameMarker, nameExpr));
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
        ctx.associateExpr(selectorFpos, match->queryExpr());
    }
}


void SourceBuilder::rebindFilepositons(BuildCtx & ctx, IHqlExpression * dataset, node_operator side, IHqlExpression * selSeq)
{
    bool savedIsVirtualLogicalFilenameUsed = isVirtualLogicalFilenameUsed;  // don't allow the rebinding to set the flag.
    rebindFilepositons(ctx, dataset, side, selSeq, true);
    rebindFilepositons(ctx, dataset, side, selSeq, false);
    OwnedHqlExpr searchLogicalFilename = getFileLogicalName(dataset);
    HqlExprAssociation * match = ctx.queryMatchExpr(searchLogicalFilename);
    if (match)
    {
        OwnedHqlExpr selector = createSelector(side, dataset, selSeq);
        OwnedHqlExpr selectorLogicalFilename = getFileLogicalName(dataset);
        ctx.associateOwn(*new HqlFilePositionDefinedValue(*this, selectorLogicalFilename, match->queryExpr()));
    }
    isVirtualLogicalFilenameUsed = savedIsVirtualLogicalFilenameUsed;
}



void SourceBuilder::buildFilenameMember()
{
    //---- virtual const char * getFileName() { return "x.d00"; } ----
    translator.buildFilenameFunction(*instance, instance->startctx, "getFileName", nameExpr, translator.hasDynamicFilename(tableExpr));
}

void SourceBuilder::buildReadMembers(IHqlExpression * expr)
{
    buildFilenameMember();
    
    //---- virtual bool needTransform() { return <bool>; } ----
    if (needToCallTransform || transformCanFilter)
        translator.doBuildBoolFunction(instance->classctx, "needTransform", true);

    //---- virtual bool needTransform() { return <bool>; } ----
    if (transformCanFilter)
        translator.doBuildBoolFunction(instance->classctx, "transformMayFilter", true);
}

void SourceBuilder::buildLimits(BuildCtx & classctx, IHqlExpression * expr, unique_id_t id)
{
    if (limitExpr)
        translator.buildLimitHelpers(classctx, limitExpr, nameExpr, id);

    if (choosenValue)
    {
        MemberFunction func(translator, classctx, "virtual unsigned __int64 getChooseNLimit()");
        OwnedHqlExpr newLimit = ensurePositiveOrZeroInt64(choosenValue);
        translator.buildReturn(func.ctx, newLimit);
    }
}

void SourceBuilder::buildTransformBody(BuildCtx & transformCtx, IHqlExpression * expr, bool returnSize, bool ignoreFilters, bool bindInputRow)
{
    if (tableExpr && bindInputRow)
    {
        IHqlExpression * mode = (tableExpr->getOperator() == no_table) ? tableExpr->queryChild(2) : NULL;
        if (mode && mode->getOperator() == no_csv)
        {
            translator.bindCsvTableCursor(transformCtx, tableExpr, "Src", no_none, NULL, true, queryCsvEncoding(mode));
        }
        else
        {
            //NOTE: The source is not link counted - it comes from a prefetched row, and does not include any virtual file position field.
            OwnedHqlExpr boundSrc = createVariable("left", makeRowReferenceType(physicalRecord));
            IHqlExpression * accessor = NULL;
            transformCtx.associateOwn(*new BoundRow(tableExpr->queryNormalizedSelector(), boundSrc, accessor, translator.queryRecordOffsetMap(physicalRecord, (accessor != NULL)), no_none, NULL));
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
    if ((expr != instance->dataset) && !ignoreFilters)
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
        if (fieldInfo.hasVirtuals())
        {
            IHqlExpression * record = expr->queryRecord();
            assertex(fieldInfo.simpleVirtualsAtEnd);
            assertex(!recordRequiresSerialization(record, diskAtom));
            CHqlBoundExpr bound;
            StringBuffer s;
            translator.getRecordSize(ctx, expr, bound);

            size32_t virtualSize = 0;
            ForEachChild(idx, record)
            {
                IHqlExpression * field = record->queryChild(idx);
                IHqlExpression * virtualAttr = field->queryAttribute(virtualAtom);
                if (virtualAttr)
                {
                    size32_t fieldSize = field->queryType()->getSize();
                    assertex(fieldSize != UNKNOWN_LENGTH);
                    virtualSize += fieldSize;
                }
            }

            if (!isFixedSizeRecord(record))
            {
                OwnedHqlExpr ensureSize = adjustValue(bound.expr, virtualSize);
                s.clear().append("crSelf.ensureCapacity(");
                translator.generateExprCpp(s, ensureSize).append(", NULL);");
                ctx.addQuoted(s);
            }

            s.clear().append("memcpy(crSelf.row(), left, ");
            translator.generateExprCpp(s, bound.expr).append(");");
            ctx.addQuoted(s);

            ForEachChild(idx2, record)
            {
                IHqlExpression * field = record->queryChild(idx2);
                IHqlExpression * virtualAttr = field->queryAttribute(virtualAtom);
                if (virtualAttr)
                {
                    IHqlExpression * self = rootSelfRow->querySelector();
                    OwnedHqlExpr target = createSelectExpr(LINK(self), LINK(field));
                    OwnedHqlExpr value = getVirtualReplacement(field, virtualAttr->queryChild(0), expr);
                    translator.buildAssign(ctx, target, value);
                }
            }
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
            if (!ignoreFilters)
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
            IHqlExpression * selSeq = querySelSeq(expr);
            OwnedHqlExpr leftSelect = createSelector(no_left, dataset, querySelSeq(expr));

            //Following is a bit nasty....
            //Converting the no_hqlproject to a no_newusertable means that some of the expressions
            //are commoned up with expressions calculated by a previous filter, reducing the code.
            //However, it isn't valid if transform contains an instance of newSelect - 
            //e.g. project(i(x), transform(exists(i....))) - see jholt25.xhql
            //And unfortunately it fails silently.
            //So we use queryReplaceSelector which fails if an ambiguity is introduced by the replacement
            OwnedHqlExpr newSelect = ensureActiveRow(dataset->queryNormalizedSelector());
            OwnedHqlExpr transform = queryNewReplaceSelector(expr->queryChild(1), leftSelect, newSelect);

            BuildCtx subctx(ctx);       // buildTargetCursor adds group if necessary
            Linked<BoundRow> tempRow;
            Linked<BoundRow> rowBuilder;
            buildTargetCursor(tempRow, rowBuilder, subctx, expr);
            if (!transform)
            {
                //The replace introduced an ambiguity => need to use the unmapped expression.
                BoundRow * prevCursor = translator.resolveSelectorDataset(ctx, dataset->queryNormalizedSelector());
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
            IHqlExpression * transform = expr->queryChild(2);
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
            BoundRow * leftCursor;
            switch (getDatasetKind(tableExpr))
            {
            case no_csv:
                leftCursor = translator.bindCsvTableCursor(subctx, dataset, "Left", no_left, querySelSeq(expr), true, queryCsvTableEncoding(tableExpr));
                break;
            case no_xml:
            case no_json:
                leftCursor = translator.bindXmlTableCursor(subctx, dataset, "xmlLeft", no_left, querySelSeq(expr), true);
                break;
            default:
                leftCursor = translator.bindTableCursor(subctx, dataset, "left", no_left, querySelSeq(expr));
                break;
            }

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
        firstFunc.start("virtual bool first()");
    }
    else
    {
        assertex(root);

        firstFunc.start("virtual bool first(const void * _src)");
        bool isProjected = (root->queryNormalizedSelector() != tableExpr->queryNormalizedSelector());
        if (!isProjected)
        {
            iterctx.addQuotedLiteral("byte * src;");
            associateFilePositions(iterctx, "activity->fpp", "activity->src");      // in case no projection in first()
            translator.associateBlobHelper(iterctx, tableExpr, "fpp");
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
            nextctx.addQuotedFunction("virtual bool next()");
            nextctx.addQuoted(s.clear().append("return (").append(cursorName).append(" = (byte *)").append(iterName).append(".next()) != 0;"));
        }
    }
    else
    {
        iterBuilder.buildCompoundIterator(instance->onlyEvalOnceContext(), iterators, cursors);

        firstFunc.ctx.addQuotedLiteral("return iter.first();");

        {
            BuildCtx nextctx(instance->startctx);
            nextctx.addQuotedFunction("virtual bool next()");
            nextctx.addQuotedLiteral("return iter.next();");
        }
    }

    ForEachItemIn(i, cursors)
        ctx.associate(cursors.item(i));
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
    if (fieldUsage && !fieldUsage->seenAll())
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
        if (fieldUsage->seenAll())
            return;

        IHqlExpression * ds = expr->queryChild(0);
        switch (expr->getOperator())
        {
        case no_fetch:
            {
                assertex(ds->queryBody() == tableExpr->queryBody());
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr left = createSelector(no_left, ds, selSeq);
                ::gatherFieldUsage(fieldUsage, expr, left);
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
    case TAKcsvread:
    case TAKxmlread:
    case TAKjsonread:
    case TAKdiskcount:
    case TAKdiskexists:
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
    useImplementationClass = translator.queryOptions().minimizeActivityClasses && translator.targetRoxie() && isSpill;

    Owned<ActivityInstance> localInstance = new ActivityInstance(translator, ctx, activityKind, expr, kind);
    if (useImplementationClass)
        localInstance->setImplementationClass(newMemorySpillReadArgId);

    if ((activityKind >= TAKdiskread) && (activityKind <= TAKdiskgroupaggregate))
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
            graphLabel.append("\nSpill");
        else
        {
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
                graphLabel.newline().append("'...").append(coloncolon);
            }
            else
                graphLabel.newline().append(filename);
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
        buildFlagsMember(expr);

        if (tableExpr)
        {
            if (fieldInfo.hasVirtualsOrDeserialize())
            {
                OwnedHqlExpr diskTable = createDataset(no_anon, LINK(physicalRecord));
                translator.buildMetaMember(instance->classctx, diskTable, false, "queryDiskRecordSize");
            }
            else
                translator.buildMetaMember(instance->classctx, tableExpr, isGrouped(tableExpr), "queryDiskRecordSize");

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
        translator.addFilenameConstructorParameter(*instance, "getFileName", nameExpr);
    }

    if (steppedExpr)
        buildSteppedHelpers();

    if (translator.targetRoxie())
        instance->addAttributeBool("_isSpill", isSpill);
    else if (needToCallTransform || transformCanFilter)
        instance->addAttributeBool("_isTransformSpill", isSpill);
    else
        instance->addAttributeBool("_isSpill", isSpill);
    if (isFiltered)
    {
        if (isKnownLikelihood(filterLikelihood))
        {
            StringBuffer text;
            filterLikelihood *= 100;
            text.setf("%3.2f%%", filterLikelihood);
            instance->addAttribute("matchLikelihood", text);
        }
    }
    IHqlExpression * spillReason = tableExpr ? queryAttributeChild(tableExpr, _spillReason_Atom, 0) : NULL;

    if (spillReason && !translator.queryOptions().obfuscateOutput)
    {
        StringBuffer text;
        getStringValue(text, spillReason);
        instance->addAttribute("spillReason", text.str());
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
            MemberFunction func(translator, instance->startctx, "virtual unsigned __int64 getKeyedLimit()");
            translator.buildReturn(func.ctx, limitValue);
            if (isZero(limitValue))
                translator.WARNING(CategoryUnusual, HQLWRN_KeyedLimitIsZero);
        }

        LinkedHqlExpr fail = keyedLimitExpr->queryChild(2);
        if (!fail || fail->isAttribute())
            fail.setown(translator.createFailAction("Keyed limit exceeded", limitValue, NULL, instance->activityId));

        {
            MemberFunction func(translator, instance->startctx, "virtual void onKeyedLimitExceeded()");
            translator.buildStmt(func.ctx, fail);
        }

        IHqlExpression * transform = queryAttributeChild(keyedLimitExpr, onFailAtom, 0);
        if (transform)
        {
            MemberFunction func(translator, instance->startctx, "virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & crSelf)");
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
    instance->classctx.addQuotedLiteral("virtual IHash * queryHash() { return &hash; }");

    BuildCtx classctx(instance->nestedctx);
    IHqlStmt * classStmt = translator.beginNestedClass(classctx, "hash", "IHash", NULL, extractBuilder);

    {
        MemberFunction func(translator, classctx, "virtual unsigned hash(const void * _self)");
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
    instance->classctx.addQuotedLiteral("virtual ICompare * queryCompareRowElement() { return &compareRowElement; }");

    BuildCtx classctx(instance->nestedctx);
    IHqlStmt * classStmt = translator.beginNestedClass(classctx, "compareRowElement", "ICompare", NULL, extractBuilder);

    {
        MemberFunction func(translator, classctx, "virtual int docompare(const void * _left, const void * _right) const");
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

    proto.append("virtual size32_t ").append(name).append("(ARowBuilder & crSelf, const void * _src)");
    MemberFunction validateFunc(translator, instance->nestedctx, proto, MFdynamicproto);

    translator.ensureRowAllocated(validateFunc.ctx, "crSelf");
    assignLocalExtract(validateFunc.ctx, extractBuilder, dataset, "_src");

    BoundRow * selfRow = translator.bindSelf(validateFunc.ctx, resultDataset, "crSelf");

    if (extractBuilder)
    {
        MemberEvalContext * evalContext = new MemberEvalContext(translator, extractBuilder, translator.queryEvalContext(validateFunc.ctx), validateFunc.ctx);
        validateFunc.ctx.associateOwn(*evalContext);
        evalContext->initContext();
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
        rowctx.addQuotedFunction("virtual void processRow(const void * src, IHThorGroupAggregateCallback * callback)");
        rowctx.addQuotedLiteral("doProcessRow((byte *)src, callback);");
    }

    //virtual void processRows(void * self, size32_t srcLen, const void * src) = 0;
    {
        MemberFunction func(translator, instance->startctx, "virtual void processRows(size32_t srcLen, const void * _left, IHThorGroupAggregateCallback * callback)");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        OwnedHqlExpr ds = createVariable("left", makeReferenceModifier(tableExpr->getType()));
        OwnedHqlExpr len = createVariable("srcLen", LINK(sizetType));
        OwnedHqlExpr fullDs = createTranslated(ds, len);

        BoundRow * curRow = translator.buildDatasetIterate(func.ctx, fullDs, false);
        s.clear().append("doProcessRow(");
        translator.generateExprCpp(s, curRow->queryBound());
        s.append(", callback);");
        func.ctx.addQuoted(s);
    }
}


void SourceBuilder::buildGroupingMonitors(IHqlExpression * expr, MonitorExtractor & monitors)
{
    IHqlExpression * aggregate = expr->queryChild(0);
    node_operator op = aggregate->getOperator();
    assertex(op == no_newaggregate || op == no_aggregate);

    IHqlExpression * dataset = aggregate->queryChild(0);
    IHqlExpression * grouping = aggregate->queryChild(3);

    //virtual void createGroupSegmentMonitors(IIndexReadContext *ctx) = 0;
    MemberFunction func(translator, instance->startctx, "virtual bool createGroupSegmentMonitors(IIndexReadContext * irc)");

    monitorsForGrouping = true;
    if (op == no_newaggregate)
        translator.bindTableCursor(func.ctx, dataset, "_dummy");
    else
        translator.bindTableCursor(func.ctx, dataset, "_dummy", no_left, querySelSeq(aggregate));
    unsigned maxOffset = 0;
    ForEachChild(i, grouping)
    {
        unsigned nextOffset = 0;
        if (!monitors.createGroupingMonitor(func.ctx, "irc", grouping->queryChild(i), nextOffset))
        {
            monitorsForGrouping = false;
            func.setIncluded(false);
            break;
        }
        if (maxOffset < nextOffset)
            maxOffset = nextOffset;
    }
    if (monitorsForGrouping)
        func.ctx.addReturn(queryBoolExpr(true));

    if (monitorsForGrouping)
        translator.doBuildUnsignedFunction(instance->classctx, "getGroupSegmentMonitorsSize", maxOffset);
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
                rowctx.addQuotedFunction("virtual size32_t numValid(const void * src)");
                rowctx.addQuotedLiteral("return valid((byte *)src);");
            }

            //virtual size32_t numValid(size32_t srcLen, const void * src);
            {
                MemberFunction func(translator, instance->startctx, "virtual size32_t numValid(size32_t srcLen, const void * _src)");
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
            MemberFunction func(translator, instance->startctx, "virtual size32_t numValid(size32_t srcLen, const void * _src)");
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
    DiskReadBuilderBase(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : SourceBuilder(_translator, _tableExpr, _nameExpr), monitors(_tableExpr, _translator, 0, true)
    {
        fpos.setown(getFilepos(tableExpr, false));
        lfpos.setown(getFilepos(tableExpr, true));
        logicalFilenameMarker.setown(getFileLogicalName(tableExpr));
        mode = tableExpr->queryChild(2);
        modeOp = mode->getOperator();
        includeFormatCrc = (modeOp != no_csv);
    }

    virtual void buildMembers(IHqlExpression * expr);
    virtual void buildTransformFpos(BuildCtx & transformCtx);
    virtual void extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds);

protected:
    virtual void buildFlagsMember(IHqlExpression * expr);

protected:
    MonitorExtractor monitors;
    OwnedHqlExpr globalGuard;
    IHqlExpression * mode;
    node_operator modeOp;
    bool includeFormatCrc;
};


void DiskReadBuilderBase::buildMembers(IHqlExpression * expr)
{
    StringBuffer s;

    //Process any KEYED() information
    if (monitors.isFiltered())
    {
        MemberFunction func(translator, instance->startctx, "virtual void createSegmentMonitors(IIndexReadContext *irc)");
        monitors.buildSegments(func.ctx, "irc", true);
    }
    instance->addAttributeBool("_isKeyed", monitors.isFiltered());

    //---- virtual unsigned getFlags()
    instance->addAttributeBool("preload", isPreloaded);

    bool matched = translator.registerGlobalUsage(tableExpr->queryChild(0));
    if (translator.getTargetClusterType() == RoxieCluster)
    {
        instance->addAttributeBool("_isOpt", tableExpr->hasAttribute(optAtom));
        instance->addAttributeBool("_isSpillGlobal", tableExpr->hasAttribute(jobTempAtom));
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


    //---- virtual bool canMatchAny() { return <value>; } ----
    LinkedHqlExpr guard = globalGuard.get();
    extendConditionOwn(guard, no_and, LINK(monitors.queryGlobalGuard()));
    if (guard)
        translator.doBuildBoolFunction(instance->startctx, "canMatchAny", guard);

    translator.buildEncryptHelper(instance->startctx, tableExpr->queryAttribute(encryptAtom));

    //---- virtual size32_t getPreloadSize() { return <value>; } ----
    if (preloadSize)
    {
        MemberFunction func(translator, instance->classctx, "virtual size32_t getPreloadSize()");
        translator.buildReturn(func.ctx, preloadSize, sizetType);
        instance->addAttributeInt("_preloadSize", preloadSize->queryValue()->getIntValue());
    }

    if (includeFormatCrc)
    {
        //Spill files can still have virtual attributes in their physical records => remove them.
        OwnedHqlExpr noVirtualRecord = removeVirtualAttributes(physicalRecord);
        translator.buildFormatCrcFunction(instance->classctx, "getFormatCrc", noVirtualRecord, NULL, 0);
    }

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
    if (isPreloaded) flags.append("|TDRpreload");
    if (monitors.isFiltered()) flags.append("|TDRkeyed");
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
    if (translator.hasDynamicFilename(tableExpr)) flags.append("|TDXdynamicfilename");
    if (isUnfilteredCount) flags.append("|TDRunfilteredcount");
    if (isVirtualLogicalFilenameUsed) flags.append("|TDRfilenamecallback");
    if (requiresOrderedMerge) flags.append("|TDRorderedmerge");

    if (flags.length())
        translator.doBuildUnsignedFunction(instance->classctx, "getFlags", flags.str()+1);
}


void DiskReadBuilderBase::buildTransformFpos(BuildCtx & transformCtx)
{
    if (modeOp == no_csv)
        associateFilePositions(transformCtx, "fpp", "dataSrc[0]");
    else
        associateFilePositions(transformCtx, "fpp", "left");
}


void DiskReadBuilderBase::extractMonitors(IHqlExpression * ds, SharedHqlExpr & unkeyedFilter, HqlExprArray & conds)
{
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
                appendFilter(unkeyedFilter, filter);
                break;
            }
        }
    }
}

//---------------------------------------------------------------------------

class DiskReadBuilder : public DiskReadBuilderBase
{
public:
    DiskReadBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr)
    { 
    }

    virtual void buildTransform(IHqlExpression * expr);
    virtual void buildMembers(IHqlExpression * expr);
};


void DiskReadBuilder::buildMembers(IHqlExpression * expr)
{
    buildReadMembers(expr);
    DiskReadBuilderBase::buildMembers(expr);

    //---- virtual const char * getPipeProgram() { return "grep"; } ----
    if (modeOp==no_pipe)
    {
        if (expr->hasAttribute(_disallowed_Atom))
            throwError(HQLERR_PipeNotAllowed);

        {
            MemberFunction func(translator, instance->startctx, "virtual const char * getPipeProgram()");
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
                s.append("virtual ICsvToRowTransformer * queryCsvTransformer() { return &").append(csvInstanceName).append("; }");
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


void DiskReadBuilder::buildTransform(IHqlExpression * expr)
{
    if (modeOp == no_pipe)
    {
        assertex(!(needToCallTransform || transformCanFilter));
        return;
    }

    if (modeOp == no_csv)
    {
        translator.buildCsvParameters(instance->nestedctx, mode, NULL, true);

        {
            MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenSrc, const char * * dataSrc)");
            translator.ensureRowAllocated(func.ctx, "crSelf");

            //associateVirtualCallbacks(*this, func.ctx, tableExpr);

            buildTransformBody(func.ctx, expr, true, false, true);
        }

        rootSelfRow = NULL;

        unsigned maxColumns = countTotalFields(tableExpr->queryRecord(), false);
        translator.doBuildUnsignedFunction(instance->classctx, "getMaxColumns", maxColumns);
        return;
    }

    MemberFunction func(translator, instance->startctx);
    if (instance->kind == TAKdiskread)
        func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");
    else
        func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, IFilePositionProvider * fpp)");
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
    info.gatherVirtualFields(tableExpr->hasAttribute(_noVirtual_Atom) || isPiped, needToSerializeRecord(modeOp));

    unsigned optFlags = (options.foldOptimized ? HOOfold : 0);
    if (!info.fieldInfo.simpleVirtualsAtEnd || info.fieldInfo.requiresDeserialize ||
        (info.recordHasVirtuals() && (modeOp == no_csv || !isSimpleSource(expr))))
    {
        OwnedHqlExpr transformed = buildTableWithoutVirtuals(info.fieldInfo, expr);
        //Need to wrap a possible no_usertable, otherwise the localisation can go wrong.
        if (expr->getOperator() == no_table)
            transformed.setown(createDataset(no_compound_diskread, LINK(transformed)));
        OwnedHqlExpr optimized = optimizeHqlExpression(queryErrorProcessor(), transformed, optFlags);
        traceExpression("after disk optimize", optimized);
        return doBuildActivityDiskRead(ctx, optimized);
    }

    OwnedHqlExpr optimized;
    if (expr->getOperator() == no_table)
        optimized.set(expr);
    else
        optimized.setown(optimizeHqlExpression(queryErrorProcessor(), expr, optFlags));

    if (optimized != expr)
        return buildActivity(ctx, optimized, false);

    if (optimized->getOperator() != no_compound_diskread)
        optimized.setown(createDataset(no_compound_diskread, LINK(optimized)));

    if (isPiped)
        return info.buildActivity(ctx, expr, TAKpiperead, "PipeRead", NULL);
    ensureDiskAccessAllowed(tableExpr);
    if (modeOp == no_csv)
        return info.buildActivity(ctx, expr, TAKcsvread, "CsvRead", NULL);
    return info.buildActivity(ctx, expr, TAKdiskread, "DiskRead", NULL);
}

//---------------------------------------------------------------------------

class DiskNormalizeBuilder : public DiskReadBuilderBase
{
public:
    DiskNormalizeBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr)
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

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf)");
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
    info.gatherVirtualFields(tableExpr->hasAttribute(_noVirtual_Atom), needToSerializeRecord(mode));

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
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr)
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
        rowctx.addQuotedFunction("virtual void processRow(ARowBuilder & crSelf, const void * src)");
        rowctx.addQuotedLiteral("doProcessRow(crSelf, (byte *)src);");
    }

    //virtual void processRows(void * self, size32_t srcLen, const void * src) = 0;
    {
        MemberFunction func(translator, instance->startctx, "virtual void processRows(ARowBuilder & crSelf, size32_t srcLen, const void * _left)");
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
    MemberFunction func(translator, instance->startctx, "void doProcessRow(ARowBuilder & crSelf, byte * left)");
    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, false, false, true);
}


//---------------------------------------------------------------------------

class DiskCountBuilder : public DiskReadBuilderBase
{
public:
    DiskCountBuilder(HqlCppTranslator & _translator, IHqlExpression *_tableExpr, IHqlExpression *_nameExpr, node_operator _aggOp)
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr)
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
        MemberFunction func(translator, instance->startctx, "size32_t valid(byte * _left)");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
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
    info.gatherVirtualFields(tableExpr->hasAttribute(_noVirtual_Atom), needToSerializeRecord(mode));

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
        info.gatherVirtualFields(tableExpr->hasAttribute(_noVirtual_Atom), needToSerializeRecord(mode));

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
        : DiskReadBuilderBase(_translator, _tableExpr, _nameExpr)
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
    MemberFunction func(translator, instance->startctx, "void doProcessRow(byte * left, IHThorGroupAggregateCallback * callback)");
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
    info.gatherVirtualFields(tableExpr->hasAttribute(_noVirtual_Atom), needToSerializeRecord(mode));

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
        : SourceBuilder(_translator, _tableExpr, _nameExpr)
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

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf)");
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
    MemberFunction func(translator, instance->startctx, "virtual void processRows(ARowBuilder & crSelf)");
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
    instance->startctx.addQuotedLiteral("virtual size32_t mergeAggregate(ARowBuilder & crSelf, const void * src) { return 0; }");
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

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf)");
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

//-----------------------------------------------------------------------------------------------
//-- Index processing
//-----------------------------------------------------------------------------------------------

/*

  Note on generating segment monitors for conditions (ty)x = Y

  1) casting from Tx to Ty loses information.  E.g., (int1)string2field = int1value.
     In this case it is almost impossible to generate a segment monitor because we would need to work out all
     the possibly values for x which could generate the value y.
     The only exception is an inequality (and no_notin), which we can use to remove some of the candidates.  The test will always need
     duplicating since we cannot remove all the expected values.

  2) Casting from Ty to Tx loses information.  E.g., (string)string2field = stringvalue
     In this case we can process the filter without prefiltering by testing whether isExact: (Ty)(Tx)Y == Y, and following the following rules:
     a) no_eq.  If isExact, add value else nothing.
     b) no_ne.  If isExtact, remove value else nothing.
     c) no_gt.  Always add > value
     d) no_ge.  If isExact add >= value else add > value
     e) no_lt.  If isExact add < value else add <= value
     f) no_le.  Always add <= value

  3) Note casts must be present on both sides to indicate exactly what type the comparison will be done as.  This includes (string)which would
     normally be missing if the field was of type string<n>.

*/

static node_operator getModifiedOp(node_operator op, bool duplicate)
{
    if (!duplicate)
        return op;

    switch (op)
    {
    case no_eq:
    case no_le:
    case no_ge:
    case no_in:
        return op;
        //err on the side of caution for the segment monitors - 
        //the test later which check it more accurately.
    case no_gt:
        return no_ge;
    case no_lt:
        return no_le;
    case no_ne:
    case no_notin:
        return no_none;
    default:
        UNIMPLEMENTED;
    }
}


void KeyFailureInfo::merge(const KeyFailureInfo & other)
{
    if (code < other.code)
        set(other.code, other.field);
}

void KeyFailureInfo::reportError(HqlCppTranslator & translator, IHqlExpression * condition)
{
    StringBuffer ecl;
    getExprECL(condition, ecl);
    switch (code)
    {
    case KFRunknown:
        translator.throwError1(HQLERR_KeyedJoinTooComplex, ecl.str());
    case KFRnokey:
        translator.throwError1(HQLERR_KeyAccessNoKeyField, ecl.str());
    case KFRtoocomplex:
        translator.throwError1(HQLERR_KeyedJoinTooComplex, ecl.str());
    case KFRcast:
        translator.throwError2(HQLERR_KeyAccessNeedCast, ecl.str(), str(field->queryName()));
    case KFRor:
        translator.throwError1(HQLERR_OrMultipleKeyfields, ecl.str());
    }
}


const char * BuildMonitorState::getSetName()
{
    if (!setNames.isItem(numActiveSets))
    {
        StringBuffer name;
        getUniqueId(name.append("set"));
        setNames.append(*new StringAttrItem(name.str()));

        StringBuffer s;
        funcctx.setNextConstructor();
        funcctx.addQuoted(s.append("Owned<IStringSet> ").append(name).append(";"));
    }

    return setNames.item(numActiveSets++).text;
}


void BuildMonitorState::popSetName()
{
    numActiveSets--;
}


IHqlExpression * KeyConditionInfo::createConjunction()
{
    LinkedHqlExpr result = preFilter;
    ForEachItemIn(i, conditions)
        extendAndCondition(result, conditions.item(i).expr);
    extendAndCondition(result, postFilter);
    return result.getClear();
}

MonitorExtractor::MonitorExtractor(IHqlExpression * _tableExpr, HqlCppTranslator & _translator, int _numKeyableFields, bool _allowTranslatedConds) : translator(_translator) 
{ 
    tableExpr = _tableExpr;
    allowTranslatedConds = _allowTranslatedConds;

    if (_numKeyableFields <= 0)
    {
        //-ve number means remove a certain number of fields from the record
        IHqlExpression * record = tableExpr->queryRecord();
        numKeyableFields = 0;
        ForEachChild(i, record)
            if (!record->queryChild(i)->isAttribute())
                numKeyableFields++;
        numKeyableFields += _numKeyableFields;          // remove payload fields.
    }
    else
        numKeyableFields = (unsigned)_numKeyableFields;

    onlyHozedCompares = !allowTranslatedConds;

    expandKeyableFields();
    cleanlyKeyedExplicitly = false;
    keyedExplicitly = false;
    allowDynamicFormatChange = !tableExpr->hasAttribute(fixedAtom);
}

void MonitorExtractor::callAddAll(BuildCtx & ctx, IHqlExpression * targetVar)
{
    HqlExprArray args;
    args.append(*LINK(targetVar));
    translator.callProcedure(ctx, addAllId, args);
}

static IHqlExpression * createExpandedRecord(IHqlExpression * expr);

static ITypeInfo * getExpandedFieldType(ITypeInfo * type, IHqlExpression * expr)
{
    Linked<ITypeInfo> expandedType = type;
    if (type->getSize() == UNKNOWN_LENGTH)
        expandedType.clear();
    switch (type->getTypeCode())
    {
    case type_packedint:
        expandedType.setown(makeIntType(type->queryPromotedType()->getSize(), type->isSigned()));
        break;
    case type_bitfield:
        expandedType.set(type->queryPromotedType());
        break;
    case type_varstring:
    case type_varunicode:
#if 0
        if (type->getSize() != UNKNOWN_LENGTH)
        {
            unsigned len = type->getStringLen();
            switch (type->getTypeCode())
            {
            case type_varstring:
                expandedType.setown(makeStringType(len, LINK(type->queryCharset()), LINK(type->queryCollation())));
                break;
            case type_varunicode:
                expandedType.setown(makeUnicodeType(len, type->queryLocale()));
                break;
            }
            break;
        }
#endif              //fall through
    case type_data:
    case type_qstring:
    case type_string:
    case type_unicode:
    case type_utf8:
        if (type->getSize() == UNKNOWN_LENGTH)
        {
            unsigned maxLength = UNKNOWN_LENGTH;
            IHqlExpression * maxSizeExpr = expr ? queryAttributeChild(expr, maxSizeAtom, 0) : NULL;
            if (maxSizeExpr)
            {
                unsigned maxSize = (unsigned)maxSizeExpr->queryValue()->getIntValue();
                switch (type->getTypeCode())
                {
                case type_data:
                case type_string:
                    maxLength = maxSize - sizeof(size32_t);
                    break;
                case type_qstring:
                    maxLength = rtlQStrLength(maxSize - sizeof(size32_t));
                    break;
                case type_unicode:
                    maxLength = (maxSize-sizeof(size32_t))/sizeof(UChar);
                    break;
                case type_utf8:
                    maxLength = (maxSize-sizeof(size32_t))/4;
                    break;
                case type_varstring:
                    maxLength = maxSize - 1;
                    break;
                case type_varunicode:
                    maxLength = (maxSize/sizeof(UChar)) - 1;
                    break;
                }
            }
            else
            {
                IHqlExpression * maxLengthExpr = expr ? queryAttributeChild(expr, maxLengthAtom, 0) : NULL;
                if (maxLengthExpr)
                    maxLength = (unsigned)maxLengthExpr->queryValue()->getIntValue();
            }

            if (maxLength != UNKNOWN_LENGTH)
            {
                switch (type->getTypeCode())
                {
                case type_data:
                    expandedType.setown(makeDataType(maxLength));
                    break;
                case type_qstring:
                    expandedType.setown(makeQStringType(maxLength));
                    break;
                case type_string:
                    expandedType.setown(makeStringType(maxLength, LINK(type->queryCharset()), LINK(type->queryCollation())));
                    break;
                case type_unicode:
                    expandedType.setown(makeUnicodeType(maxLength, type->queryLocale()));
                    break;
                case type_utf8:
                    expandedType.setown(makeUtf8Type(maxLength, type->queryLocale()));
                    break;
                case type_varstring:
                    expandedType.setown(makeVarStringType(maxLength, LINK(type->queryCharset()), LINK(type->queryCollation())));
                    break;
                case type_varunicode:
                    expandedType.setown(makeVarUnicodeType(maxLength, type->queryLocale()));
                    break;
                }
            }
        }
        else
        {
            //This could ensure the strings are ascii, but the ebcdic strings are still comparable, and the order will be more logical
            //if they remain as ebcdic.
        }
        break;
    case type_table:
    case type_groupedtable:
    case type_set:
        expandedType.clear();
        break;
    case type_row:
        {
            OwnedHqlExpr newRecord = createExpandedRecord(queryRecord(type));
            if (isEmptyRecord(newRecord))
                expandedType.clear();
            else
                expandedType.setown(makeRowType(LINK(newRecord->queryRecordType())));
            break;
        }
    case type_alien:
        {
            IHqlAlienTypeInfo * alien = queryAlienType(type);
            expandedType.set(alien->queryLogicalType());
            break;
        }
    }
    return expandedType.getClear();
}

static void createExpanded(HqlExprArray & fields, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_ifblock:
        //if blocks need to generate translated segment monitors to be keyed, so don't expand them
        break;
    case no_record:
        {
            ForEachChild(i, expr)
                createExpanded(fields, expr->queryChild(i));
            break;
        }
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            Owned<ITypeInfo> expandedType = getExpandedFieldType(type, expr);
            if (expandedType)
            {
                if (expandedType == type)
                    fields.append(*LINK(expr));
                else
                {
                    HqlExprArray attrs;
                    unwindChildren(attrs, expr);
                    //MORE: Any default will now have the wrong type => remove it for the moment (ideally it would be projected)
                    removeAttribute(attrs, defaultAtom);
                    fields.append(*createField(expr->queryId(), LINK(expandedType), attrs));
                }
            }
            break;
        }
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        fields.append(*LINK(expr));
        break;
    }
}

static IHqlExpression * createExpandedRecord(IHqlExpression * expr)
{
    HqlExprArray fields;
    createExpanded(fields, expr);
    return cloneOrLink(expr, fields);
}


bool MonitorExtractor::createGroupingMonitor(BuildCtx ctx, const char * listName, IHqlExpression * expr, unsigned & maxOffset)
{
    switch (expr->getOperator())
    {
    case no_if:
        {
            IHqlExpression * cond = expr->queryChild(0);
            if (expr->queryChild(2)->isConstant() && isIndependentOfScope(cond))
            {
                BuildCtx subctx(ctx);
                translator.buildFilter(subctx, expr->queryChild(0));
                createGroupingMonitor(subctx, listName, expr->queryChild(1), maxOffset);
                return true;        // may still be keyed
            }
            break;
        }
    case no_select:
        {
            size32_t offset = 0;
            ForEachItemIn(i, keyableSelects)
            {
                IHqlExpression & cur = keyableSelects.item(i);
                size32_t curSize = cur.queryType()->getSize();
                if (curSize == UNKNOWN_LENGTH)
                    break;
                if (expr == &cur)
                {
                    //MORE: Check the type of the field is legal.
                    ctx.addQuotedF("%s->append(createWildKeySegmentMonitor(%u, %u));", listName, offset, curSize);
                    maxOffset = offset+curSize;
                    return true;
                }
                offset += curSize;
            }
            break;
        }
    case no_constant:
        return true;
    }
    ctx.addReturn(queryBoolExpr(false));
    return false;
}

void MonitorExtractor::expandKeyableFields()
{
    HqlExprArray fields;
    IHqlExpression * tableRecord = tableExpr->queryRecord();
    unsigned cnt = 0;
    ForEachChild(i, tableRecord)
    {
        if (cnt == numKeyableFields)
            break;
        IHqlExpression * cur = tableRecord->queryChild(i);
        if (!cur->isAttribute())
        {
            fields.append(*LINK(cur));
            cnt++;
        }
    }
    keyableRecord.setown(createRecord(fields));

    expandedRecord.setown(createExpandedRecord(keyableRecord));
    IHqlExpression * selector = tableExpr->queryNormalizedSelector();

    OwnedHqlExpr expandedSelector = createDataset(no_anon, LINK(expandedRecord), createUniqueId());
    firstOffsetField = NotFound;
    expandSelects(keyableRecord, expandedRecord->querySimpleScope(), selector, expandedSelector);
    if (firstOffsetField == NotFound)
        firstOffsetField = keyableSelects.ordinality();
}

void MonitorExtractor::expandSelects(IHqlExpression * expr, IHqlSimpleScope * expandedScope, IHqlExpression * keySelector, IHqlExpression * expandedSelector)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                expandSelects(expr->queryChild(i), expandedScope, keySelector, expandedSelector);
            break;
        }
    case no_ifblock:
        expandSelects(expr->queryChild(1), expandedScope, keySelector, expandedSelector);
        break;
    case no_field:
        {
            OwnedHqlExpr match = expandedScope->lookupSymbol(expr->queryId());
            if (match)
            {
                OwnedHqlExpr keySelected = createSelectExpr(LINK(keySelector), LINK(expr));
                OwnedHqlExpr expandedSelected = createSelectExpr(LINK(expandedSelector), LINK(match));
                IHqlExpression * record = expr->queryRecord();
                if (record)
                    expandSelects(record, match->queryRecord()->querySimpleScope(), keySelected, expandedSelected);
                else
                {
                    if ((expr != match) && (firstOffsetField == NotFound))
                    {
                        ITypeInfo * exprType = expr->queryType();
                        ITypeInfo * matchType = match->queryType();
                        if ((exprType->getSize() != matchType->getSize()) ||
                            (exprType->getTypeCode() == type_bitfield || exprType->getTypeCode() == type_bitfield))
                            firstOffsetField = keyableSelects.ordinality();
                    }

                    keyableSelects.append(*LINK(keySelected));
                    expandedSelects.append(*LINK(expandedSelected));
                }
            }
            else
            {
                if (firstOffsetField == NotFound)
                    firstOffsetField = keyableSelects.ordinality();
            }
            break;
        }
    }
}

void MonitorExtractor::buildKeySegmentInExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * target, IHqlExpression & thisKey, MonitorFilterKind filterKind)
{
    //Generally this slightly increases the code size, but reduces the number of
    //temporary sets which is generally more efficient.
    OwnedHqlExpr simplified = querySimplifyInExpr(&thisKey);
    if (simplified)
    {
        OwnedHqlExpr folded = foldHqlExpression(simplified);
        buildKeySegmentExpr(buildState, selectorInfo, ctx, target, *folded, filterKind);
        return;
    }

    IHqlExpression * expandedSelector = selectorInfo.expandedSelector;
    ITypeInfo * fieldType = expandedSelector->queryType();
    unsigned curSize = fieldType->getSize();
    createStringSet(ctx, target, curSize, fieldType);

    OwnedHqlExpr targetVar = createVariable(target, makeVoidType());
    IHqlExpression * lhs = thisKey.queryChild(0);
    OwnedHqlExpr values = normalizeListCasts(thisKey.queryChild(1));

    IIdAtom * func = addRangeId;
    if (thisKey.getOperator() == no_notin)
    {
        callAddAll(ctx, targetVar);
        func = killRangeId;
    }

    if (values->getOperator() != no_list)
    {
        //iterate through the set
        BuildCtx subctx(ctx);
        CHqlBoundExpr boundCurElement;
        Owned<IHqlCppSetCursor> cursor = translator.createSetSelector(ctx, values);

        bool done = false;
        CHqlBoundExpr isAll;
        cursor->buildIsAll(subctx, isAll);
        if (isAll.expr->queryValue())
        {
            if (isAll.expr->queryValue()->getBoolValue())
            {
                callAddAll(subctx, targetVar);
                done = true;
                //If ALL allowed exceptions then we would need to do more....
            }
        }
        else
        {
            IHqlStmt * stmt = subctx.addFilter(isAll.expr);
            callAddAll(subctx, targetVar);
            subctx.selectElse(stmt);
        }

        if (!done)
        {
            cursor->buildIterateLoop(subctx, boundCurElement, false);
            OwnedHqlExpr curValue = boundCurElement.getTranslatedExpr();

            OwnedHqlExpr test = createBoolExpr(no_eq, LINK(lhs), LINK(curValue));
            OwnedHqlExpr promoted = getExplicitlyPromotedCompare(test);
            OwnedHqlExpr compare, normalized;
            extractCompareInformation(subctx, promoted, compare, normalized, expandedSelector, selectorInfo.isComputed);
            if (compare)
                translator.buildFilter(subctx, compare);

            HqlExprArray args;
            args.append(*LINK(targetVar));
            unsigned srcSize = normalized->queryType()->getSize();
            if (srcSize < curSize)
            {
                OwnedHqlExpr lengthExpr = getSizetConstant(srcSize);
                OwnedHqlExpr rangeLower = getRangeLimit(fieldType, lengthExpr, normalized, -1);
                OwnedHqlExpr rangeUpper = getRangeLimit(fieldType, lengthExpr, normalized, +1);

                CHqlBoundExpr boundLower, boundUpper;
                translator.buildExpr(subctx, rangeLower, boundLower);
                translator.buildExpr(subctx, rangeUpper, boundUpper);
                args.append(*getPointer(boundLower.expr));
                args.append(*getPointer(boundUpper.expr));
            }
            else
            {
                OwnedHqlExpr address = getMonitorValueAddress(subctx, normalized);
                args.append(*LINK(address));
                args.append(*LINK(address));
            }
            translator.callProcedure(subctx, func, args);
        }
    }
    else
    {
        ForEachChild(idx2, values)
        {
            BuildCtx subctx(ctx);
            IHqlExpression * cur = values->queryChild(idx2);
            OwnedHqlExpr test = createBoolExpr(no_eq, LINK(lhs), LINK(cur));
            OwnedHqlExpr promoted = getExplicitlyPromotedCompare(test);
            OwnedHqlExpr compare, normalized;
            extractCompareInformation(subctx, promoted, compare, normalized, expandedSelector, selectorInfo.isComputed);
            if (compare)
                translator.buildFilter(subctx, compare);

            OwnedHqlExpr address = getMonitorValueAddress(subctx, normalized);
            HqlExprArray args;
            args.append(*LINK(targetVar));
            args.append(*LINK(address));
            args.append(*LINK(address));

            translator.callProcedure(subctx, func, args);
        }
    }
}

static IHqlExpression * createCompareRecast(node_operator op, IHqlExpression * value, IHqlExpression * recastValue)
{
    if (recastValue->queryValue())
        return LINK(queryBoolExpr(op == no_ne));
    return createValue(op, makeBoolType(), LINK(value), LINK(recastValue));
}


void MonitorExtractor::extractCompareInformation(BuildCtx & ctx, IHqlExpression * expr, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector, bool isComputed)
{
    extractCompareInformation(ctx, expr->queryChild(0), expr->queryChild(1), compare, normalized, expandedSelector, isComputed);
}


void MonitorExtractor::extractCompareInformation(BuildCtx & ctx, IHqlExpression * lhs, IHqlExpression * value, SharedHqlExpr & compare, SharedHqlExpr & normalized, IHqlExpression * expandedSelector, bool isComputed)
{
    LinkedHqlExpr compareValue = value->queryBody();
    OwnedHqlExpr recastValue;
    if (isComputed)
        normalized.setown(ensureExprType(compareValue, expandedSelector->queryType()));
    else
    {
        if ((lhs->getOperator() != no_select) || (lhs->queryType() != compareValue->queryType()))
        {
            OwnedHqlExpr temp  = castToFieldAndBack(lhs, compareValue);
            if (temp != compareValue)
            {
                //Force into a temporary variable since it will be used more than once, and reapply the field casting/
                compareValue.setown(translator.buildSimplifyExpr(ctx, compareValue));
                //cast to promoted type because sometimes evaluating can convert string to string<n>
                Owned<ITypeInfo> promotedType = getPromotedECLType(lhs->queryType(), compareValue->queryType());
                compareValue.setown(ensureExprType(compareValue, promotedType));
                recastValue.setown(castToFieldAndBack(lhs, compareValue));
            }
        }

        normalized.setown(invertTransforms(lhs, compareValue));
    }
    normalized.setown(foldHqlExpression(normalized));

    if (recastValue && recastValue != compareValue)
        compare.setown(createCompareRecast(no_eq, compareValue, recastValue));
}

void MonitorExtractor::createStringSet(BuildCtx & ctx, const char * target, unsigned size, ITypeInfo * type)
{
    if (onlyHozedCompares)
        ctx.addQuotedF("%s.setown(createRtlStringSet(%u));", target, size);
    else
    {
        bool isBigEndian = !type->isInteger() || !isLittleEndian(type);
        ctx.addQuotedF("%s.setown(createRtlStringSetEx(%u,%d,%d));", target, size, isBigEndian, type->isSigned());
    }
}

void MonitorExtractor::buildKeySegmentCompareExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * targetSet, IHqlExpression & thisKey)
{
    OwnedHqlExpr targetVar = createVariable(targetSet, makeVoidType());
    createStringSet(ctx, targetSet, selectorInfo.size, selectorInfo.expandedSelector->queryType());

    if (!exprReferencesDataset(&thisKey, tableExpr))
    {
        BuildCtx subctx(ctx);
        translator.buildFilter(subctx, &thisKey);
        callAddAll(subctx, targetVar);
        return;
    }

    OwnedHqlExpr compare;
    OwnedHqlExpr normalized;
    BuildCtx subctx(ctx);
    extractCompareInformation(subctx, &thisKey, compare, normalized, selectorInfo.expandedSelector, selectorInfo.isComputed);
    OwnedHqlExpr address = getMonitorValueAddress(subctx, normalized);

    HqlExprArray args;
    args.append(*LINK(targetVar));

    node_operator op = thisKey.getOperator();
    switch (op)
    {
    case no_eq:
        if (compare)
            translator.buildFilter(subctx, compare);
        args.append(*LINK(address));
        args.append(*LINK(address));
        translator.callProcedure(subctx, addRangeId, args);
        break;
    case no_ne:
        subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
        if (compare)
            translator.buildFilter(subctx, compare);
        args.append(*LINK(address));
        args.append(*LINK(address));
        translator.callProcedure(subctx, killRangeId, args);
        break;
    case no_le:
        args.append(*createValue(no_nullptr, makeVoidType()));
        args.append(*LINK(address));
        translator.callProcedure(subctx, addRangeId, args);
        break;
    case no_lt:
        // e) no_lt.  If isExact add < value else add <= value
        if (compare)
        {
            OwnedHqlExpr invCompare = getInverse(compare);
            IHqlStmt * cond = translator.buildFilterViaExpr(subctx, invCompare);
            //common this up...
            args.append(*createValue(no_nullptr, makeVoidType()));
            args.append(*LINK(address));
            translator.callProcedure(subctx, addRangeId, args);
            subctx.selectElse(cond);
            args.append(*LINK(targetVar));
        }
        subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
        args.append(*LINK(address));
        args.append(*createValue(no_nullptr, makeVoidType()));
        translator.callProcedure(subctx, killRangeId, args);
        break;
    case no_ge:
        // d) no_ge.  If isExact add >= value else add > value
        if (compare)
        {
            OwnedHqlExpr invCompare = getInverse(compare);
            IHqlStmt * cond = translator.buildFilterViaExpr(subctx, invCompare);
            //common this up...
            subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
            args.append(*createValue(no_nullptr, makeVoidType()));
            args.append(*LINK(address));
            translator.callProcedure(subctx, killRangeId, args);
            subctx.selectElse(cond);
            args.append(*LINK(targetVar));
        }
        args.append(*LINK(address));
        args.append(*createValue(no_nullptr, makeVoidType()));
        translator.callProcedure(subctx, addRangeId, args);
        break;
    case no_gt:
        subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
        args.append(*createValue(no_nullptr, makeVoidType()));
        args.append(*LINK(address));
        translator.callProcedure(subctx, killRangeId, args);
        break;
    case no_between:
    case no_notbetween:
        {
            //NB: This should only be generated for substring queries.  User betweens are converted
            //to two separate comparisons to cope with range issues.
            args.append(*LINK(address));

            CHqlBoundExpr rhs2;
            OwnedHqlExpr adjustedUpper = invertTransforms(thisKey.queryChild(0), thisKey.queryChild(2));
            OwnedHqlExpr foldedUpper = foldHqlExpression(adjustedUpper);
            OwnedHqlExpr hozedValue = getHozedKeyValue(foldedUpper);
            IIdAtom * name = hozedValue->queryId();
            if ((name != createRangeHighId) && (name != createQStrRangeHighId))
                hozedValue.setown(ensureExprType(hozedValue, selectorInfo.expandedSelector->queryType()));
            translator.buildExpr(subctx, hozedValue, rhs2);
            translator.ensureHasAddress(subctx, rhs2);
            args.append(*getPointer(rhs2.expr));
            if (op == no_between)
                translator.callProcedure(subctx, addRangeId, args);
            else
            {
                subctx.addQuoted(StringBuffer().appendf("%s->addAll();", targetSet));
                translator.callProcedure(subctx, killRangeId, args);
            }
            break;
        }
    default:
        throwUnexpectedOp(op);
    }
}


IHqlExpression * MonitorExtractor::unwindConjunction(HqlExprArray & matches, IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    expr->unwindList(matches, op);
    OwnedHqlExpr invariant;
    ForEachItemInRev(i, matches)
    {
        IHqlExpression & cur = matches.item(i);
        if (isIndexInvariant(&cur, false))
        {
            invariant.setown(extendConditionOwn(op, LINK(&cur), invariant.getClear()));
            matches.remove(i);
        }
    }
    return invariant.getClear();
}


//Note this function may change the incoming ctx if filterKind is not NoMonitorFilter
void MonitorExtractor::buildKeySegmentExpr(BuildMonitorState & buildState, KeySelectorInfo & selectorInfo, BuildCtx & ctx, const char * requiredSet, IHqlExpression & thisKey, MonitorFilterKind filterKind)
{
    const char * targetSet = requiredSet;
    StringBuffer s;
    unsigned curSize = selectorInfo.size;
    node_operator op = thisKey.getOperator();

    BuildCtx subctx(ctx);
    BuildCtx * appendCtx = &ctx;
    StringBuffer createMonitorText;

    switch (op)
    {
    case no_in: 
    case no_notin:
        {
            if (!targetSet)
                targetSet = buildState.getSetName();
            buildKeySegmentInExpr(buildState, selectorInfo, ctx, targetSet, thisKey, filterKind);
            break;
        }
    case no_if:
        {
            MonitorFilterKind childFilter = targetSet ? NoMonitorFilter : filterKind;
            IHqlStmt * ifStmt = translator.buildFilterViaExpr(subctx, thisKey.queryChild(0));
            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, *thisKey.queryChild(1), childFilter);
            subctx.selectElse(ifStmt);
            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, *thisKey.queryChild(2), childFilter);
            break;
        }
    case no_and:
        {
            HqlExprArray matches;
            OwnedHqlExpr invariant = unwindConjunction(matches, &thisKey);
            unsigned numMatches = matches.ordinality();

            if (!targetSet && numMatches > 1)
                targetSet = buildState.getSetName();

            IHqlStmt * ifStmt = NULL;
            if (invariant)
            {
                ifStmt = translator.buildFilterViaExpr(subctx, invariant);
                if (filterKind == MonitorFilterSkipEmpty)
                    ctx.set(subctx);
            }
            
            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, matches.item(0), NoMonitorFilter);
            for (unsigned i=1; i< numMatches; i++)
            {
                IHqlExpression & cur = matches.item(i);
                const char * curTarget = buildState.getSetName();
                BuildCtx childctx(subctx);
                buildKeySegmentExpr(buildState, selectorInfo, childctx, curTarget, cur, MonitorFilterSkipAll);
                childctx.addQuotedF("%s.setown(rtlIntersectSet(%s,%s));", targetSet, targetSet, curTarget);
                buildState.popSetName();
            }

            if (invariant && (filterKind != MonitorFilterSkipEmpty))
            {
                subctx.selectElse(ifStmt);
                if (targetSet)
                    createStringSet(subctx, targetSet, curSize, selectorInfo.selector->queryType());
                else
                    buildEmptyKeySegment(buildState, subctx, selectorInfo);
            }
            break;
        }
    case no_or:
        {
            HqlExprArray matches;
            OwnedHqlExpr invariant = unwindConjunction(matches, &thisKey);
            unsigned numMatches = matches.ordinality();

            if (invariant)
            {
                if (filterKind == MonitorFilterSkipAll)
                {
                    OwnedHqlExpr test = getInverse(invariant);
                    translator.buildFilter(subctx, test);
                    ctx.set(subctx);
                }
                else
                {
                    IHqlStmt * ifStmt = translator.buildFilterViaExpr(subctx, invariant);
                    if (targetSet)
                    {
                        createStringSet(subctx, targetSet, curSize, selectorInfo.selector->queryType());
                        OwnedHqlExpr targetVar = createVariable(targetSet, makeVoidType());
                        callAddAll(subctx, targetVar);
                    }
                    else
                        buildWildKeySegment(buildState, subctx, selectorInfo);
                    subctx.selectElse(ifStmt);
                }
            }
            
            appendCtx = &subctx;
            if (!targetSet && numMatches > 1)
                targetSet = buildState.getSetName();

            buildKeySegmentExpr(buildState, selectorInfo, subctx, targetSet, matches.item(0), NoMonitorFilter);
            for (unsigned i=1; i < numMatches; i++)
            {
                IHqlExpression & cur = matches.item(i);
                const char * curTarget = buildState.getSetName();
                BuildCtx childctx(subctx);
                buildKeySegmentExpr(buildState, selectorInfo, childctx, curTarget, cur, MonitorFilterSkipEmpty);
                childctx.addQuotedF("%s.setown(rtlUnionSet(%s, %s));", targetSet, targetSet, curTarget);
                buildState.popSetName();
            }
            break;
        }
    case no_eq:
        {
            if (!targetSet)
            {
                if (buildSingleKeyMonitor(createMonitorText, selectorInfo, subctx, thisKey))
                    break;
                targetSet = buildState.getSetName();
            }
            buildKeySegmentCompareExpr(buildState, selectorInfo, ctx, targetSet, thisKey);
            break;
        }
    default:
        {
            if (!targetSet)
                targetSet = buildState.getSetName();
            buildKeySegmentCompareExpr(buildState, selectorInfo, ctx, targetSet, thisKey);
            break;
        }
    }

    if (targetSet && !requiredSet)
    {
        unsigned offset = (selectorInfo.isComputed || selectorInfo.mapOffset) ? 0 : selectorInfo.offset;
        createMonitorText.appendf("createKeySegmentMonitor(%s, %s.getClear(), %u, %u)", 
                                  boolToText(selectorInfo.keyedKind != KeyedYes), targetSet, offset, selectorInfo.size);

        buildState.popSetName();
    }

    if (createMonitorText.length())
    {
        if (selectorInfo.expandNeeded || selectorInfo.isComputed)
            generateFormatWrapping(createMonitorText, selectorInfo.selector, selectorInfo.expandedSelector, buildState.curOffset);
        else if (selectorInfo.mapOffset)
            generateOffsetWrapping(createMonitorText, selectorInfo.selector, buildState.curOffset);

        appendCtx->addQuotedF("%s->append(%s);", buildState.listName, createMonitorText.str());
    }
}


IHqlExpression * MonitorExtractor::getMonitorValueAddress(BuildCtx & ctx, IHqlExpression * _value)
{
    LinkedHqlExpr value = _value;
    CHqlBoundExpr bound;
    ITypeInfo * type = value->queryType();
    switch (type->getTypeCode())
    {
    case type_varstring: case type_varunicode:
        {
            assertex(type->getSize() != UNKNOWN_LENGTH);
            CHqlBoundTarget tempTarget;
            translator.createTempFor(ctx, type, tempTarget, typemod_none, FormatNatural);
            //clear the variable.
            HqlExprArray args;
            args.append(*getPointer(tempTarget.expr));
            args.append(*getZero());
            args.append(*getSizetConstant(type->getSize()));
            OwnedHqlExpr call = translator.bindTranslatedFunctionCall(memsetId, args);
            ctx.addExpr(call);
            //then assign over the top
            translator.buildExprAssign(ctx, tempTarget, value);
            bound.setFromTarget(tempTarget);
            break;
        }
    default:
        translator.buildExpr(ctx, value, bound);
        translator.ensureHasAddress(ctx, bound);
        break;
    }
    return getPointer(bound.expr);
}


/*
interface IKeySegmentOffsetTranslator : public IInterface
{
    virtual const void * getSegmentBase(const void * row) const = 0;
};

interface IKeySegmentFormatTranslator : public IInterface
{
    virtual void extractField(void * target, const void * row) const = 0;
};
*/

void MonitorExtractor::generateOffsetWrapping(StringBuffer & createMonitorText, IHqlExpression * selector, unsigned curOffset)
{
    unsigned curFieldIdx = getFieldNumber(tableExpr->queryNormalizedSelector(), selector);
    StringBuffer s;
    s.clear().append("createNewVarOffsetKeySegmentMonitor(").append(createMonitorText).append(",").append(curOffset).append(",").append(curFieldIdx).append(")");
    createMonitorText.swapWith(s);
}

void MonitorExtractor::generateFormatWrapping(StringBuffer & createMonitorText, IHqlExpression * selector, IHqlExpression * expandedSelector, unsigned curOffset)
{
    BuildCtx declarectx(*translator.code, declareAtom);
    StringBuffer s, suffix, instanceName, className, factoryName;
    unique_id_t id = translator.getUniqueId();
    appendUniqueId(className.append("c"), id);
    appendUniqueId(instanceName.append("i"), id);
    appendUniqueId(factoryName.append("f"), id);

    declarectx.setNextPriority(SegMonitorPrio);
    BuildCtx classctx(declarectx);
    //MORE: Use a base class for implementing this to save Link()/Release()
    s.clear().append("struct ").append(className).append(" : public RtlCInterface, public IKeySegmentFormatTranslator");
    suffix.append(instanceName).append(";");
    classctx.addQuotedCompound(s, suffix);
    classctx.addQuotedLiteral("virtual void Link() const { RtlCInterface::Link(); }");
    classctx.addQuotedLiteral("virtual bool Release() const { return RtlCInterface::Release(); }");
    classctx.addQuoted(s.clear().append("virtual const char * queryFactoryName() const { return \"").append(factoryName).append("\"; }"));
    classctx.addQuoted(s.clear().append("virtual unsigned queryHashCode() const { return ").append(getExpressionCRC(selector)).append("; }"));

    {
        MemberFunction func(translator, classctx, "virtual void extractField(void * _target, const void * _row) const");
        classctx.associateExpr(constantMemberMarkerExpr, constantMemberMarkerExpr);
        func.ctx.addQuotedLiteral("const byte * row = (const byte *)_row;");
        func.ctx.addQuotedLiteral("byte * target = (byte *)_target;");

        OwnedHqlExpr castValue = ensureExprType(selector, expandedSelector->queryType());
        LinkedHqlExpr targetField = expandedSelector->queryChild(1);
        OwnedHqlExpr simpleRecord = createRecord(targetField);
        OwnedHqlExpr targetDataset = createDataset(no_anon, LINK(simpleRecord));
        OwnedHqlExpr target = createSelectExpr(LINK(targetDataset), LINK(targetField));

        translator.bindTableCursor(func.ctx, tableExpr, "row");
        translator.bindTableCursor(func.ctx, targetDataset, "target");
        translator.buildAssign(func.ctx, target, castValue);
    }

    declarectx.setNextPriority(SegMonitorPrio);
    declarectx.addQuoted(s.clear().append("IKeySegmentFormatTranslator * ").append(factoryName).append("() { return new ").append(className).append("; }"));
    if (translator.spanMultipleCppFiles())
    {
        s.clear().append("extern IKeySegmentFormatTranslator * ").append(factoryName).append("();");
        BuildCtx protoctx(*translator.code, mainprototypesAtom);
        protoctx.addQuoted(s);
    }

    //Now generate the key segment monitor...
    s.clear().append("createTranslatedKeySegmentMonitor(").append(createMonitorText).append(",").append(curOffset).append(",").append(factoryName).append("())");
    createMonitorText.swapWith(s);
}

bool MonitorExtractor::buildSingleKeyMonitor(StringBuffer & createMonitorText, KeySelectorInfo & selectorInfo, BuildCtx & ctx, IHqlExpression & thisKey)
{
    BuildCtx subctx(ctx);
    OwnedHqlExpr compare, normalized;

    StringBuffer funcName;
    extractCompareInformation(subctx, &thisKey, compare, normalized, selectorInfo.expandedSelector, selectorInfo.isComputed);
    if (compare)
        return false;

    ITypeInfo * type = selectorInfo.expandedSelector->queryType();
    type_t tc = type->getTypeCode();
    if ((tc == type_int) || (tc == type_swapint))
    {
        if (isLittleEndian(type))
        {
            if (type->isSigned())
                funcName.append("createSingleLittleSignedKeySegmentMonitor");
            else if (type->getSize() != 1)
                funcName.append("createSingleLittleKeySegmentMonitor");
        }
        else
        {
            if (type->isSigned())
                funcName.append("createSingleBigSignedKeySegmentMonitor");
            else
                funcName.append("createSingleKeySegmentMonitor");
        }
    }
    
    if (!funcName.length())
        funcName.append("createSingleKeySegmentMonitor");

    OwnedHqlExpr address = getMonitorValueAddress(subctx, normalized);
    StringBuffer addrText;
    translator.generateExprCpp(addrText, address);

    unsigned offset = (selectorInfo.isComputed || selectorInfo.mapOffset) ? 0 : selectorInfo.offset;
    createMonitorText.append(funcName)
                     .appendf("(%s, %u, %u, %s)", 
                              boolToText(selectorInfo.keyedKind != KeyedYes), offset, selectorInfo.size, addrText.str());
    return true;
}

KeyedKind getKeyedKind(HqlCppTranslator & translator, KeyConditionArray & matches)
{
    KeyedKind keyedKind = KeyedNo;
    ForEachItemIn(i, matches)
    {
        KeyCondition & cur = matches.item(i);
        if (cur.keyedKind != keyedKind)
        {
            if (keyedKind == KeyedNo)
                keyedKind = cur.keyedKind;
            else
                translator.throwError1(HQLERR_InconsistentKeyedOpt, str(cur.selector->queryChild(1)->queryName()));
        }
    }
    return keyedKind;
}

void MonitorExtractor::buildEmptyKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo)
{
    StringBuffer s;
    ctx.addQuoted(s.appendf("%s->append(createEmptyKeySegmentMonitor(%s, %u, %u));", buildState.listName, boolToText(selectorInfo.keyedKind != KeyedYes), selectorInfo.offset, selectorInfo.size));
}


void MonitorExtractor::buildWildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned offset, unsigned size)
{
    StringBuffer s;
    ctx.addQuoted(s.appendf("%s->append(createWildKeySegmentMonitor(%u, %u));", buildState.listName, offset, size));
}


void MonitorExtractor::buildWildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, KeySelectorInfo & selectorInfo)
{
    buildWildKeySegment(buildState, ctx, selectorInfo.offset, selectorInfo.size);
}


void MonitorExtractor::buildKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned whichField, unsigned curSize)
{
    IHqlExpression * selector = &keyableSelects.item(whichField);
    IHqlExpression * expandedSelector = &expandedSelects.item(whichField);
    IHqlExpression * field = selector->queryChild(1);
    KeyConditionArray matches;
    bool isImplicit = true;
    bool prevWildWasKeyed = buildState.wildWasKeyed;
    buildState.wildWasKeyed = false;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.selector == selector)
        {
            cur.generated = true;
            if (cur.isWild)
            {
                isImplicit = false;
                if (cur.wasKeyed)
                    buildState.wildWasKeyed = true;
                else if (buildState.implicitWildField && !ignoreUnkeyed)
                {
                    StringBuffer s, keyname;
                    translator.throwError3(HQLERR_WildFollowsGap, getExprECL(field, s).str(), str(buildState.implicitWildField->queryChild(1)->queryName()), queryKeyName(keyname));
                }
            }
            else
            {
                matches.append(OLINK(cur));
                if (buildState.implicitWildField && !ignoreUnkeyed)
                {
                    StringBuffer s,keyname;
                    if (cur.isKeyed())
                        translator.throwError3(HQLERR_KeyedFollowsGap, getExprECL(field, s).str(), str(buildState.implicitWildField->queryChild(1)->queryName()), queryKeyName(keyname));
                    else if (!buildState.doneImplicitWarning)
                    {
                        translator.WARNING3(CategoryEfficiency, HQLWRN_KeyedFollowsGap, getExprECL(field, s).str(), str(buildState.implicitWildField->queryChild(1)->queryName()), queryKeyName(keyname));
                        buildState.doneImplicitWarning = true;
                    }
                }
            }
        }
    }
    if (buildState.wildWasKeyed && (matches.ordinality() == 0))
    {
        StringBuffer keyname;
        translator.WARNING2(CategoryFolding, HQLWRN_FoldRemoveKeyed, str(field->queryName()), queryKeyName(keyname));
    }

    StringBuffer s;
    KeyedKind keyedKind = getKeyedKind(translator, matches);
    KeySelectorInfo selectorInfo(keyedKind, selector, expandedSelector, buildState.curOffset, curSize, (whichField >= firstOffsetField), false);

    bool ignoreKeyedExtend = false;
    if ((keyedKind == KeyedExtend) && buildState.wildPending() && !ignoreUnkeyed)
    {
        if (keyedKind == KeyedExtend)
        {
            if (prevWildWasKeyed)
                buildState.wildWasKeyed = true;
            else
            {
                StringBuffer keyname;
                translator.WARNING2(CategoryEfficiency, HQLERR_OptKeyedFollowsWild, getExprECL(field, s).str(), queryKeyName(keyname));
            }
        }
        //previous condition folded so always true, so keyed,opt will always be a wildcard.

        if (!allowDynamicFormatChange)
            ignoreKeyedExtend = true;
        isImplicit = false;
    }

    if (matches.ordinality() && !ignoreKeyedExtend)
    {
        if (buildState.wildPending() && !ignoreUnkeyed)
        {
            buildWildKeySegment(buildState, ctx, buildState.wildOffset, buildState.curOffset-buildState.wildOffset);
            buildState.clearWild();
        }

        HqlExprArray args;
        ForEachItemIn(i, matches)
        {
            KeyCondition & cur = matches.item(i);
            args.append(*LINK(cur.expr));
        }

        OwnedHqlExpr fullExpr = createBalanced(no_and, queryBoolType(), args);
        BuildCtx subctx(ctx);
        buildKeySegmentExpr(buildState, selectorInfo, subctx, NULL, *fullExpr, ignoreUnkeyed ? MonitorFilterSkipAll : NoMonitorFilter);
    }
    else
    {
        if (isImplicit)
        {
            buildState.implicitWildField.set(selector);
            buildState.doneImplicitWarning = false;
        }

        if (buildState.wildPending() && noMergeSelects.contains(*selector))
        {
            buildWildKeySegment(buildState, ctx, buildState.wildOffset, buildState.curOffset-buildState.wildOffset);
            buildState.clearWild();
        }

        if (!buildState.wildPending())
            buildState.wildOffset = buildState.curOffset;
    }
    buildState.curOffset += selectorInfo.size;
}

void MonitorExtractor::buildArbitaryKeySegment(BuildMonitorState & buildState, BuildCtx & ctx, unsigned curSize, IHqlExpression * condition)
{
    IHqlExpression * left = condition->queryChild(0);
    node_operator op = condition->getOperator();

    StringBuffer createMonitorText;
    OwnedHqlExpr field = createField(unknownId, getExpandedFieldType(left->queryType(), NULL), NULL);
    OwnedHqlExpr pseudoSelector = createSelectExpr(getActiveTableSelector(), LINK(field));

    KeySelectorInfo selectorInfo(KeyedExtend, left, pseudoSelector, buildState.curOffset, curSize, false, true);
    BuildCtx subctx(ctx);
    buildKeySegmentExpr(buildState, selectorInfo, subctx, NULL, *condition, MonitorFilterSkipAll);
}


void MonitorExtractor::spotSegmentCSE(BuildCtx & ctx)
{
    //This could make things much better, but needs some thought
    HqlExprArray conditions;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.expr)
            conditions.append(*LINK(cur.expr));
    }

    HqlExprArray associated;
    IHqlExpression * selector = tableExpr->queryNormalizedSelector();
    translator.traceExpressions("before seg spot", conditions);
    spotScalarCSE(conditions, associated, NULL, selector, translator.queryOptions().spotCseInIfDatasetConditions);
    translator.traceExpressions("after seg spot", conditions);

    unsigned curCond = 0;
    ForEachItemIn(i, conditions)
    {
        IHqlExpression * cur = &conditions.item(i);
        switch (cur->getOperator())
        {
        case no_alias:
            translator.buildStmt(ctx, cur);
            break;
        case no_alias_scope:
            translator.expandAliasScope(ctx, cur);
            cur = cur->queryChild(0);
            //fallthrough
        default:
            for (;;)
            {   
                if (!keyed.conditions.isItem(curCond))
                    throwUnexpected();
                KeyCondition & keyCond = keyed.conditions.item(curCond++);
                if (keyCond.expr)
                {
                    keyCond.expr.set(cur);
                    break;
                }
            }
            break;
        }
    }
    for (;;)
    {   
        if (!keyed.conditions.isItem(curCond))
            break;
        KeyCondition & keyCond = keyed.conditions.item(curCond++);
        assertex(!keyCond.expr);
    }
}


void MonitorExtractor::buildSegments(BuildCtx & ctx, const char * listName, bool _ignoreUnkeyed)
{
    translator.useInclude("rtlkey.hpp");
    ignoreUnkeyed = _ignoreUnkeyed;

    if (translator.queryOptions().spotCSE)
        spotSegmentCSE(ctx);

    BuildMonitorState buildState(ctx, listName);
    ForEachItemIn(idx, keyableSelects)
    {
        IHqlExpression * selector = &keyableSelects.item(idx);
        IHqlExpression * expandedSelector = &expandedSelects.item(idx);
        IHqlExpression * field = selector->queryChild(1);
        unsigned curSize = expandedSelector->queryType()->getSize();
        assertex(curSize != UNKNOWN_LENGTH);

        //MORE: Should also allow nested record structures, and allow keying on first elements.
        //      and field->queryType()->getSize() doesn't work for alien datatypes etc.
        if(!field->hasAttribute(virtualAtom))
        {
            if (mergedSizes.isItem(idx))
                curSize = mergedSizes.item(idx);
            if (curSize)
                buildKeySegment(buildState, ctx, idx, curSize);
            else
            {
                ForEachItemIn(cond, keyed.conditions)
                {
                    KeyCondition & cur = keyed.conditions.item(cond);
                    if (cur.selector == selector)
                        cur.generated = true;
                }
            }
        }
    }

    if (buildState.wildPending() && !ignoreUnkeyed)
        buildWildKeySegment(buildState, ctx, buildState.wildOffset, buildState.curOffset-buildState.wildOffset);

    //These really don't work very sensibly - we would need an offset that was constant, possibly based on the crc of the expression.
    //I suspect they need a complete rethink.
    ForEachItemIn(i, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(i);
        if (cur.selector->isAttribute() && cur.selector->queryName() == _translated_Atom)
        {
            BuildCtx subctx(ctx);
            IHqlExpression * curExpr = cur.expr;
            unsigned curSize = curExpr->queryChild(0)->queryType()->getSize();
            buildArbitaryKeySegment(buildState, subctx, curSize, curExpr);
            buildState.curOffset += curSize;
            cur.generated = true;
        }
    }

    //check that all keyed entries have been matched
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (!cur.generated)
            translator.throwError(HQLERR_OnlyKeyFixedField);
    }
}


static UniqueSequenceCounter translatedSequence;
KeyCondition * MonitorExtractor::createTranslatedCondition(IHqlExpression * cond, KeyedKind keyedKind)
{
    OwnedHqlExpr seq = createSequence(no_attr, makeNullType(), _translated_Atom, translatedSequence.next());
    return new KeyCondition(seq, cond, keyedKind);
}

bool MonitorExtractor::isKeySelect(IHqlExpression * select)
{
    return (keyableSelects.find(*select) != NotFound);
}

bool MonitorExtractor::isEqualityFilter(IHqlExpression * search)
{
    bool matched = false;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.selector == search)
        {
            if (!cur.isWild)
            {
                if (matched)
                    return false;
                matched = true;
                IHqlExpression * matchExpr = cur.expr;
                if (matchExpr->getOperator() != no_eq)
                    return false;
            }
        }
    }
    return matched;
}

bool MonitorExtractor::isEqualityFilterBefore(IHqlExpression * select)
{
    ForEachItemIn(i, keyableSelects)
    {
        IHqlExpression & cur = keyableSelects.item(i);
        if (select == &cur)
            return true;
        if (!isEqualityFilter(&cur))
            return false;
    }
    throwUnexpected();
}

bool MonitorExtractor::isPrevSelectKeyed(IHqlExpression * select)
{
    unsigned match = keyableSelects.find(*select);
    assertex(match != NotFound);
    if (match == 0)
        return true;
    IHqlExpression * prev = &keyableSelects.item(match-1);
    ForEachItemIn(i, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(i);
        if (cur.selector == prev)
        {
            if (!cur.isWild && cur.isKeyed())
                return true;
            if (cur.wasKeyed)
                return true;
        }
    }
    return false;
}


bool MonitorExtractor::okToKey(IHqlExpression * select, KeyedKind keyedKind)
{
    if (keyedKind == KeyedYes)
        return true;

    ForEachItemIn(i, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(i);
        if (cur.selector == select && cur.isWild)
            return false;
    }

    return true;
}

bool MonitorExtractor::isIndexInvariant(IHqlExpression * expr, bool includeRoot)
{
    if (containsAssertKeyed(expr))
        return false;

    HqlExprCopyArray scopeUsed;
    expr->gatherTablesUsed(NULL, &scopeUsed);

    IHqlExpression * search = tableExpr->queryNormalizedSelector();
    ForEachItemIn(i, scopeUsed)
    {
        IHqlExpression * cur = &scopeUsed.item(i);
        for (;;)
        {
            if (cur == search)
                return false;

            if (includeRoot && (queryRoot(cur) == search))
                return false;

            IHqlExpression * parent = queryNextMultiLevelDataset(cur, true);
            if (!parent)
                break;
            cur = parent;
        }
    }
    return true;
}




IHqlExpression * MonitorExtractor::castToFieldAndBack(IHqlExpression * left, IHqlExpression * right)
{
    node_operator op = left->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
        {
            IHqlExpression * uncast = left->queryChild(0);
            ITypeInfo * castType = right->queryType();
            ITypeInfo * uncastType = uncast->queryType();

            OwnedHqlExpr castRight = ensureExprType(right, uncastType);
            OwnedHqlExpr base = castToFieldAndBack(uncast, castRight);
            //If this cast doesn't lose any information and child didn't change then don't bother
            //casting back and forwards.
            if ((base == castRight) && !castLosesInformation(uncastType, castType))
                return LINK(right);
            return ensureExprType(base, castType);
        }

    case no_select:
        {
            ITypeInfo * leftType = left->queryType();
            ITypeInfo * rightType = right->queryType();
            if (leftType == rightType || !castLosesInformation(leftType, rightType))
                return LINK(right);
            return ensureExprType(right, leftType);
        }
    case no_substring:
    case no_add:
    case no_sub:
        return castToFieldAndBack(left->queryChild(0), right);
    default:
        UNIMPLEMENTED;
    }
}



IHqlExpression * MonitorExtractor::invertTransforms(IHqlExpression * left, IHqlExpression * right)
{
    node_operator op = left->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
        {
            assertex(right->queryType()->getTypeCode() != type_set);

            IHqlExpression * uncast = left->queryChild(0);
            ITypeInfo * uncastType = uncast->queryType();


            OwnedHqlExpr castRight = ensureExprType(right, uncastType);
            return invertTransforms(uncast, castRight);
        }

    case no_select:
        {
            assertex(isKeySelect(left));
            ITypeInfo * leftType = left->queryType();
            ITypeInfo * rightType = right->queryType();
            if (leftType == rightType || !castLosesInformation(leftType, rightType))
                return LINK(right);
            return ensureExprType(right, leftType);
        }

    case no_add:
    case no_sub:
        {
            assertex(right->getOperator() != no_list);

            OwnedHqlExpr adjusted = createValue(op == no_sub ? no_add : no_sub, right->getType(), LINK(right), LINK(left->queryChild(1)));
            return invertTransforms(left->queryChild(0), adjusted);
        }
     case no_substring:
         {
             assertex(right->getOperator() != no_list);

             return invertTransforms(left->queryChild(0), right);
         }
    default:
        UNIMPLEMENTED;
    }
}



IHqlExpression * MonitorExtractor::queryKeyableSelector(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_cast:
    case no_implicitcast:
    case no_add:
    case no_sub:
        return queryKeyableSelector(expr->queryChild(0));
    case no_select:
        if (isKeySelect(expr))
            return expr;
        return NULL;
    }
    return NULL;
}

IHqlExpression * MonitorExtractor::isKeyableFilter(IHqlExpression * left, IHqlExpression * right, bool & duplicate, node_operator compareOp, KeyFailureInfo & reason, KeyedKind keyedKind)
{
    node_operator op = left->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
        {
            IHqlExpression * uncast = left->queryChild(0);
            ITypeInfo * castType = left->queryType();
            ITypeInfo * uncastType = uncast->queryType();
            //(ty)x = y.            E.g., (int1)string2field = int1value
            //if more than one value of x[uncastType] corresponds to a single value in y[castType] then we can't sensibly create
            //the key segment monitor.  Because we will get false negatives.  If it is an inverse then duplicate (see below)
            bool canRemoveCast = true;
            if (castLosesInformation(castType, uncastType))
            {
                if ((compareOp != no_ne) && (compareOp != no_notin))
                    canRemoveCast = false;
                duplicate = true;
            }

            //if more than one value of y corresponds to a single value of x then need to duplicate the test condition.
            //or pretest whether (ty)(tx)y == y.
            //if (castLosesInformation(uncastType, castType))
            //Now taken care of when the segment monitors are created

            //If the comparison is non equality and the cast changes the collation sequence then you can't remove it.
            switch (compareOp)
            {
            case no_eq:
            case no_ne:
            case no_in:
            case no_notin:
                break;
            default:
                if (!preservesOrder(castType, uncastType))
                    canRemoveCast = false;
                break;
            }

            Linked<ITypeInfo> newType = uncastType;
            if (right->queryType()->getTypeCode() == type_set)
                newType.setown(makeSetType(newType.getLink()));
            OwnedHqlExpr castRight = ensureExprType(right, newType);
            IHqlExpression * ret = isKeyableFilter(uncast, castRight, duplicate, compareOp, reason, keyedKind);
            if (canRemoveCast || !ret)
                return ret;
            reason.set(KFRcast, ret->queryChild(1));
            return NULL;
        }

    case no_select:
        if (isKeySelect(left) && okToKey(left, keyedKind))
        {
            if (isIndexInvariant(right, false))
                return left;
            reason.set(KFRtoocomplex, left);
        }
        else
            reason.set(KFRnokey);
        return NULL;

    case no_substring:
        {
            IHqlExpression * range = left->queryChild(1);
            if (range->getOperator() == no_rangeto)
            {
                IValue *end = range->queryChild(0)->queryValue();
                if (!end)
                    break;
                return isKeyableFilter(left->queryChild(0), right, duplicate, compareOp, reason, keyedKind);
            }
            else if (range->getOperator() == no_range)
            {
                IValue *start = range->queryChild(0)->queryValue();
                IValue *end = range->queryChild(1)->queryValue();
                if (!start || !end || start->getIntValue() != 1)
                    break;
                return isKeyableFilter(left->queryChild(0), right, duplicate, compareOp, reason, keyedKind);
            }
            reason.set(KFRtoocomplex, right);
            return NULL;
        }
    case no_add:
    case no_sub:
        if (isIndexInvariant(left->queryChild(1), false))
            return isKeyableFilter(left->queryChild(0), right, duplicate, compareOp, reason, keyedKind);
        reason.set(KFRtoocomplex, left);
        return NULL;
    }
    reason.set(KFRnokey);
    return NULL;
}



static IHqlExpression * getCompareValue(ITypeInfo * fieldType, unsigned subStringLen, IValue * value, int whichBoundary)
{
    type_t ftc = fieldType->getTypeCode();
    unsigned fieldLen = fieldType->getStringLen();
    unsigned lenValue = value->queryType()->getStringLen();
    const void * rawValue = value->queryValue();
    size32_t resultLen;
    rtlDataAttr result;

    if (whichBoundary < 0)
    {
        switch (ftc)
        {
        case type_qstring:
            rtlCreateQStrRangeLow(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_string:
            rtlCreateStrRangeLow(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_data:
            rtlCreateDataRangeLow(resultLen, result.refdata(), fieldLen, subStringLen, lenValue, rawValue);
            break;
        case type_unicode:
            rtlCreateUnicodeRangeLow(resultLen, result.refustr(), fieldLen, subStringLen, lenValue, static_cast<const UChar *>(rawValue));
            break;
        default:
            //should this generate a warning/error instead?
            rtlCreateRange(resultLen, result.refstr(), fieldLen, subStringLen, fieldType->getSize(), static_cast<const char *>(rawValue), 0, 0);
            break;
        }
    }
    else
    {
        switch (ftc)
        {
        case type_qstring:
            rtlCreateQStrRangeHigh(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_string:
            rtlCreateStrRangeHigh(resultLen, result.refstr(), fieldLen, subStringLen, lenValue, static_cast<const char *>(rawValue));
            break;
        case type_data:
            rtlCreateDataRangeHigh(resultLen, result.refdata(), fieldLen, subStringLen, lenValue, rawValue);
            break;
        case type_unicode:
            rtlCreateUnicodeRangeHigh(resultLen, result.refustr(), fieldLen, subStringLen, lenValue, static_cast<const UChar *>(rawValue));
            break;
        default:
            rtlCreateRange(resultLen, result.refstr(), fieldLen, subStringLen, fieldType->getSize(), static_cast<const char *>(rawValue), 255, 0);
            break;
        }
    }
    assertex(resultLen == fieldLen);
    return createConstant(createValueFromMem(LINK(fieldType), result.getdata()));
}


IHqlExpression * MonitorExtractor::getRangeLimit(ITypeInfo * fieldType, IHqlExpression * lengthExpr, IHqlExpression * value, int whichBoundary)
{
    type_t ftc = fieldType->getTypeCode();
    unsigned fieldLength = fieldType->getStringLen();
    IValue * constValue = value->queryValue();

    if (constValue && lengthExpr->queryValue())
    {
        unsigned subStringLen = (unsigned)lengthExpr->queryValue()->getIntValue();
        if ((int)subStringLen < 0) subStringLen = 0;
        if (subStringLen > fieldLength)
            translator.throwError1(HQLERR_SubstringOutOfRange, subStringLen);

        return getCompareValue(fieldType, subStringLen, constValue, whichBoundary);
    }

    IIdAtom * func;
    if (whichBoundary < 0)
    {
        switch (ftc)
        {
        case type_qstring:
            func = createQStrRangeLowId;
            break;
        case type_string:
            func = createStrRangeLowId;
            break;
        case type_data:
            func = createDataRangeLowId;
            break;
        case type_unicode:
            func = createUnicodeRangeLowId;
            break;
        default:
            func = createRangeLowId;
            break;
        }
    }
    else
    {
        switch (ftc)
        {
        case type_qstring:
            func = createQStrRangeHighId;
            break;
        case type_string:
            func = createStrRangeHighId;
            break;
        case type_data:
            func = createDataRangeHighId;
            break;
        case type_unicode:
            func = createUnicodeRangeHighId;
            break;
        default:
            func = createRangeHighId;
            break;
        }
    }

    HqlExprArray args;
    args.append(*getSizetConstant(fieldLength));
    args.append(*LINK(lengthExpr));
    args.append(*LINK(value));

    //Note: I can't change the return type of the function - because if fixed length then wrong call is made, and variable length is worse code.
    OwnedHqlExpr call = translator.bindFunctionCall(func, args);
    return createValue(no_typetransfer, LINK(fieldType), LINK(call));
}


IHqlExpression * MonitorExtractor::createRangeCompare(IHqlExpression * selector, IHqlExpression * value, IHqlExpression * lengthExpr, bool compareEqual)
{
    OwnedHqlExpr foldedValue = foldHqlExpression(value);
    ITypeInfo * fieldType = selector->queryType();
    IHqlExpression * lowExpr = getRangeLimit(fieldType, lengthExpr, foldedValue, -1);
    IHqlExpression * highExpr = getRangeLimit(fieldType, lengthExpr, foldedValue, +1);

    //Could convert to two separate tests, but code is worse, and boundary conditions aren't going to happen.
    return createValue(compareEqual ? no_between : no_notbetween, makeBoolType(), LINK(selector), lowExpr, highExpr);
}


static IHqlExpression * removeCastTrim(IHqlExpression * expr)
{
    for (;;)
    {
        if ((expr->getOperator() == no_trim) && !expr->queryChild(1))
            expr = expr->queryChild(0);
        else if (isLengthPreservingCast(expr))
            expr = expr->queryChild(0);
        else
            return expr;

        expr = queryNonAliased(expr);
    }
}

static IHqlExpression * queryLengthFromRange(IHqlExpression * range)
{
    switch (range->getOperator())
    {
    case no_rangefrom:
    case no_rangecommon:
        return NULL;
    case no_rangeto:
        return range->queryChild(0);
    case no_range:
        if (getIntValue(range->queryChild(0), 0) == 1)
            return range->queryChild(1);
        return NULL;
    default:
        if (getIntValue(range, 0) == 1)
            return range;
        return NULL;
    }
}

static void extendRangeCheck(SharedHqlExpr & globalGuard, SharedHqlExpr & localCond, IHqlExpression * selector, IHqlExpression * lengthExpr, bool compareEqual)
{
#if 0
    //This might be a good idea, but probably doesn't make a great deal of difference at runtime
    //Optimize the case where you check zero length to use a wild carded range instead
    //x[1..len=0] = y from x in range 0000000..FFFFFF to  len==0 || x in range....
    if (compareEqual)
    {
        if (!lengthExpr->queryValue())
        {
            OwnedHqlExpr testLength = createBoolExpr(no_eq, LINK(lengthExpr), ensureExprType(queryZero(), lengthExpr->queryType()));
            localCond.setown(createBoolExpr(no_or, testLength.getClear(), LINK(localCond)));
        }
    }
#endif
    //For a range check x[1..m] = y we generate
    //x in range(sizeof(x), m, y, 0) and range(maxlength(x), m, y, 255)
    //need to guard with condition len(trim(y)) <= m

    //If x[1..length(trim(y))] == y then don't add a condition
    IHqlExpression * cur = queryNonAliased(lengthExpr);
    IHqlExpression * compare = removeCastTrim(queryNonAliased(selector));
    if (cur->getOperator() == no_charlen)
    {
        cur = queryNonAliased(cur->queryChild(0));
        if (cur->getOperator() == no_trim)
        {
            cur = queryNonAliased(cur->queryChild(0));
            while (isLengthPreservingCast(cur))
                cur = queryNonAliased(cur->queryChild(0));

            IHqlExpression * compare = selector;
            for (;;)
            {
                compare = removeCastTrim(queryNonAliased(compare));
                if (cur->queryBody() == compare->queryBody())
                    return;
                //Casts between strings that reduce/increase the number of characters don't matter as long as
                //they eventually match the search string
                if (!isCast(compare) || !isStringType(compare->queryType()))
                    break;
                compare = compare->queryChild(0);
                if (!isStringType(compare->queryType()))
                    break;
            }
        }
    }

    // if x[1..n] = z[1..n] then no need for a condition
    if (compare->getOperator() == no_substring)
    {
        IHqlExpression * range = queryLengthFromRange(compare->queryChild(1));
        if (range == lengthExpr)
            return;
    }

    // if x[1..n] = (string<m>y) where m<n then no need for condition
    unsigned selectorLength = selector->queryType()->getStringLen();
    if (selectorLength <= getIntValue(lengthExpr, 0))
        return;

    //otherwise, if x[1..y] == z then add check length(trim(z)) <= y
    OwnedHqlExpr trim = createTrimExpr(selector, NULL);
    OwnedHqlExpr len = createValue(no_charlen, LINK(unsignedType), LINK(trim));
    ITypeInfo * lengthType = lengthExpr->queryType();
    Owned<ITypeInfo> compareType = getPromotedECLCompareType(unsignedType, lengthType);
    node_operator compOp = compareEqual ? no_le : no_gt;
    OwnedHqlExpr positiveLen = createValue(no_maxlist, lengthExpr->getType(), createValue(no_list, makeSetType(LINK(lengthType)), LINK(lengthExpr), createConstant(lengthType->castFrom(false, I64C(0)))));
    OwnedHqlExpr test = createValue(no_le, makeBoolType(), ensureExprType(len, compareType), ensureExprType(positiveLen, compareType));
    test.setown(foldHqlExpression(test));
    if (compareEqual)
        extendConditionOwn(globalGuard, no_and, test.getClear());
    else
        extendConditionOwn(localCond, no_or, getInverse(test));
}


bool MonitorExtractor::matchSubstringFilter(KeyConditionInfo & matches, node_operator op, IHqlExpression * left, IHqlExpression * right, KeyedKind keyedKind, bool & duplicate)
{
    LinkedHqlExpr value = right;
    duplicate = false;
    OwnedHqlExpr guard;
    ITypeInfo * guardCastType = NULL;

    if ((left->getOperator() == no_cast) || (left->getOperator() == no_implicitcast))
    {
        //code is extracted and simplified from isKeyableFilter() above - should be commoned up.
        IHqlExpression * uncast = left->queryChild(0);
        ITypeInfo * castType = left->queryType();
        ITypeInfo * uncastType = uncast->queryType();

        //(ty)x = y.
        //if more than one value of x[uncastType] corresponds to a single value in y[castType] then we can't sensibly create
        //the key segment monitor.  Because we will get false negatives.  If it is an inverse then duplicate (see below)
        bool canRemoveCast = true;
        if (castLosesInformation(castType, uncastType))
            canRemoveCast = false;

        //if more than one value of y corresponds to a single value of x then need to duplicate the test condition.
        if (!preservesOrder(castType, uncastType))
            canRemoveCast = false;

        if (!canRemoveCast)
        {
//          reason.set(KFRcast, ret->queryChild(1));
            return false;
        }

        if ((op != no_in) && (op != no_notin))
        {
            value.setown(ensureExprType(right, uncastType));

            //If a simple equality test then create a global guard to check that we aren't matching a false positive
            if (castLosesInformation(uncastType, castType))
                guard.setown(createBoolExpr(no_eq, ensureExprType(value, castType), LINK(right)));
        }
        else
        {
            //if an IN then add guards to each comparison - generated later...
            if (castLosesInformation(uncastType, castType))
                guardCastType = uncastType;
            else
            {
                Owned<ITypeInfo> targetType = makeSetType(LINK(uncastType));
                value.setown(ensureExprType(right, targetType));
            }
        }

        left = uncast;
    }

    if (left->getOperator() != no_substring)
        return false;

    if ((op == no_in) || (op == no_notin))
    {
        value.setown(normalizeListCasts(value));
        if (value->getOperator() != no_list)
            return false;
    }

    IHqlExpression * selector = left->queryChild(0);
    if (!isKeySelect(selector) || !okToKey(selector, keyedKind))
        return false;
    if (!isIndexInvariant(right, false))
        return false;
    ITypeInfo * fieldType = selector->queryType();
    unsigned fieldLength = fieldType->getStringLen();
    if (fieldLength == UNKNOWN_LENGTH)
        return false;

    OwnedHqlExpr range = foldHqlExpression(left->queryChild(1));
    IHqlExpression * lengthExpr = queryLengthFromRange(range);
    if (!lengthExpr)
        return false;

    OwnedHqlExpr newTest;
    if ((op == no_eq) || (op == no_ne))
    {
        newTest.setown(createRangeCompare(selector, value, lengthExpr, op == no_eq));
        extendRangeCheck(guard, newTest, right, lengthExpr, op == no_eq);
    }
    else //no_in, no_notin
    {
        HqlExprArray compares;
        ForEachChild(i, value)
        {
            IHqlExpression * cur = value->queryChild(i);
            LinkedHqlExpr castValue = cur;
            OwnedHqlExpr valueGuard;

            if (guardCastType)
            {
                castValue.setown(ensureExprType(castValue, guardCastType));
                valueGuard.setown(createBoolExpr(no_eq, ensureExprType(castValue, cur->queryType()), LINK(cur)));
                extendRangeCheck(valueGuard, valueGuard, cur, lengthExpr, (op == no_in));
            }
            OwnedHqlExpr cond = createRangeCompare(selector, castValue, lengthExpr, (op == no_in));
            if (valueGuard)
                cond.setown(createValue(no_and, makeBoolType(), valueGuard.getClear(), cond.getClear()));
            compares.append(*cond.getClear());
        }

        node_operator combineOp = (op == no_in) ? no_or : no_and;
        newTest.setown(createBalanced(combineOp, queryBoolType(), compares));
    }
    
    matches.appendCondition(*new KeyCondition(selector, newTest, keyedKind));
    if (guard)
        matches.appendPreFilter(guard);
    return true;
}


bool MonitorExtractor::extractSimpleCompareFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind) 
{
    OwnedHqlExpr promoted = getExplicitlyPromotedCompare(expr);
    IHqlExpression * l = promoted->queryChild(0);
    IHqlExpression * r = promoted->queryChild(1);
    bool duplicate = false;
    KeyFailureInfo reasonl, reasonr;
    node_operator op = expr->getOperator();
    IHqlExpression * matchedSelector = isKeyableFilter(l, r, duplicate, op, reasonl, keyedKind);
    Owned<KeyCondition> result;
    if (matchedSelector)
    {
        node_operator newOp = getModifiedOp(op, duplicate);
        if (newOp != no_none)
        {
            OwnedHqlExpr newFilter = createValue(newOp, expr->getType(), LINK(l), LINK(r));
            result.setown(new KeyCondition(matchedSelector, newFilter, keyedKind));
        }
    }
    else
    {
        duplicate = false;
        matchedSelector = isKeyableFilter(r, l, duplicate, op, reasonr, keyedKind);
        if (matchedSelector)
        {
            node_operator newOp = getModifiedOp(getReverseOp(op), duplicate);
            if (newOp != no_none)
            {
                OwnedHqlExpr newFilter = createValue(newOp, expr->getType(), LINK(r), LINK(l));
                result.setown(new KeyCondition(matchedSelector, newFilter, keyedKind));
            }
        }
    }

    if (!result && allowTranslatedConds)
    {
        duplicate = false;
        ITypeInfo * type = l->queryType();
        if (isKeyableType(type))
        {
            bool leftHasSelects = containsTableSelects(l);
            bool rightHasSelects = containsTableSelects(r);
            if (leftHasSelects && !rightHasSelects)
            {
                result.setown(createTranslatedCondition(expr, keyedKind));
            }
            else if (!leftHasSelects && rightHasSelects && (op != no_in) && (op != no_notin))
            {
                OwnedHqlExpr newFilter = createValue(getReverseOp(op), expr->getType(), LINK(r), LINK(l));
                result.setown(createTranslatedCondition(newFilter, keyedKind));
            }
        }
    }

    bool extracted = (result != NULL);
    if (extracted)
    {
        matches.appendCondition(*result.getClear());
    }
    else
    {
        failReason.merge(reasonl);
        failReason.merge(reasonr);
    }
    if (duplicate || !extracted)
        matches.appendPostFilter(expr);
    return extracted;
}


bool MonitorExtractor::extractOrFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    HqlExprArray conds;
    expr->unwindList(conds, no_or);

    bool validOrFilter = true;
    HqlExprAttr invariant;
    CIArrayOf<KeyConditionInfo> branches;
    ForEachItemIn(idx, conds)
    {
        IHqlExpression & cur = conds.item(idx);
        if (isIndexInvariant(&cur, false))
            extendOrCondition(invariant, &cur);
        else
        {
            KeyConditionInfo & branch = * new KeyConditionInfo;
            branches.append(branch);
            //Can't generate an OR with a pure post-filter
            if (!extractFilters(branch, &cur, keyedKind))
                validOrFilter = false;
        }
    }

    //check all the conditions that are ORd together don't contain references to multiple fields.
    KeyCondition * firstBranch = NULL;
    bool multipleBranches = branches.ordinality() > 1;
    bool multipleSelectors = false;
    bool multipleConditions = false;
    bool hasPostFilter = false;
    ForEachItemIn(i1, branches)
    {
        KeyConditionInfo & branch = branches.item(i1);
        if (branch.postFilter)
            hasPostFilter = true;
        ForEachItemIn(i2, branch.conditions)
        {
            KeyCondition & cur = branch.conditions.item(i2);
            if (!firstBranch)
                firstBranch = &cur;
            else
            {
                multipleConditions = true;
                if (i1 == 0)
                {
                    if (firstBranch->selector != cur.selector)
                        multipleSelectors = true;
                }
                else
                {
                    if ((firstBranch->selector != cur.selector) || multipleSelectors)
                        validOrFilter = false;
                }
            }
        }
    }

    if (multipleBranches && hasPostFilter)
        validOrFilter = false;

    if (validOrFilter && firstBranch)
    {
        bool optimizeSingleBranch = true;
        if (multipleSelectors || hasPostFilter || (optimizeSingleBranch && !multipleConditions))
        {
            //Invariant ored with a conjunction
            //X or (A and B) -> (X or A) AND (X or B)
            assertex(branches.ordinality() == 1);
            KeyConditionInfo & branch = branches.item(0);
            OwnedHqlExpr preFilter = branch.preFilter ? extendCondition(no_or, invariant, branch.preFilter) : NULL;
            OwnedHqlExpr postFilter = branch.postFilter ? extendCondition(no_or, invariant, branch.postFilter) : NULL;

            matches.appendPreFilter(preFilter);
            matches.appendPostFilter(postFilter);
            ForEachItemIn(i2, branch.conditions)
            {
                KeyCondition & cur = branch.conditions.item(i2);
                OwnedHqlExpr filter = extendCondition(no_or, invariant, cur.expr);
                matches.conditions.append(*new KeyCondition(cur.selector, filter, keyedKind));
            }
        }
        else
        {
            LinkedHqlExpr combinedCondition = invariant;

            ForEachItemIn(i1, branches)
            {
                KeyConditionInfo & branch = branches.item(i1);
                OwnedHqlExpr conjunction = branch.createConjunction();
                extendOrCondition(combinedCondition, conjunction);
            }
            
            matches.conditions.append(*new KeyCondition(firstBranch->selector, combinedCondition, keyedKind));
        }
        return true;
    }
    else
    {
        matches.appendPostFilter(expr);
        KeyFailureInfo reason;
        reason.set(KFRor);
        failReason.merge(reason);
        return false;
    }
}


bool MonitorExtractor::extractIfFilter(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    //MORE: This could generate better code, but I don't think it is worth the effort at the moment.
    //Really, I should analyse left and right.  Iterate each selector referenced.  If there are no post conditions then
    //generate IF(a, X, Y) compound expression, otherwise generate the default below.
    IHqlExpression * cond = expr->queryChild(0);
    if ((keyedKind != KeyedNo) && isIndexInvariant(cond, false))
    {
        //Convert IF(a, X, Y) to... IF (a, X, true) AND IF (a, true, Y) to... (NOT a OR X) AND (a OR Y)
        OwnedHqlExpr inverseCond = getInverse(cond);
        OwnedHqlExpr trueValue = createBoolExpr(no_or, LINK(inverseCond), LINK(expr->queryChild(1)));
        OwnedHqlExpr falseValue = createBoolExpr(no_or, LINK(cond), LINK(expr->queryChild(2)));
        OwnedHqlExpr combined = createBoolExpr(no_and, LINK(trueValue), LINK(falseValue));
        return extractFilters(matches, combined, keyedKind);
    }
    matches.appendPostFilter(expr);
    return false;
}


static HqlTransformerInfo selectSpotterInfo("SelectSpotter");
MonitorExtractor::SelectSpotter::SelectSpotter(const HqlExprArray & _selects) : NewHqlTransformer(selectSpotterInfo), selects(_selects) 
{ 
    hasSelects = false; 
}

void MonitorExtractor::SelectSpotter::analyseExpr(IHqlExpression * expr)
{
    if (hasSelects || alreadyVisited(expr))
        return;
    if (selects.find(*expr) != NotFound)
    {
        hasSelects = true;
        return;
    }
    NewHqlTransformer::analyseExpr(expr);
}

bool MonitorExtractor::containsTableSelects(IHqlExpression * expr)
{
    HqlExprCopyArray inScope;
    expr->gatherTablesUsed(NULL, &inScope);

    //Check that cursors for all inScope tables are already bound in the start context
    return inScope.find(*tableExpr->queryNormalizedSelector()) != NotFound;
}


void MonitorExtractor::extractFilters(IHqlExpression * expr, SharedHqlExpr & extraFilter)
{
    HqlExprArray conds;
    expr->unwindList(conds, no_and);
    extractFilters(conds, extraFilter);
}

void MonitorExtractor::extractFilters(HqlExprArray & exprs, SharedHqlExpr & extraFilter)
{
    OwnedHqlExpr savedFilter = keyed.postFilter.getClear();
    ForEachItemIn(i1, exprs)
    {
        IHqlExpression & cur = exprs.item(i1);
        switch (cur.getOperator())
        {
        case no_assertkeyed:
        case no_assertwild:
            extractFilters(keyed, &cur, KeyedNo);
            break;
        }
    }

    keyedExplicitly = (keyed.conditions.ordinality() != 0);
    cleanlyKeyedExplicitly = keyedExplicitly && !keyed.postFilter;
    ForEachItemIn(i2, exprs)
    {
        IHqlExpression & cur = exprs.item(i2);
        switch (cur.getOperator())
        {
        case no_assertkeyed:
        case no_assertwild:
            break;
        default:
            if (!keyedExplicitly)
                extractFilters(keyed, &cur, KeyedNo);
            else if (!cur.isAttribute() && isIndexInvariant(&cur, true))
                keyed.appendPreFilter(&cur);
            else
                keyed.appendPostFilter(&cur);
            break;
        }
    }

    extraFilter.set(keyed.postFilter);
    keyed.postFilter.setown(extendConditionOwn(no_and, savedFilter.getClear(), LINK(extraFilter)));
}


void MonitorExtractor::extractFiltersFromFilterDs(IHqlExpression * expr)
{
    HqlExprArray conds;
    HqlExprAttr dummy;
    unwindFilterConditions(conds, expr);
    extractFilters(conds, dummy);
}



void MonitorExtractor::extractFoldedWildFields(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_cast:
    case no_implicitcast:
    case no_add:
    case no_sub:
        //fields may have been transformed since folding...
        extractFoldedWildFields(expr->queryChild(0));
        break;
    case no_select:
        if (isKeySelect(expr))
        {
            KeyCondition * condition = new KeyCondition;
            condition->selector.set(expr);
            condition->isWild = true;
            condition->wasKeyed = true;
            keyed.conditions.append(*condition);
        }
        break;
    }
}

bool MonitorExtractor::extractBoolFieldFilter(KeyConditionInfo & matches, IHqlExpression * selector, KeyedKind keyedKind, bool compareValue)
{
    if (selector->isBoolean())
    {
        if (isKeySelect(selector) && okToKey(selector, keyedKind))
        {
            OwnedHqlExpr newFilter = createValue(no_eq, makeBoolType(), LINK(selector), createConstant(compareValue));
            matches.appendCondition(*new KeyCondition(selector, newFilter, keyedKind));
            return true;
        }
    }
    return false;
}

bool MonitorExtractor::extractFilters(KeyConditionInfo & matches, IHqlExpression * expr, KeyedKind keyedKind)
{
    if (!expr->isAttribute() && isIndexInvariant(expr, true))
    {
        extendAndCondition(matches.preFilter, expr);
        return true;
    }

    IHqlExpression *l = expr->queryChild(0);
    IHqlExpression *r = expr->queryChild(1);
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_and:
        {
            bool extracted = extractFilters(matches, l, keyedKind);
            if (!extractFilters(matches, r, keyedKind)) extracted = false;
            return extracted;
        }
    case no_or:
        return extractOrFilter(matches, expr, keyedKind);
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return true;
    case no_not:
        {
            IHqlExpression * arg = expr->queryChild(0);
            OwnedHqlExpr inverse = getInverse(arg);
            if (inverse->queryBody() != expr->queryBody())
                return extractFilters(matches, inverse, keyedKind);

            if ((arg->getOperator() == no_select) && arg->isBoolean() &&
                extractBoolFieldFilter(matches, arg, keyedKind, false))
                return true;

            matches.appendPostFilter(expr);
            return false;
        }
    case no_between:
    case no_notbetween:
        {
            //Convert this into two comparisons because that will handle weird boundary conditions much better.
            OwnedHqlExpr normalized = expandBetween(expr);
            return extractFilters(matches, normalized, keyedKind);
        }
    case no_eq:
    case no_ne:
        {
            bool duplicate = false;
            if (matchSubstringFilter(matches, op, l, r, keyedKind, duplicate) || matchSubstringFilter(matches, op, r, l, keyedKind, duplicate))
            {
                if (duplicate)
                    matches.appendPostFilter(expr);
                return true;
            }
            return extractSimpleCompareFilter(matches, expr, keyedKind);
        }
    case no_in:
    case no_notin:
        {
            bool duplicate = false;
            if (matchSubstringFilter(matches, op, l, r, keyedKind, duplicate))
            {
                if (duplicate)
                    matches.appendPostFilter(expr);
                return true;
            }
            return extractSimpleCompareFilter(matches, expr, keyedKind);
        }
    case no_gt:
    case no_lt:
    case no_ge:
    case no_le:
        return extractSimpleCompareFilter(matches, expr, keyedKind);
    case no_assertkeyed:
        {
            KeyFailureInfo reason;
            reason.merge(failReason);
            failReason.clear();
            bool extend = expr->hasAttribute(extendAtom);
            if (!extractFilters(matches, l, extend ? KeyedExtend : KeyedYes))
            {
                if (!extend)
                    failReason.reportError(translator, expr);
            }

            IHqlExpression * original = expr->queryAttribute(_selectors_Atom);
            if (original)
            {
                ForEachChild(i, original)
                    extractFoldedWildFields(original->queryChild(i));
            }
            failReason.merge(reason);
            return true;
        }
    case no_assertwild:
        {
            if (l->getOperator() == no_all)
            {
                IHqlExpression * original = expr->queryAttribute(_selectors_Atom);
                assertex(original);
                ForEachChild(i, original)
                    extractFoldedWildFields(original->queryChild(i));
            }
            else
            {
                IHqlExpression * selector = queryKeyableSelector(l);
                if (!selector)
                {
                    StringBuffer keyname;
                    translator.throwError1(HQLERR_WildNotReferenceIndex, queryKeyName(keyname));
                }
                KeyCondition * condition = new KeyCondition;
                condition->selector.set(selector);
                condition->isWild = true;
                matches.appendCondition(*condition);
            }
            return true;
        }
    case no_if:
        return extractIfFilter(matches, expr, keyedKind);
    case no_select:
        {
            if (expr->isBoolean() && extractBoolFieldFilter(matches, expr, keyedKind, true))
                return true;

            matches.appendPostFilter(expr);
            return false;
        }
    default:
        // Add this condition to the catchall expr
        matches.appendPostFilter(expr);
        return false;
    }
}


void MonitorExtractor::extractAllFilters(IHqlExpression * dataset)
{
    for (;;)
    {
        switch (dataset->getOperator())
        {
        case no_newkeyindex:
            return;
        case no_filter:
            extractAllFilters(dataset->queryChild(0));
            extractFiltersFromFilterDs(dataset);
            return;
        case no_compound_indexread:
        case no_newusertable:
        case no_hqlproject:
        case no_distributed:
        case no_preservemeta:
        case no_unordered:
        case no_sorted:
        case no_stepped:
        case no_grouped:
        case no_alias_scope:
        case no_dataset_alias:
            break;
        default:
            UNIMPLEMENTED;
        }
        dataset = dataset->queryChild(0);
    }
}


bool MonitorExtractor::isKeyed()                                            
{ 
    ForEachItemIn(i, keyed.conditions)
    {
        if (!keyed.conditions.item(i).isWild)
            return true;
    }
    return false;
}


bool expandFilename(StringBuffer & s, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_constant:
        expr->toString(s);
        return true;
    case no_getresult:
        //more
        break;
    case no_concat:
        {
            bool hadString = expandFilename(s, expr->queryChild(0));
            unsigned oldLength = s.length();
            s.append("+");
            if (expandFilename(s, expr->queryChild(1)) || hadString)
                return true;
            s.setLength(oldLength);
            return false;
        }
    case no_alias:
    case no_cast:
    case no_implicitcast:
        return expandFilename(s, expr->queryChild(0));
    }
    if (hasNamedSymbol(expr))
    {
        s.append(expr->queryName());
        return true;
    }
    s.append("...");
    return false;
}

const char * MonitorExtractor::queryKeyName(StringBuffer & s)
{
    IAtom * name = tableExpr->queryName();
    if (name)
        s.append(" \'").append(name).append("'");
    else
    {
        IHqlExpression * filename = queryTableFilename(tableExpr);
        if (filename)
        {
            if (!expandFilename(s.append(' '), filename))
                s.clear();
        }
    }
    return s.str();
}


static bool isNextField(IHqlExpression * record, IHqlExpression * prevExpr, IHqlExpression * nextExpr)
{
    if ((prevExpr->getOperator() != no_select) || (nextExpr->getOperator() != no_select))
        return false;
    IHqlExpression * prevSelect = prevExpr->queryChild(0);
    if (prevSelect != nextExpr->queryChild(0))
        return false;
    if (!containsOnlyLeft(prevExpr))
        return false;
    if (prevSelect->getOperator() != no_left)
        record = prevSelect->queryRecord();

    IHqlExpression * prevField = prevExpr->queryChild(1);
    IHqlExpression * nextField = nextExpr->queryChild(1);

    //Slow, but probably doesn't matter...
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        if (cur == prevField)
            return (record->queryChild(i+1) == nextField);
    }
    return false;
}


IHqlExpression * MonitorExtractor::querySimpleJoinValue(IHqlExpression * selector)
{
    IHqlExpression * matched = NULL;
    ForEachItemIn(cond, keyed.conditions)
    {
        KeyCondition & cur = keyed.conditions.item(cond);
        if (cur.selector == selector)
        {
            if (!cur.isWild)
            {
                if (matched)
                    return NULL;
                IHqlExpression * matchExpr = cur.expr;
                if (matchExpr->getOperator() != no_eq)
                    return NULL;
                if (matchExpr->queryChild(0) != selector)
                    return NULL;
                matched = matchExpr->queryChild(1);
            }
        }
    }

    return matched;
}


void MonitorExtractor::optimizeSegments(IHqlExpression * leftRecord)
{
    //loop to see if we have matches for key.cpt[n] = x.field[n] and key.cpt[n+1] = x.field[n+1]
    //where fields are fixed length, no casts and no modifiers.
    //if so, mark the new total size,
    //when generate, extend the size of the first monitor, and skip the others.
    //MORE: Could also combine fixed constants, but less of an advantage.
    //Don't process anything after a variable size field/something that is transformed.
    unsigned i = 0;
    for (; i < firstOffsetField; i++)
    {
        IHqlExpression * keySelector = &keyableSelects.item(i);
        unsigned mergedSize = keySelector->queryType()->getSize();
        IHqlExpression * prevValue = querySimpleJoinValue(keySelector);
        unsigned best = i;
        if (prevValue && isSameBasicType(keySelector->queryType(), prevValue->queryType()))
        {
            for (unsigned j = i+1; j < firstOffsetField; j++)
            {
                IHqlExpression * nextSelector = &keyableSelects.item(j);
                if (noMergeSelects.contains(*nextSelector))
                    break;

                IHqlExpression * nextValue = querySimpleJoinValue(nextSelector);
                if (!nextValue || !isNextField(leftRecord, prevValue, nextValue) ||
                    !isSameBasicType(nextSelector->queryType(), nextValue->queryType()))
                    break;
                prevValue = nextValue;
                mergedSize += nextSelector->queryType()->getSize();
                best = j;
            }
        }
        mergedSizes.append(mergedSize);
        for (;i < best;i++)
            mergedSizes.append(0);
    }
    while ( i < numKeyableFields)
        mergedSizes.append(expandedSelects.item(i).queryType()->getSize());
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
        : SourceBuilder(_translator, _tableExpr, _nameExpr), monitors(_tableExpr, _translator, -(int)numPayloadFields(_tableExpr), false)
    { 
        fpos.setown(getFilepos(tableExpr, false));
        lfpos.setown(getFilepos(tableExpr, true));
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
    MonitorExtractor monitors;
    SourceSteppingInfo steppingInfo;
};

void IndexReadBuilderBase::buildMembers(IHqlExpression * expr)
{
    //---- virtual void createSegmentMonitors(struct IIndexReadContext *) { ... } ----
    {
        MemberFunction func(translator, instance->startctx, "virtual void createSegmentMonitors(IIndexReadContext *irc)");
        monitors.buildSegments(func.ctx, "irc", false);
    }

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

    instance->addAttributeBool("preload", isPreloaded);
    if (translator.getTargetClusterType() == RoxieCluster)
        instance->addAttributeBool("_isIndexOpt", tableExpr->hasAttribute(optAtom));

    if (monitors.queryGlobalGuard())
        translator.doBuildBoolFunction(instance->startctx, "canMatchAny", monitors.queryGlobalGuard());

    buildKeyedLimitHelper(expr);

    translator.buildFormatCrcFunction(instance->classctx, "getFormatCrc", tableExpr, tableExpr, 1);
    IHqlExpression * originalKey = queryAttributeChild(tableExpr, _original_Atom, 0);
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
    if (translator.hasDynamicFilename(tableExpr)) flags.append("|TIRdynamicfilename");
    if (requiresOrderedMerge) flags.append("|TIRorderedmerge");

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
                if (builder.removedFilters.find(*body))
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
        MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf, const void * _left)");
        translator.ensureRowAllocated(func.ctx, "crSelf");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        translator.associateBlobHelper(func.ctx, tableExpr, "fpp");
        buildTransformBody(func.ctx, expr, true, false, true);
    }

    if (generateUnfilteredTransform)
    {
        MemberFunction func(translator, instance->startctx, "virtual size32_t unfilteredTransform(ARowBuilder & crSelf, const void * _left)");
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

    traceExpression("before index read", optimized);
    assertex(tableExpr->getOperator() == no_newkeyindex);
    NewIndexReadBuilder info(*this, tableExpr, tableExpr->queryChild(3));
    info.gatherVirtualFields(true, true);
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

    MemberFunction func(translator, instance->startctx, "virtual size32_t transform(ARowBuilder & crSelf)");
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
    traceExpression("after optimize", optimized);

    IHqlExpression *tableExpr = queryPhysicalRootTable(optimized);
    if (!tableExpr)
        return buildNullIndexActivity(*this, ctx, optimized);
    ensureDiskAccessAllowed(tableExpr);

    assertex(tableExpr->getOperator() == no_newkeyindex);
    IndexNormalizeBuilder info(*this, tableExpr, tableExpr->queryChild(3));
    info.gatherVirtualFields(true, true);
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
        rowctx.addQuotedFunction("virtual void processRow(ARowBuilder & crSelf, const void * src)");
        rowctx.addQuotedLiteral("doProcessRow(crSelf, (byte *)src);");
    }

    {
        MemberFunction func(translator, instance->startctx, "virtual void processRows(ARowBuilder & crSelf, size32_t srcLen, const void * _left)");
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


void IndexAggregateBuilder::buildTransform(IHqlExpression * expr)
{
    MemberFunction func(translator, instance->startctx, "void doProcessRow(ARowBuilder & crSelf, byte * left)");
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
        MemberFunction func(translator, instance->startctx, "virtual size32_t numValid(const void * _left)");
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
        info.gatherVirtualFields(true, true);
        return info.buildActivity(ctx, optimized, TAKindexcount, "IndexCount", NULL);
    }
    else
    {
        IndexAggregateBuilder info(*this, tableExpr, tableExpr->queryChild(3));
        info.gatherVirtualFields(true, true);
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
        MemberFunction func(translator, ctx, "virtual size32_t initialiseCountGrouping(ARowBuilder & crSelf, const void * _src)");
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
        MemberFunction func(translator, ctx, "virtual size32_t processCountGrouping(ARowBuilder & crSelf, unsigned __int64 count)");
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
    MemberFunction func(translator, instance->startctx, "void doProcessRow(byte * left, IHThorGroupAggregateCallback * callback)");
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
    info.gatherVirtualFields(true, true);
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

    GlobalClassBuilder builder(*this, declarectx, className, "CXmlToRowTransformer", interfaceName);
    builder.buildClass(XmlTransformerPrio);
    builder.setIncomplete(true);

    BuildCtx & classctx = builder.classctx;
    s.clear().append("inline ").append(className).append("(unsigned _activityId) : CXmlToRowTransformer(_activityId) {}");
    classctx.addQuoted(s);

    {
        MemberFunction func(*this, classctx, "virtual size32_t transform(ARowBuilder & crSelf, IColumnProvider * row, IThorDiskCallback * fpp)");
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
        func.start("virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenSrc, const char * * dataSrc)");
    else
        func.start("virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenSrc, const char * * dataSrc, unsigned __int64 _fpos)");

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

    classctx.addQuotedLiteral("virtual void Link() const { RtlCInterface::Link(); }");
    classctx.addQuotedLiteral("virtual bool Release() const { return RtlCInterface::Release(); }");

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
    buildFilenameFunction(*instance, instance->startctx, "getFileName", filename, hasDynamicFilename(tableExpr));
    buildEncryptHelper(instance->startctx, tableExpr->queryAttribute(encryptAtom));

    bool usesContents = false;
    doBuildXmlReadMember(*instance, tableExpr, "queryTransformer", usesContents);

    doBuildVarStringFunction(instance->classctx, "getXmlIteratorPath", queryRealChild(mode, 0));

    buildMetaMember(instance->classctx, tableExpr, false, "queryDiskRecordSize");  // A lie, but I don't care....

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
        : SourceBuilder(_translator, _tableExpr, _nameExpr)
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
    buildReadMembers(expr);

    IHqlExpression * fetch = queryFetch(expr);
    {
        MemberFunction func(translator, instance->startctx, "virtual unsigned __int64 extractPosition(const void * _right)");
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

    if (flags.length())
        translator.doBuildUnsignedFunction(instance->classctx, "getFetchFlags", flags.str()+1);

    if (tableExpr->hasAttribute(optAtom) && translator.targetRoxie())
        instance->addAttributeBool("_isOpt", true);

    buildLimits(instance->startctx, expr, instance->activityId);

    switch (getDatasetKind(tableExpr))
    {
    case no_csv:
        {
            translator.buildCsvParameters(instance->nestedctx, tableExpr->queryChild(2), NULL, true);
            unsigned maxColumns = getFieldCount(tableExpr->queryRecord());
            StringBuffer s;
            s.clear().append("virtual unsigned getMaxColumns() { return ").append(maxColumns).append("; }");
            instance->classctx.addQuoted(s);
            break;
        }
    case no_xml:
    case no_json:
        {
            // virtual const char * getXmlIteratorPath()
            translator.doBuildVarStringFunction(instance->classctx, "getXmlIteratorPath", queryRealChild(tableExpr->queryChild(2), 0));
            break;
        }
    default:
        translator.buildFormatCrcFunction(instance->classctx, "getDiskFormatCrc", physicalRecord, NULL, 0);
        break;
    }

    if (!containsOnlyLeft(fetch->queryChild(3), true))
    {
        //MORE: Need to change following if we optimize it to only extract the relevant fields.
        instance->classctx.addQuotedLiteral("virtual bool extractAllJoinFields() { return true; }");

        {
            MemberFunction func(translator, instance->startctx, "virtual size32_t extractJoinFields(ARowBuilder & crSelf, const void * _left)");
            translator.ensureRowAllocated(func.ctx, "crSelf");
            translator.buildRecordSerializeExtract(func.ctx, memoryRhsRecord);
        }

        StringBuffer s;
        MetaInstance meta(translator, serializedRhsRecord, false);
        translator.buildMetaInfo(meta);
        instance->classctx.addQuoted(s.clear().append("virtual IOutputMetaData * queryExtractedSize() { return &").append(meta.queryInstanceObject()).append("; }"));
    }
}


void FetchBuilder::buildTransform(IHqlExpression * expr)
{
    translator.xmlUsesContents = false;

    MemberFunction func(translator, instance->startctx);
    switch (getDatasetKind(tableExpr))
    {
    case no_csv:
        func.start("virtual size32_t transform(ARowBuilder & crSelf, unsigned * lenLeft, const char * * dataLeft, const void * _right, unsigned __int64 _fpos)");
        func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *)_right;");
        break;
    case no_xml:
    case no_json:
        func.start("virtual size32_t transform(ARowBuilder & crSelf, IColumnProvider * xmlLeft, const void * _right, unsigned __int64 _fpos)");
        func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *)_right;");
        break;
    default:
        func.start("virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned __int64 _fpos)");
        func.ctx.addQuotedLiteral("unsigned char * left = (unsigned char *)_left;");
        func.ctx.addQuotedLiteral("unsigned char * right = (unsigned char *)_right;");
        break;
    }

    translator.ensureRowAllocated(func.ctx, "crSelf");
    buildTransformBody(func.ctx, expr, true, false, true);

    if (translator.xmlUsesContents)
        instance->classctx.addQuotedLiteral("virtual bool requiresContents() { return true; }");
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
    info.gatherVirtualFields(false, true);//?needToSerializeRecord(mode)

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
