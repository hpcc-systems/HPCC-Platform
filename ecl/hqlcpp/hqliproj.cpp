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
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "hql.hpp"
#include "hqliproj.ipp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqlthql.hpp"
#include "hqlhtcpp.ipp"
#include "hqltcppc.ipp"
#include "hqlcatom.hpp"
#include "hqlfold.hpp"
#include "hqlpmap.hpp"
#include "hqlopt.hpp"
#include "hqlcerrors.hpp"
#include "hqlsource.ipp"
#include "hqlattr.hpp"
#include "jsort.hpp"

//#define PRESERVE_TRANSFORM_ANNOTATION     // improves the text in the graph, occasionally prevents optimizations.  Maybe add on a debug flag.
//#define POST_COMMON_ANNOTATION            // would be better if this just commoned up transforms...

enum
{
    CostMemoryCopy          = 1,
    CostNetworkGroup        = 1,
    CostGlobalTopN          = 2,
    CostManyCopy            = 3,
    CostNetworkCopy         = 10,
};


void UsedFieldSet::addUnique(IHqlExpression * field)
{
    if (!contains(*field))
        append(*LINK(field));
}

void UsedFieldSet::append(IHqlExpression & field)
{
#ifdef USE_IPROJECT_HASH
    if (!all)
        hash.add(&field);
#endif
    fields.append(field);
}

void UsedFieldSet::cloneFields(const UsedFieldSet & source)
{
#ifdef USE_IPROJECT_HASH
    ForEachItemIn(i, source.fields)
        append(OLINK(source.fields.item(i)));
#else
    appendArray(fields, source.fields);
#endif
}


void UsedFieldSet::clone(const UsedFieldSet & source)
{
    if (source.all)
        all = true;

    cloneFields(source);
}


int UsedFieldSet::compareOrder(IHqlExpression * left, IHqlExpression * right) const
{
    return fields.find(*left) - fields.find(*right);
}


bool UsedFieldSet::contains(IHqlExpression & field) const
{
    if (all)
        return true;
#ifdef USE_IPROJECT_HASH
    return hash.find(&field) != NULL;
#else
    return fields.contains(field);
#endif
}

void UsedFieldSet::getFields(HqlExprArray & target) const
{
    appendArray(target, fields);
}

void UsedFieldSet::getText(StringBuffer & s) const
{
    if (all)
        s.append("ALL");

    s.append("[");
    ForEachItemIn(i, fields)
    {
        if (i) s.append(",");
        s.append(fields.item(i).queryName());
    }
    s.append("]");
}


void UsedFieldSet::intersectFields(const UsedFieldSet & source)
{
    if (source.includeAll())
        return;

    if (includeAll())
        set(source);
    else
    {
        ForEachItemInRev(i, fields)
        {
            IHqlExpression & field = fields.item(i);
            if (!source.contains(field))
            {
                fields.remove(i);
#ifdef USE_IPROJECT_HASH
                hash.remove(&field);
#endif
            }
        }
    }
}


void UsedFieldSet::kill()
{
#ifdef USE_IPROJECT_HASH
    hash.kill();
#endif
    fields.kill();
}

void UsedFieldSet::set(const UsedFieldSet & source)
{
    all = source.all;
    kill();
    cloneFields(source);
}

void UsedFieldSet::setAll(IHqlExpression * record)
{
    if (all)
        return;

    all = true;
    kill();
    unwindFields(fields, record);
}


void UsedFieldSet::sort(ICompare & compare)
{
    qsortvec((void * *)fields.getArray(), fields.ordinality(), compare);
}

//---------------------------------------------------------


static unsigned getActivityCost(IHqlExpression * expr, ClusterType targetClusterType)
{
    switch (targetClusterType)
    {
    case ThorCluster:
    case ThorLCRCluster:
        {
            switch (expr->getOperator())
            {
            case no_sort:
                if (!expr->hasProperty(localAtom))
                    return CostNetworkCopy;
                return CostManyCopy;
            case no_group:
                if (!expr->hasProperty(localAtom))
                    return CostNetworkGroup;
                break;
            case no_keyeddistribute:
            case no_distribute:
            case no_cosort:
                return CostNetworkCopy;
            case no_topn:
                if (!expr->hasProperty(localAtom))
                    return CostGlobalTopN;
                break;
            case no_selfjoin:
                if (!expr->hasProperty(localAtom))
                    return CostNetworkCopy;
                break;
            case no_denormalize:
            case no_denormalizegroup:
            case no_join:
            case no_joincount:
                if (!expr->hasProperty(localAtom))
                {
                    if (isKeyedJoin(expr))
                        break;
                    if (expr->hasProperty(lookupAtom))
                        return CostNetworkCopy/2;       //insert on rhs.
                    return CostNetworkCopy;
                }
                break;
            //case no_dedup:  all non local, may be worth it..
            }
        }
    }
    return 0;
}

//MORE: Should cache this in the extra for a record, quite possibly with the unwound fields as well.
bool isSensibleRecord(IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            if (!isSensibleRecord(cur))
                return false;
            break;
        case no_ifblock:
            return false;
        case no_field:
            //Could loosen this condition so that it didn't use any fields within the record.
            switch (cur->queryType()->getTypeCode())
            {
            case type_alien:
                return false;
            case type_table:
            case type_groupedtable:
                {
                    //disqualify datasets with no_selfref counts/lengths
                    IHqlExpression * limit = cur->queryProperty(countAtom);
                    if (!limit)
                        limit = cur->queryProperty(sizeAtom);
                    if (limit && !limit->isConstant())
                        return false;
                    break;
                }
            }
            
            break;
        }
    }
    return true;
}

inline bool processMatchingSelector(UsedFieldSet & fields, IHqlExpression * select, IHqlExpression * ds)
{
    if (select == ds)
    {
        fields.setAll();
        return true;
    }
    IHqlExpression * curDs = select->queryChild(0);
    if (curDs == ds)
        fields.addUnique(select->queryChild(1));
    return false;
}

IHqlExpression * queryRootSelector(IHqlExpression * select)
{
    loop
    {
        if (select->hasProperty(newAtom))
            return select;
        IHqlExpression * ds = select->queryChild(0);
        if (ds->getOperator() != no_select)
            return select;
        select = ds;
    }
}

IHqlExpression * queryTransformAssign(IHqlExpression * transform, IHqlExpression * searchField)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            {
                IHqlExpression * ret = queryTransformAssign(cur, searchField);
                if (ret)
                    return ret;
                break;
            }
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0)->queryChild(1);
                if (lhs == searchField)
                    return cur->queryChild(1);
                break;
            }
        }
    }
    return NULL;
}

static node_operator queryCompoundOp(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_table:
        return no_compound_diskread;
    case no_newkeyindex:
        return no_compound_indexread;
    case no_preservemeta:
        return queryCompoundOp(expr->queryChild(0));
    }
    throwUnexpectedOp(expr->getOperator());
}

static int compareHqlExprPtr(IInterface * * left, IInterface * * right) 
{
    return *left == *right ? 0 : *left < *right ? -1 : +1;
}

//------------------------------------------------------------------------

ImplicitProjectInfo::ImplicitProjectInfo(IHqlExpression * _original, ProjectExprKind _kind) : MergingTransformInfo(_original), kind(_kind)
{
    visited = false;
    gatheredSelectsUsed = false;

//The following logically belong to the complexProjectInfo, see note in header
    childDatasetType = getChildDatasetType(original);
    canOptimize = true;
    insertProject = false;
    alreadyInScope = false;
    canReorderOutput = true;
    calcedReorderOutput = false;
    visitedAllowingActivity = false;
}


void ImplicitProjectInfo::addActiveSelect(IHqlExpression * select)
{
    if (selectsUsed.find(*select) == NotFound)
        selectsUsed.append(*select);
}

void ImplicitProjectInfo::addActiveSelects(const SelectUsedArray & src)
{
    unsigned numSrc = src.ordinality();
    if (numSrc == 0)
        return;
    if (selectsUsed.ordinality() == 0)
    {
        //No need to check for pre-existence, can be significant
        selectsUsed.ensure(numSrc);
        for (unsigned i=0; i < numSrc; i++)
            selectsUsed.append(src.item(i));
    }
    else
    {
        for (unsigned i=0; i < numSrc; i++)
            addActiveSelect(&src.item(i));
    }
}


void ImplicitProjectInfo::removeProductionSelects()
{
    ForEachItemInRev(i, selectsUsed)
    {
        IHqlExpression & cur = selectsUsed.item(i);
        if ((cur.getOperator() == no_matchattr) || (cur.queryChild(0)->getOperator() == no_matchattr))
            selectsUsed.remove(i);
    }
}


void ImplicitProjectInfo::removeScopedFields(IHqlExpression * selector)
{
    ForEachItemInRev(i, selectsUsed)
    {
        IHqlExpression & cur = selectsUsed.item(i);
        if ((&cur == selector) || (cur.queryChild(0) == selector))
            selectsUsed.remove(i);
    }
}


void ImplicitProjectInfo::removeRowsFields(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right)
{
    node_operator rowsSide = queryHasRows(expr);
    if (rowsSide == no_none)
        return;

    IHqlExpression * rowsid = expr->queryProperty(_rowsid_Atom);
    switch (rowsSide)
    {
    case no_left:
        {
            OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(left), LINK(rowsid));
            removeScopedFields(rowsExpr);
            break;
        }
    case no_right:
        {
            OwnedHqlExpr rowsExpr = createDataset(no_rows, LINK(right), LINK(rowsid));
            removeScopedFields(rowsExpr);
            break;
        }
    default:
        throwUnexpectedOp(rowsSide);
    }
}


//------------------------------------------------------------------------

ComplexImplicitProjectInfo::ComplexImplicitProjectInfo(IHqlExpression * _original, ProjectExprKind _kind) : ImplicitProjectInfo(_original, _kind) 
{
    outputInfo = NULL;
}


bool ComplexImplicitProjectInfo::addOutputField(IHqlExpression * field)
{
    if (!outputFields.contains(*field))
    {
        if (outputFields.ordinality() + 1 == outputInfo->outputFields.ordinality())
            addAllOutputs();
        else
            outputFields.append(*LINK(field));
        return true;
    }
    return false;
}

void ComplexImplicitProjectInfo::addAllOutputs()
{
    if (!outputFields.includeAll())
    {
        IHqlExpression * record = original->queryRecord();
        if (record)
        {
            outputFields.setAll(record);
            if (safeToReorderOutput() && okToOptimize())
                newOutputRecord.setown(getPackedRecord(record));
            else
                newOutputRecord.set(record);
        }
    }
}


IHqlExpression * ComplexImplicitProjectInfo::createOutputProject(IHqlExpression * ds)
{
    OwnedHqlExpr seq = createSelectorSequence();
    OwnedHqlExpr left = createSelector(no_left, ds, seq);
    OwnedHqlExpr self = getSelf(newOutputRecord);
    HqlExprArray assigns;
    ForEachItemIn(i, outputFields)
    {
        IHqlExpression * cur = &outputFields.item(i);
        assigns.append(*createAssign(createSelectExpr(LINK(self), LINK(cur)), createSelectExpr(LINK(left), LINK(cur))));
    }
    IHqlExpression * transform = createValue(no_transform, makeTransformType(newOutputRecord->getType()), assigns);
    return createDataset(no_hqlproject, LINK(ds), createComma(transform, LINK(seq)));
}


int ComplexImplicitProjectInfo::docompare(const void * l,const void * r) const
{
    IHqlExpression * lExpr = (IHqlExpression *)l;
    IHqlExpression * rExpr = (IHqlExpression *)r;
    return outputFields.compareOrder(lExpr, rExpr);
}


void ComplexImplicitProjectInfo::ensureOutputNotEmpty()
{
    if (outputInfo && (outputFields.ordinality() == 0))
    {
        //MORE: Sometimes this can pull in other data from upstream activities - should pick one that is already required if
        //there are any.  e.g., count(ds(x=0)) should pick field x.
#if 1
        IHqlExpression * best = &outputInfo->outputFields.item(0);
#else
        //Looks good, but in first field is more often used by something else. so disable...
        //choose the smallest field at a fixed offset
        IHqlExpression * best = NULL;
        unsigned bestSize = UNKNOWN_LENGTH;
        ForEachItemIn(i, outputInfo->outputFields)
        {
            IHqlExpression & cur = outputInfo->outputFields.item(i);
            ITypeInfo * curType = cur.queryType();
            type_t tc = curType->getTypeCode();
            //try not to select record fields - they tend to have 0 length size
            size32_t curSize = (tc == type_row) ? UNKNOWN_LENGTH-1 : curType->getSize();
            if (!best)
            {
                best = &cur;
                bestSize = curSize;
            }
            else if (bestSize > curSize)
            {
                switch (tc)
                {
                case type_bitfield:
                case type_alien:
                case type_row:
                    //avoid these if at all possible....
                    break;
                default:
                    best = &cur;
                    bestSize = curSize;
                    break;
                }
            }
            if (curSize == UNKNOWN_LENGTH)
                break;
        }
#endif

        outputFields.append(OLINK(*best));
    }
}

void ComplexImplicitProjectInfo::finalizeOutputRecord()
{
    //MORE: Create them in the same order as the original record + don't change if numOutputFields = numOriginalOutputFields
    if (!newOutputRecord)
    {
        HqlExprArray recordFields;
        IHqlExpression * oldRecord = original->queryRecord();
        if (outputInfo)
        {
            if (outputFields.ordinality() == outputInfo->outputFields.ordinality())
            {
                addAllOutputs();
                return;
            }

            outputFields.sort(*outputInfo);
            assertex(outputFields.ordinality() != 0);

            outputFields.getFields(recordFields);

            //Ensure that maxSize is set on the new record - if necessary
            OwnedHqlExpr newRecord = createRecord(recordFields);
            //optionally? pack the record so that it is in the optimal alignment order
            //assertex(safeToReorderOutput());
            if (safeToReorderOutput() && okToOptimize())
                newRecord.setown(getPackedRecord(newRecord));

            OwnedHqlExpr serializedRecord = getSerializedForm(newRecord);
            if (maxRecordSizeUsesDefault(serializedRecord))
            {
                //Lost some indication of the record size->add an attribute
                IHqlExpression * max = oldRecord->queryProperty(maxLengthAtom);
                if (max)
                    recordFields.append(*LINK(max));
                else
                {
                    bool isKnownSize, useDefaultRecordSize;
                    OwnedHqlExpr oldSerializedRecord = getSerializedForm(oldRecord);
                    unsigned oldRecordSize = getMaxRecordSize(oldSerializedRecord, 0, isKnownSize, useDefaultRecordSize);
                    if (!useDefaultRecordSize)
                        recordFields.append(*createAttribute(maxLengthAtom, getSizetConstant(oldRecordSize)));
                }
            }
            else
                newOutputRecord.set(newRecord);
        }
        else
            outputFields.getFields(recordFields);

        if (!newOutputRecord)
            newOutputRecord.setown(createRecord(recordFields));
    }
}


unsigned ComplexImplicitProjectInfo::queryCostFactor(ClusterType targetClusterType)
{
    //MORE: Could cache the value, but this option isn't really used, and not called a lot.
    return getActivityCost(original, targetClusterType);
}

void ComplexImplicitProjectInfo::stopOptimizeCompound(bool cascade)
{
    if (cascade)
    {
        canOptimize = false;
        ForEachItemIn(i, inputs)
            inputs.item(i).stopOptimizeCompound(cascade);
    }
    else if (kind == CompoundableActivity)
        canOptimize = false;
}

void ComplexImplicitProjectInfo::trace()
{
    StringBuffer s;
    if (original->queryName())
        s.append(original->queryName()).append(" := ");
    s.append(getOpString(original->getOperator()));
    DBGLOG("%s", s.str());

    switch (childDatasetType)
    {
    case childdataset_none: 
    case childdataset_addfiles:
    case childdataset_merge:
    case childdataset_if:
    case childdataset_case:
    case childdataset_map:
    case childdataset_dataset_noscope: 
    case childdataset_nway_left_right:
        break;
    case childdataset_dataset:
    case childdataset_datasetleft:
    case childdataset_top_left_right:
    case childdataset_same_left_right:
        trace("input", leftFieldsRequired);
        break;
    case childdataset_left:
        trace("left", leftFieldsRequired);
        break;
    case childdataset_leftright: 
        trace("left", leftFieldsRequired);
        trace("right", rightFieldsRequired);
        break;
    }

    trace("output", outputFields);
}

void ComplexImplicitProjectInfo::trace(const char * label, const UsedFieldSet & fields)
{
    StringBuffer s;
    s.append("  ").append(label).append(": ");
    fields.getText(s);

    DBGLOG("%s", s.str());
}


void ComplexImplicitProjectInfo::inheritRequiredFields(UsedFieldSet * requiredList)
{
    if (!requiredList || requiredList->includeAll())
        addAllOutputs();
    else if (!outputFields.includeAll())
    {
        ForEachItemIn(i, *requiredList)
            addOutputField(&requiredList->item(i));
    }
}

void ComplexImplicitProjectInfo::notifyRequiredFields(ComplexImplicitProjectInfo * whichInput)
{
    if (activityKind() == PassThroughActivity)
    {
        whichInput->inheritRequiredFields(&outputFields);
    }
    else if (original->getOperator() == no_fetch)
    {
        assertex(whichInput == &inputs.item(0));
        whichInput->inheritRequiredFields(&rightFieldsRequired);
    }
    else if (whichInput == &inputs.item(0))
    {
        whichInput->inheritRequiredFields(&leftFieldsRequired);
        //can occur if same dataset is used for left and right - e.g., non-symmetric self join
        if ((inputs.ordinality() > 1) && (whichInput == &inputs.item(1)))
            whichInput->inheritRequiredFields(&rightFieldsRequired);
    }
    else if (whichInput == &inputs.item(1))
    {
        whichInput->inheritRequiredFields(&rightFieldsRequired);
    }
    else if (inputs.contains(*whichInput))
        whichInput->addAllOutputs();
    else
        throwUnexpected();
}


bool ComplexImplicitProjectInfo::safeToReorderOutput()
{
    if (!calcedReorderOutput)
    {
        canReorderOutput = true;
        switch (activityKind())
        {
        case FixedInputActivity:
            //can occur with weird operations in the middle of a dataset. Should probably only set if an action.
            canReorderOutput = false;
            break;
        default:
            ForEachItemIn(i, outputs)
            {
                if (!outputs.item(i).safeToReorderInput())
                {
                    canReorderOutput = false;
                    break;
                }
            }
            break;
        }

        calcedReorderOutput = true;
    }
    return canReorderOutput;
}


bool ComplexImplicitProjectInfo::safeToReorderInput()
{
    switch (activityKind())
    {
    case CreateRecordActivity:
    case CreateRecordLRActivity:
    case ScalarSelectActivity:
        //These activities have remove the constraints of the inputs on their outputs.
        return true;
    case FixedInputActivity:
        return false;
    }
    return safeToReorderOutput();
}


void ComplexImplicitProjectInfo::setMatchingOutput(ComplexImplicitProjectInfo * other)
{
    assertex(other->newOutputRecord != NULL);
    outputFields.set(other->outputFields);
    newOutputRecord.set(other->newOutputRecord);
}


//-----------------------------------------------------------------------------------------------

static HqlTransformerInfo implicitProjectTransformerInfo("ImplicitProjectTransformer");
ImplicitProjectTransformer::ImplicitProjectTransformer(HqlCppTranslator & _translator, bool _optimizeSpills) 
: MergingHqlTransformer(implicitProjectTransformerInfo), translator(_translator)
{
    const HqlCppOptions & transOptions = translator.queryOptions();
    targetClusterType = translator.getTargetClusterType();
    options.isRoxie = (targetClusterType == RoxieCluster);
    options.optimizeProjectsPreservePersists = transOptions.optimizeProjectsPreservePersists;
    options.autoPackRecords = transOptions.autoPackRecords;
    options.notifyOptimizedProjects = translator.notifyOptimizedProjectsLevel();
    options.optimizeSpills = _optimizeSpills;
    options.enableCompoundCsvRead = translator.queryOptions().enableCompoundCsvRead;
    allowActivity = true;
    options.insertProjectCostLevel = 0;
    if (transOptions.reduceNetworkTraffic)
        options.insertProjectCostLevel = (transOptions.insertProjectCostLevel != (unsigned)-1) ? transOptions.insertProjectCostLevel : CostNetworkCopy;
}



void ImplicitProjectTransformer::analyseExpr(IHqlExpression * expr)
{
    ImplicitProjectInfo * extra = queryBodyExtra(expr);
    ComplexImplicitProjectInfo * complexExtra = extra->queryComplexInfo();
    if (complexExtra)
    {
        if (complexExtra->alreadyInScope)
            return;
        if (!options.autoPackRecords)
            complexExtra->setReorderOutput(false);
        if (extra->checkAlreadyVisited())
        {
            //Don't allow modification if referenced from activity and non-activity context
            if (allowActivity)
            {
                if (extra->activityKind() != NonActivity)
                    return;
                //either allowed before, but tagged as a non
                //If previously this was called in an allowactivity context it must have been explicitly disabled, so no point recursing.
                if (complexExtra->visitedAllowingActivity)
                    return;
                //otherwise, probably worth recursing again...
                extra->preventOptimization();
            }
            else
            {
                extra->preventOptimization();
                return;
            }
        }

        if (allowActivity)
            complexExtra->visitedAllowingActivity = true;
    }
    else
    {
        if (extra->checkAlreadyVisited())
            return;
    }

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_record:
        {
            complexExtra->outputFields.setAll(expr);
            return;
        }
    case no_constant:
    case no_attr:
        return;
    case no_transform:
    case no_newtransform:
    case no_transformlist:
    case no_list:
        if (expr->isConstant())
            return;
        break;
    }

    ITypeInfo * type = expr->queryType();
    if (allowActivity)
    {
        switch (op)
        {
        case no_evaluate:
            throwUnexpected();
        case no_select:
            if (expr->isDataset() || expr->isDatarow())
            {
                //MORE: These means that selects from a parent dataset don't project down the parent dataset.
                //I'm not sure how big an issue that would be.
                allowActivity = false;
                Parent::analyseExpr(expr);
                allowActivity = true;
                assertex(extra->activityKind() == SourceActivity);
                activities.append(*LINK(expr));
                IHqlExpression * record = expr->queryRecord();
                complexExtra->outputInfo = queryBodyComplexExtra(record);
                analyseExpr(record);
            }
            else if (isNewSelector(expr))
            {
                Parent::analyseExpr(expr);
                assertex(extra->activityKind() == ScalarSelectActivity);
                if (expr->hasProperty(newAtom))
                    connect(expr->queryChild(0), expr);
                activities.append(*LINK(expr));
            }
            return;
        case no_activerow:
            assertex(extra->activityKind() == SimpleActivity);
            allowActivity = false;
            Parent::analyseExpr(expr);
            allowActivity = true;
            activities.append(*LINK(expr));
            return;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            allowActivity = false;
            Parent::analyseExpr(expr);
            allowActivity = true;
            return;
        case no_thor:
            if (expr->isDataset() || expr->isDatarow())
            {
                assertex(extra->activityKind() == SimpleActivity);
                Parent::analyseExpr(expr);
                connect(expr->queryChild(0), expr);
            }
            else
            {
                assertex(extra->activityKind() == NonActivity);
                Parent::analyseExpr(expr);
            }
            break;
        case no_compound:
            if (expr->isDataset())
            {
                assertex(extra->activityKind() == SimpleActivity);
                Parent::analyseExpr(expr);
                connect(expr->queryChild(1), expr);
                break;
            }
            assertex(extra->activityKind() == NonActivity);
            Parent::analyseExpr(expr);
            break;
        case no_subgraph:
            assertex(extra->activityKind() == NonActivity);
            Parent::analyseExpr(expr);
            break;
        case no_libraryselect:
            assertex(extra->activityKind() == SourceActivity);
            analyseExpr(expr->queryChild(1));
            break;
        case no_libraryscopeinstance:
            {
                assertex(extra->activityKind() == NonActivity);
                ForEachChild(i, expr)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    if (cur->isDataset())
                    {
                        analyseExpr(cur);
                        queryBodyExtra(cur)->preventOptimization();
                    }
                }
                break;
            }
        case no_mergejoin:
        case no_nwayjoin:           // could probably project output of this one...
        case no_nwaymerge:
            {
                assertex(extra->activityKind() == SourceActivity);
                //Don't allow any of the inputs to be optimized - otherwise the can end up with inconsistent record types
                allowActivity = false;
                Parent::analyseExpr(expr->queryChild(0));
                allowActivity = true;
                break;
            }
        case no_setresult:
        case no_ensureresult:
            {
                IHqlExpression * value = expr->queryChild(0);
                if (value->isDataset() || value->isDatarow())// || value->isList())
                {
                    assertex(extra->activityKind() == FixedInputActivity);
                    analyseExpr(value);
                    //no need to analyse other fields since they are all constant
                    connect(value, expr);
                }
                else
                {
                    assertex(extra->activityKind() == NonActivity);
                    Parent::analyseExpr(expr);
                }
                break;
            }
        case no_newtransform:
        case no_transform:
        case no_transformlist:
            assertex(extra->kind == NonActivity);
            if (!expr->isConstant())
                Parent::analyseExpr(expr);
            return;
        default:
            {
                if (!expr->isAction() && !expr->isDataset() && !expr->isDatarow())
                {
                    switch (op)
                    {
                    case no_countfile:
                    case no_countindex:
                        //MORE: Is this the best way to handle this?
                        allowActivity = false;
                        Parent::analyseExpr(expr);
                        allowActivity = true;
                        return;
                    case NO_AGGREGATE:
                    case no_call:
                    case no_externalcall:
                    case no_createset:
                        break;
                    case no_sizeof:
                        //MORE: Is this the best way to handle this?
                        allowActivity = false;
                        Parent::analyseExpr(expr);
                        allowActivity = true;
                        return;
                    default:
                        extra->kind = NonActivity;
                        Parent::analyseExpr(expr);
                        return;
                    }
                }

                IHqlExpression * record = expr->queryRecord();
                if (!record && expr->queryChild(0))
                    record = expr->queryChild(0)->queryRecord();
                if (!record || !isSensibleRecord(record))
                    extra->preventOptimization();

                unsigned first = getFirstActivityArgument(expr);
                unsigned last = first + getNumActivityArguments(expr);
                unsigned numArgs = expr->numChildren();
                unsigned start = 0;
                switch (expr->getOperator())
                {
                case no_dedup:
                    if (dedupMatchesWholeRecord(expr))
                        extra->preventOptimization();
                    break;
                case no_process:
                    extra->preventOptimization();
                    break;
                case no_executewhen:
                    last = 1;
                    break;
                case no_newkeyindex:
//              case no_dataset:
                    //No point walking the transform for an index
                    start = 3;
                    numArgs = 4;
                    break;
                case no_compound_diskaggregate:
                case no_compound_diskcount:
                case no_compound_diskgroupaggregate:
                case no_compound_indexaggregate:
                case no_compound_indexcount:
                case no_compound_indexgroupaggregate:
                    //walk inside these... they're not compoundable, but they may be able to lose some fields from the transform.
                    last = 1;
                    break;
                }

                for (unsigned i =start; i < numArgs; i++)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    allowActivity = (i >= first) && (i < last);
                    analyseExpr(cur);
                    if (allowActivity && !cur->isAction() && !cur->isAttribute())
                    {
                        assertex(queryBodyExtra(cur)->activityKind() != NonActivity);
                        connect(cur, expr);
                    }
                }
                allowActivity = true;
            }
        }
    }
    else
    {
        extra->preventOptimization();
        switch (op)
        {
        case no_attr_expr:
            analyseChildren(expr);
            break;
        case no_newkeyindex:
//      case no_sizeof:
            //no point analysing parameters to keyed joins
            break;
        default:
            Parent::analyseExpr(expr);
        }
    }

    //Add activities in depth first order, so traversing them backwards is guaranteed to be top down.
    if (extra->activityKind() != NonActivity)
    {
        assertex(complexExtra);
        switch (extra->activityKind())
        {
        case CreateRecordActivity:
        case CreateRecordLRActivity:
        case TransformRecordActivity:
        case DenormalizeActivity:
        case CreateRecordSourceActivity:
            if (hasUnknownTransform(expr))
                complexExtra->preventOptimization();
            break;
        }

        activities.append(*LINK(expr));
    }

    IHqlExpression * record = expr->queryRecord();
    if (record && !isPatternType(type))
    {
        assertex(complexExtra);
        complexExtra->outputInfo = queryBodyComplexExtra(record);
        analyseExpr(record);
    }

    gatherFieldsUsed(expr, extra);
}


void ImplicitProjectTransformer::connect(IHqlExpression * source, IHqlExpression * sink)
{
    queryBodyComplexExtra(source)->outputs.append(*queryBodyComplexExtra(sink));
    queryBodyComplexExtra(sink)->inputs.append(*queryBodyComplexExtra(source));
}


//NB: This is very similar to the code in CHqlExpression::cacheTablesUsed()

void ImplicitProjectTransformer::gatherFieldsUsed(IHqlExpression * expr, ImplicitProjectInfo * extra)
{
    if (extra->checkGatheredSelects())
        return;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_select:
        {
            //Either inherit from the dataset if new, or add the root field (x.a.b only adds x.a)
            IHqlExpression * cur = expr;
            loop
            {
                IHqlExpression * ds = cur->queryChild(0);
                if (cur->hasProperty(newAtom))
                {
                    inheritActiveFields(extra, ds);
                    break;
                }
                node_operator dsOp = ds->getOperator();
                if (dsOp != no_select || ds->isDataset())
                {
                    if ((dsOp != no_self) && (dsOp != no_selfref))
                        extra->addActiveSelect(cur);
                    break;
                }
                cur = ds;
            }
            break;
        }
    case no_activerow:
        //active row used in some context
        extra->addActiveSelect(expr->queryChild(0));
        break;
    case no_left:
    case no_right:
        extra->addActiveSelect(expr);
        //left/right used in an expression context - assume the worse..
        break;
    case no_attr:
    case no_attr_link:
    case no_getresult:
        break;

    case no_attr_expr:
        {
            _ATOM name = expr->queryName();
            if (name != _selectors_Atom)
                inheritActiveFields(expr, extra, 0, expr->numChildren());
        }
        break;
    case no_newkeyindex:
        {
#ifdef _DEBUG
            inheritActiveFields(expr, extra, 1, expr->numChildren());
            extra->removeScopedFields(expr->queryChild(0)->queryNormalizedSelector());
            extra->removeScopedFields(queryActiveTableSelector());      // for distributed() etc,
            inheritActiveFields(expr, extra, 0, 1);
            const SelectUsedArray & selectsUsed = extra->querySelectsUsed();
            if (selectsUsed.ordinality() != 0)
            {
                StringBuffer s;
                ForEachItemIn(i, selectsUsed)
                {
                    if (i) s.append(',');
                    getExprECL(&selectsUsed.item(i), s);
                }
                throwError1(HQLERR_IndexHasActiveFields, s.str());
            }
#else
            inheritActiveFields(expr, extra, 3, 4);         // just in case the filename is based on a parent row???
#endif
            break;
        }
    case no_pat_production:
        {
            inheritActiveFields(expr, extra, 0, expr->numChildren());
            extra->removeProductionSelects();
            break;
        }
    case no_assign:
        inheritActiveFields(expr, extra, 1, 2);
        break;
    /*
    The following can be handled using the default mechanism because we're not tracking newtables.
    case NO_AGGREGATE:
    case no_countindex:
    case no_countfile:
    case no_createset:
    */
    case no_table:
        {
            inheritActiveFields(expr, extra, 0, expr->numChildren());
            IHqlExpression * parent = expr->queryChild(3);
            if (parent)
                extra->removeScopedFields(parent->queryNormalizedSelector());
            break;
        }
    default:
        {
#if 0
            //Optimization to enable later - if no active datasets, then can't have any active fields.
            //should save some processing on root datasets, but may be insignificant
            if (isIndependentOfScope(expr))
                break;
#endif
            unsigned max = expr->numChildren();
            IHqlExpression * ds = expr->queryChild(0);
            switch (getChildDatasetType(expr))
            {
            case childdataset_none: 
            case childdataset_addfiles:
            case childdataset_if:
            case childdataset_case:
            case childdataset_map:
            case childdataset_dataset_noscope: 
                inheritActiveFields(expr, extra, 0, max);
                //None of these have any scoped arguments, so no need to remove them
                break;
            case childdataset_merge:
                {
                    unsigned firstAttr = getNumChildTables(expr);
                    inheritActiveFields(expr, extra, firstAttr, max);
                    extra->removeScopedFields(queryActiveTableSelector());
                    inheritActiveFields(expr, extra, 0, firstAttr);
                    break;
                }
            case childdataset_dataset:
                {
                    inheritActiveFields(expr, extra, 1, max);
                    extra->removeScopedFields(ds->queryNormalizedSelector());
                    inheritActiveFields(expr, extra, 0, 1);
                }
                break;
            case childdataset_datasetleft:
                {
                    OwnedHqlExpr left = createSelector(no_left, ds, querySelSeq(expr));
                    inheritActiveFields(expr, extra, 1, max);
                    extra->removeScopedFields(left);
                    extra->removeScopedFields(ds->queryNormalizedSelector());
                    extra->removeRowsFields(expr, left, NULL);
                    inheritActiveFields(expr, extra, 0, 1);
                    break;
                }
            case childdataset_left:
                {
                    OwnedHqlExpr left = createSelector(no_left, ds, querySelSeq(expr));
                    inheritActiveFields(expr, extra, 1, max);
                    extra->removeScopedFields(left);
                    extra->removeRowsFields(expr, left, NULL);
                    inheritActiveFields(expr, extra, 0, 1);
                    break;
                }
            case childdataset_same_left_right:
            case childdataset_nway_left_right:
                {
                    IHqlExpression * seq = querySelSeq(expr);
                    OwnedHqlExpr left = createSelector(no_left, ds, seq);
                    OwnedHqlExpr right = createSelector(no_right, ds, seq);
                    inheritActiveFields(expr, extra, 1, max);
                    extra->removeScopedFields(left);
                    extra->removeScopedFields(right);
                    extra->removeRowsFields(expr, left, right);
                    inheritActiveFields(expr, extra, 0, 1);
                    break;
                }
            case childdataset_top_left_right:
                {
                    IHqlExpression * seq = querySelSeq(expr);
                    OwnedHqlExpr left = createSelector(no_left, ds, seq);
                    OwnedHqlExpr right = createSelector(no_right, ds, seq);
                    inheritActiveFields(expr, extra, 1, max);
                    extra->removeScopedFields(ds->queryNormalizedSelector());
                    extra->removeScopedFields(left);
                    extra->removeScopedFields(right);
                    extra->removeRowsFields(expr, left, right);
                    inheritActiveFields(expr, extra, 0, 1);
                    break;
                }
            case childdataset_leftright: 
                {
                    IHqlExpression * leftDs = expr->queryChild(0);
                    IHqlExpression * rightDs = expr->queryChild(1);
                    IHqlExpression * seq = querySelSeq(expr);
                    OwnedHqlExpr left = createSelector(no_left, leftDs, seq);
                    OwnedHqlExpr right = createSelector(no_right, rightDs, seq);
                    inheritActiveFields(expr, extra, 2, max);
                    extra->removeScopedFields(right);
                    extra->removeRowsFields(expr, left, right);
                    if (expr->getOperator() == no_normalize)
                    {
                        inheritActiveFields(expr, extra, 1, 2);
                        extra->removeScopedFields(left);
                        inheritActiveFields(expr, extra, 0, 1);
                    }
                    else
                    {
                        extra->removeScopedFields(left);
                        inheritActiveFields(expr, extra, 0, 2);
                    }
                    break;
                }
                break;
            case childdataset_evaluate:
                //handled elsewhere...
            default:
                UNIMPLEMENTED;
            }
            switch (op)
            {

            case no_newparse:
            case no_parse:
                extra->removeProductionSelects();
                break;
            }

#ifdef _DEBUG
            //MORE: This doesn't currently cope with access to parents within normalized child datasets
            //e.g, sqnormds1.hql.  
            const SelectUsedArray & selectsUsed = extra->querySelectsUsed();
            if (isIndependentOfScope(expr) && selectsUsed.ordinality() != 0)
            {
                switch (expr->getOperator())
                {
                case no_csv:
                case no_xml:
                    break;
                default:
                    {
                        StringBuffer s;
                        ForEachItemIn(i, selectsUsed)
                        {
                            if (i) s.append(',');
                            getExprECL(&selectsUsed.item(i), s);
                        }
                        throwError1(HQLERR_GlobalHasActiveFields, s.str());
                    }
                }
            }
#endif
            break;
        }
    }
}


const SelectUsedArray & ImplicitProjectTransformer::querySelectsUsed(IHqlExpression * expr) 
{ 
    ImplicitProjectInfo * extra = queryBodyExtra(expr);
//  gatherFieldsUsed(expr, extra);
    return extra->querySelectsUsed(); 
}


ProjectExprKind ImplicitProjectTransformer::getProjectExprKind(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_evaluate:
        throwUnexpected();
    case no_select:
        if (expr->isDataset() || expr->isDatarow())
            return SourceActivity;
        if (isNewSelector(expr))
            return ScalarSelectActivity;
        return NonActivity;
    case no_activerow:
        return SimpleActivity;
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return NonActivity;
    case no_typetransfer:
        if (expr->isDataset() || expr->isDatarow())
            return SourceActivity;
        return NonActivity;
    case no_thor:
        if (expr->isDataset() || expr->isDatarow())
            return SimpleActivity;
        return NonActivity;
    case no_compound:
        if (expr->isDataset())
            return SimpleActivity;
        if (expr->isDatarow())
            return ComplexNonActivity;
        return NonActivity;
    case no_subgraph:
    case no_libraryscopeinstance:
        return NonActivity;
    case no_mergejoin:
    case no_nwayjoin:           // could probably project output of this one...
    case no_nwaymerge:
    case no_libraryselect:
        return SourceActivity;
    case no_setresult:
    case no_ensureresult:
        {
            IHqlExpression * value = expr->queryChild(0);
            if (value->isDataset() || value->isDatarow())
                return FixedInputActivity;
            return NonActivity;
        }
    case no_newrow:         //only used while transforming
    case no_newaggregate:
    case no_hqlproject:
    case no_normalize:
    case no_newusertable:
    case no_newparse:
    case no_newxmlparse:
    case no_createrow:
        return CreateRecordActivity;
    case no_inlinetable:
        return CreateRecordSourceActivity;
    case no_extractresult:
        return ActionSinkActivity;
    case no_denormalizegroup:
    case no_join:
    case no_fetch:
        return CreateRecordLRActivity;
    case no_process:            // optimization currently disabled...
        return PassThroughActivity;
    case no_iterate:
    case no_rollup:
        return TransformRecordActivity;
    case no_denormalize:
        return DenormalizeActivity;
    case no_null:
        if (expr->isAction())
            return NonActivity;
        return AnyTypeActivity;
    case no_skip:
    case no_fail:
        if (expr->isDataset() || expr->isDatarow())
            return AnyTypeActivity;
        return NonActivity;
    case no_table:
        switch (expr->queryChild(2)->getOperator())
        {
        case no_thor:
        case no_flat:
            if (expr->hasProperty(_spill_Atom) && options.isRoxie)
                return SourceActivity;
            if (options.optimizeProjectsPreservePersists)
            {
                //Don't project persists because it can mess up the redistibution code.
                if (expr->hasProperty(_workflowPersist_Atom))
                    return SourceActivity;
            }
            return CompoundableActivity;
        case no_csv:
            if (options.enableCompoundCsvRead)
                return CompoundableActivity;
            return SourceActivity;
        default:
            return SourceActivity;
        }
    case no_newkeyindex:
        //Not compoundable for the moment - because it causes problems with assertwild(all, <old-expression>)
        return SourceActivity;//CompoundableActivity;
    case no_compound_diskread:
    case no_compound_disknormalize:
    case no_compound_indexread:
    case no_compound_indexnormalize:
        {
            if (options.optimizeProjectsPreservePersists)
            {
                //Don't project persists because it can mess up the redistibution code.
                IHqlExpression * root = queryRoot(expr);
                if (root && root->hasProperty(_workflowPersist_Atom))
                    return SourceActivity;
            }
            return CompoundActivity;
        }
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
        //Don't want to add projects to these...
        return SimpleActivity;
    case no_preload:
    case no_forcelocal:
    case no_workunit_dataset:
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_rows:
    case no_libraryinput:
    case no_allnodes:
        return SourceActivity;
    case no_thisnode:
        if (expr->isDataset() || expr->isDatarow())
            return SourceActivity;
        return NonActivity;
    case no_pipe:
    case no_httpcall:
    case no_soapcall:
    case no_soapcall_ds:
    case no_newsoapcall:
    case no_newsoapcall_ds:
    case no_output:
    case no_distribution:
    case no_buildindex:
    case no_nofold:
    case no_nohoist:
    case no_spill:
    case no_setgraphresult:
    case no_setgraphloopresult:
    case no_spillgraphresult:
        //MORE: Rethink these later:
    case no_combine:
    case no_combinegroup:
    case no_rollupgroup:
    case no_regroup:
    case no_loop:
    case no_loop2:
    case no_graphloop:
    case no_filtergroup:            //anything else would be tricky...
    case no_normalizegroup:
        return FixedInputActivity;
    case no_aggregate:
        if (expr->hasProperty(mergeAtom))
            return FixedInputActivity;
        return FixedInputActivity;  //MORE:TransformRecordActivity
    case no_fromxml:                // A bit bit like a source activity, no transform..., but has an input
        return FixedInputActivity;
    case no_selfjoin:
        return CreateRecordActivity;
    case no_if:
        if (expr->isDataset())
            return PassThroughActivity;
        if (expr->isDatarow())
            return ComplexNonActivity;
        return NonActivity;
    case no_addfiles:
    case no_merge:
    case no_nonempty:
    case no_cogroup:
        return PassThroughActivity;
    case no_keydiff:
        return NonActivity;
    case no_datasetfromrow:
        if (getNumActivityArguments(expr) == 0)
            return SourceActivity;
        return SimpleActivity;
    case no_newtransform:
    case no_transform:
        return ComplexNonActivity;
    case no_record:
    case no_assign:
    case no_assignall:
        return NonActivity;
    case NO_AGGREGATE:
//  case no_countfile:
//  case no_countindex:
    case no_createset:
        return SimpleActivity;
    case no_call:
    case no_externalcall:
        return SourceActivity;
    case no_commonspill:
    case no_readspill:
        return SimpleActivity;
    case no_writespill:
        return ActionSinkActivity;
    case no_preservemeta:
        if (getProjectExprKind(expr->queryChild(0)) == CompoundableActivity)
            return CompoundableActivity;
        return PassThroughActivity;
    }

    ITypeInfo * type = expr->queryType();
    if (!type)
        return NonActivity;

    type_t tc = type->getTypeCode();
    switch (tc)
    {
    case type_void:
    case type_row:
    case type_table:
    case type_groupedtable:
    case type_transform:
        break;
    default:
        return NonActivity;
    }

    if (getChildDatasetType(expr) == childdataset_none)
        return FixedInputActivity;
    if (getNumActivityArguments(expr) == 0)
        return SourceActivity;
    return SimpleActivity;
}

void ImplicitProjectTransformer::processSelectsUsedForCreateRecord(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    ForEachItemIn(i2, selectsUsed)
    {
        IHqlExpression * curSelect = &selectsUsed.item(i2);
        if (leftSelect)
            processMatchingSelector(extra->leftFieldsRequired, curSelect, leftSelect);
        if (ds)
            processMatchingSelector(extra->leftFieldsRequired, curSelect, ds);
        if (rightSelect)
            processMatchingSelector(extra->rightFieldsRequired, curSelect, rightSelect);
    }
}


void ImplicitProjectTransformer::processSelectsUsedForDenormalize(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    ForEachItemIn(i, selectsUsed)
    {
        IHqlExpression * curSelect = &selectsUsed.item(i);
        if (curSelect->queryChild(0) == leftSelect)
            extra->addOutputField(curSelect->queryChild(1));

        processMatchingSelector(extra->leftFieldsRequired, curSelect, leftSelect);
        processMatchingSelector(extra->rightFieldsRequired, curSelect, rightSelect);
    }
}

void ImplicitProjectTransformer::processSelectsUsedForTransformRecord(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    ForEachItemIn(i2, selectsUsed)
    {
        IHqlExpression * curSelect = &selectsUsed.item(i2);
        if (curSelect == leftSelect || curSelect == rightSelect || curSelect == ds)
        {
            extra->leftFieldsRequired.setAll();
            break;
        }
        IHqlExpression * curDs = curSelect->queryChild(0);
        if ((curDs == leftSelect) || (curDs == rightSelect) || (curDs == ds))
        {
            IHqlExpression * field = curSelect->queryChild(1);
            extra->leftFieldsRequired.addUnique(field);
            extra->addOutputField(field);
        }
    }
}

void ImplicitProjectTransformer::calculateFieldsUsed(IHqlExpression * expr)
{
    ComplexImplicitProjectInfo * extra = queryBodyComplexExtra(expr);

    if (!extra->okToOptimize())
    {
        extra->addAllOutputs();
    }
    else
    {
        ForEachItemIn(i1, extra->outputs)
            extra->outputs.item(i1).notifyRequiredFields(extra);

        if (extra->outputFields.includeAll())
            assertex(extra->newOutputRecord != NULL);       //extra->newOutputRecord.set(expr->queryRecord());

        //Ensure at least one field is required - otherwise meta goes wrong.  It really needs to be added here, rather than later,
        //otherwise the field tracking for iterate/rollup etc. go wrong.  Could possibly improve later if we added code to project the
        //dataset in front of a iterate/rollup
        extra->ensureOutputNotEmpty();
    }

    switch (extra->activityKind())
    {
    case CreateRecordActivity:
    case CreateRecordLRActivity:
        {
            //output will now be whatever fields are required by the output fields.
            //input will be whatever is used in the appropriate transforms.
            IHqlExpression * transform = queryNewColumnProvider(expr);
            if (!isKnownTransform(transform))
            {
                extra->leftFieldsRequired.setAll();
                extra->rightFieldsRequired.setAll();
                break;
            }

            IHqlExpression * ds = expr->queryChild(0)->queryNormalizedSelector();
            IHqlExpression * selSeq = querySelSeq(expr);
            OwnedHqlExpr dsSelect = LINK(ds);
            OwnedHqlExpr leftSelect;
            OwnedHqlExpr rightSelect;
            if (selSeq)
                leftSelect.setown(createSelector(no_left, ds, selSeq));
            if (extra->activityKind() == CreateRecordLRActivity)
                rightSelect.setown(createSelector(no_right, expr->queryChild(1), selSeq));
            if (expr->getOperator() == no_selfjoin)
                dsSelect.setown(createSelector(no_right, ds, selSeq));

            //This is here to ensure that calls that have side-effects don't get removed because the fields are removed
            if (hasSideEffects(transform))
                extra->addAllOutputs();

            //MORE: querySelectsUsedForField() could be optimized by creating a map first, but it is only ~1% of time, so not really worth it.
            ForEachItemIn(i, extra->outputFields)
            {
                IHqlExpression & cur = extra->outputFields.item(i);
                processSelectsUsedForCreateRecord(extra, querySelectsUsedForField(transform, &cur), dsSelect, leftSelect, rightSelect);
            }

            HqlExprCopyArray assigns;
            unwindTransform(assigns, transform);
            ForEachItemIn(itr, assigns)
            {
                IHqlExpression * cur = &assigns.item(itr);
                //Need to handle skip attributes...
                if (cur->getOperator() == no_skip)
                {
                    const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                    processSelectsUsedForCreateRecord(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
                }
                else if (cur->getOperator() == no_assign)
                {
                    IHqlExpression * value = cur->queryChild(1);
                    if (!value->isPure())
                    {
                        extra->addOutputField(cur->queryChild(0)->queryChild(1));
                        const SelectUsedArray & selectsUsed = querySelectsUsed(value);
                        processSelectsUsedForCreateRecord(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
                    }
                }
            }

            unsigned max = expr->numChildren();
            unsigned first = extra->inputs.ordinality();
            if (expr->getOperator() == no_fetch)
                first = 2;
            for (unsigned i2=first; i2 < max; i2++)
            {
                IHqlExpression * cur = expr->queryChild(i2);
                if (cur == transform)
                    continue;

                const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                processSelectsUsedForCreateRecord(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
            }

            switch (expr->getOperator())
            {
            case no_newusertable:
            case no_hqlproject:
                if (extra->okToOptimize())
                    extra->inputs.item(0).stopOptimizeCompound(false);
                break;
            case no_newaggregate:
                {
                    IHqlExpression * grouping = queryRealChild(expr, 3);
                    if (grouping)
                    {
                        //Need to make sure that grouping criteria fields are also in the output
                        ForEachChild(i, grouping)
                        {
                            IHqlExpression * curGrouping = grouping->queryChild(i);
                            IHqlExpression * match = NULL;
                            ForEachChild(j, transform)
                            {
                                IHqlExpression * cur = transform->queryChild(j);
                                IHqlExpression * rhs = cur->queryChild(1);
                                if (rhs->getOperator() == no_activerow)
                                    rhs = rhs->queryChild(0);
                                if (rhs == curGrouping)
                                {
                                    match = cur->queryChild(0);
                                    break;
                                }
                            }
                            assertex(match);
                            extra->addOutputField(match->queryChild(1));
                        }
                    }
                    break;
                }
            }
            break;
        }
    case CompoundActivity:
        {
            //output will now be whatever fields are required by the output fields.
            //input will be the same as the output fields, since it is just a wrapper node.
            extra->finalizeOutputRecord();
            extra->leftFieldsRequired.clone(extra->outputFields);
            extra->insertProject = true;
            assertex(extra->inputs.ordinality() == 0);
            //extra->inputs.item(0).stopOptimizeCompound(true);
            break;
        }
    case CompoundableActivity:
        {
            //Prevent preserve meta from stripping the disk read down to a single field.
            if (extra->inputs.ordinality())
                extra->inputs.item(0).stopOptimizeCompound(true);

            if (extra->okToOptimize())
                extra->finalizeOutputRecord();
            break;
        }
    case CreateRecordSourceActivity:
    case AnyTypeActivity:
        {
            if (extra->okToOptimize())
                extra->finalizeOutputRecord();
            break;
        }
    case PassThroughActivity:
        if (extra->okToOptimize())
        {
            node_operator op = expr->getOperator();
            if ((op == no_if) && expr->hasProperty(_resourced_Atom))
            {
                extra->preventOptimization();
                extra->addAllOutputs();
            }
            else if (op == no_merge)
            {
                //Ensure all the fields used by the sort order are preserved in the input streams
                IHqlExpression * selector = expr->queryChild(0)->queryNormalizedSelector();
                IHqlExpression * order = expr->queryProperty(sortedAtom);
                assertex(order);
                ForEachChild(i, order)
                {
                    IHqlExpression * cur = order->queryChild(i);
                    if (!cur->isAttribute() && !cur->isConstant())          // shouldn't really happen..
                    {
                        if (cur->getOperator() == no_select)
                        {
                            IHqlExpression * ds = cur->queryChild(0);
                            IHqlExpression * field = cur->queryChild(1);
                            if ((ds == queryActiveTableSelector()) && extra->outputInfo->outputFields.contains(*field))
                                extra->addOutputField(field);
                            else
                                extra->addAllOutputs();
                        }
                        else
                            extra->addAllOutputs();
                    }
                }
            }
        }

        //No need to do anything - inputs are taken directly from required outputs
        break;
    case ScalarSelectActivity:
        {
            IHqlExpression * root = queryRootSelector(expr);
            extra->leftFieldsRequired.append(*LINK(root->queryChild(1)));
            break;
        }
    case TransformRecordActivity:
        {
            //currently rollup and iterate
            //output record is fixed by input, and never gets changed.
            //input is all fields used required in output (since can't change record format) plus any others used inside the transform
            IHqlExpression * transform = queryNewColumnProvider(expr);
            if (hasSideEffects(transform))
                extra->addAllOutputs();

            if (extra->outputFields.includeAll())
                extra->leftFieldsRequired.setAll();
            else
            {
                IHqlExpression * ds = expr->queryChild(0)->queryNormalizedSelector();
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr leftSelect = createSelector(no_left, ds, selSeq);
                OwnedHqlExpr rightSelect = createSelector(no_right, ds, selSeq);

                //Need to handle skip attributes...
                HqlExprCopyArray assigns;
                unwindTransform(assigns, transform);
                ForEachItemIn(itr, assigns)
                {
                    IHqlExpression * cur = &assigns.item(itr);
                    if (cur->getOperator() == no_skip)
                    {
                        const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                        processSelectsUsedForTransformRecord(extra, selectsUsed, ds, leftSelect, rightSelect);
                    }
                    else if (cur->getOperator() == no_assign)
                    {
                        IHqlExpression * value = cur->queryChild(1);
                        if (!value->isPure())
                        {
                            extra->addOutputField(cur->queryChild(0)->queryChild(1));
                            const SelectUsedArray & selectsUsed = querySelectsUsed(value);
                            processSelectsUsedForTransformRecord(extra, selectsUsed, ds, leftSelect, rightSelect);
                        }
                    }
                }

                //Rollup criteria need to be included in the fields used!
                unsigned max = expr->numChildren();
                for (unsigned i2=1; i2 < max; i2++)
                {
                    if (extra->leftFieldsRequired.includeAll())
                        break;

                    IHqlExpression * cur = expr->queryChild(i2);
                    if (cur != transform)
                    {
                        const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                        processSelectsUsedForTransformRecord(extra, selectsUsed, ds, leftSelect, rightSelect);
                    }
                }

                //NB: outputfields can extend...
                for (unsigned i=0; i < extra->outputFields.ordinality(); i++)
                {
                    IHqlExpression & cur = extra->outputFields.item(i);
                    extra->leftFieldsRequired.addUnique(&cur);
                    const SelectUsedArray & selectsUsed = querySelectsUsedForField(transform, &cur);
                    processSelectsUsedForTransformRecord(extra, selectsUsed, ds, leftSelect, rightSelect);

                    if (extra->outputFields.includeAll() || extra->leftFieldsRequired.ordinality() == extra->outputInfo->outputFields.ordinality())
                        extra->leftFieldsRequired.setAll();
                    if (extra->leftFieldsRequired.includeAll())
                        break;
                }

                if (extra->leftFieldsRequired.includeAll())
                    extra->addAllOutputs();
            }
            break;
        }
    case DenormalizeActivity:
        {
            //output record is fixed by input
            //input is all fields used required in output (since can't change record format) plus any others used inside the transform
            IHqlExpression * transform = queryNewColumnProvider(expr);
            if (hasSideEffects(transform))
                extra->addAllOutputs();

            if (extra->outputFields.includeAll())
                extra->leftFieldsRequired.setAll();

            IHqlExpression * left = expr->queryChild(0)->queryNormalizedSelector();
            IHqlExpression * right = expr->queryChild(1)->queryNormalizedSelector();
            IHqlExpression * selSeq = querySelSeq(expr);
            OwnedHqlExpr leftSelect = createSelector(no_left, left, selSeq);
            OwnedHqlExpr rightSelect = createSelector(no_right, right, selSeq);

            HqlExprCopyArray assigns;
            unwindTransform(assigns, transform);
            ForEachItemIn(itr, assigns)
            {
                IHqlExpression * cur = &assigns.item(itr);
                if (cur->getOperator() == no_skip)
                {
                    const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                    processSelectsUsedForDenormalize(extra, selectsUsed, leftSelect, rightSelect);
                }
                else if (cur->getOperator() == no_assign)
                {
                    IHqlExpression * value = cur->queryChild(1);
                    if (!value->isPure())
                    {
                        extra->addOutputField(cur->queryChild(0)->queryChild(1));
                        const SelectUsedArray & selectsUsed = querySelectsUsed(value);
                        processSelectsUsedForDenormalize(extra, selectsUsed, leftSelect, rightSelect);
                    }
                }
            }

            //include all other attributes except for the transform
            unsigned max = expr->numChildren();
            for (unsigned i2=2; i2 < max; i2++)
            {
                IHqlExpression * cur = expr->queryChild(i2);
                if (cur != transform)
                {
                    const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                    processSelectsUsedForDenormalize(extra, selectsUsed, leftSelect, rightSelect);
                }
            }

            //NB: May need to loop twice - if all outputs are added then the order of output fields will change, so need to process list again
            //use a list to ensure that we don't do too much processing.
            HqlExprCopyArray processed;
            loop
            {
                bool allOutputsUsed = extra->outputFields.includeAll();
                //NB: outputfields can extend... can also get completely reordered is includeAll() is set.
                for (unsigned i=0; i < extra->outputFields.ordinality(); i++)
                {
                    IHqlExpression & cur = extra->outputFields.item(i);
                    if (!processed.contains(cur))
                    {
                        processed.append(cur);
                        extra->leftFieldsRequired.addUnique(&cur);
                        const SelectUsedArray & selectsUsed = querySelectsUsedForField(transform, &cur);
                        processSelectsUsedForDenormalize(extra, selectsUsed, leftSelect, rightSelect);
                    }
                }

                //All outputs didn't change, so processed whole list correctly
                if (allOutputsUsed == extra->outputFields.includeAll())
                    break;
            }
            break;
        }
    case FixedInputActivity:
        {
            extra->leftFieldsRequired.setAll();
            if (getNumChildTables(expr) >= 2)
                extra->rightFieldsRequired.setAll();
            extra->addAllOutputs();
            break;
        }
    case SourceActivity:
        {
            //No inputs to worry about, and not compoundable so output record won't change.
            extra->addAllOutputs();
            break;
        }
    case ActionSinkActivity:
    case SimpleActivity:
        {
            //inputs will be outputs required plus any fields used within the function
            //outputs will eventually match inputs when finished percolating.
            if (extra->outputFields.includeAll())
                extra->leftFieldsRequired.setAll();
            else
            {
                extra->leftFieldsRequired.cloneFields(extra->outputFields);

                IHqlExpression * ds = expr->queryChild(0)->queryNormalizedSelector();
                IHqlExpression * selSeq = querySelSeq(expr);
                //Left and right are here because of the dedup criteria.  It would be better to
                //special case that, but first lets get it working
                OwnedHqlExpr leftSelect = selSeq ? createSelector(no_left, ds, selSeq) : NULL;
                OwnedHqlExpr rightSelect = selSeq ? createSelector(no_right, ds, selSeq) : NULL;
                unsigned max = expr->numChildren();
                for (unsigned i2=1; i2 < max; i2++)
                {
                    const SelectUsedArray & selectsUsed = querySelectsUsed(expr->queryChild(i2));
                    ForEachItemIn(i3, selectsUsed)
                    {
                        IHqlExpression * curSelect = &selectsUsed.item(i3);
                        if (curSelect == leftSelect || curSelect == rightSelect || curSelect == ds)
                        {
                            extra->leftFieldsRequired.setAll();
                            break;
                        }
                        IHqlExpression * curDs = curSelect->queryChild(0);
                        if ((curDs == leftSelect) || (curDs == rightSelect) || (curDs == ds))
                            extra->leftFieldsRequired.addUnique(curSelect->queryChild(1));
                    }
                    if (extra->leftFieldsRequired.includeAll())
                        break;
                }

                if (extra->leftFieldsRequired.includeAll())
                    extra->addAllOutputs();
            }
            break;
        }
    default:
        throwUnexpected();
    }
}


void ImplicitProjectTransformer::createFilteredAssigns(HqlExprArray & assigns, IHqlExpression * transform, const UsedFieldSet & fields, IHqlExpression * newSelf, const UsedFieldSet * exceptions)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * cur = transform->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0);
                IHqlExpression * field = lhs->queryChild(1);
                IHqlExpression * oldSelf = lhs->queryChild(0);
                if (fields.contains(*field))
                {
                    IHqlExpression * newLhs = createSelectExpr(LINK(newSelf), LINK(field));
                    if (exceptions && exceptions->contains(*field))
                        assigns.append(*createAssign(newLhs, createNullExpr(newLhs)));
                    else
                        assigns.append(*createAssign(newLhs, LINK(cur->queryChild(1))));
                }
                break;
            }
        case no_assignall:
            createFilteredAssigns(assigns, cur, fields, newSelf, exceptions);
            break;
        default:
            assigns.append(*LINK(cur));
        }
    }
}

IHqlExpression * ImplicitProjectTransformer::createFilteredTransform(IHqlExpression * transform, const UsedFieldSet & fields, IHqlExpression * record, const UsedFieldSet * exceptions)
{
    OwnedHqlExpr self = getSelf(record);
    HqlExprArray assigns;
    createFilteredAssigns(assigns, transform, fields, self, exceptions);
    OwnedHqlExpr ret = createValue(transform->getOperator(), makeTransformType(record->getType()), assigns);
#if defined(PRESERVE_TRANSFORM_ANNOTATION)
    return transform->cloneAllAnnotations(ret);
#else
    return ret.getClear();
#endif
}


void ImplicitProjectTransformer::logChange(const char * message, IHqlExpression * expr, const UsedFieldSet & fields)
{
    _ATOM exprName = expr->queryName();
    if (!exprName && isCompoundSource(expr))
        exprName = expr->queryChild(0)->queryName();

    StringBuffer name, fieldText;
    if (exprName)
        name.append(exprName).append(" ");
    name.append(getOpString(expr->getOperator()));

    fieldText.append("(").append(fields.ordinality());
    if (expr->queryRecord())
        fieldText.append("/").append(queryBodyComplexExtra(expr->queryRecord())->outputFields.ordinality());
    fieldText.append(")");
    //MORE: If number removed << number remaining just display fields removed.
    fields.getText(fieldText);

    const char * const format = "ImplicitProject: %s %s now %s";
    DBGLOG(format, message, name.str(), fieldText.str());
    if (options.notifyOptimizedProjects)
    {
        if (options.notifyOptimizedProjects >= 2 || exprName)
        {
            StringBuffer messageText;
            messageText.appendf(format, message, name.str(), fieldText.str());
            translator.addWorkunitException(ExceptionSeverityInformation, 0, messageText.str(), NULL);
        }
    }
}


void ImplicitProjectTransformer::getTransformedChildren(IHqlExpression * expr, HqlExprArray & children)
{
    switch (getChildDatasetType(expr))
    {
    case childdataset_dataset: 
    case childdataset_datasetleft: 
    case childdataset_top_left_right:
        {
            IHqlExpression * arg0 = expr->queryChild(0);
            OwnedHqlExpr child = transform(arg0);
            children.append(*LINK(child));
            pushChildContext(arg0, child);
            transformChildren(expr, children);
            popChildContext();
            break;
        }
    case childdataset_evaluate:
        throwUnexpected();
    default:
        transformChildren(expr, children);
        break;
    }
}

IHqlExpression * ImplicitProjectTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->isConstant())
    {
        switch (expr->getOperator())
        {
        case no_transform:
        case no_newtransform:
        case no_transformlist:
        case no_list:
            return LINK(expr);
        }
    }

    //Can't call Parent::createTransformed as a default because the TranformRecordActivities trigger asserts when types mismatch.
    ImplicitProjectInfo * extra = queryBodyExtra(expr);
    ComplexImplicitProjectInfo * complexExtra = extra->queryComplexInfo();
    if (!complexExtra)
        return Parent::createTransformed(expr);

    OwnedHqlExpr transformed;
    switch (extra->activityKind())
    {
    case DenormalizeActivity:
    case TransformRecordActivity:
        {
            //Always reduce things that create a new record so they only project the fields they need to
            if (complexExtra->outputChanged() || complexExtra->fieldsToBlank.ordinality())
            {
                unsigned transformPos = queryTransformIndex(expr);

                //Walk transform, only including assigns that are in the output list.
                HqlExprArray args;
                getTransformedChildren(expr, args);

                //MORE: If the input's output contains fields that are not required in this transforms output then
                //include them, but assign them default values to stop them pulling in other variables.
                IHqlExpression * transform = &args.item(transformPos);
                IHqlExpression * newTransform = createFilteredTransform(transform, complexExtra->outputFields, complexExtra->newOutputRecord, &complexExtra->fieldsToBlank);
                args.replace(*newTransform, transformPos);
                transformed.setown(expr->clone(args));
                transformed.setown(updateSelectors(transformed, expr));
                logChange("Transform", expr, complexExtra->outputFields);
            }
            else
            {
#ifdef _DEBUG
                IHqlExpression * ds = expr->queryChild(0);
                OwnedHqlExpr transformedDs = transform(ds);
                assertex(recordTypesMatch(ds, transformedDs));
#endif
                transformed.setown(Parent::createTransformed(expr));
                //MORE: Need to replace left/right with their transformed varieties because the record may have changed format
                transformed.setown(updateSelectors(transformed, expr));
            }
            break;
        }
    case CreateRecordActivity:
    case CreateRecordLRActivity:
        {
            //Always reduce things that create a new record so they only project the fields they need to
            if (complexExtra->outputChanged())
            {
                unsigned transformPos = queryTransformIndex(expr);

                //Walk transform, only including assigns that are in the output list.
                HqlExprArray args;
                getTransformedChildren(expr, args);

                IHqlExpression * transform = &args.item(transformPos);
                IHqlExpression * newTransform = createFilteredTransform(transform, complexExtra->outputFields, complexExtra->newOutputRecord);
                args.replace(*newTransform, transformPos);
                if (transform->getOperator() == no_newtransform)
                    args.replace(*LINK(complexExtra->newOutputRecord), transformPos-1);
                IHqlExpression * onFail = queryProperty(onFailAtom, args);
                if (onFail)
                {
                    IHqlExpression * newTransform = createFilteredTransform(onFail->queryChild(0), complexExtra->outputFields, complexExtra->newOutputRecord);
                    IHqlExpression * newOnFail = createExprAttribute(onFailAtom, newTransform);
                    args.replace(*newOnFail, args.find(*onFail));
                }

                //We may have converted a count project into a project..... (see bug18839.xhql)
                if (expr->getOperator() == no_hqlproject)
                {
                    IHqlExpression * countProjectAttr = queryProperty(_countProject_Atom, args);
                    if (countProjectAttr && !transformContainsCounter(newTransform, countProjectAttr->queryChild(0)))
                        args.zap(*countProjectAttr);
                }

                transformed.setown(expr->clone(args));
                transformed.setown(updateSelectors(transformed, expr));
                logChange("Minimize", expr, complexExtra->outputFields);
            }
            else
            {
                transformed.setown(Parent::createTransformed(expr));
                //MORE: Need to replace left/right with their transformed varieties because the record may have changed format
                transformed.setown(updateSelectors(transformed, expr));
            }
            break;
        }
    case CreateRecordSourceActivity:
        {
            assertex(expr->getOperator() == no_inlinetable);
            //Always reduce things that create a new record so they only project the fields they need to
            if (complexExtra->outputChanged())
            {
                IHqlExpression * transforms = expr->queryChild(0);
                HqlExprArray newTransforms;
                ForEachChild(i, transforms)
                {
                    IHqlExpression * transform = transforms->queryChild(i);
                    newTransforms.append(*createFilteredTransform(transform, complexExtra->outputFields, complexExtra->newOutputRecord));
                }

                HqlExprArray args;
                args.append(*transforms->clone(newTransforms));
                args.append(*LINK(complexExtra->newOutputRecord));
                unwindChildren(args, expr, 2);
                transformed.setown(expr->clone(args));
                logChange("Minimize", expr, complexExtra->outputFields);
            }
            else
            {
                transformed.setown(Parent::createTransformed(expr));
                //MORE: Need to replace left/right with their transformed varieties because the record may have changed format
                transformed.setown(updateSelectors(transformed, expr));
            }
            break;
        }
    case CompoundActivity:
        {
            transformed.setown(Parent::createTransformed(expr));
            if (complexExtra->outputChanged())
            {
                HqlExprArray args;
                args.append(*complexExtra->createOutputProject(transformed->queryChild(0)));
                transformed.setown(transformed->clone(args));
                logChange("Project output from compound", expr, complexExtra->outputFields);
                break;
            }
        }
    case CompoundableActivity:
        {
            transformed.setown(Parent::createTransformed(expr));
            //insert a project after the record.
            if (complexExtra->outputChanged())
            {
                transformed.setown(complexExtra->createOutputProject(transformed));
                transformed.setown(createWrapper(queryCompoundOp(expr), transformed.getClear()));
                logChange("Project output from", expr, complexExtra->outputFields);
            }
            break;
        }
    case AnyTypeActivity:
        {
            transformed.setown(Parent::createTransformed(expr));
            //insert a project after the record.
            if (complexExtra->outputChanged())
            {
                logChange("Change format of dataset", expr, complexExtra->outputFields);
                HqlExprArray args;
                args.append(*LINK(complexExtra->newOutputRecord));
                unwindChildren(args, transformed, 1);
                transformed.setown(transformed->clone(args));
            }
            break;
        }
    case FixedInputActivity:
    case SourceActivity:
    case NonActivity:
    case ScalarSelectActivity:
    case ActionSinkActivity:
        transformed.setown(Parent::createTransformed(expr));
        //can't change...
        break;
    case PassThroughActivity:
        if (complexExtra->outputChanged())
        {
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                OwnedHqlExpr next = transform(cur);
                if (cur->isDataset() || cur->isDatarow())
                {
                    //Ensure all inputs have same format..
                    if (cur->queryRecord() != complexExtra->newOutputRecord)
                        next.setown(complexExtra->createOutputProject(next));
                }
                args.append(*next.getClear());
            }
            transformed.setown(expr->clone(args));
            logChange("Passthrough modified", expr, complexExtra->outputFields);
        }
        else
            transformed.setown(Parent::createTransformed(expr));
        break;
    case SimpleActivity:
        {
            transformed.setown(Parent::createTransformed(expr));
            IHqlExpression * onFail = transformed->queryProperty(onFailAtom);
            if (onFail)
            {
                IHqlExpression * newTransform = createFilteredTransform(onFail->queryChild(0), complexExtra->outputFields, complexExtra->newOutputRecord);
                IHqlExpression * newOnFail = createExprAttribute(onFailAtom, newTransform);
                transformed.setown(replaceOwnedProperty(transformed, newOnFail));
            }
            if (complexExtra->insertProject)
            {
                HqlExprArray args;
                OwnedHqlExpr inputProject = complexExtra->createOutputProject(transformed->queryChild(0));
                OwnedHqlExpr replacement = replaceChildDataset(transformed, inputProject, 0);
                transformed.setown(updateSelectors(replacement, expr));
                logChange("Insert project before", expr, complexExtra->outputFields);
            }
            else
                transformed.setown(updateSelectors(transformed, expr));
            break;
        }
    default:
        throwUnexpected();
    }
    
    return transformed.getClear();
}


ANewTransformInfo * ImplicitProjectTransformer::createTransformInfo(IHqlExpression * expr)
{
    ProjectExprKind kind = getProjectExprKind(expr);
    node_operator op = expr->getOperator();
    if (kind == NonActivity)
    {
        switch (op)
        {
        case no_record:
        case no_rowset:
        case no_rowsetrange:
        case no_datasetlist:
            break;
        default:
            return CREATE_NEWTRANSFORMINFO2(ImplicitProjectInfo, expr, kind);
        }
    }
    if (kind == ComplexNonActivity)
        kind = NonActivity;
    return CREATE_NEWTRANSFORMINFO2(ComplexImplicitProjectInfo, expr, kind);
}


void ImplicitProjectTransformer::finalizeFields()
{
    ForEachItemIn(i, activities)
        finalizeFields(&activities.item(i));
}



static bool requiresFewerFields(const UsedFieldSet & fields, ComplexImplicitProjectInfo & input)
{
    if (fields.includeAll())
        return false;
    return (fields.ordinality() < input.outputFields.ordinality());
}


void ImplicitProjectTransformer::finalizeFields(IHqlExpression * expr)
{
    ComplexImplicitProjectInfo * extra = queryBodyComplexExtra(expr);
    if (!extra->okToOptimize())
        return;

    switch (extra->activityKind())
    {
    case CreateRecordActivity:
    case CreateRecordLRActivity:
    case CompoundActivity:
    case CompoundableActivity:
    case CreateRecordSourceActivity:
    case AnyTypeActivity:
        extra->finalizeOutputRecord();
        break;
    case DenormalizeActivity:
    case TransformRecordActivity:
        {
            //output must always match the input..., but any fields that are in the input, but not needed in the output we'll add as exceptions
            //and assign default values to them, otherwise it can cause other fields to be required in the input + causes chaos
            if (!extra->outputFields.includeAll())
            {
                UsedFieldSet & inFields = extra->inputs.item(0).outputFields;
                ForEachItemIn(i, inFields)
                {
                    IHqlExpression & cur = inFields.item(i);
                    if (extra->addOutputField(&cur))
                    {
                        DBGLOG("ImplicitProject: Field %s for %s not required by outputs - so blank in transform", cur.queryName()->str(), getOpString(expr->getOperator()));
                        extra->fieldsToBlank.append(OLINK(cur));
                    }
                }
            }
            extra->finalizeOutputRecord();
            break;
        }
    case FixedInputActivity:
    case SourceActivity:
    case ScalarSelectActivity:
        break;
    case PassThroughActivity:
        {
            bool anyProjected = false;
            unsigned numInputs = extra->inputs.ordinality();
            for (unsigned i=0; i != numInputs; i++)
            {
                ComplexImplicitProjectInfo & cur = extra->inputs.item(i);
                if (!cur.outputFields.includeAll())
                {
                    extra->outputFields.set(cur.outputFields);

                    for (unsigned i2=i+1; i2 != numInputs; i2++)
                    {
                        ComplexImplicitProjectInfo & cur = extra->inputs.item(i2);
                        extra->outputFields.intersectFields(cur.outputFields);
                    }
                    extra->finalizeOutputRecord();
                    anyProjected = true;
                    break;
                }
            }
            
            if (!anyProjected)
                extra->setMatchingOutput(&extra->inputs.item(0));
            break;
        }
    case ActionSinkActivity:
    case SimpleActivity:
        if (extra->insertProject && requiresFewerFields(extra->leftFieldsRequired, extra->inputs.item(0)))
        {
            extra->outputFields.set(extra->leftFieldsRequired);
            extra->finalizeOutputRecord();
        }
        else
            extra->setMatchingOutput(&extra->inputs.item(0));
        break;
    default:
        throwUnexpected();
    }
}


void ImplicitProjectTransformer::inheritActiveFields(IHqlExpression * expr, ImplicitProjectInfo * extra, unsigned min, unsigned max)
{
    for (unsigned i = min; i < max; i++)
        inheritActiveFields(extra, expr->queryChild(i));
}


void ImplicitProjectTransformer::inheritActiveFields(ImplicitProjectInfo * target, IHqlExpression * source)
{
    if (source->queryBody()->queryTransformExtra())
    {
        target->addActiveSelects(querySelectsUsed(source));
    }
}


void ImplicitProjectTransformer::insertProjects()
{
    ForEachItemIn(i, activities)
        insertProjects(&activities.item(i));
}



void ImplicitProjectTransformer::insertProjects(IHqlExpression * expr)
{
    ComplexImplicitProjectInfo * extra = queryBodyComplexExtra(expr);
    if (!extra->okToOptimize())
        return;

    if (options.optimizeSpills && (expr->getOperator() == no_commonspill))
    {
        if (requiresFewerFields(extra->leftFieldsRequired, extra->inputs.item(0)))
            extra->insertProject = true;
        return;
    }
        
    if (options.insertProjectCostLevel == 0)
        return;
    
    if (extra->queryCostFactor(targetClusterType) < options.insertProjectCostLevel)
        return;

    switch (extra->activityKind())
    {
    case SimpleActivity:
        if (requiresFewerFields(extra->leftFieldsRequired, extra->inputs.item(0)))
            extra->insertProject = true;
        break;
    }
}


void ImplicitProjectTransformer::percolateFields()
{
    ForEachItemInRev(i, activities)
        calculateFieldsUsed(&activities.item(i));
}


IHqlExpression * ImplicitProjectTransformer::process(IHqlExpression * expr)
{
    cycle_t time1 = msTick();
    analyse(expr, 0);           // gather a list of activities, and link them together.
    cycle_t time2 = msTick();
    //DEBUG_TIMERX(translator.queryTimeReporter(), "EclServer: implicit.analyse", time2-time1);
    percolateFields();
    cycle_t time3 = msTick();
    //DEBUG_TIMERX(translator.queryTimeReporter(), "EclServer: implicit.percolate", time3-time2);
    switch (targetClusterType)
    {
    case RoxieCluster:
        //worth inserting projects after sources that can be compound.
        //also may be worth projecting before iterating since an iterate
        //copies data but can't change the fields in use
        break;
    case HThorCluster:
        // same as roxie, but also maybe worth inserting projects to minimise the amount of data that is spilled.
        break;
    case ThorCluster:
    case ThorLCRCluster:
        //worth inserting projects to reduce copying, spilling, but primarily data transfered between nodes.
        if (options.insertProjectCostLevel || options.optimizeSpills)
            insertProjects();
        break;
    }
    cycle_t time4 = msTick();
    //DEBUG_TIMERX(translator.queryTimeReporter(), "EclServer: implicit.reduceData", time4-time3);
    finalizeFields();
    cycle_t time5 = msTick();
    //DEBUG_TIMERX(translator.queryTimeReporter(), "EclServer: implicit.finalize", time5-time4);
    //traceActivities();
    OwnedHqlExpr ret = transformRoot(expr);
    cycle_t time6 = msTick();
    //DEBUG_TIMERX(translator.queryTimeReporter(), "EclServer: implicit.transform", time6-time5);
    return ret.getClear();
}


void ImplicitProjectTransformer::traceActivities()
{
    ForEachItemIn(i, activities)
        queryBodyComplexExtra(&activities.item(i))->trace();
}


IHqlExpression * ImplicitProjectTransformer::updateSelectors(IHqlExpression * newExpr, IHqlExpression * oldExpr)
{
    switch (getChildDatasetType(newExpr))
    {
    case childdataset_none: 
    case childdataset_addfiles:
    case childdataset_merge:
    case childdataset_if:
    case childdataset_case:
    case childdataset_map:
    case childdataset_dataset_noscope: 
        return LINK(newExpr);
        //None of these have any scoped arguments, so no need to remove them
        break;
    case childdataset_dataset:
        return LINK(newExpr);
    case childdataset_datasetleft:
    case childdataset_left:
        {
            IHqlExpression * selSeq = querySelSeq(newExpr);
            assertex(selSeq == querySelSeq(oldExpr));
            OwnedHqlExpr newLeft = createSelector(no_left, newExpr->queryChild(0), selSeq);
            OwnedHqlExpr oldLeft = createSelector(no_left, oldExpr->queryChild(0), selSeq);
            return updateChildSelectors(newExpr, oldLeft, newLeft, 1);
        }
    case childdataset_same_left_right:
    case childdataset_top_left_right:
    case childdataset_nway_left_right:
        {
            IHqlExpression * selSeq = querySelSeq(newExpr);
            assertex(selSeq == querySelSeq(oldExpr));
            OwnedHqlExpr newLeft = createSelector(no_left, newExpr->queryChild(0), selSeq);
            OwnedHqlExpr oldLeft = createSelector(no_left, oldExpr->queryChild(0), selSeq);
            OwnedHqlExpr ds1 = updateChildSelectors(newExpr, oldLeft, newLeft, 1);

            OwnedHqlExpr newRight = createSelector(no_right, newExpr->queryChild(0), selSeq);
            OwnedHqlExpr oldRight = createSelector(no_right, oldExpr->queryChild(0), selSeq);
            return updateChildSelectors(ds1, oldRight, newRight, 1);
        }
    case childdataset_leftright: 
        {
            IHqlExpression * selSeq = querySelSeq(newExpr);
            assertex(selSeq == querySelSeq(oldExpr));
            OwnedHqlExpr newLeft = createSelector(no_left, newExpr->queryChild(0), selSeq);
            OwnedHqlExpr oldLeft = createSelector(no_left, oldExpr->queryChild(0), selSeq);
            unsigned firstLeft = (newExpr->getOperator() == no_normalize) ? 1 : 2;
            OwnedHqlExpr ds1 = updateChildSelectors(newExpr, oldLeft, newLeft, firstLeft);

            OwnedHqlExpr newRight = createSelector(no_right, newExpr->queryChild(1), selSeq);
            OwnedHqlExpr oldRight = createSelector(no_right, oldExpr->queryChild(1), selSeq);
            return updateChildSelectors(ds1, oldRight, newRight, 2);
        }
        break;
    default:
        throwUnexpected();
    }
}

IHqlExpression * ImplicitProjectTransformer::updateChildSelectors(IHqlExpression * expr, IHqlExpression * oldSelector, IHqlExpression * newSelector, unsigned firstChild)
{
    if (oldSelector == newSelector)
        return LINK(expr);
    unsigned max = expr->numChildren();
    unsigned i;
    HqlExprArray args;
    for (i = 0; i < firstChild; i++)
        args.append(*LINK(expr->queryChild(i)));
#if 0
    //Possibly more efficient
    HqlExprArray argsToUpdate;
    unwindChildren(argsToUpdate, expr, firstChild);
    newReplaceSelector(args, argsToUpdate, oldSelector, newSelector);
#else
    for (; i < max; i++)
        args.append(*replaceSelector(expr->queryChild(i), oldSelector, newSelector));
#endif
    return expr->clone(args);
}

const SelectUsedArray & ImplicitProjectTransformer::querySelectsUsedForField(IHqlExpression * transform, IHqlExpression * field)
{
    IHqlExpression * transformValues = queryTransformAssign(transform, field);
    if (!transformValues)
         transformValues = queryTransformAssign(transform, field);
    return querySelectsUsed(transformValues);
}


#include "hqlttcpp.ipp"
IHqlExpression * insertImplicitProjects(HqlCppTranslator & translator, IHqlExpression * expr, bool optimizeSpills)
{
#if defined(POST_COMMON_ANNOTATION)
    HqlExprArray ret;
    {
        ImplicitProjectTransformer transformer(translator, optimizeSpills);
        ret.append(*transformer.process(expr));
    }
    normalizeAnnotations(translator, ret);
    return createActionList(ret);
#else
    ImplicitProjectTransformer transformer(translator, optimizeSpills);
    return transformer.process(expr);
#endif
}

void insertImplicitProjects(HqlCppTranslator & translator, HqlExprArray & exprs)
{
    if (exprs.ordinality())
    {
        OwnedHqlExpr compound = createActionList(exprs);
        OwnedHqlExpr ret = insertImplicitProjects(translator, compound, false);
        exprs.kill();
        ret->unwindList(exprs, no_actionlist);
    }
}

/*

  To Implement field gathering would need to do the following:

  - Could assert that only non-nested fields are considered.  That means all field references are in the form (ds.field).
    This would simplify gathering, filtering, and translating the dataset selector....
  - All no_selects on in-scope datasets (or sub fields?) get added to list of active fields in self.
  - analyseExpr() clones all active fields from children into self.
  - active datasets used in a dataset context need to also be added.
  - any item inherits all child fields from non-dataset children, and removes all references to parent datasets (in what ever form)
    very similar to the inScope Table processing.  [Only really needed for non-global activities]
  - any activity inherits all inScope fields from its parent.

  - those expressions with scoped dataset inputs need two lists (childrensFieldAccess and inscopeFields)
  - may be worth having different classes for handling the different categories:
    (simpleExpression, scopedExpression, activity) with virtuals to handle differences

  - removingChildReferences
    would need matchesSelector(list, selector) which worked recursively.  

  */
