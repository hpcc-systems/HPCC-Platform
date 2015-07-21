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
//#define PARANOID_CHECKING

//MORE: Extend UsedFieldSet/NestedField so that once all is set it is never modified and then allow a LINK from
//a nestedfield to clone.
//Problems:
//  combine the two classes for simplicity
//  save links in the complexextra rather than objects.
//  Ensure all functions that modify now return objects + clone if modifying and was all.
//  Need to implement gathered-enough in a different way so it isn't shared.

enum
{
    CostMemoryCopy          = 1,
    CostNetworkGroup        = 1,
    CostGlobalTopN          = 2,
    CostManyCopy            = 3,
    CostNetworkCopy         = 10,
};

//-------------------------------------------------------------------------------------------------

void UsedFieldSet::addUnique(IHqlExpression * field)
{
    //MORE: Add if (!all test to short-circuit contains)
    if (!contains(*field))
        appendField(*LINK(field));
}

NestedField * UsedFieldSet::addNested(IHqlExpression * field)
{
    NestedField * match = findNested(field);
    if (!match)
    {
        assertex(originalFields);
#ifdef PARANOID_CHECKING
        assertex(!contains(*field));
        assertex(originalFields->contains(*field));
#endif
        NestedField * original = originalFields->findNested(field);
        if (!original)
        {
            EclIR::dump_ir(field, originalFields->findNestedByName(field)->field);
            throwUnexpected();
        }
        match = new NestedField(field, &original->used);
        appendNested(*LINK(field), match);
    }
    else
    {
        assertex(contains(*field));
    }

    return match;
}

void UsedFieldSet::appendNested(IHqlExpression & ownedField, NestedField * ownedNested)
{
    appendField(ownedField);
    nested.append(*ownedNested);
}

bool UsedFieldSet::checkAllFieldsUsed()
{
    if (all)
        return true;
    assertex(originalFields);
    if (fields.ordinality() != originalFields->fields.ordinality())
        return false;
    ForEachItemIn(i, nested)
    {
        NestedField & cur = nested.item(i);
        if (!cur.used.checkAllFieldsUsed())
            return false;
    }
    all = true;
    return true;
}

bool UsedFieldSet::allGathered() const
{
    if (maxGathered == (unsigned)-1)
        return true;
    if (maxGathered < fields.ordinality())
        return false;
    ForEachItemIn(i, nested)
    {
        if (!nested.item(i).used.allGathered())
            return false;
    }
    return true;
}

void UsedFieldSet::appendField(IHqlExpression & ownedField)
{
#ifdef PARANOID_CHECKING
    assertex(!contains(ownedField));
#endif
#ifdef USE_IPROJECT_HASH
    if (!all)
        hash.add(&ownedField);
#endif
    fields.append(ownedField);
#ifdef PARANOID_CHECKING
    if (originalFields)
        assertex(originalFields->contains(ownedField));
#endif
}

void UsedFieldSet::clone(const UsedFieldSet & source)
{
    if (originalFields != source.originalFields)
    {
        //In very rare circumstances it is possible to have non-identical records with identical structures
        //A typical case is the same structure defined in two different places, using a symbol to specify a maximum
        //length.  The locations of the symbols differ, so the records do not match exactly.
        assertex(recordTypesMatch(queryOriginalRecord(), source.queryOriginalRecord()));
    }
    ForEachItemIn(i, source.fields)
        appendField(OLINK(source.fields.item(i)));

    ForEachItemIn(i1, source.nested)
        nested.append(*source.nested.item(i1).clone());

    if (source.all)
        all = true;

    finalRecord.set(source.finalRecord);
}


unsigned UsedFieldSet::getOriginalPosition(IHqlExpression * field) const
{
    assertex(originalFields == this);
    unsigned match = fields.find(*field);
    if (match != NotFound)
        return match;
    assertex(field->isDatarow());
    assertex(finalRecord);
    OwnedHqlExpr originalField = finalRecord->querySimpleScope()->lookupSymbol(field->queryId());
    assertex(originalField && originalField != field);
    unsigned matchOriginal = fields.find(*originalField);
    assertex(matchOriginal != NotFound);
    return matchOriginal;
}


int UsedFieldSet::compareOrder(IHqlExpression * left, IHqlExpression * right) const
{
    return (int)(getOriginalPosition(left) - getOriginalPosition(right));
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

bool UsedFieldSet::contains(IAtom * name) const
{
    if (all)
        return true;
    ForEachItemIn(i, fields)
    {
        if (fields.item(i).queryName() == name)
            return true;
    }
    return false;
}

//Calculate left - right
void UsedFieldSet::createDifference(const UsedFieldSet & left, const UsedFieldSet & right)
{
    if (right.includeAll())
        return;

    //if all are used and non modifyable this code will need changing.
    ForEachItemIn(i, left.fields)
    {
        IHqlExpression & cur = left.fields.item(i);
        if (cur.isDatarow())
        {
            NestedField * leftNested = left.findNested(&cur);
            NestedField * rightNested = right.findNested(&cur);
            assertex(leftNested);
            if (rightNested)
            {
                Owned<NestedField> diffNested = new NestedField(&cur, leftNested->used.queryOriginal());
                diffNested->used.createDifference(leftNested->used, rightNested->used);
                if (!diffNested->isEmpty())
                    appendNested(OLINK(cur), diffNested.getClear());
            }
            else if (!leftNested->isEmpty())
            {
                appendNested(OLINK(cur), leftNested->clone());
            }
        }
        else
        {
            if (!right.contains(cur))
                appendField(OLINK(cur));
        }
    }
}


NestedField * UsedFieldSet::findNested(IHqlExpression * field) const
{
    ForEachItemIn(i, nested)
    {
        NestedField & cur = nested.item(i);
        if (cur.field == field)
            return &cur;
    }
    return NULL;
}

NestedField * UsedFieldSet::findNestedByName(IHqlExpression * field) const
{
    ForEachItemIn(i2, nested)
    {
        NestedField & cur = nested.item(i2);
        if (cur.field->queryName() == field->queryName())
            return &cur;
    }
    return NULL;
}

IHqlExpression * UsedFieldSet::createFilteredAssign(IHqlExpression * field, IHqlExpression * value, IHqlExpression * newSelf, const UsedFieldSet * exceptions) const
{
    if (!contains(*field))
        return NULL;

    OwnedHqlExpr newValue = LINK(value);
    OwnedHqlExpr newField;
    if (field->isDatarow())
    {
        NestedField * match = findNested(field);
        assertex(match);
        NestedField * exception = exceptions ? exceptions->findNested(field) : NULL;
        if (!match->isEmpty())
        {
            bool createSubset = true;
            if (match->used.checkAllFieldsUsed())
            {
                if (!exception || exception->isEmpty())
                {
                    createSubset = false;
                }
                else if (exception && exception->used.checkAllFieldsUsed())
                {
                    newValue.setown(createNullExpr(field));
                    createSubset = false;
                }
            }

            if (createSubset)
            {
                newField.setown(finalRecord->querySimpleScope()->lookupSymbol(field->queryId()));
                assertex(newField);
                assertex(exception || newField != field);

                //Two options - this is either a no_createrow, and we extract the assignments from the transform
                //or we create a no_createrow to extract the values from the other record
                OwnedHqlExpr newTransform;
                UsedFieldSet * exceptionFields = exception ? &exception->used : NULL;
                if (value->getOperator() == no_createrow)
                {
                    newTransform.setown(match->used.createFilteredTransform(value->queryChild(0), exceptionFields));
                }
                else if (value->getOperator() == no_select)
                {
                    newTransform.setown(match->used.createRowTransform(value, exceptionFields));
                }
                else
                {
                    OwnedHqlExpr row = createRow(no_newrow, LINK(value));
                    newTransform.setown(match->used.createRowTransform(row, exceptionFields));
                }


                newValue.setown(createRow(no_createrow, newTransform.getClear()));
#if defined(PRESERVE_TRANSFORM_ANNOTATION)
                newValue.setown(value->cloneAllAnnotations(newValue));
#endif
            }
        }
        else
            newValue.clear();
    }
    else
    {
        if (exceptions && exceptions->contains(*field))
        {
            newValue.setown(createNullExpr(newField ? newField.get() : field));
        }
    }

    if (newValue)
    {
        IHqlExpression * rhs = newField ? newField.get() : field;
        OwnedHqlExpr newLhs = createSelectExpr(LINK(newSelf), LINK(rhs));
        return createAssign(newLhs.getClear(), newValue.getClear());
    }
    return NULL;
}

void UsedFieldSet::createFilteredAssigns(HqlExprArray & assigns, IHqlExpression * transform, IHqlExpression * newSelf, const UsedFieldSet * exceptions) const
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
                IHqlExpression * value = cur->queryChild(1);
                IHqlExpression * assign = createFilteredAssign(field, value, newSelf, exceptions);
                if (assign)
                    assigns.append(*assign);
                break;
            }
        case no_assignall:
            createFilteredAssigns(assigns, cur, newSelf, exceptions);
            break;
        default:
            assigns.append(*LINK(cur));
            break;
        }
    }
}


IHqlExpression * UsedFieldSet::createFilteredTransform(IHqlExpression * transform, const UsedFieldSet * exceptions) const
{
    OwnedHqlExpr self = getSelf(finalRecord);
    HqlExprArray assigns;
    createFilteredAssigns(assigns, transform, self, exceptions);
    OwnedHqlExpr ret = createValue(transform->getOperator(), makeTransformType(finalRecord->getType()), assigns);
#if defined(PRESERVE_TRANSFORM_ANNOTATION)
    return transform->cloneAllAnnotations(ret);
#else
    return ret.getClear();
#endif
}

IHqlExpression * UsedFieldSet::createRowTransform(IHqlExpression * row, const UsedFieldSet * exceptions) const
{
    OwnedHqlExpr self = getSelf(finalRecord);
    HqlExprArray assigns;
    ForEachItemIn(i, fields)
    {
        IHqlExpression & field = fields.item(i);
        OwnedHqlExpr value = createSelectExpr(LINK(row), LINK(&field));
        OwnedHqlExpr assign = createFilteredAssign(&field, value, self, exceptions);
        if (assign)
            assigns.append(*assign.getClear());
    }

    return createValue(no_transform, makeTransformType(finalRecord->getType()), assigns);
}


void UsedFieldSet::calcFinalRecord(bool canPack, bool ignoreIfEmpty)
{
    assertex(originalFields);
    if (finalRecord)
        return;

    ForEachItemIn(i1, nested)
        nested.item(i1).used.calcFinalRecord(canPack, true);

    IHqlExpression * originalRecord = queryOriginalRecord();
    if (checkAllFieldsUsed())
    {
        if (canPack)
            finalRecord.setown(getPackedRecord(originalRecord));
        else
            finalRecord.set(originalRecord);
        return;
    }

    HqlExprArray recordFields;
    ForEachItemIn(i, fields)
    {
        IHqlExpression & cur = fields.item(i);
        if (cur.isDatarow())
        {
            NestedField * match = findNested(&cur);
            assertex(match);
            IHqlExpression * record = cur.queryRecord();
            IHqlExpression * newRecord = match->used.queryFinalRecord();
            if (record == newRecord)
            {
                recordFields.append(OLINK(cur));
            }
            else if (newRecord)
            {
                HqlExprArray args;
                unwindChildren(args, &cur);
                //MORE: Any default will now have the wrong type => remove it for the moment (ideally it would be projected)
                removeAttribute(args, defaultAtom);
                OwnedHqlExpr newField = createField(cur.queryId(), makeRowType(newRecord->getType()), args);
                recordFields.append(*newField.getClear());
            }
        }
        else
            recordFields.append(OLINK(cur));
    }

    if (originalFields)
    {
        //Reorder the record to match the original fields
        RecordOrderComparer compare(*originalFields);
        qsortvec((void * *)recordFields.getArray(), recordFields.ordinality(), compare);
    }

    if (recordFields.ordinality() == 0)
    {
        if (ignoreIfEmpty)
            return;
        recordFields.append(*createAttribute(_nonEmpty_Atom));
    }

    finalRecord.setown(createRecord(recordFields));
    if (canPack)
        finalRecord.setown(getPackedRecord(finalRecord));
}

void UsedFieldSet::gatherExpandSelectsUsed(HqlExprArray * selfSelects, HqlExprArray * parentSelects, IHqlExpression * selector, IHqlExpression * source)
{
    assertex(selfSelects ? selector != NULL : true);
    for (unsigned i1 = maxGathered; i1 < fields.ordinality(); i1++)
    {
        IHqlExpression & cur = fields.item(i1);
        if (!cur.isDatarow())
        {
            if (selfSelects)
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(&cur));
                selfSelects->append(*selected.getClear());
            }
            if (parentSelects)
            {
                OwnedHqlExpr sourceSelected = createSelectExpr(LINK(source), LINK(&cur));
                parentSelects->append(*sourceSelected.getClear());
            }
        }
    }
    maxGathered = fields.ordinality();

    ForEachItemIn(i2, nested)
    {
        NestedField & curNested = nested.item(i2);
        IHqlExpression * field = curNested.field;
        OwnedHqlExpr selected = selector ? createSelectExpr(LINK(selector), LINK(field)) : NULL;
        OwnedHqlExpr sourceSelected = createSelectExpr(LINK(source), LINK(field));

        if (!curNested.includeAll())
        {
            curNested.used.gatherExpandSelectsUsed(selfSelects, parentSelects, selected, sourceSelected);
            sourceSelected.clear();
        }
        else
        {
            curNested.used.noteGatheredAll();
            if (selfSelects)
                selfSelects->append(*selected.getClear());
            if (parentSelects)
                parentSelects->append(*sourceSelected.getClear());
        }
    }
}

inline bool isSelector(IHqlExpression * expr)
{
    return (expr->getOperator() == no_select) && !isNewSelector(expr);
}

void UsedFieldSet::gatherTransformValuesUsed(HqlExprArray * selfSelects, HqlExprArray * parentSelects, HqlExprArray * values, IHqlExpression * selector, IHqlExpression * transform)
{
    for (unsigned i = maxGathered; i < fields.ordinality(); i++)
    {
        IHqlExpression & cur = fields.item(i);
        if (!cur.isDatarow())
        {
            if (selfSelects)
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(&cur));
                selfSelects->append(*selected.getClear());
            }
            if (values)
            {
                IHqlExpression * transformValue = queryTransformAssignValue(transform, &cur);
                //If no transform value is found then we almost certainly have an invalid query (e.g, LEFT inside a
                //global).  Don't add the value - you'll definitely get a later follow on error
                assertex(transformValue);
                values->append(*LINK(transformValue));
            }
        }
    }
    maxGathered = fields.ordinality();

    ForEachItemIn(i2, nested)
    {
        NestedField & curNested = nested.item(i2);
        if (!curNested.isEmpty() && !curNested.used.allGathered())
        {
            IHqlExpression * field = curNested.field;
            OwnedHqlExpr selected = selector ? createSelectExpr(LINK(selector), LINK(field)) : NULL;
            IHqlExpression * transformValue = queryTransformAssignValue(transform, field);
            assertex(transformValue);
            bool includeThis = true;
            if (!curNested.includeAll() && transformValue->isPure())
            {
                if (transformValue->getOperator() == no_createrow)
                {
                    curNested.used.gatherTransformValuesUsed(selfSelects, parentSelects, values, selected, transformValue->queryChild(0));
                    includeThis = false;
                }
                else if (isAlwaysActiveRow(transformValue) || isSelector(transformValue))
                {
                    curNested.used.gatherExpandSelectsUsed(selfSelects, parentSelects, selected, transformValue);
                    includeThis = false;
                }
                //otherwise use the whole value.
            }

            if (includeThis)
            {
                curNested.used.noteGatheredAll();
                if (selfSelects)
                    selfSelects->append(*selected.getClear());
                if (values)
                    values->append(*LINK(transformValue));
            }
        }
    }
}

void UsedFieldSet::getText(StringBuffer & s) const
{
    if (all)
        s.append("ALL");

    s.append("[");
    ForEachItemIn(i, fields)
    {
        IHqlExpression & cur = fields.item(i);
        if (i) s.append(",");
        s.append(cur.queryName());
        if (cur.isDatarow())
        {
            NestedField * match = findNested(&cur);
            assertex(match);
            if (!match->used.checkAllFieldsUsed())
                match->used.getText(s);
        }
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
        finalRecord.clear();
        ForEachItemInRev(i1, fields)
        {
            IHqlExpression & field = fields.item(i1);
            if (!field.isDatarow() && !source.contains(field))
            {
                fields.remove(i1);
#ifdef USE_IPROJECT_HASH
                hash.remove(&field);
#endif
            }
        }
        ForEachItemInRev(i2, nested)
        {
            NestedField & cur = nested.item(i2);
            NestedField * match = source.findNested(cur.field);
            //MORE: If we never modify items that have been all set then the following will need changing:
            if (match)
            {
                cur.used.intersectFields(match->used);
            }
            else
            {
                cur.clear();
            }
        }
    }
}


void UsedFieldSet::optimizeFieldsToBlank(const UsedFieldSet & allAssigned, IHqlExpression * transform)
{
    //MORE:
    //this contains a list of fields that can be blanked instead of assigning.
    //If there is a sequence of assignments SELF.x := LEFT.x then
    //a) the the field will already be in the input and output records (since it is a rollup/iterate)
    //b) if the previous field is assigned, then it may generate more efficient code to also assign this
    //   field rather than blanking it.
    //Therefore we should walk the transform, and if a field is an exception and previous field is used
    //and possibly the exception is fixed length, then remove it from the exceptions.
}

bool UsedFieldSet::requiresFewerFields(const UsedFieldSet & other) const
{
    if (includeAll())
        return false;
    return (fields.ordinality() < other.fields.ordinality());
}

void UsedFieldSet::unionFields(const UsedFieldSet & source)
{
    if (includeAll())
        return;

    if (source.includeAll())
        set(source);
    else
    {
        ForEachItemIn(i, source.fields)
        {
            IHqlExpression & field = source.fields.item(i);
            if (!contains(field))
                appendField(OLINK(field));
        }
        ForEachItemIn(i1, source.nested)
        {
            NestedField & cur = source.nested.item(i1);
            NestedField * match = findNested(cur.field);
            if (match)
                match->used.unionFields(cur.used);
            else
                nested.append(*cur.clone());
        }
    }
}

bool UsedFieldSet::isEmpty() const
{
    ForEachItemIn(i1, fields)
    {
        IHqlExpression & cur = fields.item(i1);
        if (!cur.isDatarow())
            return false;
    }
    ForEachItemIn(i2, nested)
    {
        if (!nested.item(i2).isEmpty())
            return false;
    }
    return true;
}

void UsedFieldSet::kill()
{
#ifdef USE_IPROJECT_HASH
    hash.kill();
#endif
    fields.kill();
    nested.kill();
    all = false;
    maxGathered = 0;
    finalRecord.clear();
}

void UsedFieldSet::set(const UsedFieldSet & source)
{
    kill();
    clone(source);
}

void UsedFieldSet::setAll()
{
    if (all)
        return;
    assertex(originalFields);
    kill();
    clone(*originalFields);
}

void UsedFieldSet::setRecord(IHqlExpression * record)
{
    assertex(fields.ordinality() == 0);
    all = true;
    unwindFields(fields, record);
    ForEachItemIn(i, fields)
    {
        IHqlExpression & cur = fields.item(i);
        if (cur.isDatarow())
        {
            NestedField * child = new NestedField(&cur, NULL);
            child->used.setRecord(cur.queryRecord());
            nested.append(*child);
        }
    }
    finalRecord.set(record->queryBody());
    originalFields = this;
}


static UsedFieldSet * addNestedField(UsedFieldSet & fields, IHqlExpression * expr, IHqlExpression * selector)
{
    if (expr == selector)
        return &fields;
    IHqlExpression * ds = expr->queryChild(0);
    UsedFieldSet * parent = addNestedField(fields, ds, selector);
    if (parent)
    {
        NestedField * nested = parent->addNested(expr->queryChild(1));
        if (!nested || nested->includeAll())
            return NULL;
        return &nested->used;
    }
    return NULL;
}

bool processMatchingSelector(UsedFieldSet & fields, IHqlExpression * select, IHqlExpression * selector)
{
    if (select == selector)
    {
        fields.setAll();
        return true;
    }

    if (select->getOperator() != no_select)
        return false;

    //Could be <root>.blah.ds - queryDatasetSelector needs to be applied to the lhs.
    IHqlExpression * root = queryDatasetCursor(select->queryChild(0));
    if (root == selector)
    {
        if (select->isDatarow())
        {
            UsedFieldSet * nested = addNestedField(fields, select, selector);
            if (nested)
                nested->setAll();
        }
        else
        {
            IHqlExpression * ds = select->queryChild(0);
            IHqlExpression * field = select->queryChild(1);
            UsedFieldSet * nested = addNestedField(fields, ds, selector);
            if (nested)
                nested->addUnique(field);
        }
    }
    return false;
}

//---------------------------------------------------------------------------------------------------------------------

int RecordOrderComparer::docompare(const void * l,const void * r) const
{
    IHqlExpression * lExpr = (IHqlExpression *)l;
    IHqlExpression * rExpr = (IHqlExpression *)r;
    return fields.compareOrder(lExpr, rExpr);
}

//---------------------------------------------------------------------------------------------------------------------



static unsigned getActivityCost(IHqlExpression * expr, ClusterType targetClusterType)
{
    switch (targetClusterType)
    {
    case ThorLCRCluster:
        {
            switch (expr->getOperator())
            {
            case no_sort:
                //MORE: What about checking for grouped!
                if (!expr->hasAttribute(localAtom))
                    return CostNetworkCopy;
                return CostManyCopy;
            case no_subsort:
                if (!expr->hasAttribute(localAtom) && !isGrouped(expr))
                    return CostNetworkCopy;
                break;
            case no_group:
                if (!expr->hasAttribute(localAtom))
                    return CostNetworkGroup;
                break;
            case no_keyeddistribute:
            case no_distribute:
            case no_cosort:
                return CostNetworkCopy;
            case no_topn:
                if (!expr->hasAttribute(localAtom))
                    return CostGlobalTopN;
                break;
            case no_selfjoin:
                if (!expr->hasAttribute(localAtom))
                    return CostNetworkCopy;
                break;
            case no_denormalize:
            case no_denormalizegroup:
            case no_join:
            case no_joincount:
                if (!expr->hasAttribute(localAtom))
                {
                    if (isKeyedJoin(expr))
                        break;
                    if (expr->hasAttribute(lookupAtom))
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
                    IHqlExpression * limit = cur->queryAttribute(countAtom);
                    if (!limit)
                        limit = cur->queryAttribute(sizeAtom);
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

IHqlExpression * queryRootSelector(IHqlExpression * select)
{
    loop
    {
        if (select->hasAttribute(newAtom))
            return select;
        IHqlExpression * ds = select->queryChild(0);
        if (ds->getOperator() != no_select)
            return select;
        select = ds;
    }
}

static node_operator queryCompoundOp(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_table:
        return no_compound_diskread;
    case no_newkeyindex:
        return no_compound_indexread;
    case no_dataset_alias:
    case no_preservemeta:
        return queryCompoundOp(expr->queryChild(0));
    }
    throwUnexpectedOp(expr->getOperator());
}

static int compareHqlExprPtr(IInterface * * left, IInterface * * right) 
{
    return *left == *right ? 0 : *left < *right ? -1 : +1;
}

inline bool hasActivityType(IHqlExpression * expr)
{
    return (expr->isDataset() || expr->isDatarow() || expr->isDictionary());
}

//------------------------------------------------------------------------

ImplicitProjectInfo::ImplicitProjectInfo(IHqlExpression * _original, ProjectExprKind _kind) : NewTransformInfo(_original), kind(_kind)
{
    visited = false;
    gatheredSelectsUsed = false;

//The following logically belong to the complexProjectInfo, see note in header
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
        //MORE: Should only check if exists in pre-existing selects otherwise O(N^2) in items added
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
        if ((&cur == selector) ||
            ((cur.getOperator() == no_select) && (queryDatasetCursor(cur.queryChild(0)) == selector)))
            selectsUsed.remove(i);
    }
}


void ImplicitProjectInfo::removeRowsFields(IHqlExpression * expr, IHqlExpression * left, IHqlExpression * right)
{
    node_operator rowsSide = queryHasRows(expr);
    if (rowsSide == no_none)
        return;

    IHqlExpression * rowsid = expr->queryAttribute(_rowsid_Atom);
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
}


void ComplexImplicitProjectInfo::addAllOutputs()
{
    outputFields.setAll();
}


IHqlExpression * ComplexImplicitProjectInfo::createOutputProject(IHqlExpression * ds)
{
    if (ds->getOperator() == no_null)
        return createDataset(no_null, LINK(queryOutputRecord()));
    OwnedHqlExpr seq = createSelectorSequence();
    OwnedHqlExpr left = createSelector(no_left, ds, seq);
    OwnedHqlExpr self = getSelf(queryOutputRecord());
    IHqlExpression * transform = createMappingTransform(self, left);
    if (ds->isDataset())
        return createDataset(no_hqlproject, LINK(ds), createComma(transform, LINK(seq)));
    else
        assertex(!ds->isDictionary());
    return createRow(no_projectrow, LINK(ds), createComma(transform, LINK(seq)));
}


void ComplexImplicitProjectInfo::finalizeOutputRecord()
{
    //MORE: Create them in the same order as the original record + don't change if numOutputFields = numOriginalOutputFields
    if (!queryOutputRecord())
    {
        bool canPack = (safeToReorderOutput() && okToOptimize());
        outputFields.calcFinalRecord(canPack, false);
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

    switch (getChildDatasetType(original))
    {
    case childdataset_none: 
    case childdataset_many_noscope:
    case childdataset_many:
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


void ComplexImplicitProjectInfo::inheritRequiredFields(const UsedFieldSet & requiredList)
{
    //Temporary code to avoid a check.  It is permissible for the fields of an AnyTypeActivity to not match
    if ((activityKind() == AnyTypeActivity) && !outputFields.includeAll() && requiredList.includeAll())
        outputFields.setOriginal(requiredList.queryOriginal());
    outputFields.unionFields(requiredList);
}

void ComplexImplicitProjectInfo::notifyRequiredFields(ComplexImplicitProjectInfo * whichInput)
{
    if (activityKind() == PassThroughActivity)
    {
        whichInput->inheritRequiredFields(outputFields);
    }
    else if ((activityKind() == RollupTransformActivity) || (activityKind() == IterateTransformActivity))
    {
        whichInput->inheritRequiredFields(leftFieldsRequired);
        whichInput->inheritRequiredFields(rightFieldsRequired);
    }
    else if (original->getOperator() == no_fetch)
    {
        assertex(whichInput == &inputs.item(0));
        whichInput->inheritRequiredFields(rightFieldsRequired);
    }
    else if (whichInput == &inputs.item(0))
    {
        whichInput->inheritRequiredFields(leftFieldsRequired);
        //can occur if same dataset is used for left and right - e.g., non-symmetric self join
        if ((inputs.ordinality() > 1) && (whichInput == &inputs.item(1)))
            whichInput->inheritRequiredFields(rightFieldsRequired);
    }
    else if (whichInput == &inputs.item(1))
    {
        whichInput->inheritRequiredFields(rightFieldsRequired);
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
    assertex(other->queryOutputRecord());
    outputFields.set(other->outputFields);
}


//-----------------------------------------------------------------------------------------------

static HqlTransformerInfo implicitProjectTransformerInfo("ImplicitProjectTransformer");
ImplicitProjectTransformer::ImplicitProjectTransformer(HqlCppTranslator & _translator, bool _optimizeSpills)
: NewHqlTransformer(implicitProjectTransformerInfo), translator(_translator)
{
    const HqlCppOptions & transOptions = translator.queryOptions();
    targetClusterType = translator.getTargetClusterType();
    options.isRoxie = (targetClusterType == RoxieCluster);
    options.optimizeProjectsPreservePersists = transOptions.optimizeProjectsPreservePersists;
    options.autoPackRecords = transOptions.autoPackRecords;
    options.notifyOptimizedProjects = translator.notifyOptimizedProjectsLevel();
    options.optimizeSpills = _optimizeSpills;
    options.enableCompoundCsvRead = translator.queryOptions().enableCompoundCsvRead;
    options.projectNestedTables = translator.queryOptions().projectNestedTables;
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
                if ((extra->activityKind() != NonActivity) || (expr->getOperator() == no_record))
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
            complexExtra->outputFields.setRecord(expr);
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
            if (hasActivityType(expr))
            {
                //MORE: These means that selects from a parent dataset don't project down the parent dataset.
                //I'm not sure how big an issue that would be.
                allowActivity = false;
                Parent::analyseExpr(expr);
                allowActivity = true;
                assertex(extra->activityKind() == SourceActivity);
                activities.append(*LINK(expr));
                IHqlExpression * record = expr->queryRecord();
                complexExtra->setOriginalRecord(queryBodyComplexExtra(record));
                analyseExpr(record);
            }
            else if (isNewSelector(expr))
            {
                Parent::analyseExpr(expr);
                assertex(extra->activityKind() == ScalarSelectActivity);
                if (expr->hasAttribute(newAtom))
                    connect(expr->queryChild(0), expr);
                activities.append(*LINK(expr));
            }
            gatherFieldsUsed(expr, extra);
            return;
        case no_activerow:
            {
                assertex(extra->activityKind() == SourceActivity);
                allowActivity = false;
                Parent::analyseExpr(expr);
                allowActivity = true;
                break;
            }
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            allowActivity = false;
            Parent::analyseExpr(expr);
            allowActivity = true;
            return;
        case no_thor:
            if (hasActivityType(expr))
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
        case no_executewhen:
            if (expr->isDataset() || expr->isDatarow())
            {
                assertex(extra->activityKind() == SimpleActivity);
                Parent::analyseExpr(expr);
                connect(expr->queryChild(0), expr);
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
                unsigned numArgs = expr->numChildren();
                unsigned first = 0;
                unsigned last = numArgs;
                unsigned start = 0;
                if (!expr->isAction() && !expr->isDataset() && !expr->isDatarow() && !expr->isDictionary())
                {
                    switch (op)
                    {
                    case NO_AGGREGATE:
                    case no_createset:
                        last = 1;
                        break;
                    case no_sizeof:
                        last = 0;
                        break;
                    default:
                        extra->kind = NonActivity;
                        break;
                    }
                }
                else
                {
                    IHqlExpression * record = expr->queryRecord();
                    if (!record && expr->queryChild(0))
                        record = expr->queryChild(0)->queryRecord();
                    if (!record || !isSensibleRecord(record))
                        extra->preventOptimization();

                    first = getFirstActivityArgument(expr);
                    last = first + getNumActivityArguments(expr);
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
                }

                for (unsigned i =start; i < numArgs; i++)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    allowActivity = (i >= first) && (i < last);
                    analyseExpr(cur);
                    if (allowActivity)
                    {
                        if (extra->kind == NonActivity)
                        {
                            ImplicitProjectInfo * childExtra = queryBodyExtra(cur);
                            childExtra->preventOptimization();
                        }
                        else if (!cur->isAction() && !cur->isAttribute())
                        {
                            connect(cur, expr);
                        }
                    }
                }

                if (extra->kind == NonActivity)
                    gatherFieldsUsed(expr, extra);

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
            break;
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
        case RollupTransformActivity:
        case IterateTransformActivity:
        case DenormalizeActivity:
        case CreateRecordSourceActivity:
            if (hasUnknownTransform(expr))
                complexExtra->preventOptimization();
            break;
        }

        activities.append(*LINK(expr));

        IHqlExpression * child = expr->queryChild(0);
        switch (extra->activityKind())
        {
        case CreateRecordActivity:
            setOriginal(complexExtra->leftFieldsRequired, child);
            break;
        case CreateRecordLRActivity:
            setOriginal(complexExtra->leftFieldsRequired, child);
            setOriginal(complexExtra->rightFieldsRequired, expr->queryChild(1));
            break;
        case CompoundActivity:
        case CompoundableActivity:
        case CreateRecordSourceActivity:
        case AnyTypeActivity:
            break;
        case RollupTransformActivity:
        case IterateTransformActivity:
            setOriginal(complexExtra->leftFieldsRequired, child);
            setOriginal(complexExtra->rightFieldsRequired, child);
            break;
        case DenormalizeActivity:
            setOriginal(complexExtra->leftFieldsRequired, child);
            setOriginal(complexExtra->rightFieldsRequired, expr->queryChild(1));
            break;
        case FixedInputActivity:
            assertex(child && child->queryRecord());
            setOriginal(complexExtra->leftFieldsRequired, child);
            if (getNumChildTables(expr) >= 2)
                setOriginal(complexExtra->rightFieldsRequired, expr->queryChild(1));
            break;
        case SourceActivity:
        case PassThroughActivity:
        case ScalarSelectActivity:
            break;
        case SinkActivity:
            setOriginal(complexExtra->leftFieldsRequired, child);
            break;
        case SimpleActivity:
            if (expr->getOperator() == no_compound)
                setOriginal(complexExtra->leftFieldsRequired, expr->queryChild(1));
            else
                setOriginal(complexExtra->leftFieldsRequired, child);
            break;
        default:
            throwUnexpected();
        }
    }

    IHqlExpression * record = expr->queryRecord();
    if (record && !isPatternType(type) && !expr->isTransform())
    {
        assertex(complexExtra);
        complexExtra->setOriginalRecord(queryBodyComplexExtra(record));
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
            if (options.projectNestedTables)
            {
                bool isNew;
                IHqlExpression * ds = querySelectorDataset(expr, isNew);
                if (isNew)
                    inheritActiveFields(extra, ds);
                else
                    extra->addActiveSelect(expr);
            }
            else
            {
                //Either inherit from the dataset if new, or add the root field (x.a.b only adds x.a)
                IHqlExpression * cur = expr;
                loop
                {
                    IHqlExpression * ds = cur->queryChild(0);
                    if (cur->hasAttribute(newAtom))
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
            IAtom * name = expr->queryName();
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
            case childdataset_many_noscope:
            case childdataset_if:
            case childdataset_case:
            case childdataset_map:
            case childdataset_dataset_noscope: 
                inheritActiveFields(expr, extra, 0, max);
                //None of these have any scoped arguments, so no need to remove them
                break;
            case childdataset_many:
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
            //The following code is commented out because it doesn't work with implicit normalize of child datasets
            //E.g., ds(count(ds.x.y(ds.x != 0)))
            //The problem is that it is hard to determine when ds.x is no longer valid. (It is implicitly brought
            //into scope by the use of ds.x.y.  The correct solution is for it to be removed by the last thing
            //that uses the dataset operator - i.e. the count, or once normalized the [1] on the no_newaggregate.
            //There are (semi-pathological) examples of this in the regression suite.
            //Until it is revisited and fixed the following lines should stay commented out.
#if 0
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
        if (hasActivityType(expr))
            return SourceActivity;
        if (isNewSelector(expr))
            return ScalarSelectActivity;
        return NonActivity;
    case no_activerow:
        return SourceActivity;
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return NonActivity;
    case no_typetransfer:
        if (hasActivityType(expr))
            return SourceActivity;
        return NonActivity;
    case no_thor:
        if (hasActivityType(expr))
            return SimpleActivity;
        return NonActivity;
    case no_compound:
        if (expr->isDataset() || expr->isDictionary())
            return SimpleActivity;
        if (expr->isDatarow())
            return ComplexNonActivity;
        return NonActivity;
    case no_executewhen:
        if (hasActivityType(expr))
            return SimpleActivity;
        return NonActivity;
    case no_subgraph:
    case no_libraryscopeinstance:
        return NonActivity;
    case no_mergejoin:
    case no_nwayjoin:           // could probably project output of this one...
    case no_nwaymerge:
    case no_libraryselect:
    case no_datasetfromdictionary:
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
    case no_rollupgroup:
    case no_projectrow:
        return CreateRecordActivity;
    case no_inlinetable:
    case no_dataset_from_transform:
        return CreateRecordSourceActivity;
    case no_createdictionary:
        return FixedInputActivity;
    case no_selectmap:
        return SourceActivity;
    case no_extractresult:
    case no_apply:
        return SinkActivity;
    case no_denormalizegroup:
    case no_join:
    case no_fetch:
        return CreateRecordLRActivity;
    case no_process:            // optimization currently disabled...
        return PassThroughActivity;
    case no_iterate:
        return IterateTransformActivity;
    case no_rollup:
        return RollupTransformActivity;
    case no_denormalize:
        return DenormalizeActivity;
    case no_null:
        if (expr->isAction())
            return NonActivity;
        return AnyTypeActivity;
    case no_skip:
    case no_fail:
        if (hasActivityType(expr))
            return AnyTypeActivity;
        return NonActivity;
    case no_table:
        switch (expr->queryChild(2)->getOperator())
        {
        case no_thor:
        case no_flat:
            if (expr->hasAttribute(_spill_Atom) && options.isRoxie)
                return SourceActivity;
            if (options.optimizeProjectsPreservePersists)
            {
                //Don't project persists because it can mess up the redistibution code.
                if (expr->hasAttribute(_workflowPersist_Atom))
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
                if (root && root->hasAttribute(_workflowPersist_Atom))
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
        return SourceActivity;
    case no_getresult:
        if (hasActivityType(expr))
            return SourceActivity;
        return NonActivity;
    case no_allnodes:
    case no_httpcall:
    case no_soapcall:
    case no_newsoapcall:
    case no_libraryinput:
    case no_thisnode:
        if (hasActivityType(expr))
            return SourceActivity;
        return NonActivity;
    case no_pipe:
    case no_nofold:
    case no_nohoist:
        if (hasActivityType(expr))
            return FixedInputActivity;
        return NonActivity;
    case no_soapcall_ds:
    case no_newsoapcall_ds:
    case no_output:
    case no_distribution:
    case no_buildindex:
    case no_spill:
    case no_setgraphresult:
    case no_setgraphloopresult:
    case no_spillgraphresult:
        //MORE: Rethink these later:
    case no_combine:
    case no_combinegroup:
    case no_regroup:
    case no_loop:
    case no_graphloop:
    case no_filtergroup:            //anything else would be tricky...
    case no_normalizegroup:
    case no_getgraphloopresultset:
        return FixedInputActivity;
    case no_aggregate:
        if (expr->hasAttribute(mergeTransformAtom))
            return FixedInputActivity;
        return FixedInputActivity;  //MORE:???? Should be able to optimize this
    case no_fromxml:                // A bit bit like a source activity, no transform..., but has an input
    case no_fromjson:
        return SourceActivity;
    case no_selfjoin:
        return CreateRecordActivity;
    case no_if:
        if (hasActivityType(expr))
            return PassThroughActivity;
        return NonActivity;
    case no_addfiles:
    case no_merge:
    case no_nonempty:
    case no_cogroup:
    case no_chooseds:
        return PassThroughActivity;
    case no_keydiff:
    case no_keypatch:
        return NonActivity;
    case no_datasetfromrow:
        return SimpleActivity;
    case no_newtransform:
    case no_transform:
        return NonActivity;
        return ComplexNonActivity;
    case no_record:
    case no_assign:
    case no_assignall:
        return NonActivity;
    case NO_AGGREGATE:
    case no_createset:
        return SinkActivity;
    case no_call:
    case no_externalcall:
        if (hasActivityType(expr))
            return SourceActivity;
        //MORE: What about parameters??
        return NonActivity;
    case no_commonspill:
    case no_readspill:
        return SimpleActivity;
    case no_writespill:
        return SinkActivity;
    case no_preservemeta:
    case no_dataset_alias:
        if (getProjectExprKind(expr->queryChild(0)) == CompoundableActivity)
            return CompoundableActivity;
        return PassThroughActivity;
    case no_serialize:
    case no_deserialize:
        //This needs to map fields by name.  Until that is implemented don't project these types.
        return FixedInputActivity;
    }

    ITypeInfo * type = expr->queryType();
    if (!type)
        return NonActivity;

    type_t tc = type->getTypeCode();
    switch (tc)
    {
    case type_void:
        if (getNumChildTables(expr) > 0)
            return SinkActivity;
        return NonActivity;
    case type_row:
    case type_table:
    case type_groupedtable:
        break;
    case type_dictionary:
        return FixedInputActivity;
    case type_transform:
        return NonActivity;
    default:
        return NonActivity;
    }

    if (getNumActivityArguments(expr) == 0)
        return SourceActivity;
    return SimpleActivity;
}

void ImplicitProjectTransformer::processSelect(ComplexImplicitProjectInfo * extra, IHqlExpression * curSelect, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    if (leftSelect)
        processMatchingSelector(extra->leftFieldsRequired, curSelect, leftSelect);
    if (ds)
        processMatchingSelector(extra->leftFieldsRequired, curSelect, ds);
    if (rightSelect)
        processMatchingSelector(extra->rightFieldsRequired, curSelect, rightSelect);

    switch (extra->activityKind())
    {
    case DenormalizeActivity:
        //For DENORMALIZE the transform is always called, possibly multiple times.  Therefore
        //if a field is used from the output it must be included in the input (but could be blanked)
        //if a field is used from LEFT then it must be in the input and the output
        processMatchingSelector(extra->outputFields, curSelect, leftSelect);
        break;
    case RollupTransformActivity:
    case IterateTransformActivity:
        //For ROLLUP/ITERATE the transform may or may not be called.  Therefore
        //if a field is used from the output it is used from the input [ handled in the main processing loop]
        //if a field is used from LEFT then it must be in the input and the output
        //Anything used from the input must be in the output (but could be blanked) - handled elsewhere
        processMatchingSelector(extra->outputFields, curSelect, leftSelect);
        if (ds)
            processMatchingSelector(extra->outputFields, curSelect, ds);
        break;
    }
}



void ImplicitProjectTransformer::processSelects(ComplexImplicitProjectInfo * extra, SelectUsedArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    ForEachItemIn(i2, selectsUsed)
    {
        IHqlExpression * curSelect = &selectsUsed.item(i2);
        processSelect(extra, curSelect, ds, leftSelect, rightSelect);
    }
}

void ImplicitProjectTransformer::processSelects(ComplexImplicitProjectInfo * extra, HqlExprArray const & selectsUsed, IHqlExpression * ds, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    ForEachItemIn(i2, selectsUsed)
    {
        IHqlExpression * curSelect = &selectsUsed.item(i2);
        processSelect(extra, curSelect, ds, leftSelect, rightSelect);
    }
}


void ImplicitProjectTransformer::processTransform(ComplexImplicitProjectInfo * extra, IHqlExpression * transform, IHqlExpression * dsSelect, IHqlExpression * leftSelect, IHqlExpression * rightSelect)
{
    HqlExprCopyArray assigns;
    unwindTransform(assigns, transform);
    ForEachItemIn(itr, assigns)
    {
        IHqlExpression * cur = &assigns.item(itr);
        //Need to handle skip attributes...
        switch (cur->getOperator())
        {
        case no_assign:
            {
                IHqlExpression * value = cur->queryChild(1);
                if (!value->isPure())
                {
                    IHqlExpression * lhs = cur->queryChild(0);
                    processMatchingSelector(extra->outputFields, lhs, lhs->queryChild(0));
                    const SelectUsedArray & selectsUsed = querySelectsUsed(value);
                    processSelects(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
                }
                break;
            }
        case no_attr:
        case no_attr_expr:
            break;
        default:
            {
                const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                processSelects(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
                break;
            }
        }
    }
}


void ImplicitProjectTransformer::calculateFieldsUsed(IHqlExpression * expr)
{
    ComplexImplicitProjectInfo * extra = queryBodyComplexExtra(expr);

    if (!extra->okToOptimize())
    {
        if (expr->queryRecord() && !expr->isDictionary())
            extra->addAllOutputs();
    }
    else
    {
        ForEachItemIn(i1, extra->outputs)
            extra->outputs.item(i1).notifyRequiredFields(extra);

        if (extra->outputFields.includeAll())
            assertex(extra->queryOutputRecord() != NULL);
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
                if (extra->activityKind() == CreateRecordLRActivity)
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
            HqlExprArray parentSelects;
            HqlExprArray values;
            extra->outputFields.gatherTransformValuesUsed(NULL, &parentSelects, &values, NULL, transform);
            processSelects(extra, parentSelects, dsSelect, leftSelect, rightSelect);
            ForEachItemIn(i, values)
                processSelects(extra, querySelectsUsed(&values.item(i)), dsSelect, leftSelect, rightSelect);
            if (!extra->outputFields.allGathered())
                assertex(extra->outputFields.allGathered());

            processTransform(extra, transform, dsSelect, leftSelect, rightSelect);

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
                processSelects(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
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
                            //All groupings have entries in the transform - find the corresponding field.
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
                            //There may possibly be no match if it hasn't been normalized yet.
                            if (match)
                                processMatchingSelector(extra->outputFields, match, match->queryChild(0));
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
            //MORE: Not sure this is neededextra->leftFieldsRequired.clone(extra->outputFields);
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
            if ((op == no_if) && expr->hasAttribute(_resourced_Atom))
            {
                extra->preventOptimization();
                extra->addAllOutputs();
            }
            else if (op == no_merge)
            {
                //Ensure all the fields used by the sort order are preserved in the input streams
                IHqlExpression * order = expr->queryAttribute(sortedAtom);
                assertex(order);
                ForEachChild(i, order)
                {
                    IHqlExpression * cur = order->queryChild(i);
                    if (!cur->isAttribute() && !cur->isConstant())          // shouldn't really happen..
                    {
                        if ((cur->getOperator() == no_select) && !isNewSelector(cur))
                        {
                            IHqlExpression * ds = queryDatasetCursor(cur);
                            if (ds == queryActiveTableSelector())
                                processMatchingSelector(extra->outputFields, cur, queryActiveTableSelector());
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
            extra->leftFieldsRequired.appendField(*LINK(root->queryChild(1)));
            break;
        }
    case RollupTransformActivity:
    case IterateTransformActivity:
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
                IHqlExpression * dsSelect = expr->queryChild(0)->queryNormalizedSelector();
                IHqlExpression * selSeq = querySelSeq(expr);
                OwnedHqlExpr leftSelect = createSelector(no_left, dsSelect, selSeq);
                OwnedHqlExpr rightSelect = createSelector(no_right, dsSelect, selSeq);

                //Need to handle skip attributes...
                processTransform(extra, transform, dsSelect, leftSelect, rightSelect);

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
                        processSelects(extra, selectsUsed, dsSelect, leftSelect, rightSelect);
                    }
                }

                //NB: outputfields can extend...
                while (!extra->outputFields.allGathered())
                {
                    HqlExprArray parentSelects;
                    HqlExprArray values;
                    HqlExprArray selfSelects;
                    extra->outputFields.gatherTransformValuesUsed(&selfSelects, &parentSelects, &values, dsSelect, transform);
\
                    //For ROLLUP/ITERATE the transform may or may not be called.  Therefore
                    //if a field is used from the output it is used from the input
                    //if a field is used from LEFT then it must be in the input and the output
                    //if a field is used from RIGHT it muse be in the output (but could be blanked) - handled elsewhere

                    //Ensure all output rows are also included in the input dataset
                    ForEachItemIn(i1, selfSelects)
                        processMatchingSelector(extra->leftFieldsRequired, &selfSelects.item(i1), dsSelect);

                    processSelects(extra, parentSelects, NULL, leftSelect, rightSelect);
                    ForEachItemIn(i2, values)
                        processSelects(extra, querySelectsUsed(&values.item(i2)), NULL, leftSelect, rightSelect);

                    //If all fields selected from the output then select all fields from the input
                    if (extra->outputFields.checkAllFieldsUsed())
                        extra->leftFieldsRequired.setAll();

                    //if selected all fields from the input then already done.
                    if (extra->leftFieldsRequired.includeAll())
                    {
                        extra->outputFields.setAll();
                        break;
                    }
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

            processTransform(extra, transform, NULL, leftSelect, rightSelect);

            //include all other attributes except for the transform
            unsigned max = expr->numChildren();
            for (unsigned i2=2; i2 < max; i2++)
            {
                IHqlExpression * cur = expr->queryChild(i2);
                if (cur != transform)
                {
                    const SelectUsedArray & selectsUsed = querySelectsUsed(cur);
                    processSelects(extra, selectsUsed, NULL, leftSelect, rightSelect);
                }
            }

            while (!extra->outputFields.allGathered())
            {
                HqlExprArray parentSelects;
                HqlExprArray values;
                HqlExprArray selfSelects;
                extra->outputFields.gatherTransformValuesUsed(&selfSelects, &parentSelects, &values, left, transform);

                //For DENORMALIZE the transform is always called, possibly multiple times.  Therefore
                //if a field is used from the output it must be included in the input (but could be blanked)
                //if a field is used from LEFT then it must be in the input and the output

                //Ensure all output rows are also included in the input dataset
                ForEachItemIn(i1, selfSelects)
                    processMatchingSelector(extra->leftFieldsRequired, &selfSelects.item(i1), left);  // more: Could blank

                processSelects(extra, parentSelects, NULL, leftSelect, rightSelect);
                ForEachItemIn(i2, values)
                    processSelects(extra, querySelectsUsed(&values.item(i2)), NULL, leftSelect, rightSelect);
            }
            break;
        }
    case FixedInputActivity:
        {
            extra->leftFieldsRequired.setAll();
            extra->rightFieldsRequired.setAllIfAny();
            if (expr->queryRecord())
                extra->addAllOutputs();
            break;
        }
    case SourceActivity:
        {
            //No inputs to worry about, and not compoundable so output record won't change.
            extra->addAllOutputs();
            break;
        }
    case SinkActivity:
    case SimpleActivity:
        {
            //inputs will be outputs required plus any fields used within the function
            //outputs will eventually match inputs when finished percolating.
            if (extra->outputFields.includeAll())
                extra->leftFieldsRequired.setAll();
            else
            {
                if (extra->activityKind() != SinkActivity)
                    extra->leftFieldsRequired.clone(extra->outputFields);

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
                        processMatchingSelector(extra->leftFieldsRequired, curSelect, leftSelect);
                        processMatchingSelector(extra->leftFieldsRequired, curSelect, rightSelect);
                        processMatchingSelector(extra->leftFieldsRequired, curSelect, ds);
                        if (extra->leftFieldsRequired.includeAll())
                            break;
                    }
                    if (extra->leftFieldsRequired.includeAll())
                        break;
                }

                if (extra->activityKind() != SinkActivity)
                {
                    if (extra->leftFieldsRequired.includeAll())
                        extra->addAllOutputs();
                }
            }
            break;
        }
    default:
        throwUnexpected();
    }
}


void ImplicitProjectTransformer::logChange(const char * message, IHqlExpression * expr, const UsedFieldSet & fields)
{
    IAtom * exprName = expr->queryName();
    if (!exprName && isCompoundSource(expr))
        exprName = expr->queryChild(0)->queryName();

    StringBuffer name, fieldText;
    if (exprName)
        name.append(exprName).append(" ");
    name.append(getOpString(expr->getOperator()));

    const UsedFieldSet * original = fields.queryOriginal();
    assertex(original);
    fieldText.append("(").append(fields.numFields());
    fieldText.append("/").append(original->numFields());
    fieldText.append(")");

    //If number removed < number remaining just log the fields removed.
    if (fields.numFields() * 2 > original->numFields())
    {
        UsedFieldSet removed;
        removed.createDifference(*original, fields);
        fieldText.append(" removed ");
        removed.getText(fieldText);
    }
    else
        fields.getText(fieldText);


    const char * const format = "ImplicitProject: %s %s now %s";
    DBGLOG(format, message, name.str(), fieldText.str());
    if (options.notifyOptimizedProjects)
    {
        if (options.notifyOptimizedProjects >= 2 || exprName)
        {
            StringBuffer messageText;
            messageText.appendf(format, message, name.str(), fieldText.str());
            translator.addWorkunitException(SeverityInformation, 0, messageText.str(), NULL);
        }
    }
}


void ImplicitProjectTransformer::getTransformedChildren(IHqlExpression * expr, HqlExprArray & children)
{
    transformChildren(expr, children);
}

IHqlExpression * ImplicitProjectTransformer::createParentTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = Parent::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);
    return transformed.getClear();
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
        return createParentTransformed(expr);

    OwnedHqlExpr transformed;
    switch (extra->activityKind())
    {
    case DenormalizeActivity:
    case RollupTransformActivity:
    case IterateTransformActivity:
        {
            //Always reduce things that create a new record so they only project the fields they need to
            if (complexExtra->outputChanged() || !complexExtra->fieldsToBlank.isEmpty())
            {
                unsigned transformPos = queryTransformIndex(expr);

                //Walk transform, only including assigns that are in the output list.
                HqlExprArray args;
                getTransformedChildren(expr, args);

                //MORE: If the input's output contains fields that are not required in this transforms output then
                //include them, but assign them default values to stop them pulling in other variables.
                IHqlExpression * transform = &args.item(transformPos);
                IHqlExpression * newTransform = complexExtra->outputFields.createFilteredTransform(transform, &complexExtra->fieldsToBlank);
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
                transformed.setown(createParentTransformed(expr));
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
                IHqlExpression * newTransform = complexExtra->outputFields.createFilteredTransform(transform, NULL);
                args.replace(*newTransform, transformPos);
                if (transform->getOperator() == no_newtransform)
                    args.replace(*LINK(complexExtra->queryOutputRecord()), transformPos-1);
                IHqlExpression * onFail = queryAttribute(onFailAtom, args);
                if (onFail)
                {
                    IHqlExpression * newTransform = complexExtra->outputFields.createFilteredTransform(onFail->queryChild(0), NULL);
                    IHqlExpression * newOnFail = createExprAttribute(onFailAtom, newTransform);
                    args.replace(*newOnFail, args.find(*onFail));
                }

                //We may have converted a count project into a project..... (see bug18839.xhql)
                if (expr->getOperator() == no_hqlproject)
                {
                    IHqlExpression * countProjectAttr = queryAttribute(_countProject_Atom, args);
                    if (countProjectAttr && !transformContainsCounter(newTransform, countProjectAttr->queryChild(0)))
                        args.zap(*countProjectAttr);
                }

                transformed.setown(expr->clone(args));
                transformed.setown(updateSelectors(transformed, expr));
                logChange("Minimize", expr, complexExtra->outputFields);
            }
            else
            {
                transformed.setown(createParentTransformed(expr));
                //MORE: Need to replace left/right with their transformed varieties because the record may have changed format
                transformed.setown(updateSelectors(transformed, expr));
            }
            break;
        }
    case CreateRecordSourceActivity:
        {
            assertex(expr->getOperator() == no_inlinetable || expr->getOperator() == no_dataset_from_transform);
            //Always reduce things that create a new record so they only project the fields they need to
            if (complexExtra->outputChanged())
            {
                HqlExprArray args;
                switch (expr->getOperator())
                {
                case no_inlinetable:
                    {
                        IHqlExpression * transforms = expr->queryChild(0);
                        HqlExprArray newTransforms;
                        ForEachChild(i, transforms)
                        {
                            IHqlExpression * transform = transforms->queryChild(i);
                            newTransforms.append(*complexExtra->outputFields.createFilteredTransform(transform, NULL));
                        }
                        args.append(*transforms->clone(newTransforms));
                        break;
                    }
                case no_dataset_from_transform:
                    {
                        IHqlExpression * transform = expr->queryChild(1);
                        args.append(*LINK(expr->queryChild(0)));
                        args.append(*complexExtra->outputFields.createFilteredTransform(transform, NULL));
                        break;
                    }
                }
                args.append(*LINK(complexExtra->queryOutputRecord()));
                unwindChildren(args, expr, 2);
                transformed.setown(expr->clone(args));

                logChange("Minimize", expr, complexExtra->outputFields);
            }
            else
            {
                transformed.setown(createParentTransformed(expr));
                //MORE: Need to replace left/right with their transformed varieties because the record may have changed format
                transformed.setown(updateSelectors(transformed, expr));
            }
            break;
        }
    case CompoundActivity:
        {
            transformed.setown(createParentTransformed(expr));
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
            transformed.setown(createParentTransformed(expr));
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
            transformed.setown(createParentTransformed(expr));
            //insert a project after the record.
            if (complexExtra->outputChanged())
            {
                logChange("Change format of dataset", expr, complexExtra->outputFields);
                HqlExprArray args;
                args.append(*LINK(complexExtra->queryOutputRecord()));
                unwindChildren(args, transformed, 1);
                transformed.setown(transformed->clone(args));
            }
            break;
        }
    case FixedInputActivity:
    case SourceActivity:
    case NonActivity:
    case ScalarSelectActivity:
    case SinkActivity:
        transformed.setown(createParentTransformed(expr));
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
                    if (next->queryRecord() != complexExtra->queryOutputRecord())
                        next.setown(complexExtra->createOutputProject(next));
                }
                args.append(*next.getClear());
            }
            transformed.setown(expr->clone(args));
            logChange("Passthrough modified", expr, complexExtra->outputFields);
        }
        else
            transformed.setown(createParentTransformed(expr));
        break;
    case SimpleActivity:
        {
            transformed.setown(createParentTransformed(expr));
            IHqlExpression * onFail = transformed->queryAttribute(onFailAtom);
            if (onFail)
            {
                IHqlExpression * newTransform = complexExtra->outputFields.createFilteredTransform(onFail->queryChild(0), NULL);
                IHqlExpression * newOnFail = createExprAttribute(onFailAtom, newTransform);
                transformed.setown(replaceOwnedAttribute(transformed, newOnFail));
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
    return fields.requiresFewerFields(input.outputFields);
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
    case RollupTransformActivity:
    case IterateTransformActivity:
        {
            //output must always match the input..., but any fields that are in the input, but not needed in the output we'll add as exceptions
            //and assign default values to them, otherwise it can cause other fields to be required in the input + causes chaos
            extra->fieldsToBlank.createDifference(extra->inputs.item(0).outputFields, extra->outputFields);
            extra->outputFields.unionFields(extra->fieldsToBlank);
            extra->fieldsToBlank.optimizeFieldsToBlank(extra->outputFields, queryNewColumnProvider(expr));
            if (!extra->fieldsToBlank.isEmpty())
            {
                const char * opString = getOpString(expr->getOperator());
                StringBuffer fieldText;
                extra->fieldsToBlank.getText(fieldText);
                DBGLOG("ImplicitProject: Fields %s for %s not required by outputs - so blank in transform", fieldText.str(), opString);
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
            //Branches coming into this IF/MERGE etc. may have different fields (e.g., because of ITERATEs), and
            //the output fields may be smaller (e.g., no merge sort conditions, no fields used and inputs filter)
            //So use the intersection of the inputfields as the output record.  90% of the time they will be
            //the same so no projects will be introduced.
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
    case SinkActivity:
        break;
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
    case ThorLCRCluster:
        //worth inserting projects to reduce copying, spilling, but primarily data transferred between nodes.
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
    //MORE: Clean me up using new flags when they are merged
    IHqlExpression * newDs = newExpr->queryChild(0);
    IHqlExpression * oldDs = oldExpr->queryChild(0);
    switch (getChildDatasetType(newExpr))
    {
    case childdataset_none: 
    case childdataset_many_noscope:
    case childdataset_many:
    case childdataset_if:
    case childdataset_case:
    case childdataset_map:
    case childdataset_dataset_noscope: 
        return LINK(newExpr);
        //None of these have any scoped arguments, so no need to remove them
        break;
    case childdataset_dataset:
        {
            return updateMappedFields(newExpr, oldDs->queryNormalizedSelector(), newDs->queryNormalizedSelector(), 1);
        }
    case childdataset_datasetleft:
        {
            OwnedHqlExpr mapped = updateMappedFields(newExpr, oldDs->queryNormalizedSelector(), newDs->queryNormalizedSelector(), 1);
            IHqlExpression * selSeq = querySelSeq(newExpr);
            assertex(selSeq == querySelSeq(oldExpr));
            OwnedHqlExpr newLeft = createSelector(no_left, newDs, selSeq);
            OwnedHqlExpr oldLeft = createSelector(no_left, oldDs, selSeq);
            return updateChildSelectors(mapped, oldLeft, newLeft, 1);
        }

    case childdataset_left:
        {
            IHqlExpression * selSeq = querySelSeq(newExpr);
            assertex(selSeq == querySelSeq(oldExpr));
            OwnedHqlExpr newLeft = createSelector(no_left, newDs, selSeq);
            OwnedHqlExpr oldLeft = createSelector(no_left, oldDs, selSeq);
            return updateChildSelectors(newExpr, oldLeft, newLeft, 1);
        }
    case childdataset_same_left_right:
    case childdataset_top_left_right:
    case childdataset_nway_left_right:
        {
            OwnedHqlExpr mapped = updateMappedFields(newExpr, oldDs->queryNormalizedSelector(), newDs->queryNormalizedSelector(), 1);

            IHqlExpression * selSeq = querySelSeq(newExpr);
            assertex(selSeq == querySelSeq(oldExpr));
            OwnedHqlExpr newLeft = createSelector(no_left, newExpr->queryChild(0), selSeq);
            OwnedHqlExpr oldLeft = createSelector(no_left, oldExpr->queryChild(0), selSeq);
            OwnedHqlExpr ds1 = updateChildSelectors(mapped, oldLeft, newLeft, 1);

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

const SelectUsedArray & ImplicitProjectTransformer::querySelectsUsedForField(IHqlExpression * transform, IHqlExpression * field)
{
    IHqlExpression * transformValues = queryTransformAssignValue(transform, field);
    assertex(transformValues);
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
