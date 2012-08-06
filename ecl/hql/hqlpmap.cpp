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
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "hqlerrors.hpp"
#include "hqlpmap.hpp"
#include "hqlutil.hpp"
#include "hqltrans.ipp"
#include "hqlthql.hpp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"

bool isNullMapping(IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    return (oldDataset == newDataset);
}

//-------------------------------------------------------------------------------------------------

IHqlExpression * FullExpandMonitor::onExpandSelector()
{
    return LINK(createRow);
//  return createValue(no_newrow, LINK(createRow));
}

//-------------------------------------------------------------------------------------------------

static IHqlExpression * optimizedReplaceSelector(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    switch (expr->getOperator())
    {
    case no_constant:
    case no_attr:
        return LINK(expr);
    case no_select:
        {
            IHqlExpression * lhs = expr->queryChild(0);
            IHqlExpression * field = expr->queryChild(1);
            OwnedHqlExpr newLhs;
            if (expr->hasProperty(newAtom))
            {
                newLhs.setown(optimizedReplaceSelector(lhs, oldDataset, newDataset));
            }
            else
            {
                if (lhs == oldDataset)
                {
                    if (newDataset->getOperator() == no_newrow)
                        return createNewSelectExpr(LINK(newDataset->queryChild(0)), LINK(field));

                    if (newDataset->getOperator() == no_activerow)
                        newDataset = newDataset->queryChild(0);

                    return createSelectExpr(LINK(newDataset->queryNormalizedSelector()), LINK(field));
                }
                else
                    newLhs.setown(optimizedReplaceSelector(lhs, oldDataset, newDataset));
            }

            if (newLhs)
                return replaceChild(expr, 0, newLhs);
            return NULL;
        }
    case no_implicitcast:
    case no_cast:
        {
            IHqlExpression * newUncast = optimizedReplaceSelector(expr->queryChild(0), oldDataset, newDataset);
            if (!newUncast)
                return NULL;
            OwnedHqlExpr ret = createValue(expr->getOperator(), expr->getType(), newUncast);
            return expr->cloneAllAnnotations(ret);
        }
    case no_hash:
    case no_hash32:
    case no_hash64:
    case no_sortlist:
    case no_concat:
    case no_trim:
    case no_negate:
    case no_eq:
    case no_ne:
    case no_sizeof:
    case no_attr_expr:
    case no_add:
        {
            HqlExprArray args;
            ForEachChild(i, expr)
            {
                IHqlExpression * cur = expr->queryChild(i);
                IHqlExpression * newCur = optimizedReplaceSelector(cur, oldDataset, newDataset);
                if (!newCur)
                    return NULL;
                args.append(*newCur);
            }
            return expr->clone(args);
        }
    }
    return NULL;
}


//oldDataset is either an active selector (e.g., no_left) or a dataset.
//newDataset is either an active selector (e.g., no_left) or a dataset (implying activerow(dataset) or newrow(dataset).
IHqlExpression * replaceSelector(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    if (!expr) return NULL;
    if (isNullMapping(oldDataset, newDataset))
        return LINK(expr);

    IHqlExpression *ret = optimizedReplaceSelector(expr, oldDataset->queryNormalizedSelector(), newDataset);
    if (ret)
        return ret;

    node_operator op = oldDataset->getOperator();
    if (op == no_left || op == no_right)
        return newReplaceSelector(expr, oldDataset, newDataset);

    HqlMapSelectorTransformer transformer(oldDataset, newDataset);
    return transformer.transformRoot(expr);
}


void replaceSelectors(HqlExprArray & out, IHqlExpression * expr, unsigned first, IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    unsigned max = expr->numChildren();
    unsigned iChild;
    for (iChild = first; iChild < max; iChild++)
    {
        IHqlExpression *ret = optimizedReplaceSelector(expr->queryChild(iChild), oldDataset->queryNormalizedSelector(), newDataset);
        if (!ret)
            break;
        out.append(*ret);
    }

    if (iChild == max)
        return;

    node_operator op = oldDataset->getOperator();
    if (op == no_left || op == no_right)
    {
        NewSelectorReplacingTransformer transformer;
        transformer.initSelectorMapping(oldDataset, newDataset);

        for (; iChild < max; iChild++)
            out.append(*transformer.transformRoot(expr->queryChild(iChild)));
    }
    else
    {
        HqlMapSelectorTransformer transformer(oldDataset, newDataset);
        for (; iChild < max; iChild++)
            out.append(*transformer.transformRoot(expr->queryChild(iChild)));
    }
}

//---------------------------------------------------------------------------------------------------------------------

void replaceSelectors(HqlExprArray & exprs, unsigned first, IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    unsigned max = exprs.ordinality();
    unsigned iChild;
    for (iChild = first; iChild < max; iChild++)
    {
        IHqlExpression *ret = optimizedReplaceSelector(&exprs.item(iChild), oldDataset->queryNormalizedSelector(), newDataset);
        if (!ret)
            break;
        exprs.replace(*ret, iChild);
    }

    if (iChild == max)
        return;

    node_operator op = oldDataset->getOperator();
    if (op == no_left || op == no_right)
    {
        NewSelectorReplacingTransformer transformer;
        transformer.initSelectorMapping(oldDataset, newDataset);

        for (; iChild < max; iChild++)
            exprs.replace(*transformer.transformRoot(&exprs.item(iChild)), iChild);
    }
    else
    {
        HqlMapSelectorTransformer transformer(oldDataset, newDataset);
        for (; iChild < max; iChild++)
            exprs.replace(*transformer.transformRoot(&exprs.item(iChild)), iChild);
    }
}


//NB: This can not be derived from NewHqlTransformer since it is called before the tree is normalised, and it creates
//inconsistent expression trees.
class HqlSelfRefTransformer
{
public:
    HqlSelfRefTransformer(IHqlExpression * _newDataset) : newDataset(_newDataset) {}

    virtual IHqlExpression * transform(IHqlExpression * expr)
    {
        IHqlExpression * ret = (IHqlExpression *)expr->queryTransformExtra();
        if (ret)
            return LINK(ret);

        ret = createTransformed(expr);
        expr->setTransformExtra(ret);
        return ret;
    }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_record:
        case no_ifblock:
        case no_temptable:
            return LINK(expr);

        case no_selfref:
            return LINK(newDataset);
        }

        HqlExprArray args;
        bool same = true;
        ForEachChild(i, expr)
        {
            IHqlExpression * cur = expr->queryChild(i);
            IHqlExpression * transformed = transform(cur);
            args.append(*transformed);
            if (cur != transformed)
                same = false;
        }

        if (same)
            return LINK(expr);
        return expr->clone(args);
    }

protected:
    LinkedHqlExpr newDataset;
};

IHqlExpression * replaceSelfRefSelector(IHqlExpression * expr, IHqlExpression * newDataset)
{
    if (!expr) return NULL;

    HqlSelfRefTransformer transformer(newDataset);
    TransformMutexBlock procedure;
    return transformer.transform(expr);
}

IHqlExpression * scopedReplaceSelector(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset)
{
    if (!expr) return NULL;
    if (isNullMapping(oldDataset, newDataset))
        return LINK(expr);

    HqlScopedMapSelectorTransformer transformer(oldDataset, newDataset);
    return transformer.transformRoot(expr);
}

//-------------------------------------------------------------------------------------------------

//MORE: Any child selectors need normalizing.... but not necessarily removing.
void NewProjectMapper2::addMapping(IHqlExpression * select, IHqlExpression * expr)
{
    assertex(select->getOperator() == no_select || select->getOperator() == no_self);
    targets.append(*LINK(select));
    sources.append(*LINK(expr));
    assertex(expr->getOperator() != no_assign);
    if (expr->getOperator() == no_createrow)
    {
        NewProjectMapper2 * child = new NewProjectMapper2;
        child->setMapping(expr->queryChild(0));
        childIndex.append(targets.ordinality()-1);
        children.append(*child);
    }
}

void NewProjectMapper2::addTransformMapping(IHqlExpression * tgt, IHqlExpression * src)
{
#ifdef _DEBUG
    assertex(tgt->getOperator() == no_select);
    IHqlExpression * sel = queryDatasetCursor(tgt->queryChild(0));
    assertex(sel == self);
#endif
    OwnedHqlExpr castRhs = ensureExprType(src, tgt->queryType());
    addMapping(tgt, castRhs);
}

void NewProjectMapper2::setMapping(IHqlExpression * _mapping)
{
    mapping = queryNonDelayedBaseAttribute(_mapping);
    self.setown(getSelf(mapping));
}

void NewProjectMapper2::setUnknownMapping()
{
    mapping = queryUnknownAttribute();
}

void NewProjectMapper2::initMapping()
{
    if (targets.ordinality())
        return;

    switch (mapping->getOperator())
    {
    case no_record:
        setRecord(mapping);
        break;
    case no_newtransform:
        setTransform(mapping);
        break;
    case no_transform:
        setTransform(mapping);
        break;
    case no_alias_scope:
    case no_none:
    case no_externalcall:
    case no_outofline:
    case no_attr:
        break;              // avoid internal error when values not provided for a record structure
    default:
        UNIMPLEMENTED_XY("mapping", getOpString(mapping->getOperator()));
        break;
    }
}


void NewProjectMapper2::initSelf(IHqlExpression * dataset)
{
    self.setown(getSelf(dataset));
}

bool NewProjectMapper2::isMappingKnown()
{
    initMapping();
    return targets.ordinality() != 0;
}

void NewProjectMapper2::setRecord(IHqlExpression * record, IHqlExpression * selector)
{
    unsigned kids = record->numChildren();
    for (unsigned i = 0; i < kids; i++)
    {
        IHqlExpression * cur = record->queryChild(i);

        switch (cur->getOperator())
        {
        case no_field:
            {
                IHqlExpression * value = queryRealChild(cur, 0);
                OwnedHqlExpr selected = createSelectExpr(LINK(selector), LINK(cur));
                if (value)
                {
                    OwnedHqlExpr castValue = ensureExprType(value, cur->queryType());
                    addMapping(selected, castValue);
                }
                else if (cur->isDatarow())
                {
                    setRecord(cur->queryRecord(), selected);
                }
                else
                {
                    //Don't throw an exception because the locationIndependent version of a no_usertable/no_selectfields has no values in the record
                    //throwUnexpected();
                }
            }
            break;
        case no_ifblock:
            setRecord(cur->queryChild(1), selector);
            break;
        case no_record:
            setRecord(cur, selector);
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        }
    }
}


void NewProjectMapper2::setRecord(IHqlExpression * record)
{
    setRecord(record, self);
}


void NewProjectMapper2::setTransformRowAssignment(IHqlExpression * nestedSelf, IHqlExpression * lhs, IHqlExpression * rhs, IHqlExpression * record, TableProjectMapper & mapper)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            setTransformRowAssignment(nestedSelf, lhs, rhs, cur, mapper);
            break;
        case no_ifblock:
            setTransformRowAssignment(nestedSelf, lhs, rhs, cur->queryChild(1), mapper);
            break;
        case no_field:
            {
                OwnedHqlExpr search = createSelectExpr(LINK(nestedSelf), LINK(cur));
                OwnedHqlExpr expanded = mapper.expandFields(search, nestedSelf, nestedSelf, nestedSelf);
                OwnedHqlExpr lhsSelect = createSelectExpr(LINK(lhs), LINK(cur));
                addTransformMapping(lhsSelect, expanded);
                if (cur->queryType()->getTypeCode() == type_row)
                    setTransformRowAssignment(lhsSelect, expanded, cur->queryRecord());
            }
            break;
        }
    }
}


void NewProjectMapper2::setTransformRowAssignment(IHqlExpression * lhs, IHqlExpression * rhs, IHqlExpression * record)
{
    if (false && rhs->getOperator() == no_createrow)            //possibly should check if system created.  Try enabling when have some examples it affects
    {
        TableProjectMapper mapper(rhs);
        OwnedHqlExpr nestedSelf = getSelf(rhs);
        setTransformRowAssignment(nestedSelf, lhs, rhs, record, mapper);
        return;
    }

    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            setTransformRowAssignment(lhs, rhs, cur);
            break;
        case no_ifblock:
            setTransformRowAssignment(lhs, rhs, cur->queryChild(1));
            break;
        case no_field:
            {
                OwnedHqlExpr lhsSelect = createSelectExpr(LINK(lhs), LINK(cur));
                OwnedHqlExpr rhsSelect = createNewSelectExpr(LINK(rhs), LINK(cur));
                addTransformMapping(lhsSelect, rhsSelect);

                if (cur->queryType()->getTypeCode() == type_row)
                    setTransformRowAssignment(lhsSelect, rhsSelect, cur->queryRecord());
            }
            break;
        }
    }
}


void NewProjectMapper2::setTransform(IHqlExpression * transform)
{
    unsigned max = transform->numChildren();
    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression * cur = transform->queryChild(idx);
        switch (cur->getOperator())
        {
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0);
                IHqlExpression * rhs = cur->queryChild(1);
                addTransformMapping(lhs, rhs);
#if 0
                if (lhs->isDatarow() && rhs->isDatarow())
                {
                    //MORE: Can the following be delayed, and would it be better if it was?
                    IHqlExpression * lRecord = lhs->queryRecord();
                    IHqlExpression * rRecord = rhs->queryRecord();
                    assertex(lRecord == rRecord);
                    setTransformRowAssignment(lhs, rhs, rRecord);
                }
#endif
            }
            break;
        case no_assignall:
            setTransform(cur);
            break;
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
        case no_alias_scope:
        case no_skip:
        case no_assert:
            break;
        default:
            assertex(!"Transforms should only contain assignments");
            break;
        }
    }
}


inline bool isConstantValue(IHqlExpression * expr) { return expr->isConstant(); }

IHqlExpression * NewProjectMapper2::recursiveExpandSelect(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent)
{
    IHqlExpression * cur = expr->queryChild(0);
    if (cur->queryNormalizedSelector() == oldDataset)
//  if (cur == oldDataset)
    {
        OwnedHqlExpr plain = replaceSelector(expr, oldDataset, self);

        unsigned match = targets.find(*plain);
        if (match == NotFound)
        {
            if (ignoreMissingFields)
                return LINK(expr);
#if 0
            dbglogExpr(plain.get());
            ForEachItemIn(i, targets)
                dbglogExpr(&targets.item(i));
#endif
            IHqlExpression * field = expr->queryChild(1);
            throwError1(HQLERR_SelectedFieldXNotInDataset, field->queryName()->str());
        }
        IHqlExpression * expanded = &sources.item(match);
        if ((newDataset == oldParent) || !newDataset)
            return LINK(expanded);
        return replaceSelector(expanded, oldParent, newDataset); 
    }

    if ((cur->getOperator() != no_select) || !cur->isDatarow())
        return NULL;

    OwnedHqlExpr newDs = recursiveExpandSelect(cur, oldDataset, newDataset, oldParent);
    if (newDs)
    {
        if (newDs->getOperator() == no_select)
            return createSelectExpr(newDs.getClear(), LINK(expr->queryChild(1)));
        return createNewSelectExpr(newDs.getClear(), LINK(expr->queryChild(1)));
    }
    return NULL;
}


IHqlExpression * NewProjectMapper2::doNewExpandSelect(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent)
{
    OwnedHqlExpr expanded = recursiveExpandSelect(expr, oldDataset, newDataset, oldParent);
    if (expanded)
    {
        if (expandCallback)
            expandCallback->onExpand(expr, expanded);
        expr->setTransformExtra(expanded);
        return expanded.getClear();
    }
    return NULL;
}


IHqlExpression * NewProjectMapper2::doExpandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent)
{
    IInterface * extra = expr->queryTransformExtra();
    if (extra)
        return (IHqlExpression *)LINK(extra);

    if (expandCallback && (expr == oldDataset))
    {
        OwnedHqlExpr mapped = expandCallback->onExpandSelector();
        if (mapped)
            return mapped.getClear();
    }

    IHqlExpression * ret = NULL;
    switch (expr->getOperator())
    {
    case no_attr:
    case no_field:
    case no_record:
        ret = LINK(expr);
        break;
    case no_activerow:
        {
            IHqlExpression * ds = expr->queryChild(0);
            if (ds->getOperator() == no_select)
                ret = doNewExpandSelect(ds, oldDataset, newDataset, oldParent);
            break;
        }
    case no_projectrow:
        {
            if (expr->queryChild(0) == oldDataset)
            {
                OwnedHqlExpr oldLeft = createSelector(no_left, oldDataset, querySelSeq(expr));
                OwnedHqlExpr newTransform = expandFields(expr->queryChild(1), oldLeft, newDataset, oldParent, expandCallback);
                //Strange, but could have a no_projectrow where the fields refer to fields in the parent dataset instead of LEFT
                //E.g., hfrs3(4954)
                OwnedHqlExpr newTransform2 = expandFields(newTransform, oldDataset, newDataset, oldParent, expandCallback);
                return createRow(no_createrow, newTransform2.getClear());
            }
        }
        break;
    case no_select:
        {
            ret = doNewExpandSelect(expr, oldDataset, newDataset, oldParent);
            break;
        }
    }

    if (!ret)
    {
        unsigned max = expr->numChildren();
        if (max != 0)
        {
            unsigned numNonHidden = activityHidesSelectorGetNumNonHidden(expr, oldDataset);
            if (numNonHidden == 0)
                numNonHidden = max;
            HqlExprArray args;
            args.ensure(max);
            bool same = true;
            for (unsigned idx=0; idx < max; idx++)
            {
                IHqlExpression * cur = expr->queryChild(idx);
                IHqlExpression * expand;
                if (idx < numNonHidden)
                    expand = doExpandFields(cur, oldDataset, newDataset, oldParent);
                else
                    expand = LINK(cur);
                if (cur != expand)
                    same = false;
                args.append(*expand);
            }
            if (!same)
            {
                ret = expr->clone(args);
                if (expandCallback && expr->isDataset())
                    expandCallback->onDatasetChanged(ret, expr);
            }
            else
                ret = LINK(expr);
        }
        else
            ret = LINK(expr);
    }

    expr->setTransformExtra(ret);
    return ret;
}

IHqlExpression * NewProjectMapper2::expandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent, IExpandCallback * _expandCallback)
{
    if (!expr) return NULL;
    if (!containsActiveDataset(expr)) return LINK(expr);

    TransformMutexBlock procedure;
    ensureMapping();
    expandCallback = _expandCallback;
    if (!isMappingKnown() && expandCallback)
        expandCallback->onUnknown();
    return doExpandFields(expr, oldDataset->queryNormalizedSelector(), 
                                newDataset ? newDataset->queryNormalizedSelector() : NULL, 
                                oldParent ? oldParent->queryNormalizedSelector() : NULL);
}



void NewProjectMapper2::expandFields(HqlExprArray & target, const HqlExprArray & src, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent, IExpandCallback * _expandCallback)
{
    unsigned num = src.ordinality();
    unsigned idx = 0;
    for (; idx < num; idx++)
    {
        IHqlExpression & cur = src.item(idx);
        if (containsActiveDataset(&cur))
            break;
        target.append(OLINK(cur));
    }

    if (idx < num)
    {
        TransformMutexBlock procedure;
        ensureMapping();
        expandCallback = _expandCallback;
        if (!isMappingKnown() && expandCallback)
            expandCallback->onUnknown();
        for (; idx < num; idx++)
        {
            target.append(*doExpandFields(&src.item(idx), oldDataset->queryNormalizedSelector(), 
                                newDataset ? newDataset->queryNormalizedSelector() : NULL, 
                                oldParent ? oldParent->queryNormalizedSelector() : NULL));
        }
    }
}



IHqlExpression * NewProjectMapper2::collapseChildFields(IHqlExpression * expr, IHqlExpression * newDataset)
{
    ensureMapping();
    unsigned match = sources.find(*expr);
    if (match != NotFound)
        return replaceSelector(&targets.item(match), self, newDataset);

    ForEachItemIn(j, children)
    {
        unsigned transformIndex = childIndex.item(j);
        OwnedHqlExpr match = children.item(j).collapseChildFields(expr, &targets.item(transformIndex));
        if (match)
            return replaceSelector(match, self, newDataset);
    }
    return NULL;
}


IHqlExpression * NewProjectMapper2::doCollapseFields(IHqlExpression * expr, IHqlExpression * oldParent, IHqlExpression * newDataset)
{
    switch (expr->getOperator())
    {
    case no_attr:
    case no_record:
    case no_field:
        return LINK(expr);
    }

    unsigned match = sources.find(*expr);
    if (match != NotFound)
    {
        IHqlExpression & collapsed = targets.item(match);
        return replaceSelector(&collapsed, self, newDataset);
    }

    ForEachItemIn(j, children)
    {
        unsigned transformIndex = childIndex.item(j);
        OwnedHqlExpr match = children.item(j).collapseChildFields(expr, &targets.item(transformIndex));
        if (match)
            return replaceSelector(match, self, newDataset);
    }

    if (expr->queryNormalizedSelector() == oldParent)
    //  if (expr == oldDataset)
    {
        matchedAll = false;
        return LINK(expr);
    }

    unsigned max = expr->numChildren();
    if (max == 0)
        return LINK(expr);

    unsigned numNonHidden = activityHidesSelectorGetNumNonHidden(expr, oldParent);
    if (numNonHidden == 0)
        numNonHidden = max;
    HqlExprArray args;
    args.ensure(max);
    bool same = true;
    for (unsigned idx=0; idx < max; idx++)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        if (idx < numNonHidden)
        {
            IHqlExpression * collapsed = cacheCollapseFields(cur, oldParent, newDataset);
            args.append(*collapsed);
            if (cur != collapsed)
                same = false;
            else if (!matchedAll)
                return LINK(expr);
        }
        else
            args.append(*LINK(cur));
    }
    if (!same)
        return expr->clone(args);
    return LINK(expr);
}


IHqlExpression * NewProjectMapper2::collapseFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IHqlExpression * oldParent, bool * collapsedAll)
{
    if (!expr || !containsActiveDataset(expr))
    {
        if (collapsedAll)
            *collapsedAll = true;
        return LINK(expr);
    }

    TransformMutexBlock procedure;
    ensureMapping();
    oldDataset = oldDataset->queryNormalizedSelector();
    newDataset = newDataset->queryNormalizedSelector();
    oldParent = oldParent->queryNormalizedSelector();

    OwnedHqlExpr mapped;
    if ((oldDataset == oldParent) || !oldDataset)
        mapped.set(expr);
    else
        mapped.setown(replaceSelector(expr, oldDataset, oldParent));
    matchedAll = true;
    IHqlExpression * ret = cacheCollapseFields(mapped, oldParent, newDataset);
    if (collapsedAll)
        *collapsedAll = matchedAll;
    return ret;
}


//---------------------------------------------------------------------------

IHqlExpression * getParentDatasetSelector(IHqlExpression * ds)
{
    switch (getChildDatasetType(ds))
    {
    case childdataset_dataset:
        return LINK(ds->queryChild(0)->queryNormalizedSelector());
    case childdataset_datasetleft:
        if (queryNewColumnProvider(ds)->getOperator() == no_transform)
            return createSelector(no_left, ds->queryChild(0), querySelSeq(ds));
        return LINK(ds->queryChild(0)->queryNormalizedSelector());
    case childdataset_left: 
        return createSelector(no_left, ds->queryChild(0), querySelSeq(ds));
    }

    switch (ds->getOperator())
    {
    case no_newkeyindex:
        return LINK(ds->queryChild(0)->queryNormalizedSelector());
    case no_aggregate:
        return createSelector(no_left, ds->queryChild(0), querySelSeq(ds));
    }

    return NULL;
}


TableProjectMapper::TableProjectMapper(IHqlExpression * ds)
{
    if (ds)
        setDataset(ds);
}

bool TableProjectMapper::setDataset(IHqlExpression * ds)
{
    IHqlExpression * mapping = queryNewColumnProvider(ds);
    if (!mapping)
        return false;

    NewProjectMapper2::setMapping(mapping);
    mapParent.setown(getParentDatasetSelector(ds));
    return true;
}

void TableProjectMapper::setMapping(IHqlExpression * mapping, IHqlExpression * ds)
{
    NewProjectMapper2::setMapping(mapping);
    if (ds)
        mapParent.set(ds->queryNormalizedSelector());
}

IHqlExpression * TableProjectMapper::expandFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, IExpandCallback * _expandCallback)
{
    assertex(mapParent || !newDataset);
    return NewProjectMapper2::expandFields(expr, oldDataset, newDataset, mapParent, _expandCallback);
}

IHqlExpression * TableProjectMapper::collapseFields(IHqlExpression * expr, IHqlExpression * oldDataset, IHqlExpression * newDataset, bool * collapsedAll)
{
    if (!mapParent)
    {
        if (collapsedAll)
            *collapsedAll = false;
        return LINK(expr);
    }
    return NewProjectMapper2::collapseFields(expr, oldDataset, newDataset, mapParent, collapsedAll);
}

TableProjectMapper * createProjectMapper(IHqlExpression * ds)
{
    IHqlExpression * mapping = queryNewColumnProvider(ds);
    if (!mapping)
        return NULL;
    OwnedHqlExpr parent = getParentDatasetSelector(ds);
    return createProjectMapper(mapping, parent);
}

TableProjectMapper * createProjectMapper(IHqlExpression * mapping, IHqlExpression * parent)
{
    TableProjectMapper * mapper = new TableProjectMapper;
    mapper->setMapping(mapping, parent);
    return mapper;
}

//-----------------------------------------------------------------------------------------------

bool leftRecordIsSubsetOfRight(IHqlExpression * left, IHqlExpression * right)
{
    unsigned rightIndex = 0;
    ForEachChild(i, left)
    {
        IHqlExpression * nextLeft = left->queryChild(i);
        if (nextLeft->isAttribute())
            continue;
        IHqlExpression * nextRight;
        loop
        {
            nextRight = right->queryChild(rightIndex++);
            if (!nextRight)
                return false;
            if (!nextRight->isAttribute())
                break;
        }
        if (nextLeft->getOperator() != nextRight->getOperator())
            return false;
        switch (nextLeft->getOperator())
        {
        case no_field:
            if (nextLeft != nextRight)
                return false;
            break;
        case no_record:
        case no_ifblock:
            //Could be more sophisticated, but might get nasty
            if (nextLeft != nextRight)
                return false;
            break;
        }
    }
    return true;
}

static bool isTrivialTransform(IHqlExpression * expr, IHqlExpression * selector)
{
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            {
                if (!isTrivialTransform(cur, selector))
                    return false;
                break;
            }
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0);
                IHqlExpression * rhs = cur->queryChild(1);
                if ((lhs->getOperator() != no_select) || (rhs->getOperator() != no_select))
                    return false;
                if (lhs->queryChild(1) != rhs->queryChild(1))
                    return false;
                if (rhs->queryChild(0) != selector)
                    return false;
                if (lhs->queryChild(0)->getOperator() != no_self)
                    return false;
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            break;
        //case no_skip: case no_assert: etc. etc.
        default:
            return false;
        }
    }
    return true;
}


bool isNullProject(IHqlExpression * expr, bool canLoseFieldsFromEnd)
{
    IHqlExpression * ds = expr->queryChild(0);
    if (!recordTypesMatch(expr, ds))
    {
        if (canLoseFieldsFromEnd)
        {
            //check fields are only lost from the end of the record
            if (!leftRecordIsSubsetOfRight(expr->queryRecord(), ds->queryRecord()))
                return false;
        }
        else
            return false;
    }
    return isSimpleProject(expr);
}

bool isSimpleProject(IHqlExpression * expr)
{
    IHqlExpression * ds = expr->queryChild(0);
    LinkedHqlExpr selector;
    switch (expr->getOperator())
    {
    case no_hqlproject:
    case no_projectrow:
        selector.setown(createSelector(no_left, ds, querySelSeq(expr)));
        break;
    case no_newusertable:
         if (isAggregateDataset(expr))
             return false;
         selector.set(ds->queryNormalizedSelector());
         break;
    default:
        return false;
    }
    return isTrivialTransform(queryNewColumnProvider(expr), selector);
}

bool transformReturnsSide(IHqlExpression * expr, node_operator side, unsigned inputIndex)
{
    IHqlExpression * ds = expr->queryChild(inputIndex);
    if (expr->queryRecord() != ds->queryRecord())
        return false;

    OwnedHqlExpr selector = createSelector(side, ds, querySelSeq(expr));
    return isTrivialTransform(queryNewColumnProvider(expr), selector);
}

IHqlExpression * getExtractSelect(IHqlExpression * expr, IHqlExpression * field)
{
    if (expr->getInfoFlags() & (HEFcontainsSkip))
        return NULL;

    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_assignall:
            {
                IHqlExpression * ret = getExtractSelect(cur, field);
                if (ret)
                    return ret;
                break;
            }
        case no_assign:
            {
                IHqlExpression * lhs = cur->queryChild(0);
                if (lhs->getOperator() != no_select)
                    break;
                if (lhs->queryChild(1) != field)
                    break;
                if (lhs->queryChild(0)->getOperator() != no_self)
                    break;
                return ensureExprType(cur->queryChild(1), lhs->queryType());
            }
        }
    }
    return NULL;
}


static IHqlExpression * getTrivialSelect(IHqlExpression * expr, IHqlExpression * selector, IHqlExpression * field)
{
    OwnedHqlExpr match = getExtractSelect(expr, field);
    if (!match)
        return NULL;

    //More: could accept a cast field.
    if (match->getOperator() != no_select)
        return NULL;
    if (match->queryChild(0) != selector)
        return NULL;
    IHqlExpression * matchField = match->queryChild(1);
    //MORE: Could create a type cast if they differed,
    if (matchField->queryType() != field->queryType())
        return NULL;
    return LINK(matchField);
}


IHqlExpression * transformTrivialSelectProject(IHqlExpression * select)
{
    IHqlExpression * newAttr = select->queryProperty(newAtom);
    if (!newAttr)
        return NULL;

    IHqlExpression * row = select->queryChild(0);
    if (row->getOperator() != no_selectnth)
        return NULL;

    IHqlExpression * expr = row->queryChild(0);
    IHqlExpression * transform = queryNewColumnProvider(expr);
    if (!transform)
        return NULL;
    if (!transform->isPure() && transformHasSkipAttr(transform))
        return NULL;

    IHqlExpression * ds = expr->queryChild(0);
    LinkedHqlExpr selector;
    switch (expr->getOperator())
    {
    case no_hqlproject:
    case no_projectrow:
        selector.setown(createSelector(no_left, ds, querySelSeq(expr)));
        break;
    case no_newusertable:
         if (isAggregateDataset(expr))
             return NULL;
         selector.set(ds->queryNormalizedSelector());
         break;
    default:
        return NULL;
    }

    OwnedHqlExpr match = getTrivialSelect(transform, selector, select->queryChild(1));
    if (match)
    {
        IHqlExpression * newRow = createRow(no_selectnth, LINK(ds), LINK(row->queryChild(1)));
        return createNewSelectExpr(newRow, LINK(match));
    }
    return NULL;
}


//-----------------------------------------------------------------------------------------------


void RecordTransformCreator::createAssignments(HqlExprArray & assigns, IHqlExpression * expr, IHqlExpression * targetSelector, IHqlExpression * sourceSelector)
{
    switch (expr->getOperator())
    {
    case no_field:
        {
            OwnedHqlExpr target = createSelectExpr(LINK(targetSelector), LINK(expr));
            OwnedHqlExpr source;
            OwnedHqlExpr sourceField = sourceSelector->queryRecord()->querySimpleScope()->lookupSymbol(expr->queryName());
            if (sourceField)
                source.setown(createSelectExpr(LINK(sourceSelector), LINK(sourceField)));
            else
                source.setown(getMissingAssignValue(expr));

            ITypeInfo * sourceType = source->queryType();
            ITypeInfo * targetType = target->queryType();
            if (!recordTypesMatch(sourceType, targetType))
            {
                type_t tc = sourceType->getTypeCode();
                if (tc == type_record || tc == type_row)
                {
                    OwnedHqlExpr tform = createMappingTransform(no_transform, target->queryRecord(), source);
                    source.setown(createRow(no_createrow, tform.getClear()));
                }
                else if ((tc == type_table) || (tc == type_groupedtable))
                {
                    //self.target := project(srcSelect, transform(...));
                    OwnedHqlExpr seq = createSelectorSequence();
                    OwnedHqlExpr leftSelector = createSelector(no_left, source, seq);
                    OwnedHqlExpr transform = createMappingTransform(no_transform, target->queryRecord(), leftSelector);
                    source.setown(createDataset(no_hqlproject, LINK(source), createComma(LINK(transform), LINK(seq))));
                }
            }

            assigns.append(*createAssign(LINK(target), LINK(source)));
            break;
        }
    case no_ifblock:
        {
            createAssignments(assigns, expr->queryChild(1), targetSelector, sourceSelector);
            break;
        }
    case no_record:
        {
            ForEachChild(idx, expr)
                createAssignments(assigns, expr->queryChild(idx), targetSelector, sourceSelector);
            break;
        }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        break;
    default:
        UNIMPLEMENTED;
    }
}

IHqlExpression * RecordTransformCreator::createMappingTransform(node_operator op, IHqlExpression * targetRecord, IHqlExpression * sourceSelector)
{
    OwnedHqlExpr self = getSelf(targetRecord);
    HqlExprArray assigns;
    createAssignments(assigns, targetRecord, self, sourceSelector);
    return createValue(op, makeTransformType(targetRecord->getType()), assigns);
}

IHqlExpression * createRecordMappingTransform(node_operator op, IHqlExpression * targetRecord, IHqlExpression * sourceSelector)
{
    RecordTransformCreator mapper;
    return mapper.createMappingTransform(op, targetRecord, sourceSelector);
}


IHqlExpression * replaceMemorySelectorWithSerializedSelector(IHqlExpression * expr, IHqlExpression * memoryRecord, node_operator side, IHqlExpression * selSeq)
{
    if (!expr) 
        return NULL;

    assertex(side);
    assertex(memoryRecord->isRecord());
    OwnedHqlExpr serializedRecord = getSerializedForm(memoryRecord);
    if (memoryRecord == serializedRecord)
        return LINK(expr);

    //The expression/transform has references to the in-memory selector, but the selector provided to transform will be serialised.
    //so create a mapping <unserialized> := f(serialized)
    //and then use it to expand references to the unserialized format
    OwnedHqlExpr memorySelector = createSelector(side, memoryRecord, selSeq);
    OwnedHqlExpr serializedSelector = createSelector(side, serializedRecord, selSeq);

    OwnedHqlExpr mapTransform = createRecordMappingTransform(no_transform, memoryRecord, serializedSelector);
    NewProjectMapper2 mapper;
    mapper.setMapping(mapTransform);

    //If all else fails, need to replace old-selector with ROW(transform))
    OwnedHqlExpr project = createRow(no_createrow, LINK(mapTransform));
    FullExpandMonitor monitor(project);
    return mapper.expandFields(expr, memorySelector, serializedSelector, serializedSelector, &monitor);
}


