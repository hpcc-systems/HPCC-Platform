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
#include "hqlcse.ipp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqlthql.hpp"
#include "hqlcatom.hpp"
#include "hqlfold.hpp"
#include "hqlpmap.hpp"
#include "hqlopt.hpp"
#include "hqlcerrors.hpp"
#include "hqlttcpp.ipp"

#ifdef _DEBUG
//#define TRACE_CSE
#endif

//The following allows x != y and x == y to be commoned up.  It works, but currently disabled
//because cse doesn't preserve short circuit of AND and OR, and some examples mean it will do more
//work because the alias will always be evaluated.  (e.g., salt1.xhql)
//Really aliases need to be functional and executed on demand or something similar.
//#define OPTIMIZE_INVERSE

//---------------------------------------------------------------------------

inline bool canWrapWithCSE(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_mapto:
        return false;
    }
    return true;
}


bool canCreateTemporary(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_range:
    case no_rangefrom:
    case no_rangeto:
    case no_rangecommon:
    case no_constant:
    case no_all:
    case no_mapto:
    case no_record:
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
    case no_joined:
    case no_sizeof:
    case no_offsetof:
    case no_newtransform:
    case no_transform:
    case no_assign:
    case no_assignall:
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_activetable:
    case no_alias:
    case no_skip:
    case no_assert:
    case no_counter:
    case no_sortlist:
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchposition:
    case no_matchlength:
    case no_matchattr:
    case no_matchrow:
    case no_matchutf8:
    case no_recordlist:
    case no_transformlist:
    case no_rowvalue:
    case no_pipe:
    case no_colon:
    case no_globalscope:
    case no_subgraph:
    case no_forcelocal:
    case no_forcenolocal:
    case no_allnodes:
    case no_thisnode:
    case no_libraryscopeinstance:
    case no_loopbody:
        return false;
    }
    return !expr->isAction() && !expr->isTransform();
}


//---------------------------------------------------------------------------

/*

  Cse spotting...

  * Don't remove named symbols from items that aren't transformed.
  * Common items up regardless of the named symbol used to reference it.


  */

CseSpotterInfo::CseSpotterInfo(IHqlExpression * expr) : NewTransformInfo(expr) 
{ 
    numRefs = 0; 
    numAssociatedRefs = 0; 
    alreadyAliased = false; 
    canAlias = false; 
    dontTransform = false;
    dontTransformSelector = false;
    treatAsAliased = false;
    inverse = NULL;
    annotatedExpr = NULL;
}

//worth aliasing if referenced more than once, and used more than once in the expressions that are going to be evaluated now
bool CseSpotterInfo::worthAliasingOnOwn()
{ 
    return numRefs > 1 && (numRefs != numAssociatedRefs); 
}

bool CseSpotterInfo::worthAliasing()
{
    if (!inverse)
        return worthAliasingOnOwn();
    //no_not will always traverse the inverse (at least once), so don't sum the two counts - just use the non inverted count
    if (original->getOperator() == no_not)
        return worthAliasingOnOwn() || inverse->worthAliasingOnOwn();
    if (inverse->original->getOperator() == no_not)
        return worthAliasingOnOwn();

    unsigned totalRefs = numRefs + inverse->numRefs;
    unsigned totalAssociatedRefs = numAssociatedRefs + inverse->numAssociatedRefs;
    if ((totalRefs > 1) && (totalRefs != totalAssociatedRefs))
        return true;
    return false;
}

//Do we create an alias for this node, or the other one?
bool CseSpotterInfo::useInverseForAlias()
{
    if (!inverse)
        return false;

    if (numRefs == numAssociatedRefs)
        return true;

    node_operator op = original->getOperator();
    switch (op)
    {
    case no_not:
    case no_ne:
    case no_notin:
    case no_notbetween:
    case no_notexists:
        return inverse->worthAliasingOnOwn();
    }

    node_operator invOp = inverse->original->getOperator();
    switch (invOp)
    {
    case no_not: return false;      //No otherwise we'll expand recursively!
    case no_ne:
    case no_notin:
    case no_notbetween:
    case no_notexists:
        return !worthAliasingOnOwn();
    }
    return op > invOp;
}


static HqlTransformerInfo cseSpotterInfo("CseSpotter");
CseSpotter::CseSpotter() 
: NewHqlTransformer(cseSpotterInfo)
{
    canAlias = true;
    isAssociated = false;
    spottedCandidate = false;
    invariantSelector = NULL;
    createLocalAliases = false;
    createdAlias = false;
}

void CseSpotter::analyseAssociated(IHqlExpression * expr, unsigned pass)
{
    isAssociated = true;
    analyse(expr, pass);
    isAssociated = false;
}


void CseSpotter::analyseExpr(IHqlExpression * expr)
{
    CseSpotterInfo * extra = queryBodyExtra(expr);
    if (!extra->annotatedExpr && expr->isAnnotation())
        extra->annotatedExpr = expr;

    if (isAssociated)
        extra->numAssociatedRefs++;
    
    node_operator op = expr->getOperator();
#ifdef OPTIMIZE_INVERSE
    if (getInverseOp(op) != no_none)
    {
        OwnedHqlExpr inverse = getInverse(expr);
        CseSpotterInfo * inverseExtra = queryBodyExtra(inverse);
        extra->inverse = inverseExtra;
        inverseExtra->inverse = extra;
    }
#endif
    
    if (op == no_alias)
    {
        queryBodyExtra(expr->queryChild(0))->alreadyAliased = true;
        extra->alreadyAliased = true;
    }

    if (extra->numRefs++ != 0)
    {
        if (!spottedCandidate && extra->worthAliasing() && (op != no_alias))
            spottedCandidate = true;
        //if (canCreateTemporary(expr))
            return;
    }

    if (!containsPotentialCSE(expr))
        return;

    if (canAlias && !expr->isDataset())
        extra->canAlias = true;

    bool savedCanAlias = canAlias;
    if (expr->isDataset() && (op != no_select))// && (op != no_if))
    {
        //There is little point looking for CSEs within dataset expressions, because only a very small
        //minority which would correctly cse, and it can cause lots of problems - e.g., join conditions.
        unsigned first = getFirstActivityArgument(expr);
        unsigned num = getNumActivityArguments(expr);
        HqlExprArray children;
        bool defaultCanAlias = canAlias;
        ForEachChild(i, expr)
        {
            IHqlExpression * cur = expr->queryChild(i);
            if (i >= first && i < first+num)
                canAlias = defaultCanAlias;
            else
                canAlias = false;

            analyseExpr(cur);
        }
    }
    else
        PARENT::analyseExpr(expr);
    canAlias = savedCanAlias;
}

IHqlExpression * CseSpotter::createAliasOwn(IHqlExpression * expr, CseSpotterInfo * extra)
{
#ifdef TRACE_CSE
    StringBuffer s;
    DBGLOG("Create alias for %s (%d refs)", getExprIdentifier(s, expr).str(), extra->numRefs);
#endif
    extra->alreadyAliased = true;
    if (createLocalAliases)
        return ::createAliasOwn(expr, createLocalAttribute());
    return ::createAliasOwn(expr, NULL);
}


IHqlExpression * CseSpotter::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchposition:
    case no_matchlength:
    case no_matchrow:
    case no_matchutf8:
        //These actually go wrong if we remove the named symbols, so traverse under no circumstances.
        //others can be traversed to patch up references to datasets that have changed.
    case no_translated:
        return LINK(expr);
    }
    OwnedHqlExpr transformed = PARENT::createTransformed(expr);

    CseSpotterInfo * splitter = queryBodyExtra(expr);
    //MORE: Possibly add a unique number to the alias when this starts worrying about child scopes.
    if (splitter->canAlias && splitter->worthAliasing() && checkPotentialCSE(expr, splitter))
    {
        if (splitter->useInverseForAlias())
        {
            OwnedHqlExpr inverse = getInverse(expr);
            OwnedHqlExpr transformedInverse = transform(inverse);
            return getInverse(transformedInverse);
        }

        createdAlias = true;
        //Use the transformed body to ensure that any cses only create a single instance,
        //But annotate with first annotation spotted, try and retain the symbols to aid debugging.
        LinkedHqlExpr aliasValue = transformed->queryBody();
//      if (splitter->annotatedExpr)
//          aliasValue.setown(splitter->annotatedExpr->cloneAllAnnotations(aliasValue));
        OwnedHqlExpr alias = createAliasOwn(aliasValue.getClear(), splitter);
        return alias.getClear();
        return expr->cloneAllAnnotations(alias);
    }
    return transformed.getClear();
}

ANewTransformInfo * CseSpotter::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(CseSpotterInfo, expr);
}


bool CseSpotter::containsPotentialCSE(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
    case no_joined:
    case no_sizeof:
    case no_offsetof:
    case no_field:
    case no_evaluate:   // MORE: This is an example of introducing a new scope...
    case no_translated: // Causes recursion otherwise....
    case no_left:
    case no_right:
    case no_top:
    case no_self:
    case no_selfref:
    case no_activetable:
    case no_filepos:
    case no_file_logicalname:
    case no_countfile:
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchposition:
    case no_matchrow:
    case no_matchlength:
    case no_matchutf8:
    case no_catch:
    case no_projectrow:
//  case no_evalonce:
        return false;
    case no_select:
        return false; //isNewSelector(expr);
    case NO_AGGREGATE:
        //There may possibly be cses, but we would need to do lots of scoping analysis to work out whether they were
        //really common.
        return false;
    case no_assign:
    case no_assignall:
    case no_transform:
    case no_newtransform:
    case no_range:
    case no_rangefrom:
    case no_rangeto:
    case no_rangecommon:
    case no_skip:
        return true;
    case no_compound_diskread:
    case no_compound_indexread:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
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
        return false;
#if 0
        //Strictly speaking, we shouldn't common up conditional expressions, but it generally provides such a reduction in code
        //that it will stay enabled until I come up with a better scheme.
    case no_if:
    case no_rejected:
    case no_which:
    case no_case:
    case no_map:
        return false;
#endif
    }
    ITypeInfo * type = expr->queryType();
    if (type && type->getTypeCode() == type_void)
        return false;
    return !expr->isConstant();// || expr->isDataset() || expr->isDatarow();
}

bool CseSpotter::checkPotentialCSE(IHqlExpression * expr, CseSpotterInfo * extra)
{
    if (extra->alreadyAliased)
        return false;

    if (!expr->isPure() || !canCreateTemporary(expr))
        return false;

    if (invariantSelector && exprReferencesDataset(expr, invariantSelector))
        return false;
    
    switch (expr->getOperator())
    {
    case no_eq:
    case no_ne:
    case no_gt:
    case no_ge:
    case no_lt:
    case no_le:
        {
            //Don't combine integer comparisons into a CSE - not worth it...
            ITypeInfo * type = expr->queryChild(0)->queryType();
            switch (type->getTypeCode())
            {
            case type_boolean:
            case type_int:
                return false;
            }
            return true;
        }
    case no_not:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (queryBodyExtra(child)->isAliased())
                return false;
            break;
        }
    case no_charlen:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (queryBodyExtra(child)->isAliased() || child->getOperator() == no_select)
            {
                type_t tc = child->queryType()->getTypeCode();
                switch (tc)
                {
                case type_varstring:
                case type_varunicode:
                    return true;
                }

                //prevent (trivial-cast)length(x) from being serialized etc.
                extra->treatAsAliased = true;
                return false;
            }
            break;
        }
    case no_field:
        throwUnexpected();
    case no_select:
        return false; //expr->hasProperty(newAtom);
    case no_list:
    case no_datasetlist:
    case no_getresult:      // these are commoned up in the code generator, so don't do it twice.
    case no_getgraphresult:
    case no_getgraphloopresult:
    case no_translated: // Causes recursion otherwise....
    case no_random:
        return false;
    case no_call:
    case no_externalcall:
    case no_libraryinput:
    case no_counter:
        return true;
    case no_substring:
        {
            IHqlExpression * child = expr->queryChild(0);
//          if (queryBodyExtra(child)->isAliased())
            {
                SubStringHelper helper(expr);
                return !helper.canGenerateInline();
            }
            return true;
        }
    case no_cast:
    case no_implicitcast:
        {
            ITypeInfo * exprType = expr->queryType();
            if (exprType->getTypeCode() == type_set)
                return false;
            IHqlExpression * uncast = expr->queryChild(0);
            if (uncast->queryValue())
                return false;
            //Ignore integral casts of items that have already been aliased
            if (queryBodyExtra(uncast)->isAliased())
            {
                if (exprType->isInteger() && uncast->queryType()->isInteger())
                {
                    if (extra->numRefs < 5)
                        return false;
                }
            }
            break;
        }
        //Following are all source datasets - no point in commoning them up
        //although probably exceptions e.g., table(,pipe) 
    case no_none:
    case no_null:
    case no_anon:
    case no_pseudods:
    case no_all:
//  case no_table:          - normally work commoning up
    case no_temptable:
    case no_inlinetable:
    case no_xmlproject:
    case no_datasetfromrow:
    case no_preservemeta:
    case no_workunit_dataset:
    case no_left:
    case no_right:
    case no_top:
    case no_self:
    case no_selfref:
    case no_keyindex:
    case no_newkeyindex:
    case no_fail:
    case no_activetable:
    case no_soapcall:
    case no_newsoapcall:
    case no_id2blob:
    case no_cppbody:
    case no_rows:
        return false;

    }
    if (!expr->queryType())
        return false;
    return (expr->numChildren() > 0);
}


IHqlExpression * CseSpotter::transform(IHqlExpression * expr)
{
    return PARENT::transform(expr);
}

IHqlExpression * CseSpotter::queryAlreadyTransformed(IHqlExpression * expr)
{
    CseSpotterInfo * extra = queryBodyExtra(expr);
    if (extra->dontTransform)
        return expr;
    IHqlExpression * ret = PARENT::queryAlreadyTransformed(expr);
    if (!ret)
    {
        IHqlExpression * body = expr->queryBody();
        if (body != expr)
        {
            ret = PARENT::queryAlreadyTransformed(body);
            if (ret == body)
                return NULL;
        }
    }
    return ret;
}

IHqlExpression * CseSpotter::queryAlreadyTransformedSelector(IHqlExpression * expr)
{
    CseSpotterInfo * extra = queryBodyExtra(expr);
    if (extra->dontTransformSelector)
        return expr;
    return PARENT::queryAlreadyTransformedSelector(expr);
}


void CseSpotter::stopTransformation(IHqlExpression * expr)
{
    IHqlExpression * normalized = expr->queryNormalizedSelector();
    queryBodyExtra(expr)->dontTransform = true;
    queryBodyExtra(normalized)->dontTransformSelector = true;
}


//---------------------------------------------------------------------------

static HqlTransformerInfo conjunctionTransformerInfo("ConjunctionTransformer");
ConjunctionTransformer::ConjunctionTransformer() : NewHqlTransformer(conjunctionTransformerInfo)
{
}

IHqlExpression * ConjunctionTransformer::createTransformed(IHqlExpression * expr)
{
    node_operator op = expr->getOperator();
    OwnedHqlExpr transformed;
    switch (op)
    {
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchlength:
    case no_matchposition:
    case no_matchrow:
    case no_matchutf8:
        return LINK(expr);
        //not so sure why the following causes problems - because the tables get changed I think.
    case no_filepos:
    case no_file_logicalname:
    case no_sizeof:
    case no_offsetof:
        return LINK(expr);
    case no_and:
    case no_or:
        {
            IHqlExpression * left = expr->queryChild(0);
            if (left->getOperator() == op)
            {
                HqlExprArray args, transformedArgs;
                left->unwindList(args, op);
                ForEachItemIn(i, args)
                    transformedArgs.append(*transform(&args.item(i)));
                transformedArgs.append(*transform(expr->queryChild(1)));
                transformed.setown(createLeftBinaryList(op, transformedArgs));
//              return expr->cloneAllAnnotations(transformed);
            }
            break;
        }
    }

    if (!transformed)
        transformed.setown(NewHqlTransformer::createTransformed(expr));

    return transformed.getClear();
}


//---------------------------------------------------------------------------

#ifdef NEW_CSE_PROCESSING
inline bool canInsertCodeAlias(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_range:
    case no_rangefrom:
    case no_rangeto:
    case no_rangecommon:
    case no_mapto:
    case no_recordlist:
    case no_transformlist:
    case no_rowvalue:
    case no_sortlist:
        return false;
    default:
        return true;
    }
}


static HqlTransformerInfo cseScopeTransformerInfo("CseScopeTransformer");
CseScopeTransformer::CseScopeTransformer() 
: NewHqlTransformer(cseScopeTransformerInfo)
{
    activeParent = NULL;
    seq = 0;
    conditionDepth = 0;
}

void CseScopeTransformer::analyseExpr(IHqlExpression * expr)
{
    expr = expr->queryBody();
    if (!containsNonGlobalAlias(expr))
        return;

    node_operator op = expr->getOperator();
    CseScopeInfo * splitter = queryExtra(expr);
    if (splitter->seq)
    {
        splitter->hasSharedParent = true;
        splitter->addParent(activeParent);
        return;
    }

    splitter->firstParent = activeParent;
    splitter->seq = ++seq;
    splitter->isUnconditional = (conditionDepth == 0);

    {
        IHqlExpression * savedParent = activeParent;
        activeParent = expr;
        switch (op)
        {
        case no_if:
        case no_or:
        case no_and:
        case no_case:
            {
                analyseExpr(expr->queryChild(0));
                conditionDepth++;
                ForEachChildFrom(i, expr, 1)
                    analyseExpr(expr->queryChild(i));
                conditionDepth--;
                break;
            }
        default:
            NewHqlTransformer::analyseExpr(expr);
            break;
        }
        activeParent = savedParent;
    }

    //Add here so the cse are in the correct order to cope with dependencies...
    if (op == no_alias)
    {
        assertex(!expr->hasProperty(globalAtom));
        allCSEs.append(*LINK(splitter));
    }
}

bool CseScopeTransformer::attachCSEs(IHqlExpression * root)
{
    bool changed = false;
    ForEachItemIn(idx, allCSEs)
    {
        CseScopeInfo& cur = allCSEs.item(idx);
        IHqlExpression * aliasLocation = findAliasLocation(&cur);
        if (!aliasLocation && cur.isUnconditional)
            aliasLocation = root;

        if (aliasLocation && aliasLocation != cur.original)
        {
            queryExtra(aliasLocation)->aliasesToDefine.append(*LINK(cur.original));
            changed = true;
        }
    }
    return changed;
}

IHqlExpression * CseScopeTransformer::createTransformed(IHqlExpression * expr)
{
    //Can't short-circuit transformation if (!containsAlias(expr)) because it means references to transformed datasets won't get patched up
    IHqlExpression * body = expr->queryBody(true);
    if (body != expr)
    {
        OwnedHqlExpr ret = transform(body);
        return expr->cloneAnnotation(ret);
    }
    //slight difference from before...

    IHqlExpression * transformed = NewHqlTransformer::createTransformed(expr);
    CseScopeInfo * splitter = queryExtra(expr);
    if (splitter->aliasesToDefine.ordinality())
    {
        HqlExprArray args;
        args.append(*transformed);
        ForEachItemIn(idx, splitter->aliasesToDefine)
        {
            IHqlExpression * value = &splitter->aliasesToDefine.item(idx);
            args.append(*transform(value));
        }
        if (expr->isDataset())
            transformed = createDataset(no_alias_scope, args);
        else if (expr->isDatarow())
            transformed = createRow(no_alias_scope, args);
        else
            transformed = createValue(no_alias_scope, transformed->getType(), args);
    }

    return transformed;
}


ANewTransformInfo * CseScopeTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(CseScopeInfo, expr);
}


//First find the highest shared parent node (or this if no parents are shared)
CseScopeInfo * CseScopeTransformer::calcCommonLocation(CseScopeInfo * extra)
{
    if (extra->calcedCommonLocation)
        return extra->commonLocation;
    CseScopeInfo * commonLocation = extra;
    if (extra->firstParent)
    {
        CseScopeInfo * firstParentExtra = queryExtra(extra->firstParent);
        CseScopeInfo * commonParent = calcCommonLocation(firstParentExtra);
        if ((extra->parents.ordinality() == 0) && (!firstParentExtra->hasSharedParent || extra->firstParent->getOperator() == no_alias))
//      if ((extra->parents.ordinality() == 0) && !firstParentExtra->hasSharedParent)
        {
            //assertex(commonParent == firstParentExtra);
            //commonParent = extra;
        }
        else
        {
            extra->hasSharedParent = true;
            commonLocation = commonParent;
            ForEachItemIn(i, extra->parents)
            {
                CseScopeInfo * nextExtra = calcCommonLocation(queryExtra(extra->parents.item(i)));
                if (nextExtra->isUnconditional)
                    extra->isUnconditional = true;
                commonLocation = findCommonPath(commonLocation, nextExtra);
                if (!commonLocation && extra->isUnconditional)
                    break;
            }
        }
    }
    else
    {
        if (extra->hasSharedParent)
            commonLocation = NULL;
    }
    extra->calcedCommonLocation = true;
    extra->commonLocation = commonLocation;
    return commonLocation;
}

IHqlExpression * CseScopeTransformer::findAliasLocation(CseScopeInfo * extra)
{
    CseScopeInfo * best = calcCommonLocation(extra);
    loop
    {
        if (!best)
            return NULL;
        IHqlExpression * bestLocation = best->original;
        if (canInsertCodeAlias(bestLocation))
            return bestLocation;
        best = selectParent(best);
    }
}


CseScopeInfo * CseScopeTransformer::selectParent(CseScopeInfo * info)
{
    if (info->hasSharedParent)
        return info->commonLocation;
    if (!info->firstParent)
        return NULL;
    return queryExtra(info->firstParent);
}

CseScopeInfo * CseScopeTransformer::findCommonPath(CseScopeInfo * left, CseScopeInfo * right)
{
    loop
    {
        if (!left || !right)
            return NULL;
        if (left == right)
            return left;

        if (left->seq > right->seq)
            left = selectParent(left);
        else
            right = selectParent(right);
    }
}
#else
CSEentry::CSEentry(IHqlExpression * _value, PathArray & _path)
{
    value.set(_value);

    unsigned depth=_path.ordinality();
    path.ensure(depth);
    ForEachItemIn(idx, _path)
        path.append(_path.item(idx));
    ensurePathValid();
}

void CSEentry::ensurePathValid()
{
    //It is not valid to insert a no_code_alias at certain points....
    while (path.ordinality())
    {
        switch (path.tos().getOperator())
        {
        case no_range:
        case no_rangefrom:
        case no_rangeto:
        case no_rangecommon:
        case no_mapto:
        case no_recordlist:
        case no_transformlist:
        case no_rowvalue:
        case no_sortlist:
            path.pop();
            break;
        default:
            return;
        }
    }
}


void CSEentry::findCommonPath(PathArray & otherPath)
{
    unsigned prevPath = path.ordinality();
    unsigned maxPath = path.ordinality();
    if (maxPath > otherPath.ordinality())
        maxPath = otherPath.ordinality();
    unsigned idx;
    for (idx = 0; idx < maxPath; idx++)
    {
        IHqlExpression * l = &path.item(idx);
        IHqlExpression * r = &otherPath.item(idx);
        if (l != r)
            break;
    }
    //Ensure the new location is valid for receiving the CSE
    while (idx != 0)
    {
        if (canWrapWithCSE(&path.item(idx-1)))
            break;
        idx--;
    }
    path.trunc(idx);

    if (prevPath != path.ordinality())
    {
        ForEachItemIn(idx2, dependsOn)
            dependsOn.item(idx2).findCommonPath(path);
    }
    ensurePathValid();
}

static HqlTransformerInfo cseScopeTransformerInfo("CseScopeTransformer");
CseScopeTransformer::CseScopeTransformer() 
: NewHqlTransformer(cseScopeTransformerInfo)
{
}

void CseScopeTransformer::analyseExpr(IHqlExpression * expr)
{
    expr = expr->queryBody();
    if (!containsNonGlobalAlias(expr))
        return;
    CSEentry * cse = NULL;
    node_operator op = expr->getOperator();
    if (op == no_alias)
    {
        assertex(!expr->hasProperty(globalAtom));

        CseScopeInfo * splitter = queryExtra(expr);

        //PrintLog("splitter: %s", expr->toString(StringBuffer()).str());
        if (splitter->cseUse)
        {
            //Find the common path, and map the alias.
            CSEentry * cse = splitter->cseUse;
            cse->findCommonPath(path);

            if (activeCSE.ordinality())
                activeCSE.tos().dependsOn.append(*LINK(cse));
            return;
        }

        cse = new CSEentry(expr, path);
        splitter->cseUse.setown(cse);

        if (activeCSE.ordinality())
            activeCSE.tos().dependsOn.append(*LINK(cse));
        activeCSE.append(*LINK(cse));
    }

#if 0
    if ((op == no_transform) || (op == no_newtransform))
    {
        //For a transform add each assignment as a path point - so the aliases for assignments don't end up
        //before aliases for skip attributes.
        path.append(*expr);
        ForEachChild(i, expr)
        {
            IHqlExpression * cur = expr->queryChild(i);
            analyseExpr(cur);
            path.append(*cur);
        }
        ForEachChild(i2, expr)
            path.pop();
        path.pop();
    }
    else
#endif

    {
        path.append(*expr);
        NewHqlTransformer::analyseExpr(expr);
        path.pop();
    }

    //Add here so the cse are in the correct order to cope with dependencies...
    if (cse)
    {
        allCSEs.append(*LINK(cse));
        activeCSE.pop();
    }
}

bool CseScopeTransformer::attachCSEs(IHqlExpression * /*root*/)
{
    bool changed = false;
    ForEachItemIn(idx, allCSEs)
    {
        CSEentry & cur = allCSEs.item(idx);
        if (cur.path.ordinality())
        {
            IHqlExpression & location = cur.path.tos();
            queryExtra(&location)->cseDefine.append(OLINK(cur));
            changed = true;
        }
    }
    return changed;
}

IHqlExpression * CseScopeTransformer::createTransformed(IHqlExpression * expr)
{
    //Can't short-circuit transformation if (!containsAlias(expr)) because it means references to transformed datasets won't get patched up
    IHqlExpression * body = expr->queryBody(true);
    if (body != expr)
    {
        OwnedHqlExpr ret = transform(body);
        return expr->cloneAnnotation(ret);
    }
    //slight difference from before...

    IHqlExpression * transformed = NewHqlTransformer::createTransformed(expr);
    CseScopeInfo * splitter = queryExtra(expr);
    if (splitter->cseDefine.ordinality())
    {
        HqlExprArray args;
        args.append(*transformed);
        ForEachItemIn(idx, splitter->cseDefine)
        {
            CSEentry & cur = splitter->cseDefine.item(idx);
            args.append(*transform(cur.value));
        }
        if (expr->isDataset())
            transformed = createDataset(no_alias_scope, args);
        else if (expr->isDatarow())
            transformed = createRow(no_alias_scope, args);
        else
            transformed = createValue(no_alias_scope, transformed->getType(), args);
    }

    return transformed;
}


ANewTransformInfo * CseScopeTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(CseScopeInfo, expr);
}
#endif


IHqlExpression * spotScalarCSE(IHqlExpression * expr, IHqlExpression * limit)
{
    if (expr->isConstant())
        return LINK(expr);
    switch (expr->getOperator())
    {
    case no_select:
        if (!expr->hasProperty(newAtom))
            return LINK(expr);
        break;
    }

    OwnedHqlExpr transformed = LINK(expr); //removeNamedSymbols(expr);

    bool addedAliases = false;
    //First spot the aliases - so that restructuring the ands doesn't lose any existing aliases.
    {
        CseSpotter spotter;
        spotter.analyse(transformed, 0);
        if (spotter.foundCandidates())
        {
            if (limit)
                spotter.stopTransformation(limit);
            transformed.setown(spotter.transformRoot(transformed));

            addedAliases = spotter.createdNewAliases();
        }
    }

    if (!containsAlias(transformed))
        return transformed.getClear();

    //Transform conjunctions so they are (a AND (b AND (c AND d))) not (((a AND b) AND c) AND d)
    //so that alias scope can be introduced in a better place.
    {
        ConjunctionTransformer tr;
        transformed.setown(tr.transformRoot(transformed));
    }

    if (!addedAliases)
        return transformed.getClear();

    //Now work out where in the tree the aliases should be evaluated.
    {
        CseScopeTransformer scoper;
        scoper.analyse(transformed, 0);
        if (scoper.attachCSEs(transformed))
            transformed.setown(scoper.transformRoot(transformed));
    }

    return transformed.getClear();
}


void spotScalarCSE(SharedHqlExpr & expr, SharedHqlExpr & associated, IHqlExpression * limit, IHqlExpression * invariantSelector)
{
    CseSpotter spotter;
    spotter.analyse(expr, 0);
    if (associated)
        spotter.analyseAssociated(associated, 0);
    if (!spotter.foundCandidates())
        return;
    if (limit)
        spotter.stopTransformation(limit);
    if (invariantSelector)
        spotter.setInvariantSelector(invariantSelector);
    expr.setown(spotter.transformRoot(expr));
    associated.setown(spotter.transformRoot(associated));
}


void spotScalarCSE(HqlExprArray & exprs, HqlExprArray & associated, IHqlExpression * limit, IHqlExpression * invariantSelector)
{
    CseSpotter spotter;
    spotter.analyseArray(exprs, 0);
    ForEachItemIn(ia, associated)
        spotter.analyseAssociated(&associated.item(ia), 0);
    if (!spotter.foundCandidates())
        return;
    if (limit)
        spotter.stopTransformation(limit);
    if (invariantSelector)
        spotter.setInvariantSelector(invariantSelector);

    HqlExprArray newExprs;
    HqlExprArray newAssociated;
    spotter.transformRoot(exprs, newExprs);
    spotter.transformRoot(associated, newAssociated);

    replaceArray(exprs, newExprs);
    replaceArray(associated, newAssociated);
}


//---------------------------------------------------------------------------

//The TableInvariantTransformer is important for ensuring that getResultXXX code is executed in the code context, amongst other things
//It must ensure that any global aliases couldn't contain some other global aliases inside a child query, otherwise when the child query is
//evaluated the result won't be in the correct place.
//
//MORE: This could be improved to work out whether it is worth creating an alias (which will then be serialized...)
//e.g., don't alias i) <alias<n>> +- offset or ii) extension of an alias's size., iii) substring of a fixed size string. iv) length(string
//however it is pretty good as it stands.
//ideally it would need information about how many times the expression is likely to be evaluated (e.g., 1/many)
//so that could be taken into account (e.g, filenames which are 'string' + conditional)

static bool canHoistInvariant(IHqlExpression * expr)
{
    if (!canCreateTemporary(expr))
    {
        if ((expr->getOperator() != no_alias) || expr->hasProperty(globalAtom))
            return false;
    }
    if (!expr->isPure())
        return false;
    switch (expr->getOperator())
    {
    case no_list:
    case no_datasetlist:
        return false;       // probably don't want to hoist these
    }
    return true;
}



static HqlTransformerInfo tableInvariantTransformerInfo("TableInvariantTransformer");
TableInvariantTransformer::TableInvariantTransformer() : NewHqlTransformer(tableInvariantTransformerInfo)
{ 
    canAlias = true; 
}

bool TableInvariantTransformer::isInvariant(IHqlExpression * expr)
{
    TableInvariantInfo * extra = queryBodyExtra(expr);
    if (extra->cachedInvariant)
        return extra->isInvariant;

    bool invariant = false;
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_record:
    case no_null:
    case no_activetable:
    case no_activerow:
    case no_left:
    case no_right:
    case no_self:
    case no_top:
    case no_selfref:
    case no_filepos:
    case no_file_logicalname:
    case no_joined:
    case no_offsetof:
    case no_sizeof:
    case NO_AGGREGATE:
        break;
    case no_preservemeta:
        invariant = isInvariant(expr->queryChild(0));
        break;
    case no_constant:
    case no_workunit_dataset:
    case no_getresult:
    case no_getgraphresult:
    case no_countfile:
        invariant = true;
        break;
    case no_select:
        {
            IHqlExpression * ds = expr->queryChild(0);
            if ((expr->hasProperty(newAtom) || ds->isDatarow()) && !expr->isDataset())
                invariant = isInvariant(ds);
            break;
        }
    case no_newaggregate:
        {
            //Allow these on a very strict subset of the datasets - to ensure that no potential globals can be included in the dataset
            if (!isInvariant(expr->queryChild(0)))
                break;
            switch (querySimpleAggregate(expr, false, true))
            {
            case no_existsgroup:
            case no_countgroup:
                invariant = true;
                break;
            }
            break;
        }
    case no_selectnth:
        switch (expr->queryChild(1)->getOperator())
        {
        case no_constant:
        case no_counter:
            invariant = isInvariant(expr->queryChild(0));
            break;
        }
        break;
    default:
        if (!isContextDependent(expr))
        {
            if (!expr->isAction())// && !expr->isDataset() && !expr->isDatarow())
            {
                invariant = true;
                ForEachChild(i, expr)
                {
                    IHqlExpression * cur = expr->queryChild(i);
                    if (!isInvariant(cur))
                    {
                        invariant = false;
                        break;
                    }
                }
            }
        }
        break;
    }

    extra->cachedInvariant = true;
    extra->isInvariant = invariant;
    return invariant;
}

#if 0
void TableInvariantTransformer::analyseExpr(IHqlExpression * expr)
{
    expr = expr->queryBody();

    if (alreadyVisited(expr))
        return;

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_record:
    case no_constant:
        return;
    }

    if (isInvariant(expr) && !expr->isAttribute() && !expr->isConstant() && canHoistInvariant(expr))
    {
        TableInvariantInfo * extra = queryBodyExtra(expr);
        if (op == no_alias)
        {
            if (!expr->hasProperty(globalAtom))
                extra->createAlias = true;
        }
        else
            extra->createAlias = true;
        return;
    }

    if (op == no_attr_expr)
        analyseChildren(expr);
    else
        NewHqlTransformer::analyseExpr(expr);
}
#else
void TableInvariantTransformer::analyseExpr(IHqlExpression * expr)
{
    expr = expr->queryBody();
    TableInvariantInfo * extra = queryBodyExtra(expr);

    if (alreadyVisited(expr))
        return;

    //More - these need to be handled properly...
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_record:
    case no_constant:
        return;
    }

    //We are trying to ensure that any expressions that don't access fields that are dependent on the activeDatasets/context are only 
    //evaluated once => check for active dataset rather than any dataset
    bool candidate = false;
    if (!isContextDependent(expr) && !expr->isAttribute())
    {
        if (isInlineTrivialDataset(expr) && !expr->isConstant())
        {
            candidate = (op != no_null);
        }
        else
        {
            if (!containsActiveDataset(expr))
            {
                //MORE: We should be able to hoist constant datasets (e.g., temptables), but it causes problems
                //e.g., stops items it contains from being aliased.  So 
                if (!expr->isAction() && !expr->isDataset() && !expr->isDatarow())
                {
                    switch (op)
                    {
                    case no_alias:
                        if (!expr->hasProperty(globalAtom))
                            extra->createAlias = true;
                        return;
                    default:
                        //MORE: We should be able to hoist constant datasets (e.g., temptables), but it causes problems
                        //e.g., stops items it contains from being aliased.
                        candidate = !expr->isConstant();
                        break;
                    }
                }
            }
        }

        if (candidate && canHoistInvariant(expr))
        {
            extra->createAlias = true;
            return;
        }
    }

    if (op == no_attr_expr)
        analyseChildren(expr);
    else
        NewHqlTransformer::analyseExpr(expr);
}

#endif

bool TableInvariantTransformer::isTrivialAlias(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_cast:
    case no_implicitcast:
        //Don't create aliases for items that are simply integral casts of other aliases.
        {
            ITypeInfo * type = expr->queryType();
            if (type->isInteger())
            {
                IHqlExpression * cast = expr->queryChild(0);
                ITypeInfo * castType = cast->queryType();
                if (castType->isInteger() && (queryBodyExtra(cast)->createAlias || cast->getOperator() == no_alias))
                {
                    switch (type->getSize())
                    {
                    case 1: case 2: case 4: case 8:
                        return true;
                    }
                }
            }
            break;
        }
    case no_not:
        {
            IHqlExpression * child = expr->queryChild(0);
            if (queryBodyExtra(child)->createAlias || child->getOperator() == no_alias)
                return true;
            break;
        }
    }
    return false;
}


IHqlExpression * TableInvariantTransformer::createTransformed(IHqlExpression * expr)
{
    if (expr->getOperator() == no_alias)
    {
        OwnedHqlExpr newChild = transform(expr->queryChild(0));
        if (newChild->getOperator() == no_alias)
            return newChild.getClear();
    }

    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
    if (queryBodyExtra(expr)->createAlias)
    {
        if (!isTrivialAlias(expr))
        {
            OwnedHqlExpr attr = createAttribute(globalAtom);
            if (transformed->getOperator() == no_alias)
                transformed.set(transformed->queryChild(0));
            return createAlias(transformed->queryBody(), attr);
        }
    }
    return transformed.getClear();
}

//---------------------------------------------------------------------------

IHqlExpression * spotTableInvariant(IHqlExpression * expr)
{
    TableInvariantTransformer transformer;
    transformer.analyse(expr, 0);
    return transformer.transformRoot(expr);
}



IHqlExpression * spotTableInvariantChildren(IHqlExpression * expr)
{
    TableInvariantTransformer transformer;
    ForEachChild(i1, expr)
        transformer.analyse(expr->queryChild(i1), 0);
    return transformer.transformRoot(expr);
}


//---------------------------------------------------------------------------

static HqlTransformerInfo globalAliasTransformerInfo("GlobalAliasTransformer");
GlobalAliasTransformer::GlobalAliasTransformer() : NewHqlTransformer(globalAliasTransformerInfo)
{ 
    insideGlobal = false; 
}

void GlobalAliasTransformer::analyseExpr(IHqlExpression * expr)
{
    if (!containsAlias(expr))
        return;

    bool wasInsideGlobal = insideGlobal;
    GlobalAliasInfo * extra = queryBodyExtra(expr);
    extra->numUses++;
    if (expr->getOperator() == no_alias)
    {
        if (expr->hasProperty(globalAtom))
        {
//          assertex(!containsActiveDataset(expr) || isInlineTrivialDataset(expr));
            if (!insideGlobal)
                extra->isOuter = true;
        }

        if (extra->numUses > 1)
            return;

        if (extra->isOuter)
            insideGlobal = true;
    }
    else
    {
        //ugly, but we need to walk children more than once even if we've already been here.
        //What is important is if visited >1 or occur globally, so can short circuit based on that condition.
        //This currently links too many times because subsequent cse generation may common up multiple uses of the same item
        //but it's not too bad.
        //We could rerun this again if that was a major issue.
        if (insideGlobal)
        {
            if (extra->numUses > 2)  
                return;     // may need to visit children more than once so that alias is linked twice.
        }
        else
        {
            if (extra->isOuter && (extra->numUses > 2))
                return;
            extra->isOuter = true;
        }
    }

    if (expr->getOperator() == no_attr_expr)
        analyseChildren(expr);
    else
        NewHqlTransformer::analyseExpr(expr);
    insideGlobal = wasInsideGlobal;
}

IHqlExpression * GlobalAliasTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);

    if ((expr->getOperator() == no_alias))
    {
        GlobalAliasInfo * extra = queryBodyExtra(expr);
        if (expr->hasProperty(globalAtom))
        {
            if (!extra->isOuter)
            {
                if (extra->numUses == 1)
                    return LINK(transformed->queryChild(0));
                if (!expr->hasProperty(localAtom))
                    return appendLocalAttribute(transformed);
            }
            else if (expr->hasProperty(localAtom))
            {
                //Should never occur - but just about conceivable that some kind of constant folding
                //might cause a surrounding global alias to be removed.
                return removeLocalAttribute(transformed);
            }
        }
        else
        {
            if ((extra->numUses == 1) && !expr->hasProperty(internalAtom))
                return LINK(transformed->queryChild(0));
        }
    }
    return transformed.getClear();
}

//---------------------------------------------------------------------------

IHqlExpression * optimizeActivityAliasReferences(IHqlExpression * expr)
{
    if (!containsAlias(expr))
        return LINK(expr);

    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);
    bool foundAlias = false;
    ForEachChild(i1, expr)
    {
        IHqlExpression * cur = expr->queryChild(i1);
        if (((i1 < first) || (i1 >= last)) && containsAlias(cur))
        {
            foundAlias = true;
            break;
        }
    }
    if (!foundAlias)
        return LINK(expr);

    GlobalAliasTransformer transformer;
    ForEachChild(i2, expr)
    {
        IHqlExpression * cur = expr->queryChild(i2);
        if (((i2 < first) || (i2 >= last)) && containsAlias(cur))
            transformer.analyse(cur, 0);
    }

    HqlExprArray args;
    ForEachChild(i3, expr)
    {
        IHqlExpression * cur = expr->queryChild(i3);
        if ((i3 < first) || (i3 >= last))
            args.append(*transformer.transformRoot(cur));
        else
            args.append(*LINK(cur));
    }
    return cloneOrLink(expr, args);
}


