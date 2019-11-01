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
//#define TRACE_CSE_SCOPE
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
    case no_external:
        return false;
    }
    ITypeInfo * type = expr->queryType();
    if (!type)
        return false;
    switch (type->getTypeCode())
    {
    case type_transform:
    case type_null:
    case type_void:
    case type_rule:
    case type_pattern:
    case type_token:
    case type_event:
        return false;
    default:
        return true;
    }
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
        return inverse->worthAliasingOnOwn();
    }

    node_operator invOp = inverse->original->getOperator();
    switch (invOp)
    {
    case no_not: return false;      //No otherwise we'll expand recursively!
    case no_ne:
    case no_notin:
    case no_notbetween:
        return !worthAliasingOnOwn();
    }
    return op > invOp;
}


static HqlTransformerInfo cseSpotterInfo("CseSpotter");
CseSpotter::CseSpotter(bool _spotCseInIfDatasetConditions)
: NewHqlTransformer(cseSpotterInfo), spotCseInIfDatasetConditions(_spotCseInIfDatasetConditions)
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

    switch (op)
    {
    case no_assign:
    case no_transform:
    case no_newtransform:
    case no_range:
    case no_rangefrom:
        if (expr->isConstant())
            return;
        break;
    case no_constant:
        return;
    }

    if (extra->numRefs++ != 0)
    {
        if (op == no_alias)
            return;
        if (!spottedCandidate && extra->worthAliasing())
            spottedCandidate = true;
        if (canCreateTemporary(expr))
            return;

        //Ugly! This is here as a temporary hack to stop branches of maps being commoned up and always
        //evaluated.  The alias spotting and generation really needs to take conditionality into account....
        if (op == no_mapto)
            return;
    }

    if (!containsPotentialCSE(expr))
        return;

    if (canAlias && !expr->isDataset())
        extra->canAlias = true;

    bool savedCanAlias = canAlias;
    if (expr->isDataset() && (op != no_select) && (!spotCseInIfDatasetConditions || (op != no_if)))
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
        //return expr->cloneAllAnnotations(alias);
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

    if (!expr->isPure())
        return false;

    if (!canCreateTemporary(expr))
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
        //MORE: ds[n].x would probably be worth cseing.
        return false;
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
            SubStringHelper helper(expr);
            return !helper.canGenerateInline();
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
    case no_datasetfromdictionary:
    case no_preservemeta:
    case no_dataset_alias:
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
    case no_embedbody:
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


//Which expression node contains all references to left and right?
CseScopeInfo * findCommonPath(CseScopeInfo * left, CseScopeInfo * right)
{
    for (;;)
    {
        if (!left || !right)
            return NULL;
        if (left == right)
            return left;

        //By using the sequence number we originally traversed the tree, if we always selected the higher sequence we can guarantee that we
        //will converge on a common node if there is one (since seq of a parent is always less than all its children)
        if (left->seq > right->seq)
            left = left->queryCommonParent();
        else
            right = right->queryCommonParent();
    }
}

void CseScopeInfo::calcCommonLocation()
{
    assertex(!calcedCommonLocation);

    CseScopeInfo * common = this;
    CseScopeInfo * thisLocation = this;
    CseScopeInfo * parent = nullptr;
    bool singleParent = true; // Does this specific node only have a single parent node?
    moved = false;
    if (firstParent)
    {
        CseScopeInfo *firstParentExtra = firstParent;
        parent = firstParentExtra;
        //CseScopeInfo * parentPath = parent ? parent->queryValidPath() : nullptr;
        moved = firstParent->hasMoved();
        ForEachItemIn(i, parents)
        {
            CseScopeInfo * parentExtra = parents.item(i);
            //REVISIT: Think about IF() conditions.... - otherwise duplicates could be removed.
            if (parentExtra != firstParentExtra)
            {
                singleParent = false;
                CseScopeInfo * newParent = findCommonPath(parent, parentExtra);

                if (isAlias)
                {
                    //If the common parent is the same as the firstParent then don't treat it as being moved
                    if (newParent != parent)
                    {
                        // If parent nodes are no_mapto or similar, they wouldn't be treated as different
                        if (parent->queryValidPath() != newParent->queryValidPath())
                        {
                            //The common parent is definitely different..
                            moved = true;
                        }
                    }
                }
                else
                    moved = true;

                parent = newParent;
                if (!parent)
                    break;
            }
        }

        if (!singleParent)
            common = parent;

        if (parent)
        {
            if (isAlias)
            {
                //If there are multiple paths to this node, but the common nodes do not create aliases (e.g., no_mapto)
                //then this needs to be evaluated on that nodes commonParent's location.
                CseScopeInfo * parentLocation = parent->queryEvalLocation();
                if (moved)
                    thisLocation = parentLocation;

                //If the path to the parent has changed, then the path to this alias has changed.
                if (!parentLocation || parent->hasMoved())
                {
                    thisLocation = parentLocation;
                    moved = true;
                }

                //If the path to the calculated parent doesn't match the path to the first parent, then this alias will
                //need to be evaluated in the parent's location
                if ((parent != firstParentExtra) &&
                    (parent->queryValidPath() != firstParentExtra->queryValidPath()))
                {
                    thisLocation = parentLocation;;
                    moved = true;
                }

                //If any parent alias has moved (so it will be evaluated earlier), then the insertion point for this alias
                //will be the restricted to the insertion point for the parent alias - to ensure it is evaluated first.
                ForEachItemIn(i, parentAliases)
                {
                    CseScopeInfo & alias = parentAliases.item(i);
                    if (alias.hasMoved())
                    {
                        CseScopeInfo * aliasLocation = alias.queryEvalLocation();
                        CseScopeInfo * newLocation;
                        //Need to find the common path between the parent and the aliases
                        if (thisLocation == this)
                            newLocation = findCommonPath(parent, aliasLocation);
                        else
                            newLocation = findCommonPath(thisLocation, aliasLocation);

                        //If the shared location is different from the parent, or if the parent is higher up the graph
                        //than the alias (e.g. if it was already the root)
                        if (newLocation != parent)
//REVISIT: The following also improved the generated code
                        //if ((newLocation != parent) || parent->minDepth < alias.minDepth)
//REVISIT: This improved code for some queries, but isn't really correct!
//                      if (newLocation != parent || parent->seq < alias.seq)
                        {
                            moved = true;

                            if (newLocation)
                                thisLocation=newLocation->queryValidPath();
                            else
                                thisLocation=newLocation;
                        }
                    }
                }
            }
            else
            {
                if (parent->hasMoved())
                {
                    common = parent->queryCommonNode();
                    moved = true;
                }

                if (singleParent && !parent->hasMoved())
                {
                    thisLocation = this;
                }
                else
                {
                    thisLocation = parent->queryEvalLocation();
                }
            }
        }
        else
        {
            //Multiple paths with no common parent
            common = nullptr;
            thisLocation = nullptr;
            moved = true;
        }
    }

    calcedCommonLocation = true;
    commonNode = common;
    evalLocation = thisLocation;

#ifdef TRACE_CSE_SCOPE
    StringBuffer parentText, aliasText;
    if (firstParent)
    {
        parentText.append(firstParent->seq);
        ForEachItemIn(i1, parents)
            parentText.append(",").append(parents.item(i1)->seq);
    }
    ForEachItemIn(i2, parentAliases)
    {
        if (i2)
            aliasText.append(",");
        aliasText.append(parentAliases.item(i2).seq);
    }
    CseScopeInfo * commonParent = queryCommonParent();
    CseScopeInfo * parentAlias = commonParent ? commonParent->queryAlias() : nullptr;
    CseScopeInfo * myLocation = queryScopeLocation();
    printf("%u: %s [%s:%s] parent(%u:%u) common(%u) location(%u%s) child(%u)\n", seq, getOpString(original->getOperator()),
                    parentText.str(), aliasText.str(),
                    parent ? parent->seq : 0,
                    parentAlias ? parentAlias->seq : 0,
                    commonNode ? commonNode->seq : 0,
                    myLocation ? myLocation->seq : 0, moved ? ",moved" : "",
                    evalLocation ? evalLocation->seq : 0
                    );
#endif
}

void CseScopeInfo::cloneAliases(ICopyArrayOf<CseScopeInfo> & target) const
{
    unsigned max = childAliases.ordinality();
    target.ensure(target.ordinality() + max);
    for (unsigned i=0; i < max; ++i)
        target.append(childAliases.item(i));
}

void CseScopeInfo::connectChildAliases(CseScopeInfo * parent)
{
    ForEachItemIn(i, childAliases)
        childAliases.item(i).addParentAlias(parent);

    if (inheritedAliases)
        inheritedAliases->connectChildAliases(parent);
}

void CseScopeInfo::expandInheritedAliases()
{
    if (inheritedAliases)
    {
        inheritedAliases->cloneAliases(childAliases);
        inheritedAliases = nullptr;
    }
}

CseScopeInfo * CseScopeInfo::queryScopeLocation()
{
    CseScopeInfo * location = queryEvalLocation();
    if (!location)
        return nullptr;

    if (location != this)
        return location->queryValidPath();

    //REVISIT: could probably return nullptr if it isn't moving to avoid creating a no_aliasscope
    CseScopeInfo * parent = queryCommonParent();
    if (!parent)
        return nullptr;

    CseScopeInfo * best = parent->queryEvalLocation();
    return best->queryValidPath();
}

CseScopeInfo * CseScopeInfo::queryValidPath()
{
    //REVISIT: Is it worth caching this expression?
    if (canInsertCodeAlias(original))
        return this;

    CseScopeInfo * parent = queryCommonParent();
    if (!parent)
        return nullptr;
    return parent->queryValidPath();
}

static HqlTransformerInfo cseScopeTransformerInfo("CseScopeTransformer");
CseScopeTransformer::CseScopeTransformer() : NewHqlTransformer(cseScopeTransformerInfo)
{
}

void CseScopeTransformer::analyseExpr(IHqlExpression * expr)
{
    expr = expr->queryBody();
    if (!containsNonGlobalAlias(expr))
        return;

    CseScopeInfo * extra = queryBodyExtra(expr);
    if (extra->seq)
    {
        //REVISIT: Flawed since this will not update the depth for any child expressions.
        if (depth < extra->minDepth)
            extra->minDepth = depth;
        extra->addParent(activeParent);
    }
    else
    {
        extra->firstParent = activeParent;
        extra->seq = ++seq;
        extra->isUnconditional = (conditionDepth == 0);
        extra->minDepth = depth;

        node_operator op = expr->getOperator();
        if (op == no_alias)
            extra->isAlias = true;

        depth++;
        {
            CseScopeInfo * savedParent = activeParent;
            activeParent = extra;
            switch (op)
            {
            case no_if:
            case no_or:
            case no_and:
            case no_case:
                {
                    //REVISIT: isUnconditional is not currently used - could delete this code if there is no benefit
                    //         or could use a base class that calculates it for us
                    analyseExpr(expr->queryChild(0));
                    conditionDepth++;
                    ForEachChildFrom(i, expr, 1)
                        analyseExpr(expr->queryChild(i));
                    conditionDepth--;
                    break;
                }
            //REVISIT: Do something clever with activeParent for assignments within transforms to improve insert location.
            default:
                NewHqlTransformer::analyseExpr(expr);
                break;
            }
            activeParent = savedParent;
        }
        depth--;

        //Add here so the cse are in the correct order to cope with dependencies...
        if (op == no_alias)
        {
            extra->connectChildAliases();
            assertex(!expr->hasAttribute(globalAtom));
            allCSEs.append(*LINK(extra));
            //REVISIT: Can now kill the child alias array since no longer required.
        }
    }

    if (activeParent)
    {
        if (extra->isAlias)
            activeParent->addChildAlias(extra);
        else if (!extra->childAliases.empty())
            activeParent->inheritAliases(extra);
        else
            activeParent->inheritAliases(extra->inheritedAliases);
    }
}

bool CseScopeTransformer::attachCSEs(IHqlExpression * root)
{
    bool changed = false;
    ForEachItemIn(idx, allCSEs)
    {
        CseScopeInfo& cur = allCSEs.item(idx);
        CseScopeInfo * aliasLocation = cur.queryScopeLocation();

        //REVISIT:
        //if (!aliasLocation && cur.isUnconditional)
        //    aliasLocation = root;

        //REVISIT:
        //if (aliasLocation && aliasLocation != cur.original)
        if (aliasLocation)
        {
#ifdef TRACE_CSE_SCOPE
            printf("Attach %u at %u\n", cur.seq, aliasLocation->seq);
#endif
            aliasLocation->aliasesToDefine.append(*LINK(cur.original));
            changed = true;
        }
        else
        {
#ifdef TRACE_CSE_SCOPE
            printf("Nowhere to attach %u\n", cur.seq);
#endif
        }
    }
    return changed;
}

IHqlExpression * CseScopeTransformer::createTransformed(IHqlExpression * expr)
{
    //NB: Can not short-circuit transformation if (!containsAlias(expr)) because references to transformed datasets will not get patched up
    IHqlExpression * body = expr->queryBody(true);
    if (body != expr)
    {
        OwnedHqlExpr ret = transform(body);
        return expr->cloneAnnotation(ret);
    }

    IHqlExpression * transformed = NewHqlTransformer::createTransformed(expr);
    CseScopeInfo * splitter = queryBodyExtra(expr);
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


IHqlExpression * spotScalarCSE(IHqlExpression * expr, IHqlExpression * limit, bool spotCseInIfDatasetConditions)
{
    if (expr->isConstant())
        return LINK(expr);
    switch (expr->getOperator())
    {
    case no_select:
        if (!isNewSelector(expr))
            return LINK(expr);
        break;
    }

    OwnedHqlExpr transformed = LINK(expr); //removeNamedSymbols(expr);

    bool addedAliases = false;
    //First spot the aliases - so that restructuring the ands doesn't lose any existing aliases.
    {
        CseSpotter spotter(spotCseInIfDatasetConditions);
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
    bool modified = false;
    {
        ConjunctionTransformer tr;
        OwnedHqlExpr result = tr.transformRoot(transformed);
        if (result != transformed)
            modified = true;
        transformed.swap(result);
    }

    if (modified)
    {
        CseSpotter spotter(spotCseInIfDatasetConditions);
        spotter.analyse(transformed, 0);
        if (spotter.foundCandidates())
        {
            if (limit)
                spotter.stopTransformation(limit);
            transformed.setown(spotter.transformRoot(transformed));

            if (spotter.createdNewAliases())
                addedAliases = true;
        }
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


void spotScalarCSE(SharedHqlExpr & expr, SharedHqlExpr & associated, IHqlExpression * limit, IHqlExpression * invariantSelector, bool spotCseInIfDatasetConditions)
{
    CseSpotter spotter(spotCseInIfDatasetConditions);
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


void spotScalarCSE(HqlExprArray & exprs, HqlExprArray & associated, IHqlExpression * limit, IHqlExpression * invariantSelector, bool spotCseInIfDatasetConditions)
{
    CseSpotter spotter(spotCseInIfDatasetConditions);
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
        if ((expr->getOperator() != no_alias) || expr->hasAttribute(globalAtom))
            return false;
    }

    if (!expr->isPure())
        return false;
    if (expr->isFunction())
        return false;

    switch (expr->getOperator())
    {
    case no_list:
    case no_datasetlist:
    case no_createdictionary:
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
    case no_workunit_dataset:
    case no_getresult:
        if (expr->hasAttribute(wuidAtom))
            invariant = isInvariant(expr->queryAttribute(wuidAtom));
        else
            invariant = true;
        break;
    case no_constant:
    case no_getgraphresult:
        invariant = true;
        break;
    case no_select:
        {
            if (!expr->isDataset())
            {
                IHqlExpression * ds = expr->queryChild(0);
                if (expr->hasAttribute(newAtom) || ds->isDatarow())
                    invariant = isInvariant(ds);
            }
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
        //MORE: The following line is needed if the xml/parse flags are removed from the context, but it causes problems
        //preventing counts from being hoisted as aliases.  That is really correct - but it makes code worse for some examples.
        //if (!isContextDependent(expr) && expr->isIndependentOfScope())
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
            if (!expr->hasAttribute(globalAtom))
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
                        if (!expr->hasAttribute(globalAtom))
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

bool TableInvariantTransformer::isAlwaysAlias(IHqlExpression * expr)
{
    if (queryBodyExtra(expr)->createAlias)
        return true;
    switch (expr->getOperator())
    {
    case no_alias:
    case no_getresult:      // these are commoned up in the code generator, so don't do it twice.
    case no_getgraphresult:
    case no_getgraphloopresult:
        return true;
    }
    return false;
}

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
                if (castType->isInteger() && isAlwaysAlias(cast))
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
            if (isAlwaysAlias(child))
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
    updateOrphanedSelectors(transformed, expr);
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
        if (expr->hasAttribute(globalAtom))
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
        if (expr->hasAttribute(globalAtom))
        {
            if (!extra->isOuter)
            {
                if (extra->numUses == 1)
                    return LINK(transformed->queryChild(0));
                if (!expr->hasAttribute(localAtom))
                    return appendLocalAttribute(transformed);
            }
            else if (expr->hasAttribute(localAtom))
            {
                //Should never occur - but just about conceivable that some kind of constant folding
                //might cause a surrounding global alias to be removed.
                return removeLocalAttribute(transformed);
            }
        }
        else
        {
            if ((extra->numUses == 1) && !expr->hasAttribute(internalAtom))
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


