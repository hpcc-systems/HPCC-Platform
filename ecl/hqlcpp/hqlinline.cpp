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

#include "hql.hpp"
#include "hqlcpp.ipp"
#include "hqlhtcpp.ipp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"
#include "hqltcppc.ipp"
#include "hqlwcpp.hpp"
#include "hqlcatom.hpp"
#include "hqlccommon.hpp"
#include "hqlcerrors.hpp"
#include "hqlpmap.hpp"
#include "hqlinline.hpp"

//I don't think the following is needed yet, but when it is, just enable following...
//#define ONSTART_IN_NESTED_TOPLEVEL

/*
Allow the following operations to be evaluated in line:

  a) project of an dataset that can be iterated
*/

enum
{
    HEFprocessinline    = 0x0001,
    HEFassigninline     = 0x0002,
    HEFiterateinline    = 0x0004,
    HEFevaluateinline   = 0x0008,                   // can evaluate without any temporary dataset being created (temporary row is ok)
//    HEFspillinline      = 0x0010,                   // I'm not sure I can do this - because whether it spills depends on how it is being used.

    RETassign       = HEFassigninline|HEFprocessinline,
    RETiterate      = HEFiterateinline|HEFassigninline|HEFprocessinline,
    RETevaluate     = HEFevaluateinline|HEFiterateinline|HEFassigninline|HEFprocessinline,

};

#define canAssignNoSpill(childFlags)        ((childFlags & HEFassigninline) == HEFassigninline)
#define canIterateNoSpill(childFlags)       ((childFlags & HEFiterateinline) == HEFiterateinline)
#define canEvaluateNoSpill(childFlags)      ((childFlags & HEFevaluateinline) == HEFevaluateinline)

// assign is a subset of iterate, iterate is a subset of evaluate


static unsigned getInlineFlags(BuildCtx * ctx, IHqlExpression * expr);

static unsigned calcInlineFlags(BuildCtx * ctx, IHqlExpression * expr)
{
    //The following improves a few graphs, but sometimes significantly (e.g., bc10.xhql, seep11.xhql)
    //But it would be really good if the code could be made context independent - then it could go in hqlattr and be cached.
    if (ctx)
    {
        if (expr->isDataset() || expr->isDictionary())
        {
            if (ctx->queryMatchExpr(expr))
                return RETevaluate;
        }
        else
        {
            if (ctx->queryAssociation(expr, AssocRow, NULL))
                return RETevaluate;
        }
    }

    //This function should return the first value that matches:
    // RETevaluate - the dataset is completely available, and all elements can be accessed directly
    // RETiterate - each element in the dataset can be accessed without evaluating the entire dataset
    // RETassign - the dataset can be assigned to a temporary
    // 0 - the dataset cannot be evaluated inline, it requires a child query.

    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_workunit_dataset:
        if (expr->hasAttribute(wuidAtom))
            return 0;
        return RETassign;
    case no_alias:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;

            //This always creates a temporary, so can be evaluated efficiently
            return RETevaluate;
        }
    case no_dataset_alias:
        return getInlineFlags(ctx, expr->queryChild(0));
    case no_call:
    case no_externalcall:               // no so sure about this - should possibly be assignable only. (also no_call above)
    case no_getresult:
        return RETassign;
    }

    if (isGrouped(expr))
        return 0;

    switch (op)
    {
    case no_select:
        //MORE: What about child datasets in nested records?
        if (isNewSelector(expr))
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if ((childFlags & HEFevaluateinline) && isMultiLevelDatasetSelector(expr, false))
                childFlags &= ~HEFevaluateinline;
            return childFlags;
        }
        return RETevaluate;
    case no_newaggregate:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if ((childFlags == 0) || queryRealChild(expr, 3))
                return 0;
            //Always effectively requires a temporary
            return RETassign;
        }
    case no_hqlproject:
        //can't do a skip inside an inline project - since the generated code doesn't allow "continue" to be used.
        if (transformContainsSkip(expr->queryChild(1)))
            return 0;
        //fallthrough
    case no_newusertable:
        {
            if (expr->hasAttribute(prefetchAtom))
                return 0;
            IHqlExpression * ds = expr->queryChild(0);
            unsigned childFlags = getInlineFlags(ctx, ds);
            if (childFlags == 0)
                return 0;
            if (hasSingleRow(ds))
            {
                //Probably not worth iterating...
                return RETassign;
            }
            if (canIterateNoSpill(childFlags))
                return RETiterate;
            return RETassign;
        }
    case no_selectmap:
    case no_selectnth:
        {
            IHqlExpression * ds = expr->queryChild(0);
            unsigned childFlags = getInlineFlags(ctx, ds);
            if (childFlags == 0)
            {
                if (isSelectSortedTop(expr))
                {
                    childFlags = getInlineFlags(ctx, ds->queryChild(0));
                    if (childFlags == 0)
                        return 0;
                    //Dataset will be calculated => can just access the 1st element
                    return RETevaluate;
                }
                return 0;
            }
            if (isTrivialSelectN(expr) && canEvaluateNoSpill(childFlags))
                return RETevaluate;
            if (canIterateNoSpill(childFlags))
                return RETevaluate;
            return RETassign;
        }
    case no_filter:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;
            if (!canIterateNoSpill(childFlags) && filterIsTableInvariant(expr))
                return RETassign;
            return RETiterate;
        }
    case no_choosen:        
    case no_index:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;
//            if (canEvaluateNoSpill(childFlags))
//                return RETevaluate;
            if (canIterateNoSpill(childFlags))
                return RETiterate;
            return RETassign;
        }
    case no_limit:
        {
            if (expr->hasAttribute(skipAtom) || expr->hasAttribute(onFailAtom))
                return 0;
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;
            if (canEvaluateNoSpill(childFlags))
                return RETevaluate;
            if (canIterateNoSpill(childFlags))
                return RETiterate;
            return RETassign;
        }
    case no_fail:
        return RETevaluate;
    case no_catchds:
        return 0;       // for the moment always do this out of line 
    case no_table:
        return 0;
    case no_createdictionary:
        return RETassign;
    case no_datasetfromdictionary:
        return RETiterate;
    case no_owned_ds:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;
            return RETassign;
        }
    case no_addfiles:
        {
            for (unsigned i=0; i < 2; i++)
            {
                unsigned childFlags = getInlineFlags(ctx, expr->queryChild(i));
                if (childFlags == 0)
                    return 0;
            }
            return RETassign;
        }
    case no_if:
    case no_choose:
    case no_chooseds:
        {
            unsigned ret = expr->isDatarow() ? RETevaluate : RETassign;
            ForEachChildFrom(i, expr, 1)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (cur->isAttribute())
                    continue;
                unsigned childFlags = getInlineFlags(ctx, cur);
                if (childFlags == 0)
                    return 0;
            }
            return ret;
        }
    case no_compound_childread:
    case no_compound_childnormalize:
    case no_compound_childaggregate:
    case no_compound_selectnew:
    case no_compound_inline:
    case no_datasetfromrow:
    case no_sorted:
    case no_distributed:
    case no_preservemeta:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
    case no_alias_scope:
    case no_serialize:
    case no_deserialize:
    case no_dataset_alias:
        return getInlineFlags(ctx, expr->queryChild(0));
    case no_forcegraph:
        return 0;
    case no_section:
        if (expr->hasAttribute(graphAtom))       // force it to appear in the graph
            return 0;
        return getInlineFlags(ctx, expr->queryChild(0));
    case no_getgraphresult:
        if (expr->hasAttribute(_distributed_Atom) || expr->hasAttribute(_streaming_Atom))
            return 0;
        return expr->isDatarow() ? RETevaluate : RETassign;
    case no_temptable:
        return RETassign;
    case no_xmlproject:
        return RETiterate;
    case no_dataset_from_transform:
    {
        if (expr->hasAttribute(distributedAtom))
            return 0;
        if (transformContainsSkip(expr->queryChild(1)))
            return 0;
        return RETassign;
    }
    case no_inlinetable:
        {
            IHqlExpression * transforms = expr->queryChild(0);
            if (transformListContainsSkip(transforms))
                return 0;
            if (isConstantDataset(expr))
                return RETevaluate;
            return RETassign;
        }
    case no_createrow:
        //MORE: We should probably be able to handle this...
        if (containsSkip(expr->queryChild(0)))
            return 0;
        return RETevaluate;
    case no_translated:
        return RETevaluate;
    case no_projectrow:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;
            return RETevaluate;
        }
    case no_null:
    case no_temprow:
    case no_left:
    case no_right:
    case no_top:
    case no_id2blob:
    case no_activerow:
    case no_typetransfer:
    case no_rows:
    case no_skip:
    case no_matchattr:
    case no_matchrow:
    case no_libraryinput:
    case no_fromxml:
    case no_fromjson:
        return RETevaluate;
    case no_apply:
        {
            unsigned childFlags = getInlineFlags(ctx, expr->queryChild(0));
            if (childFlags == 0)
                return 0;
            return RETevaluate;
        }
    case no_activetable:
        return RETassign;
    case no_join:
        {
            if (!expr->hasAttribute(allAtom) || isKeyedJoin(expr))
                return 0;
            //conservatively check we support the attributes.
            unsigned max = expr->numChildren();
            for (unsigned i=4; i < max; i++)
            {
                IHqlExpression * cur = expr->queryChild(i);
                if (!cur->isAttribute())
                    return 0;
                IAtom * name = cur->queryName();
                //possibly implement keep as well.  (Local on a child join does nothing.)
                if ((name != leftouterAtom) && (name != leftonlyAtom) && (name != innerAtom) && (name != allAtom) && (name != localAtom) && !isInternalAttributeName(name))
                    return 0;
            }
            unsigned childLFlags = getInlineFlags(ctx, expr->queryChild(0));
            unsigned childRFlags = getInlineFlags(ctx, expr->queryChild(1));
            if ((childLFlags == 0) || (childRFlags == 0))
                return 0;
            if (canIterateNoSpill(childLFlags) && canIterateNoSpill(childRFlags))
                return RETiterate;
            return RETassign;
        }
    case no_compound:
        return getInlineFlags(ctx, expr->queryChild(1));
    default:
        return 0;
    }
}


unsigned getInlineFlags(BuildCtx * ctx, IHqlExpression * expr)
{
    //This could one day use flags stored in CHqlExpression if it was considered necessary.
    return calcInlineFlags(ctx, expr);
}


bool canProcessInline(BuildCtx * ctx, IHqlExpression * expr)
{
    return getInlineFlags(ctx, expr) != 0;
}


bool canIterateInline(BuildCtx * ctx, IHqlExpression * expr)
{
    return canIterateNoSpill(getInlineFlags(ctx, expr));
}

bool canEvaluateInline(BuildCtx * ctx, IHqlExpression * expr)
{
    return canEvaluateNoSpill(getInlineFlags(ctx, expr));
}

bool canAssignInline(BuildCtx * ctx, IHqlExpression * expr)
{
    return canAssignNoSpill(getInlineFlags(ctx, expr));
}

bool canAssignNotEvaluateInline(BuildCtx * ctx, IHqlExpression * expr)
{
    unsigned flags = getInlineFlags(ctx, expr);
    return canAssignNoSpill(flags) && !canEvaluateNoSpill(flags);
}



bool alwaysEvaluatesToBound(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_select:
        return !isMultiLevelDatasetSelector(expr, false);
    case no_owned_ds:
    case no_translated:
    case no_alias:
    case no_call:               
    case no_externalcall:               // no so sure about this - should possibly be assignable only. (also no_call above)
    case no_getresult:
    case no_getgraphresult:
    case no_workunit_dataset:
    case no_activetable:
        return !hasStreamedModifier(expr->queryType());
    case no_temptable:
        //MORE! when we have constant datasets
    default:
        return canEvaluateInline(NULL, expr);
    }
}

//============================================================================


//============================================================================

/*
  Notes on Graph distribution/localisation etc. 

  There are several different things to take into account when noting the localisation of an activity, including the problem
  that meaning of "local" is overloaded.  Currently we have something like the following, for each logical instance of an activity:

  Roxie
    SingleNode      - only evaluated on a single node
                      [Split, filter, project, rollup, sort, simpleresult]
    SingleNodeLocal - evaluated on a single node, and only access local files.
                      [local keyed join, local index read]
    PrimarySlave    - Evaluated on many nodes, only using local file parts [if any], interaction between nodes may occur on the primary
                      [keyed join, index read, remote]

  Thor
    Master          - only evalauted on the master
                      [simpleresult]
    Distributed     - Evaluated on many nodes
                      [sort, distribute, group]
    DistributedIndependent - Evaluated on many nodes, no interaction between nodes
                      [local sort, local group]
    DistributedLocal- Evaluated on many nodes using local file parts
                      [local index read, local keyed join]

  HThor only has singlenode as an option

  Child Queries can be essentially be any of the roxie options.  In addition there is the question of where the child will be
  evaluated in relation to its parent.  The options are

    NoAccess        - no context dependent information in the helper
    CoLocal         - Will be evaluated on the same slave node as the parent
    NonLocal        - May be evaluated on a different slave node from the parent


  We'll use the following logical flags to keep track of the different options:

  _singlenode_|_independentnodes_|_
  independent       - no interaction between records on the different nodes.
  correlated        - interaction between records on the different nodes.
  _localparts_|_allparts_ 
                    - access local file parts or all file parts.
  allfile           - access all file parts.
  local             - either access local file parts, or no interaction between nodes.

  Ideally we want the expression tree to be independent of the current engine type, and so that derived attributes and optimizations 
  can work independently of the engine if possible.

  => spill needs to indicate whether it is a purely memory spill
  => sort order and local interactions need resolving.

*/


inline GraphLocalisation mergeLocalisation(GraphLocalisation l, GraphLocalisation r)
{
    //options are GraphCoLocal, GraphNonLocal, GraphCoNonLocal, GraphNoAccess
    if (l == GraphNeverAccess)
        l = GraphNoAccess;
    if (r == GraphNeverAccess)
        r = GraphNoAccess;
    if (l == GraphNoAccess)
        return r;
    if (r == GraphNoAccess)
        return l;
    if ((l == GraphCoNonLocal) || (r == GraphCoNonLocal))
        return GraphCoNonLocal;
    if (l != r)
        return GraphCoNonLocal;
    return l;
}

bool exprExcludingInputNeedsParent(IHqlExpression * expr)
{
    unsigned first = getFirstActivityArgument(expr);
    unsigned last = first + getNumActivityArguments(expr);

    ForEachChild(i, expr)
    {
        if (i >= first && i < last)
            continue;

        IHqlExpression * cur = expr->queryChild(i);
        if (isGraphDependent(cur) || usesRuntimeContext(cur))
            return true;
    }

    if (!expr->isIndependentOfScopeIgnoringInputs())
        return true;

    return false;
}

bool exprIncludingInputNeedsParent(IHqlExpression * expr)
{
    if (isGraphDependent(expr) || usesRuntimeContext(expr))
        return true;
    if (!expr->isIndependentOfScope())
        return true;
    return false;
}

bool activityNeedsParent(IHqlExpression * expr)
{
    //This should always err on the side of yes...
    switch (expr->getOperator())
    {
    case no_select:
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
        if (exprIncludingInputNeedsParent(expr))
            return true;
        break;
    case no_libraryinput:
        return true;
    case no_hqlproject:
    case no_newusertable:
        //Filters might be merged into projects, so need to walk the inputs.
        loop
        {
            if (activityNeedsParent(expr))
                return true;
            expr = expr->queryChild(0);
            switch (expr->getOperator())
            {
            case no_sorted:
            case no_filter:
                break;
            default:
                return false;
            }
        }
    case no_libraryscopeinstance:
        {
            //Obscure way to get at the name of the library being called.
            IHqlExpression * moduleFunction = expr->queryBody()->queryDefinition();
            IHqlExpression * module = moduleFunction->queryChild(0);
            assertex(module->getOperator() == no_libraryscope);
            IHqlExpression * nameAttr = module->queryAttribute(nameAtom);
            if (activityNeedsParent(nameAttr))
                return true;
            return exprExcludingInputNeedsParent(expr);
        }
    default:
        if (expr->isDatarow())
        {
            switch (expr->getOperator())
            {
            case no_split:
            case no_spill:
            case no_selectnth:
                if (exprExcludingInputNeedsParent(expr))
                    return true;
                break;
            default:
                if (exprIncludingInputNeedsParent(expr))
                    return true;
                break;
            }
        }
        else
        {
            if (exprExcludingInputNeedsParent(expr))
                return true;
        }
        break;
    }

    //Assume the worse for queries based on ds.childdataset
    if (getChildDatasetType(expr) & childdataset_hasdataset)
    {
        IHqlExpression * ds = queryRoot(expr);
        if (ds && ds->getOperator() == no_select)
            return true;
    }
    return false;
}


// A minimal function to catch the obviously context invariant items.
static bool accessesData(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_attr:
    case no_constant:
    case no_record:
        return false;
    case no_assign:
        return accessesData(expr->queryChild(1));
    case no_assignall:
    case no_transform:
    case no_newtransform:
    case no_transformlist:
    case no_attr_expr:
    case no_fail:
        {
            ForEachChild(i, expr)
            {
                if (accessesData(expr->queryChild(i)))
                    return true;
            }
            return false;
        }
    default:
        return true;
    }
}

GraphLocalisation queryActivityLocalisation(IHqlExpression * expr, bool optimizeParentAccess)
{
    node_operator op = expr->getOperator();
    switch (op)
    {
    case no_compound_diskread:
        {
            if (isLocalActivity(expr) || expr->hasAttribute(_colocal_Atom))
                return GraphCoLocal;
            //If a compound operation has been added, but with no other effect then don't allow that to change the localisation
            IHqlExpression * ds = expr->queryChild(0);
            if (ds->getOperator() == no_table)
                return queryActivityLocalisation(ds, optimizeParentAccess);
            return GraphNonLocal;
        }
    case no_table:
        if (expr->hasAttribute(_noAccess_Atom))
            return GraphNeverAccess;
        //fallthrough
    case no_keyindex:
    case no_newkeyindex:
    case no_compound_disknormalize:
    case no_compound_diskaggregate:
    case no_compound_diskcount:
    case no_compound_diskgroupaggregate:
    case no_compound_indexread:
    case no_compound_indexnormalize:
    case no_compound_indexaggregate:
    case no_compound_indexcount:
    case no_compound_indexgroupaggregate:
        if (!isLocalActivity(expr) && !expr->hasAttribute(_colocal_Atom))
            return GraphNonLocal;
        break;
    case no_compound_fetch:
    case no_fetch:
        //Maybe there should be a version that only fetches locally???
        return GraphNonLocal;       // simplest if we do it this way
    case no_allnodes:
        return GraphNonLocal;       // simplest if we do it this way
    case no_join:
    case no_denormalize:
    case no_denormalizegroup:
        if (isKeyedJoin(expr) && !expr->hasAttribute(localAtom))
            return GraphNonLocal;
        break;
    case no_output:
        if (expr->hasAttribute(_spill_Atom))
            return GraphNeverAccess;
        break;
    case no_setgraphresult:
    case no_spillgraphresult:
    case no_setgraphloopresult:
    case no_null:
    case no_split:
    case no_spill:
    case no_readspill:
    case no_writespill:
    case no_commonspill:
    case no_addfiles:
    case no_subgraph:
    case no_nofold:
    case no_nohoist:
    case no_nocombine:
    case no_regroup:
    case no_sorted:
    case no_distributed:
    case no_preservemeta:
    case no_grouped:
    case no_alias_scope:
    case no_sequential:
    case no_parallel:
    case no_deserialize:
    case no_serialize:
    case no_actionlist:
    case no_orderedactionlist:
    case no_definesideeffect:
    case no_dataset_alias:
        return GraphNeverAccess;               // Will never access any data values from anywhere
    case no_hqlproject:
    case no_newusertable:
        //Many more of these could return GraphNoAccess if I determined that only constants and the input
        //row were accessed.  Examples are project, sort, group etc. etc.
        //if (isSimpleProject(expr))
        ///    return GraphNoAccess;
        return GraphCoLocal;
    case no_datasetfromrow:
        return GraphNeverAccess;
    case no_workunit_dataset:
        return GraphCoLocal; // weird exception in roxie
    case no_getgraphresult:
    case no_datasetfromdictionary:
        return GraphCoLocal;
    case no_createrow:
    case no_inlinetable:
        {
            unsigned max = expr->numChildren();
            for (unsigned i=0; i<max; i++)
            {
                if (accessesData(expr->queryChild(i)))
                    return GraphCoLocal;
            }
            return GraphNeverAccess;
        }
    case no_group:
    case no_choosen:
    case no_limit:
    case no_catchds:
    case no_selectnth:
        {
            unsigned max = expr->numChildren();
            for (unsigned i=1; i<max; i++)
            {
                if (accessesData(expr->queryChild(i)))
                    return GraphCoLocal;
            }
            return GraphNeverAccess;
        }
    case no_newaggregate:
        if (!queryRealChild(expr, 3))
        {
            node_operator op = querySimpleAggregate(expr, false, false);
            if (op == no_existsgroup || op == no_countgroup)
                return GraphNeverAccess;
            //Need to check if it accesses anything in the context!
        }
        break;
    }

    if (optimizeParentAccess && !activityNeedsParent(expr))
    {
        switch (op)
        {
        case no_if:
        case no_case:
        case no_map:
            //May be combined into a switch.
            break;
        default:
            return GraphNoAccess;
        }
    }
    return GraphCoLocal;
}

bool isNonLocal(IHqlExpression * expr, bool optimizeParentAccess)
{
    return (queryActivityLocalisation(expr, optimizeParentAccess) == GraphNonLocal);
}


static GraphLocalisation doGetGraphLocalisation(IHqlExpression * expr, bool optimizeParentAccess);
static GraphLocalisation queryGraphLocalisation(IHqlExpression * expr, bool optimizeParentAccess)
{
    GraphLocalisation localisation = queryActivityLocalisation(expr, optimizeParentAccess);

    if (isSourceActivity(expr))
        return localisation;

    unsigned firstChild = 0;
    unsigned numChildren;
    switch (expr->getOperator())
    {
    case no_if:
    case no_chooseds:
        firstChild = 1;
        numChildren = expr->numChildren();
        break;
    case no_compound:
    case no_subgraph:
        numChildren = expr->numChildren();
        break;
    case no_attr:
    case no_attr_expr:
        return GraphNeverAccess;
    default:
        numChildren = getNumActivityArguments(expr);
        break;
    }

    for (unsigned i = firstChild; i < numChildren; i++)
    {
        localisation = mergeLocalisation(localisation, doGetGraphLocalisation(expr->queryChild(i), optimizeParentAccess));
        if (localisation == GraphCoNonLocal)
            return localisation;
    }
    return localisation;
}

static GraphLocalisation doGetGraphLocalisation(IHqlExpression * expr, bool optimizeParentAccess)
{
    IHqlExpression * cached = (IHqlExpression *)expr->queryTransformExtra();
    if (cached)
        return (GraphLocalisation)cached->queryValue()->getIntValue();
    GraphLocalisation ret = queryGraphLocalisation(expr, optimizeParentAccess);
    if (ret == GraphNeverAccess)
        ret = GraphNoAccess;
    expr->setTransformExtraOwned(getSizetConstant((unsigned)ret));
    return ret;
}

static GraphLocalisation getGraphLocalisation(IHqlExpression * expr, bool optimizeParentAccess)
{
    TransformMutexBlock lock;
    return doGetGraphLocalisation(expr, optimizeParentAccess);
}

bool HqlCppTranslator::isAlwaysCoLocal()
{
    return targetHThor();
}

GraphLocalisation HqlCppTranslator::getGraphLocalisation(IHqlExpression * expr, bool isInsideChildQuery)
{
    if (isAlwaysCoLocal()) return GraphCoLocal;
    if (targetThor() && !isInsideChildQuery)
        return GraphNonLocal;

    return ::getGraphLocalisation(expr, options.optimizeParentAccess);
}

bool HqlCppTranslator::isNeverDistributed(IHqlExpression * expr)
{
    if (targetHThor())
        return true;
    switch (expr->getOperator())
    {
    case no_if:
        return expr->isAction();
    case no_setresult:
        return true;
    }
    return false;
}


    
//============================================================================

ParentExtract::ParentExtract(HqlCppTranslator & _translator, PEtype _type, IHqlExpression * _graphId, GraphLocalisation _localisation, EvalContext * _container)
: HqlExprAssociation(parentExtractMarkerExpr), translator(_translator), type(_type), graphId(_graphId)
{
    localisation = _localisation;
    container = _container;
    if (!container)
        throwError(HQLERR_ExpectedParentContext);

    Owned<ITypeInfo> nullRow = makeRowType(queryNullRecord()->getType());
    Owned<ITypeInfo> declType = makeModifier(makeWrapperModifier(LINK(nullRow)), typemod_builder);

    translator.getUniqueId(extractName.append("ex"));
    boundBuilder.expr.setown(createVariable(extractName.str(), LINK(declType)));
    boundExtract.expr.setown(createVariable(extractName.str(), makeReferenceModifier(LINK(nullRow))));

    serialization = NULL;
    childSerialization = NULL;
    canDestroyExtract = false;
}

ParentExtract::~ParentExtract()
{
    ::Release(serialization);
}

void ParentExtract::associateCursors(BuildCtx & declarectx, BuildCtx & evalctx, GraphLocalisation childLocalisation)
{
    const CursorArray * boundCursors = NULL;
    switch (childLocalisation)
    {
    case GraphCoLocal:
        if (localisation == GraphRemote)
            boundCursors = &nonlocalBoundCursors;
        else if ((localisation == GraphCoLocal) || (localisation == GraphCoNonLocal))
            boundCursors = &colocalBoundCursors;
        else
            throwError2(HQLERR_InconsisentLocalisation, (int)childLocalisation, (int)localisation);
        break;
    case GraphNonLocal:
    case GraphRemote:
        if ((localisation == GraphCoLocal) || (localisation == GraphNoAccess))
            throwError2(HQLERR_InconsisentLocalisation, (int)childLocalisation, (int)localisation);
        boundCursors = &nonlocalBoundCursors;
        break;
    case GraphCoNonLocal:
        throwUnexpected();
    case GraphNoAccess:
        if (localisation == GraphRemote)
            boundCursors = &nonlocalBoundCursors;
        else
        {
            //Make sure we get an error if we do try and access something from the parent.
            return;
        }
        break;
    default:
        throwUnexpected();
    }

    ForEachItemIn(i, *boundCursors)
    {
        BoundRow & cur = boundCursors->item(i);
        IHqlExpression * aliasExpansion = cur.queryAliasExpansion();
        if (aliasExpansion)
        {
            //NB: If this ever generates any code then we will need to process it later...
            CHqlBoundExpr bound;
            translator.buildExpr(evalctx, aliasExpansion, bound);
            declarectx.associateOwn(*cur.clone(bound.expr));
        }
        else
            declarectx.associateOwn(OLINK(cur));
    }
}


void ParentExtract::beginCreateExtract(BuildCtx & ctx, bool doDeclare)
{
    buildctx.setown(new BuildCtx(ctx));

    // Don't leak the serialization row into other sub calls - may want to do this a different way so
    // cses get commoned up by tagging the serialization somehow.
    serialization = SerializationRow::create(translator, boundBuilder.expr, container ? container->queryActivity() : NULL);

    OwnedHqlExpr finalFixedSize = serialization->getFinalFixedSizeExpr();

    //Probably do this later and allow it to be null.  Will need a mechanism for calling some finalisation code
    //after all code is generated in order to do it.
    if (doDeclare)
    {
        declarectx.setown(new BuildCtx(*buildctx));
        declarectx->addGroup();
    }

    buildctx->associateOwn(*LINK(serialization));

    //Ensure the row is large enough to cope with any fixed fields - will only get relocated if variable fields are serialised
    if (!doDeclare)
    {
        HqlExprArray args;
        args.append(*LINK(serialization->queryBound()));
        args.append(*LINK(finalFixedSize));
        translator.callProcedure(*buildctx, ensureRowAvailableId, args);
    }

    //Collect a list of cursors together... NB these are in reverse order..
    gatherActiveRows(*buildctx);

    childSerialization->setBuilder(this);
}


void ParentExtract::beginNestedExtract(BuildCtx & clonectx)
{
    //Collect a list of cursors together... NB these are in reverse order..
    gatherActiveRows(clonectx);
}


void ParentExtract::beginChildActivity(BuildCtx & declareCtx, BuildCtx & startCtx, GraphLocalisation childLocalisation, IHqlExpression * colocal, bool nested, bool ignoreSelf, ActivityInstance * activityRequiringCast)
{
    if (type == PETcallback)
        assertex(ignoreSelf);

    //MORE: If we ever generate grand children - nested classes then the following isn't going to work
    //because accessing colocal->exNNN isn't going to get at the extract defined in the child class+passed to the grandchild.
    //Simplest would be to define the builders in the activity, and access them as activity->exXXX in the (grand)child.
    bool isColocal = (colocal != NULL);
    const CursorArray & boundCursors = isColocal ? colocalBoundCursors : nonlocalBoundCursors;
    ForEachItemIn(i, boundCursors)
    {
        BoundRow & cur = boundCursors.item(i);
        if (cur.isSerialization())
        {
            IHqlExpression * bound = cur.queryBound();
            if (!colocal && bound != boundExtract.expr)
                break;

            //If this is the parent extract for a local callback, it will be passed as a parameter, so it shouldn't
            //be added as a member of the child class
            if (ignoreSelf && (bound == boundExtract.expr))
                continue;

            declareCtx.addDeclare(bound);

            OwnedHqlExpr src;
            if (bound == boundExtract.expr)
            {
                //MORE: This cast is a hack.  We should process const correctly in helper functions etc.
                if (!nested)
                    src.setown(createVariable("(byte *)pe", bound->getType()));
            }

            if (!src)
            {
                src.setown(addMemberSelector(bound, colocal));

                //yuk yuk yuk....  A nasty solution to a nasty problem.
                //If the parent is a nested class with an extract (used by the compound group aggregate)
                //then any other extracts are going to be builders in the parent activity, but pointers in the nested
                //class.  This means we need to add a .getBytes() call to extract the pointer
                //Also if we ever have grandchild nested classes they will need to change where the builder is defined.
                if (activityRequiringCast && (cur.queryActivity() == activityRequiringCast))
                {
                    HqlExprArray args;
                    args.append(*LINK(src));
                    src.setown(translator.bindTranslatedFunctionCall(getBytesFromBuilderId, args));
                }
            }

            startCtx.addAssign(bound, src);
        }
    }
    associateCursors(declareCtx, startCtx, childLocalisation);
}

bool ParentExtract::canEvaluate(IHqlExpression * expr)
{
    if (!buildctx)
        return false;
    return translator.canEvaluateInContext(*buildctx, expr);
}

void ParentExtract::endChildActivity()
{
}

bool ParentExtract::requiresOnStart() const
{
    switch (type)
    {
    case PETchild:
    case PETremote:
    case PETloop:
    case PETlibrary:
        return true;
    case PETnested:
    case PETcallback:
        return container->requiresOnStart();
    default:
        throwUnexpected();
    }
}


bool ParentExtract::insideChildQuery() const
{
    switch (type)
    {
    case PETchild:
        return true;
    }
    return container->insideChildQuery();
}

bool ParentExtract::areGraphResultsAccessible(IHqlExpression * searchGraphId) const
{
    if (graphId == searchGraphId)
        return true;

    switch (type)
    {
    case PETloop:
        return container->areGraphResultsAccessible(searchGraphId);
    }
    return false;
}


void ParentExtract::endCreateExtract(CHqlBoundExpr & boundExtract)
{
    //NB: This can be called more than once - so need to be careful about any processing that
    //is done in here.
    childSerialization->setBuilder(NULL);
    CHqlBoundExpr boundSize;
    translator.getRecordSize(*buildctx, serialization->queryDataset(), boundSize);
    boundExtract.length.set(boundSize.expr);
    boundExtract.expr.set(boundBuilder.expr);
}

void ParentExtract::endUseExtract(BuildCtx & ctx)
{
    childSerialization->finalize();

    if (declarectx)
    {
        unsigned minSize = serialization->getTotalMinimumSize();
        OwnedHqlExpr finalFixedSize = getSizetConstant(minSize);
        if (serialization->isFixedSize())
        {
            Owned<ITypeInfo> nullRow = makeRowType(queryNullRecord()->getType());
            Owned<ITypeInfo> declType = makeModifier(makeWrapperModifier(LINK(nullRow)), typemod_builder, LINK(finalFixedSize));
            OwnedHqlExpr fixedSizeBuilder = createVariable(extractName, LINK(declType));
            declarectx->addDeclare(fixedSizeBuilder);
        }
        else
        {
            declarectx->addDeclare(boundBuilder.expr, finalFixedSize);
        }
    }

    //Not so sure about the lifetime of this.  If the extract was saved for a later occasion (e.g., prefetch project) then may be destroyed too soon.
    if (canDestroyExtract)
        translator.doGenerateMetaDestruct(ctx, serialization->queryDataset(), childSerialization->queryRecord());
}

void ParentExtract::buildAssign(IHqlExpression * serializedTarget, IHqlExpression * originalValue)
{
    translator.buildAssign(*buildctx, serializedTarget, originalValue);
}


//This function converts an expression evaluated in the parent to an expression that can be evaluated in the child.
//Note, all extracts from all parent colocal activities are cloned into the child activity, so same expression is used to access them in the child
void ParentExtract::ensureAccessible(BuildCtx & ctx, IHqlExpression * expr, const CHqlBoundExpr & bound, CHqlBoundExpr & tgt, IHqlExpression * colocal)
{
    //MORE: Need to know whether it is 
    //a) a member variable b) a serialization member or c) local and needs seerializing.
    //so when then do
    //a) add colcocal-> or create local references to the variables.
    //b) don't need to do anything
    //c) need to serialize and return value from that.
    //Probably need to tag global declares with an attribute - e.g., typemod_member
    //If activity is not colocal then always need to serialize
    if (colocal)
    {
        //I wish I didn't have to do this.  wrapper classes have a cast added when they are converted to
        //expressions.  I should probably look at removing it, but until that happens they need to be
        //stripped so the member variables get matched correctly.
        IHqlExpression * boundExpr = bound.expr;
        if (boundExpr->getOperator() == no_implicitcast)
        {
            IHqlExpression * uncast = boundExpr->queryChild(0);
            if (hasModifier(uncast->queryType(), typemod_wrapper) && 
                (queryUnqualifiedType(boundExpr->queryType()) == queryUnqualifiedType(uncast->queryType())))
                boundExpr = uncast;
        }

        //If already bound into a serialization then no need to do anything.
        ITypeInfo * serializedModifier = queryModifier(boundExpr->queryType(), typemod_serialized);
        if (serializedModifier)
        {
            //If serialized rebind the expression, rather than using the bound, otherwise temporary variables used for sizes
            //of fields can get lost (see extractbug.hql) for an example
            IHqlExpression * originalMapped = static_cast<IHqlExpression *>(serializedModifier->queryModifierExtra());
            translator.buildAnyExpr(ctx, originalMapped, tgt);
            tgt.expr.setown(addExpressionModifier(tgt.expr, typemod_serialized, originalMapped));
            return;
        }

        //Special case where this extract is used for member functions within the same class
        if (hasModifier(boundExpr->queryType(), typemod_member))
        {
            if (colocal == colocalSameClassPreserveExpr)
            {
                tgt.set(bound);
                return;
            }

            //Need to create new (reference) members in the new class, and code to assign them in
            //the setParent() call.
            tgt.isAll.setown(addMemberSelector(bound.isAll, colocal));
            tgt.count.setown(addMemberSelector(bound.count, colocal));
            tgt.length.setown(addMemberSelector(bound.length, colocal));
            tgt.expr.setown(addMemberSelector(boundExpr, colocal));
            return;
        }
    }

    //nonLocal child and local variables to a colocal child.
    assertex(childSerialization);
    OwnedHqlExpr value = bound.getTranslatedExpr();
    OwnedHqlExpr mapped = childSerialization->ensureSerialized(value, canDestroyExtract ? colocal : NULL, false);
    translator.buildAnyExpr(ctx, mapped, tgt);
    //add a serialized modifier to the type so we can track expressions already serialized, the extra saves the original mappiing
    tgt.expr.setown(addExpressionModifier(tgt.expr, typemod_serialized, mapped));
    ctx.associateExpr(expr, tgt);
}

void ParentExtract::addSerializedExpression(IHqlExpression * value, ITypeInfo * type)
{
    OwnedHqlExpr mapped = childSerialization->addSerializedValue(value, type, NULL, false);
}

AliasKind ParentExtract::evaluateExpression(BuildCtx & ctx, IHqlExpression * value, CHqlBoundExpr & tgt, IHqlExpression * colocal, bool evaluateLocally)
{
    CHqlBoundExpr bound;
    AliasKind kind;
    if (buildctx)
        kind = translator.doBuildAliasValue(*buildctx, value, bound, NULL);
    else
        kind = container->evaluateExpression(ctx, value, bound, evaluateLocally);
    if (kind != NotFoundAlias)
        ensureAccessible(ctx, value, bound, tgt, colocal);
    if (!colocal)
        return RuntimeAlias;
    return kind;
}

void ParentExtract::gatherActiveRows(BuildCtx & ctx)
{
    //Collect a list of cursors together... NB these are in reverse order..
    CursorArray activeRows;
    RowAssociationIterator iter(ctx);
    ForEach(iter)
    {
        BoundRow & cur = iter.get();
        if ((cur.querySide() != no_self) && !cur.isBuilder())
        {
            bool ok = true;
            IHqlExpression * represents = cur.queryDataset();
            switch(represents->getOperator())
            {
            case no_null:
                ok = !represents->hasAttribute(clearAtom);           // Don't serialize rows used as default clear rows
                break;
            case no_anon:
                ok = !represents->hasAttribute(selfAtom);
                break;
            default:
                if (cur.isResultAlias())
                    ok = false;

                //MORE: This should only be done if the child query etc. actually references the datarow.
                //ideally from a colocal activity.
#if 0
            //Theoretically the following is true.  However it can mean that for the cost of serializing an extra 4 bytes here and
            //there you end up serializing several hundred in some other situations.
            //So on balance, worth including them even if strictly unnecessary.
                if (represents->isDatarow() && !isAlwaysActiveRow(represents))
                    ok = false;
#endif
                break;
            }
            if (ok)
                activeRows.append(OLINK(cur));
        }
    }

    //NB: Serialization needs to be added processed first in the child scope
    if (serialization)
    {
        childSerialization = (SerializationRow *)serialization->clone(boundExtract.expr);
        colocalBoundCursors.append(*childSerialization);
        nonlocalBoundCursors.append(*LINK(childSerialization));
    }

    //MORE: Should possibly create two sets of cursors one if children are colocal, other if children aren't
    //so colocal cursors can be used wherever possible.  Would change following to localisation != GraphNonLocal
    //and remove else
    if (localisation == GraphCoLocal || localisation == GraphCoNonLocal)
    {
        OwnedHqlExpr colocal = createQuoted("colocal", makeVoidType());
        ForEachItemInRev(i, activeRows)
        {
            BoundRow & cur = activeRows.item(i);
            IHqlExpression * bound = cur.queryBound();
            OwnedHqlExpr colocalBound = addMemberSelector(bound, colocal);

            BoundRow * newRow = NULL;
            if (cur.isSerialization() || cur.queryDataset()->getOperator() == no_pseudods)
            {
                //an extract/serialization.  Same cursor is added to all colocal children.
                newRow = LINK(&cur);
            }
            else if (cur.queryAliasExpansion() || cur.isNonLocal())
            {
                //A cursor who's pointer has already been serialized into an extract
                //NB: Alias expansions get rebound when they are bound into the context.
                newRow = LINK(&cur);
            }
            else if (!cur.isBinary())
            {
                //CSV and xml datasets need their elements serialized into the parent extract
                newRow = new NonLocalIndirectRow(cur, NULL, childSerialization);
            }
            else if (serialization)
            {
                //A cursor active in the current scope => add it to the extract, and create an alias in the
                //child contexts.
                //Need to add these fields to the extract record, and do the assignments at the point of call
                Owned<ITypeInfo> fieldType = makeRowReferenceType(cur.queryDataset());
                if (hasOutOfLineModifier(bound->queryType()))
                    fieldType.setown(makeAttributeModifier(LINK(fieldType), getLinkCountedAttr()));
                OwnedHqlExpr expandedAlias = serialization->createField(NULL, fieldType);
                OwnedHqlExpr castSource = createValue(no_implicitcast, LINK(fieldType), LINK(bound));
                OwnedHqlExpr translated = createTranslated(castSource);
                translator.buildAssign(ctx, expandedAlias, translated);

                newRow = new BoundAliasRow(cur, NULL, expandedAlias);
                newRow->setInherited(true);
            }
            else
            {
                //A row defined in a activity class instance, being used from a nested class.
                //E.g., row used to clear a value.  Should be possible to recreate in the nested class
                if (cur.getKind() != AssocRow)
                    throwUnexpected();
            }
            if (newRow)
                colocalBoundCursors.append(* newRow);

        }

        //May also need to associate any counters in scope.  May be more than one!
        //Possibly other things like filepositions, .....  How do I recognise them all?
        //Use a dynamic offset map to represent whatever is serialized.
        //MORE: This may actually mean we want to bind these non-row contexts on demand - but then it is harder to share.
    }
    
    if (localisation != GraphCoLocal)
    {
        ForEachItemInRev(i, activeRows)
        {
            BoundRow & cur = activeRows.item(i);
            nonlocalBoundCursors.append(*new NonLocalIndirectRow(cur, NULL, childSerialization));
        }
    }
}


//----------------------------------------------------------------------------

ParentExtract * HqlCppTranslator::createExtractBuilder(BuildCtx & ctx, PEtype type, IHqlExpression * graphId, GraphLocalisation localisation, bool doDeclare)
{
    ParentExtract * extractor = new ParentExtract(*this, type, graphId, localisation, queryEvalContext(ctx));
    extractor->beginCreateExtract(ctx, doDeclare);
    return extractor;
}


ParentExtract * HqlCppTranslator::createExtractBuilder(BuildCtx & ctx, PEtype type, IHqlExpression * graphId, IHqlExpression * expr, bool doDeclare)
{
    if (isAlwaysCoLocal())
        return createExtractBuilder(ctx, type, graphId, GraphCoLocal, doDeclare);
    bool isInsideChildQuery = (type == PETchild) || insideChildQuery(ctx);
    return createExtractBuilder(ctx, type, graphId, getGraphLocalisation(expr, isInsideChildQuery), doDeclare);
}


//----------------------------------------------------------------------------

AliasKind HqlCppTranslator::buildExprInCorrectContext(BuildCtx & ctx, IHqlExpression * expr, CHqlBoundExpr & tgt, bool evaluateLocally)
{
    EvalContext * instance = queryEvalContext(ctx);
    if (instance)
        return instance->evaluateExpression(ctx, expr, tgt, evaluateLocally);
    if (ctx.getMatchExpr(expr, tgt))
        return RuntimeAlias;
    return NotFoundAlias;
}


//---------------------------------------------------------------------------

void CtxCollection::createFunctionStructure(HqlCppTranslator & translator, BuildCtx & ctx, bool canEvaluate, const char * serializeFunc)
{
    clonectx.set(ctx);
    clonectx.addGroup();

    if (serializeFunc)
    {
        BuildCtx condctx(ctx);
        OwnedHqlExpr cond = createVariable("in", makeBoolType());
        IHqlStmt * stmt = condctx.addFilter(cond);
        deserializectx.setown(new BuildCtx(condctx));
        condctx.selectElse(stmt);
        evalctx.setown(new BuildCtx(condctx));

        //virtual void serializeCreateContext(MemoryBuffer & out)
        serializectx.setown(new BuildCtx(declarectx));
        StringBuffer s;
        s.append("virtual void ").append(serializeFunc).append("(MemoryBuffer & out)");
        serializectx->addQuotedCompoundOpt(s.str());
    } else if (canEvaluate)
    {
        evalctx.setown(new BuildCtx(ctx));
        evalctx->addGroup();
    }
    childctx.set(ctx);
}

//---------------------------------------------------------------------------

EvalContext::EvalContext(HqlCppTranslator & _translator, ParentExtract * _parentExtract, EvalContext * _parent) 
: HqlExprAssociation(classMarkerExpr), translator(_translator)
{
    parent = _parent;
    parentExtract.set(_parentExtract);
}

IHqlExpression * EvalContext::createGraphLookup(unique_id_t id, bool isChild)
{
    assertex(parent);
    return parent->createGraphLookup(id, isChild);
}

bool EvalContext::needToEvaluateLocally(BuildCtx & ctx, IHqlExpression * expr)
{
    return mustEvaluateInContext(ctx, expr);
}

bool EvalContext::evaluateInParent(BuildCtx & ctx, IHqlExpression * expr, bool hasOnStart)
{
    if (!parent) 
        return false;

    if (parent->isLibraryContext())
        return (expr->getOperator() == no_libraryinput);

    switch (expr->getOperator())
    {
    case no_libraryinput:
    case no_rows:
    case no_filepos:
    case no_file_logicalname:
    case no_counter:
    case no_variable:       // this really should happen
        return true;        // would have been bound if found
    case no_id2blob:
        return !queryBlobHelper(ctx, expr->queryChild(0));
    case no_loopcounter:
        return !translator.isCurrentActiveGraph(ctx, expr->queryChild(0));
    case no_getgraphresult:
        if (expr->hasAttribute(externalAtom))
        {
            IHqlExpression * resultInstance = queryAttributeChild(expr, externalAtom, 0);
            return !ctx.queryMatchExpr(resultInstance);
        }
        return !translator.isCurrentActiveGraph(ctx, expr->queryChild(1));
    case no_getresult:
    case no_workunit_dataset:
        return translator.needToSerializeToSlave(expr);
    case no_failcode:
    case no_failmessage:
    case no_fail:
        return !ctx.queryMatchExpr(activeFailureMarkerExpr);
    case no_xmltext:
    case no_xmlunicode:
    case no_xmlproject:
        return !ctx.queryMatchExpr(xmlColumnProviderMarkerExpr);
    case no_matched:
    case no_matchtext:
    case no_matchunicode:
    case no_matchlength:
    case no_matchposition:
    case no_matchrow:
    case no_matchutf8:
        return !ctx.queryMatchExpr(activeNlpMarkerExpr);
    case no_matchattr:
        return !ctx.queryMatchExpr(activeNlpMarkerExpr) && !ctx.queryMatchExpr(activeProductionMarkerExpr);
    }

    //Some function of the above => eveluate where we are...
    if (isContextDependent(expr))
        return false;

    if (isIndependentOfScope(expr))
        return true;//isColocal();

    //If can evaluate in parent's start context then always worth doing there.
    if (parent->isRowInvariant(expr))
        return true;

    if (parentExtract && parentExtract->canEvaluate(expr))
    {
        if (!isColocal() || !hasOnStart)
            return true;
        if (translator.queryEvaluateCoLocalRowInvariantInExtract())
            return true;
        return false;
    }

    return false;
}

bool EvalContext::insideChildQuery() const
{
    if (parentExtract)
        return parentExtract->insideChildQuery();
    if (parent)
        return parent->insideChildQuery();
    return false;
}


bool EvalContext::requiresOnStart() const
{
    if (parentExtract)
        return parentExtract->requiresOnStart();
    if (parent)
        return parent->requiresOnStart();
    return false;
}


ActivityInstance * EvalContext::queryActivity()
{
    if (parent)
        return parent->queryActivity();
    return NULL;
}


bool EvalContext::areGraphResultsAccessible(IHqlExpression * searchGraphId) const
{
    if (parentExtract)
        return parentExtract->areGraphResultsAccessible(searchGraphId);
    if (parent)
        return parent->areGraphResultsAccessible(searchGraphId);
    return false;
}



IHqlExpression * HqlCppTranslator::doCreateGraphLookup(BuildCtx & declarectx, BuildCtx & resolvectx, unique_id_t id, const char * activity, bool isChild)
{
    StringBuffer s, var;
    if (isChild)
    {
        appendUniqueId(var.append("child"), id);

        s.clear().append("Owned<IThorChildGraph> ").append(var).append(";");
        declarectx.addQuoted(s);

        s.clear().append(var).append(".setown(ctx->resolveChildQuery(").append(id).append(",").append(activity).append("));");
        resolvectx.addQuoted(s);
    }
    else
    {
        //NB: resolveLocalQuery (unlike children) can't link otherwise you get a circular dependency.
        appendUniqueId(var.append("graph"), id);
        OwnedHqlExpr memberExpr = createQuoted(var, makeBoolType());
        if (declarectx.queryMatchExpr(memberExpr))
            return memberExpr.getClear();
    
        s.clear().append("IEclGraphResults * ").append(var).append(";");
        declarectx.addQuoted(s);
        declarectx.associateExpr(memberExpr, memberExpr);

        s.clear().append(var).append(" = ctx->resolveLocalQuery(").append(id).append(");");
        resolvectx.addQuoted(s);
    }

    return createQuoted(var, makeBoolType());
}


//---------------------------------------------------------------------------

ClassEvalContext::ClassEvalContext(HqlCppTranslator & _translator, ParentExtract * _parentExtract, EvalContext * _parent, BuildCtx & createctx, BuildCtx & startctx) 
: EvalContext(_translator, _parentExtract, _parent), onCreate(createctx), onStart(startctx)
{
}

IHqlExpression * ClassEvalContext::cloneExprInClass(CtxCollection & ctxs, IHqlExpression * expr)
{
    if (!expr)
        return NULL;

    if (expr->getOperator() != no_pselect)
        return LINK(expr);

    LinkedHqlExpr value = expr;
    Linked<ITypeInfo> type = expr->queryType();
    assertex(hasModifier(type, typemod_member));
    if (isTypePassedByAddress(type))
    {
        value.setown(getPointer(value));
        type.setown(value->getType());
    }
    else
    {
        if (hasModifier(type, typemod_wrapper))
        {
            type.setown(removeModifier(type, typemod_wrapper));
            value.setown(createValue(no_implicitcast, LINK(type), LINK(value)));
        }
    }

    if (!hasModifier(type, typemod_member))
        type.setown(makeModifier(value->getType(), typemod_member));

    OwnedHqlExpr decl = ctxs.declarectx.getTempDeclare(type, NULL);
    ctxs.clonectx.addAssign(decl, value);
    return LINK(decl);
}


void ClassEvalContext::cloneAliasInClass(CtxCollection & ctxs, const CHqlBoundExpr & bound, CHqlBoundExpr & tgt)
{
//  assertex(!bound.count);
    ensureHelpersExist();
    tgt.isAll.setown(cloneExprInClass(ctxs, bound.isAll));
    tgt.count.setown(cloneExprInClass(ctxs, bound.count));
    tgt.length.setown(cloneExprInClass(ctxs, bound.length));
    tgt.expr.setown(cloneExprInClass(ctxs, bound.expr));
}

void ClassEvalContext::createMemberAlias(CtxCollection & ctxs, BuildCtx & ctx, IHqlExpression * value, CHqlBoundExpr & tgt)
{
    if (ctxs.declarectx.getMatchExpr(value, tgt))
        return;

    //Should never be called for a nested class - that should be done in the context.
    assertex(ctxs.evalctx != NULL);
    translator.expandAliases(*ctxs.evalctx, value, NULL);

    IAtom * serializeForm = internalAtom; // The format of serialized expressions in memory must match the internal serialization format
    CHqlBoundTarget tempTarget;
    if (translator.needToSerializeToSlave(value))
    {
        translator.buildTempExpr(*ctxs.evalctx, ctxs.declarectx, tempTarget, value, FormatNatural, false);
        ensureSerialized(ctxs, tempTarget, serializeForm);
    }
    else
    {
        translator.buildTempExpr(ctxs.clonectx, ctxs.declarectx, tempTarget, value, FormatNatural, false);
    }

    tgt.setFromTarget(tempTarget);
    ctxs.declarectx.associateExpr(value, tgt);
}

void ClassEvalContext::doCallNestedHelpers(const char * member, const char * activity, IHqlStmt * onCreateStmt, IHqlStmt * onStartStmt)
{
    StringBuffer s;

    onCreate.childctx.addQuoted(s.clear().append(member).append(".onCreate(ctx, ").append(activity).append(");"));

    BuildCtx childctx(onStart.childctx);
    if (!requiresOnStart())
        childctx.addConditionalGroup(onStartStmt);
    childctx.addQuoted(s.clear().append(member).append(".onStart();"));
}


void ClassEvalContext::ensureSerialized(CtxCollection & ctxs, const CHqlBoundTarget & target, IAtom * serializeForm)
{
    if (ctxs.serializectx)
        translator.ensureSerialized(target, *ctxs.serializectx, *ctxs.deserializectx, "*in", "out", serializeForm);
}

AliasKind ClassEvalContext::evaluateExpression(BuildCtx & ctx, IHqlExpression * value, CHqlBoundExpr & tgt, bool evaluateLocally)
{
    if (onStart.declarectx.getMatchExpr(value, tgt))
    {
        if (onCreate.declarectx.queryMatchExpr(value))
            return CreateTimeAlias;
        return StartTimeAlias;
    }
    if (ctx.getMatchExpr(value, tgt))
        return RuntimeAlias;

    ///If contains a nasty like counter/matchtext then evaluate here for the moment...
    if (!needToEvaluateLocally(ctx, value))
    {
        if (evaluateInParent(ctx, value, (onStart.evalctx != NULL)))
        {
            if (!parentExtract)
                throwError(HQLERR_NoParentExtract);
            CHqlBoundExpr bound;
            AliasKind kind = parentExtract->evaluateExpression(ctx, value, bound, colocalMember, evaluateLocally);

            switch (kind)
            {
            case RuntimeAlias:
                tgt.set(bound);
                break;
            case CreateTimeAlias:
                cloneAliasInClass(onCreate, bound, tgt);
                onCreate.declarectx.associateExpr(value, tgt);
                break;
            case StartTimeAlias:
                cloneAliasInClass(onStart, bound, tgt);
                onStart.declarectx.associateExpr(value, tgt);
                break;
            }
            return kind;
        }

        if (!evaluateLocally)
            return NotFoundAlias;

        if (!isContextDependentExceptGraph(value))
        {
            if (!isContextDependent(value) && !containsActiveDataset(value) && value->isIndependentOfScope())
            {
                createMemberAlias(onCreate, ctx, value, tgt);
                return CreateTimeAlias;
            }

            bool evaluateInOnStart = false;
            if (isRowInvariant(value) && onStart.evalctx)
            {
                evaluateInOnStart = true;
                
                if (isGraphDependent(value))
                {
                    //Need to find out which graph it is dependent on, and check that isn't defined locally
                    HqlExprCopyArray graphs;
                    gatherGraphReferences(graphs, value, true);
                    if (graphs.ordinality() != 0)
                        evaluateInOnStart = false;
                }
            }

            if (evaluateInOnStart)
            {
                translator.traceExpression("alias", value);
                createMemberAlias(onStart, ctx, value, tgt);
                return StartTimeAlias;
            }
        }
    }

    if (!evaluateLocally)
        return NotFoundAlias;

    translator.expandAliases(ctx, value, NULL);
    translator.buildTempExpr(ctx, value, tgt);
    return RuntimeAlias;
}


void ClassEvalContext::tempCompatiablityEnsureSerialized(const CHqlBoundTarget & tgt)
{
    IAtom * serializeForm = internalAtom; // The format of serialized expressions in memory must match the internal serialization format
    ensureSerialized(onCreate, tgt, serializeForm);
}

bool ClassEvalContext::isRowInvariant(IHqlExpression * expr)
{
    return translator.canEvaluateInContext(onStart.declarectx, expr);
}

//If this is called on something that is shared, then need to ensure it will be valid for all subsequent calls
//therefore, we can't test whether it is inside an onCreate function since that may not be true the first time, but may be subsequent times.
//and we need to generate the code where we guarantee it will have been executed before (almost) everything else.
//if not independent, then it can't be shared - so generate it after the code to extract temporary values
bool ClassEvalContext::getInvariantMemberContext(BuildCtx * ctx, BuildCtx * * declarectx, BuildCtx * * initctx, bool isIndependentMaybeShared, bool invariantEachStart)
{
    if (invariantEachStart)
    {
        if (declarectx)
            *declarectx = &onStart.declarectx;
        if (initctx && (!ctx || isIndependentMaybeShared || !ctx->queryMatchExpr(insideOnStartMarker)))
        {
            ensureHelpersExist();
            if (isIndependentMaybeShared)
                *initctx = &onStart.clonectx;           // before anything else happens.
            else
                *initctx = &onStart.childctx;           // after global variables have been read or de-serialized.
        }
    }
    else
    {
        if (declarectx)
            *declarectx = &onCreate.declarectx;
        if (initctx && (!ctx || isIndependentMaybeShared || !ctx->queryMatchExpr(insideOnCreateMarker)))
        {
            ensureHelpersExist();
            if (isIndependentMaybeShared)
                *initctx = &onCreate.clonectx;          // before anything else happens.
            else
                *initctx = &onCreate.childctx;          // after global variables have been read or de-serialized.
        }
    }
    return true;
}

//---------------------------------------------------------------------------

GlobalClassEvalContext::GlobalClassEvalContext(HqlCppTranslator & _translator, ParentExtract * _parentExtract, EvalContext * _parent, BuildCtx & createctx, BuildCtx & startctx) 
: ClassEvalContext(_translator, _parentExtract, _parent, createctx, startctx)
{
}

void GlobalClassEvalContext::ensureHelpersExist()
{
}

void GlobalClassEvalContext::callNestedHelpers(const char * member, IHqlStmt * onCreateStmt, IHqlStmt * onStartStmt)
{
    doCallNestedHelpers(member, "this", onCreateStmt, onStartStmt);
}

//---------------------------------------------------------------------------

ActivityEvalContext::ActivityEvalContext(HqlCppTranslator & _translator, ActivityInstance * _activity, ParentExtract * _parentExtract, EvalContext * _parent, IHqlExpression * _colocal, BuildCtx & createctx, BuildCtx & startctx) 
: ClassEvalContext(_translator, _parentExtract, _parent, createctx, startctx)
{
    activity = _activity;
    colocalMember.set(_colocal);
}

IHqlExpression * ActivityEvalContext::createGraphLookup(unique_id_t id, bool isChild)
{
    return translator.doCreateGraphLookup(onCreate.declarectx, onCreate.childctx, id, "this", isChild);
}

void ActivityEvalContext::ensureHelpersExist()
{
}


void ActivityEvalContext::callNestedHelpers(const char * member, IHqlStmt * onCreateStmt, IHqlStmt * onStartStmt)
{
    doCallNestedHelpers(member, "this", onCreateStmt, onStartStmt);
}

ActivityInstance * ActivityEvalContext::queryActivity()
{
    return activity;
}

//---------------------------------------------------------------------------

NestedEvalContext::NestedEvalContext(HqlCppTranslator & _translator, const char * _memberName, ParentExtract * _parentExtract, EvalContext * _parent, IHqlExpression * _colocal, BuildCtx & createctx, BuildCtx & startctx)
: ClassEvalContext(_translator, _parentExtract, _parent, createctx, startctx)
{
    colocalMember.set(_colocal);
    helpersExist = false;
    memberName.set(_memberName);
}

IHqlExpression * NestedEvalContext::createGraphLookup(unique_id_t id, bool isChild)
{
    ensureHelpersExist();
    return translator.doCreateGraphLookup(onCreate.declarectx, onCreate.childctx, id, "activity", isChild);
}

void NestedEvalContext::ensureHelpersExist()
{
    assertex(parentExtract);
    if (!helpersExist)
    {
        if (parent)
            parent->ensureHelpersExist();

        StringBuffer s;
        ActivityInstance * rootActivity = queryActivity();

        //void onStart(ICodeContext * _ctx, <ActivityClass> * _activity)
        BuildCtx oncreatectx(onCreate.declarectx);
        IHqlStmt * onCreateStmt  = oncreatectx.addQuotedCompound(s.clear().append("inline void onCreate(ICodeContext * _ctx, ").append(rootActivity->className).append(" * _activity)"));
        oncreatectx.addQuoted(s.clear().append("activity = _activity;"));
        oncreatectx.addQuotedLiteral("ctx = _ctx;");

        onCreate.declarectx.addQuotedLiteral("ICodeContext * ctx;");
        onCreate.declarectx.addQuoted(s.clear().append(rootActivity->className).append(" * activity;"));
        onCreate.declarectx.associateExpr(codeContextMarkerExpr, codeContextMarkerExpr);

        onCreate.createFunctionStructure(translator, oncreatectx, false, NULL);

        //void onStart(const byte * parentExtract)
        BuildCtx onstartctx(onStart.declarectx);
        IHqlStmt * onStartStmt = onstartctx.addQuotedCompound("inline void onStart()");
        if (parentExtract)
            parentExtract->beginChildActivity(onStart.declarectx, onstartctx, GraphCoLocal, colocalMember, true, parentExtract->canSerializeFields(), NULL);

        onstartctx.associateExpr(insideOnStartMarker, NULL);
        onStart.createFunctionStructure(translator, onstartctx, false, NULL);

        parent->callNestedHelpers(memberName, onCreateStmt, onStartStmt);
        if (!requiresOnStart())
            onStartStmt->finishedFramework();

        helpersExist = true;
    }
}


void NestedEvalContext::initContext()
{
    if (requiresOnStart())
        ensureHelpersExist();
    else if (parentExtract && parentExtract->canSerializeFields())
    {
        ensureHelpersExist();
        parentExtract->associateCursors(onStart.declarectx, onStart.declarectx, GraphCoLocal);
    }
}

bool NestedEvalContext::evaluateInParent(BuildCtx & ctx, IHqlExpression * expr, bool hasOnStart) 
{ 
    //This is a bit ugly.  Latter condition is to cope with group aggregate callbacks
    return parent->isRowInvariant(expr) || parentExtract->canEvaluate(expr);
}

void NestedEvalContext::callNestedHelpers(const char * member, IHqlStmt * onCreateStmt, IHqlStmt * onStartStmt)
{
    doCallNestedHelpers(member, "activity", onCreateStmt, onStartStmt);
}

//---------------------------------------------------------------------------


MemberEvalContext::MemberEvalContext(HqlCppTranslator & _translator, ParentExtract * _parentExtract, EvalContext * _parent, BuildCtx & _ctx)
: EvalContext(_translator, _parentExtract, _parent), ctx(_ctx)
{
    assertex(parent);
    colocalMember.set(colocalSameClassPreserveExpr);
}

void MemberEvalContext::callNestedHelpers(const char * member, IHqlStmt * onCreateStmt, IHqlStmt * onStartStmt)
{
    parent->callNestedHelpers(member, onCreateStmt, onStartStmt);
}

IHqlExpression * MemberEvalContext::createGraphLookup(unique_id_t id, bool isChild)
{
    return parent->createGraphLookup(id, isChild);
}

AliasKind MemberEvalContext::evaluateExpression(BuildCtx & ctx, IHqlExpression * value, CHqlBoundExpr & tgt, bool evaluateLocally)
{
    return parentExtract->evaluateExpression(ctx, value, tgt, colocalMember, evaluateLocally);
}


bool MemberEvalContext::isRowInvariant(IHqlExpression * expr)
{
    return translator.canEvaluateInContext(ctx, expr);
}


void MemberEvalContext::ensureHelpersExist()
{
    parent->ensureHelpersExist();
}


void MemberEvalContext::initContext()
{
    parentExtract->associateCursors(ctx, ctx, GraphCoLocal);
}


bool MemberEvalContext::getInvariantMemberContext(BuildCtx * ctx, BuildCtx * * declarectx, BuildCtx * * initctx, bool isIndependentMaybeShared, bool invariantEachStart)
{
    return parent->getInvariantMemberContext(ctx, declarectx, initctx, isIndependentMaybeShared, invariantEachStart);
}

void MemberEvalContext::tempCompatiablityEnsureSerialized(const CHqlBoundTarget & tgt)
{
    parent->tempCompatiablityEnsureSerialized(tgt);
}


//---------------------------------------------------------------------------

void HqlCppTranslator::ensureContextAvailable(BuildCtx & ctx)
{
    EvalContext * instance = queryEvalContext(ctx);
    if (instance)
        instance->ensureContextAvailable();
}

/*
The following is the structure that we are going to generate in the C++ for an activity

<<xx>> marks a context that is preserved


virtual void onCreate(ICodeContext * _ctx, IHThorArg * _colocal, MemoryBuffer * in) {
    colocal = (c2*)_colocal;
    ctx = _ctx;
    <<onCreate.clonectx>>                   
        // clone values from colocal-> into local variables.
    if (in) {
        //deserialize any values from
        <<onCreate.deserializectx>>     
    }
    else {
        //evaluate any query-invariant values.
        <<onCreate.evalctx>>
    }
    <<onCreate.childctx>>                   
        // create graph lookups.
    nestedObject.onCreate(ctx, this);
}
virtual void serializeCreateContext(MemoryBuffer & out) {
    //serialize any query-invariant values
}
virtual void onStart(const byte * pe) {
    extractVar = (byte *)pe;
    <<onStart.clonectx>>
    <<onStart.evalctx>>
    <<onStart.childctx>>
    nestedObject.onStart(pe);
}

NestedClass:
    void onCreate(ICodeContext * _ctx, cH * _activity) {
        activity = _activity;
        ctx = _ctx;
        <<onCreate.clonectx>>
        <<onCreate.childctx>>
        //clone values from parent class into this class
    }
    virtual void onStart(const byte * pe) {
        extractVar = (byte *)pe;        //This means that all values retrieved from the extract have the same format in the nested class
        <<onStart.clonectx>>
        <<onStart.childctx>>
    }



virtual void onCreate(ICodeContext * _ctx, IHThorArg * _colocal) {
    colocal = (c2*)_colocal;
    ctx = _ctx;
    <<onCreate.clonectx>>                   
        // clone values from colocal-> into local variables.
    <<onCreate.evalctx>>
        // evaluate graph invariant expressions
        //local graph lookups
    commonCreate();
}
inline void commonCreate()
    <<onCreate.childctx>>
        // create child graph lookups.
        // nestedObject.onCreate(ctx, this);
}
virtual void serializeContext(MemoryBuffer & out) {
    //serialize any query-invariant values
}
virtual void onStart(const byte * pe) {
    extractVar = (byte *)pe;
    <<onStart.clonectx>>
    <<onStart.evalctx>>
    <<onStart.childctx>>
}

inline void commonOnStart()
{
    nestedObject.onStart(pe);
}

virtual void onCreateStart(ICodeContext * _ctx, MemoryBuffer * in, const byte * pe)
{
    ctx = _ctx;
    colocal = NULL;
    <<onCreate.deserialize>>
    commonCreate();
    extractVar = (byte *)pe;
    commonOnStart();
}

xxx.clonectx - The point where members are cloned from colocal parent activities and parent classes.
xxx.evalctx - The point where anything that needs to be evaluated is placed.

Notes on onCreate/onStart
a) onStart needs to be created/called for child activities (and their nested classes) in order to bind the variables correctly.
b) onCreate needs to be called for nested classes in order to access grand parent colocal extracts.
c) If a function is created it needs to be called from parent (which stops us using compoundOpt unless the base class contains a default implementation)
d) We don't want to call onCReate() + onStart all the time because it generates lots of extra code.

=>

1) For any child class (activity or nested) we always generate + call them even if they don't have any extra code.
2) For nested classes of top level activities the functions are generated and called when something is added that needs it.

Notes
* Serialization + evaluation of expressions
  a) if an activity is not executed remotely there is no need to serialize on Create
     => serialize remote roxie activites
     => serialize remote thor child activities.
     => optionally serialize all root thor activities depending on whether Jake makes use of it.
     => add a debug flag to force serialization for the purpose of testing.
  b) table-invariant values.
     => evaluated once in the onCreate() of the activity.  Not retrieved from a parent activity.
  c) row invariant values
     => if a value can be evaluated in a parent extract context then it is evaluated there, and
        then cloned/serialized into the current context.
  d) since row invariant values are calulated in the parent context there is nothing to be gained from serializing the
     start context. => Remove the code + methods from the eclagent helper definition.
  e) Nested class values.
     These are always evaluated in the parent context, and then cloned into the current class.


- The extracts from all parent colocal activities are cloned into the child activity
- All members from a parent activity are copied into members in the current activity + accessed from there.
  [This is partly to simplify the access expressions from colocal children]

Evaluation of expressions:
o Table-invariant
  - Do in the onCreate of the activity.  
  - Could also serialize from parent to child queries, so only evaluated once.
  = Trade off between re-eveluation and cloning/extra data serialized to child query.

o Other expressions
  - Each expression has a minimal level it can be evaluated at.
  - main decision is whether to evaluate in the parent's build ctx, or in a local onStart()
  = If there is no onStart, then always evaluate in parent build ctx.
  = If there is an onStart, then you are trading off the cost of serializing the values required to compute
    the expression, and possibly multiple evaluations, against the cost of serializing the expression.
  - For non-local always evaluate in the parent build ctx.
  - For colocal, it is debatable - I'll add as a flag.

o non-local

o COUNTER, MATCHTEXT, XMLTEXT, FAILCODE/FAILMESSAGE
  - These are all context dependent in different ways.
  - They all need to be evaluated in the correct context (not too deep or too shallow)
  - Simplest is to evaluate any expression that contains them at the deepest level, and actual expressions evaluated
    at higher levels.

Activity
    Invariant:  evaluate in onCreateContext


Nested
    Evaluate in parent.  If 



General


  * see codegen.txt for more details.
  */
