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
#ifndef __HQLCSE_IPP_
#define __HQLCSE_IPP_

#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

#include "hqltrans.ipp"

//---------------------------------------------------------------------------

class CseSpotterInfo : public NewTransformInfo
{
public:
    CseSpotterInfo(IHqlExpression * expr);

    bool worthAliasing();
    bool worthAliasingOnOwn();
    bool useInverseForAlias();
    bool isAliased()                        { return alreadyAliased || treatAsAliased; }

    unsigned            numRefs;
    unsigned            numAssociatedRefs;
    CseSpotterInfo *    inverse;
    IHqlExpression *    annotatedExpr;
    bool                alreadyAliased : 1;
    bool                canAlias : 1;
    bool                dontTransform : 1;
    bool                dontTransformSelector : 1;
    bool                treatAsAliased : 1;
};


class CseSpotter : public NewHqlTransformer
{
    typedef NewHqlTransformer PARENT;
public:
    CseSpotter(bool _spotCseInIfDatasetConditions);

    void analyseAssociated(IHqlExpression * expr, unsigned pass);
    bool foundCandidates() const                            { return spottedCandidate; }
    bool createdNewAliases() const { return createdAlias; }
    void setInvariantSelector(IHqlExpression * _invariantSelector) { invariantSelector = _invariantSelector; }
    void stopTransformation(IHqlExpression * expr);

protected:
    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    inline bool containsPotentialCSE(IHqlExpression * expr);
    virtual bool checkPotentialCSE(IHqlExpression * expr, CseSpotterInfo * extra);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);
    virtual IHqlExpression * transform(IHqlExpression * expr);

    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr);
    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);

    inline CseSpotterInfo * queryBodyExtra(IHqlExpression * expr) { return (CseSpotterInfo*)PARENT::queryTransformExtra(expr->queryBody()); }

    IHqlExpression * doCreateTransformed(IHqlExpression * expr);
    IHqlExpression * createAliasOwn(IHqlExpression * expr, CseSpotterInfo * extra);

protected:
    IHqlExpression * invariantSelector;
    bool canAlias;
    bool isAssociated;
    bool spottedCandidate;
    bool createLocalAliases;
    bool createdAlias;
    bool spotCseInIfDatasetConditions;
};

class ConjunctionTransformer : public NewHqlTransformer
{
public:
    ConjunctionTransformer();

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
};


/*
 * This class is used to track the information for a single IHqlExpression - its parent expressions and parent alises.
 * This is then used to work out the "best" place in the expression graph to evaluate the aliases.
 * It can be seen as converts a declarative lazy-evaluated graph to an imperative graph - with nodes to force aliases
 * to be evaluated.
 */
class CseScopeInfo : public NewTransformInfo
{
public:
    CseScopeInfo(IHqlExpression * expr) : NewTransformInfo(expr) 
    { 
    }

    void addParent(CseScopeInfo * nextParent)
    {
        parents.append(nextParent);
//REVISIT: Consider removing duplicate parents with the following code - but may have implications for processing conditionals
//      if (firstParent)
//      {
//          if (nextParent)
//          {
//              if ((nextParent != firstParent) && !parents.contains(nextParent))
//                  parents.append(nextParent);
//          }
//          else
//              firstParent = NULL;
//      }
    }

    void addChildAlias(CseScopeInfo * alias)
    {
        if (!childAliases.contains(*alias))
            childAliases.append(*alias);
    }

    void inheritAliases(CseScopeInfo * alias)
    {
        if (!alias || (alias == inheritedAliases))
            return;
        if (inheritedAliases)
        {
            //REVISIT: The following call may not be needed if the order of the aliases isn't significant.
            //Check later if it reduces the processing.
            expandInheritedAliases();
            alias->cloneAliases(childAliases);
        }
        else
            inheritedAliases = alias;
    }

    void addParentAlias(CseScopeInfo * alias)
    {
        if (!parentAliases.contains(*alias))
            parentAliases.append(*alias);
    }

    void cloneAliases(ICopyArrayOf<CseScopeInfo> & target) const;
    void connectChildAliases(CseScopeInfo * parent);
    void connectChildAliases() { connectChildAliases(this); }
    void expandInheritedAliases();
    CseScopeInfo * queryScopeLocation();

    bool hasMultipleParents()
    {
        ensureCalculated();
        return (commonNode != this);
    }

    //What should I take as my parent to walk up the expressions - either my direct parent, or my common ancestor
    CseScopeInfo * queryCommonNode()
    {
        ensureCalculated();
        return commonNode;
    }

    CseScopeInfo * queryCommonParent()
    {
        ensureCalculated();
        if (!commonNode || commonNode != this)
            return commonNode;
        return firstParent;
    }

    CseScopeInfo * queryEvalLocation()
    {
        ensureCalculated();
        return evalLocation;
    }

    CseScopeInfo * queryAlias()
    {
        if (isAlias)
            return this;
        CseScopeInfo * parent = queryCommonParent();
        if (!parent)
            return nullptr;
        return parent->queryAlias();
    }

    bool queryAliasMoved()
    {
        CseScopeInfo * alias = queryAlias();
        return alias && alias->hasMoved();
    }

    CseScopeInfo * queryValidPath();

    bool hasMoved()
    {
        ensureCalculated();
        return moved;
    }

private:
    inline void ensureCalculated() __attribute__((always_inline))
    {
        if (!calcedCommonLocation)
            calcCommonLocation();
    }
    void calcCommonLocation();

public:
    HqlExprArray aliasesToDefine;            // Which aliases need to be evaluated when this expression is evaluated
    CseScopeInfo * firstParent = nullptr;    // use a pointer for the first match to avoid reallocation for unshared items
    PointerArrayOf<CseScopeInfo> parents;    // list of all parent expressions - other than firstParent
    ICopyArrayOf<CseScopeInfo> childAliases; // List of child aliases - interpret in conjunction with inheritedAliases
    CseScopeInfo * inheritedAliases = nullptr;// Which expression aliases should be inherited from - saves appending to childAliases for common case.
    ICopyArrayOf<CseScopeInfo> parentAliases;// List of all aliases which this alias is used by.  (Not set for non-aliases)
    unsigned seq = 0;                        // A unique sequence number used for tracing and finding a common parent
    unsigned minDepth = UINT_MAX;            // REVISIT: Not sure this actually works!  Currently unused.  May need a shortest path pointer instead.
    bool isUnconditional = false;            // Is this expression unconditional = not currently used.
    bool isAlias = false;                    // Is this an alias?

private:
    //The following are calculated after walking the graph
    bool calcedCommonLocation = false;       // Has the derived location information been calculated for this node?
    bool moved = false;                      // Should this expression be evaluated in a different location in the graph?
    CseScopeInfo * commonNode = nullptr;     // Either self, or with multiple parents the node that contains all instances of self
    CseScopeInfo * evalLocation = nullptr;   // Where should this alias logically be evaluated
};


/*
 * This class introduces no_alias_scope nodes into the expression graph to indicate where cses (no_alias) should be evaluated.
 * It is complicated by
 * - multiple uses of the same expression.
 * - Some shared nodes not being able to have no_alias_scope added around them (e.g, no_mapto)
 * - Dependencies between aliases meaning if one alias moves any aliases it relies on may also need to.
 */
class CseScopeTransformer : public NewHqlTransformer
{
public:
    CseScopeTransformer();

    virtual void analyseExpr(IHqlExpression * expr);
            bool attachCSEs(IHqlExpression * root);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    inline CseScopeInfo * queryBodyExtra(IHqlExpression * expr) { return (CseScopeInfo*)NewHqlTransformer::queryTransformExtra(expr->queryBody()); }

protected:
    IArrayOf<CseScopeInfo> allCSEs;             // all aliases encountered - aliases are never depenedent on a later item in the list
    CseScopeInfo * activeParent = nullptr;      // current active parent - used when analysing the graph
    unsigned seq = 0;                           // current sequence number
    unsigned conditionDepth = 0;                // used to track whether the current expression is conditional
    unsigned depth = 0;                         // how deep.  REVISIT - currently flawed
};


IHqlExpression * spotScalarCSE(IHqlExpression * expr, IHqlExpression * limit, bool spotCseInIfDatasetConditions);
void spotScalarCSE(SharedHqlExpr & expr, SharedHqlExpr & associated, IHqlExpression * limit, IHqlExpression * invariantSelector, bool spotCseInIfDatasetConditions);
void spotScalarCSE(HqlExprArray & exprs, HqlExprArray & associated, IHqlExpression * limit, IHqlExpression * invariantSelector, bool spotCseInIfDatasetConditions);

//---------------------------------------------------------------------------

class TableInvariantInfo : public NewTransformInfo
{
public:
    TableInvariantInfo(IHqlExpression * expr) : NewTransformInfo(expr) { createAlias = false; couldAlias = false; cachedInvariant = false; isInvariant = false; }
    bool                createAlias;
    bool                couldAlias;
    bool                cachedInvariant;
    bool                isInvariant;
};

class TableInvariantTransformer : public NewHqlTransformer
{
public:
    TableInvariantTransformer();
    virtual void analyseExpr(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)  { return CREATE_NEWTRANSFORMINFO(TableInvariantInfo, expr); }
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    bool isInvariant(IHqlExpression * expr);
    bool isTrivialAlias(IHqlExpression * expr);
    bool isAlwaysAlias(IHqlExpression * expr);
    inline TableInvariantInfo * queryBodyExtra(IHqlExpression * expr) { return (TableInvariantInfo*)NewHqlTransformer::queryTransformExtra(expr->queryBody()); }

protected:
    bool canAlias;
};

//---------------------------------------------------------------------------

class GlobalAliasInfo : public NewTransformInfo
{
public:
    GlobalAliasInfo(IHqlExpression * expr) : NewTransformInfo(expr) { isOuter = false; numUses = 0; }
    bool                isOuter;
    unsigned            numUses;
};

class GlobalAliasTransformer : public NewHqlTransformer
{
public:
    GlobalAliasTransformer();
    virtual void analyseExpr(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr)  { return CREATE_NEWTRANSFORMINFO(GlobalAliasInfo, expr); }
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    inline GlobalAliasInfo * queryBodyExtra(IHqlExpression * expr) { return (GlobalAliasInfo*)NewHqlTransformer::queryTransformExtra(expr->queryBody()); }

    bool insideGlobal;
};

//---------------------------------------------------------------------------
IHqlExpression * spotTableInvariant(IHqlExpression * expr);
IHqlExpression * spotTableInvariantChildren(IHqlExpression * expr);
IHqlExpression * optimizeActivityAliasReferences(IHqlExpression * expr);

#endif
