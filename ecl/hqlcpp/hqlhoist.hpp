/*##############################################################################

    Copyright (C) 2012 HPCC Systems®.

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
#ifndef __HQLHOIST_HPP_
#define __HQLHOIST_HPP_

#include "hqltrans.ipp"

bool canSurroundWithAlias(IHqlExpression * expr);

//This class represents the guard/protection condition for a single candiate expression
class CHqlExprGuard : public CInterface
{
public:
    CHqlExprGuard(IHqlExpression * _guard, IHqlExpression * _original, bool _guardContainsCandidate)
        : guard(_guard), original(_original), guardContainsCandidate(_guardContainsCandidate)
    {
    }

    bool guardContainsCandidate;
    LinkedHqlExpr guard;
    LinkedHqlExpr original;
};

//This class represents a list of the guards for each of the guarded expressions used within the owning expression.
class CHqlExprMultiGuard : public CInterface
{
public:
    CHqlExprMultiGuard() {}
    CHqlExprMultiGuard(const CHqlExprMultiGuard * other)
    {
        if (other)
        {
            CloneArray(guarded, other->guarded);
        }
    }

    void addGuarded(IHqlExpression * original);
    void addGuarded(IHqlExpression * cond, IHqlExpression * original, bool guardContainsCandidate);

    void combine(CHqlExprMultiGuard & other);
    void combine(CHqlExprGuard & other);
    void gatherCandidates(HqlExprCopyArray & candidiates) const;
    bool guardContainsCandidate(IHqlExpression * original) const;
    IHqlExpression * queryGuardCondition(IHqlExpression * original) const;

public:
    CIArrayOf<CHqlExprGuard> guarded;
};

//---------------------------------------------------------------------------------------------------------------------

class ReverseGraphTransformInfo : public ConditionalTransformInfo
{
public:
    ReverseGraphTransformInfo(IHqlExpression * expr) : ConditionalTransformInfo(expr)
    {
        firstParent = NULL;
        seq = 0;
        hasSharedParent = false;
    }

    void addExtraParent(ReverseGraphTransformInfo * nextParent, bool noteManyUnconditionalParents)
    {
        hasSharedParent = true;
        //We only care about parents of conditional expressions
        if (!isUnconditional() || noteManyUnconditionalParents || canSurroundWithAlias(original))
        {
            extraParents.append(nextParent);
        }
    }

    bool usedOnMultiplePaths() const;
    void getAllParents(PointerArrayOf<ReverseGraphTransformInfo> & parents);

    inline void setFirstParent(ReverseGraphTransformInfo * parent) { firstParent = parent; }

public:
    // use a pointer for the first match to avoid reallocation for unshared items
    ReverseGraphTransformInfo * firstParent;  // NULL for the root item
    // First annotated expression seen (if any)
    IHqlExpression * firstAnnotatedExpr;
    //If conditional, a list of all the other places it is used.
    PointerArrayOf<ReverseGraphTransformInfo> extraParents;
    //A sequence number use to ensure it is efficient to find a common parent
    unsigned seq;
    bool hasSharedParent;
};

//---------------------------------------------------------------------------------------------------------------------

class ReverseGraphTransformer : public ConditionalHqlTransformer
{
protected:
    ReverseGraphTransformer(HqlTransformerInfo & info, bool _noteManyUnconditionalParents);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    void analyseConditionalParents(IHqlExpression * expr);
    inline ReverseGraphTransformInfo * queryBodyExtra(IHqlExpression * expr) { return (ReverseGraphTransformInfo*)NewHqlTransformer::queryTransformExtra(expr->queryBody()); }

protected:
    ReverseGraphTransformInfo * activeParent = nullptr;
    unsigned seq = 0;
    bool noteManyUnconditionalParents;
};

//---------------------------------------------------------------------------------------------------------------------

class ConditionalContextInfo : public ReverseGraphTransformInfo
{
public:
    ConditionalContextInfo(IHqlExpression * expr) : ReverseGraphTransformInfo(expr)
    {
        commonLocation = NULL;
        moveTo = NULL;
        firstAnnotatedExpr = NULL;
        calcedCommonLocation = false;
        isCandidateExpr = false;
        createAlias = false;
    }

    void calcInheritedGuards();
    bool isCandidateThatMoves() const;

    inline bool hasDefinitions() const { return definitions.ordinality() != 0; }
    inline bool changesLocation() const { return moveTo != this; }

public:
    bool calcedCommonLocation;
    //Is this a candidate for evaluating in a single location with guards?
    bool isCandidateExpr;
    bool createAlias;

    // if this expression is conditional, then which expression contains all uses of it?
    ConditionalContextInfo * commonLocation;
    // where will the definition be moved to.  NULL if it will not be moved.
    ConditionalContextInfo * moveTo;
    // First annotated expression seen (if any)
    IHqlExpression * firstAnnotatedExpr;
    //Which guarded expressions need to be created at this point?
    HqlExprArray definitions;
    // guards for all expressions that this expression contains
    Owned<CHqlExprMultiGuard> guards;
    //All guards that need to be passed on the container/parent expressions [excludes definitions]
    Owned<CHqlExprMultiGuard> inheritedGuards;
    //A sequence number use to ensure it is efficient to find a common parent
};

//---------------------------------------------------------------------------------------------------------------------

class ConditionalContextTransformer : public ReverseGraphTransformer
{
public:
    enum { PassFindConditions,
           PassFindCandidates,
           PassFindParents,
           PassGatherGuards,
    };

public:
    virtual void analyseExpr(IHqlExpression * expr);
            bool findSingleCommonLocation();
            bool findCommonLocations();
            bool associateCandidatesWithRoot();

    bool analyseNeedsTransform(const HqlExprArray & exprs);
    bool hasSingleConditionalCandidate() const;
    void transformAll(HqlExprArray & exprs, bool forceRootFirst);

protected:
    ConditionalContextTransformer(HqlTransformerInfo & info, bool _alwaysEvaluateGuardedTogether);
    virtual void transformCandidate(ConditionalContextInfo * candidate) = 0;
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    bool hasUnconditionalCandidate() const;

    void analyseGatherGuards(IHqlExpression * expr);

    ConditionalContextInfo * calcCommonLocation(ConditionalContextInfo * extra);
    ConditionalContextInfo * findCandidateLocation(ConditionalContextInfo * extra);
    ConditionalContextInfo * findCommonPath(ConditionalContextInfo * left, ConditionalContextInfo * right);
    ConditionalContextInfo * selectParent(ConditionalContextInfo * info);

    CHqlExprMultiGuard * calcGuard(ConditionalContextInfo * cur, unsigned firstGuard);
    CHqlExprMultiGuard * createAndOrGuard(ConditionalContextInfo * cur);
    CHqlExprMultiGuard * createCaseMapGuard(ConditionalContextInfo * cur, node_operator op);
    CHqlExprMultiGuard * createIfGuard(ConditionalContextInfo * cur);
    CHqlExprMultiGuard * createIfGuard(IHqlExpression * ifCond, CHqlExprMultiGuard * condGuard, CHqlExprMultiGuard * trueGuard, CHqlExprMultiGuard * falseGuard);
    CHqlExprMultiGuard * queryGuards(IHqlExpression * expr);

    void addDefinition(ConditionalContextInfo * location, ConditionalContextInfo * candidate);
    void removeDefinition(ConditionalContextInfo * location, ConditionalContextInfo * candidate);
    virtual IHqlExpression * createDefinitions(ConditionalContextInfo * extra) = 0;

    inline ConditionalContextInfo * queryBodyExtra(IHqlExpression * expr) { return (ConditionalContextInfo*)NewHqlTransformer::queryTransformExtra(expr->queryBody()); }
    void noteCandidate(ConditionalContextInfo * extra);
    inline void noteCandidate(IHqlExpression * expr) { noteCandidate(queryBodyExtra(expr)); }

    IHqlExpression * getGuardCondition(ConditionalContextInfo * extra, IHqlExpression * expr);
    IHqlExpression * createGuardedDefinition(ConditionalContextInfo * extra, IHqlExpression * expr);
    IHqlExpression * createGuardedDefinition(ConditionalContextInfo * extra, IHqlExpression * expr, IHqlExpression * condition);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    void transformAllCandidates();

protected:
    IArrayOf<ConditionalContextInfo> candidates;
    OwnedHqlExpr rootExpr;
    CICopyArrayOf<CHqlExprMultiGuard> childGuards;
    ICopyArrayOf<ConditionalContextInfo> insertLocations;
    bool alwaysEvaluateGuardedTogether;
    bool hasConditionalCandidate;
    bool createRootGraph;
};

IHqlExpression * mapExternalToInternalResults(IHqlExpression * expr, IHqlExpression * graph);

#endif
