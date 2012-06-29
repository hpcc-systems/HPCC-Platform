/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

class ConditionalContextInfo : public ConditionalTransformInfo
{
public:
    ConditionalContextInfo(IHqlExpression * expr) : ConditionalTransformInfo(expr)
    {
        firstParent = NULL;
        commonLocation = NULL;
        moveTo = NULL;
        firstAnnotatedExpr = NULL;
        seq = 0;
        hasSharedParent = false;
        calcedCommonLocation = false;
        isCandidateExpr = false;
        createAlias = false;
    }

    void addExtraParent(ConditionalContextInfo * nextParent, bool noteManyUnconditionalParents)
    {
        hasSharedParent = true;
        //We only care about parents of conditional expressions
        if (!isUnconditional() || noteManyUnconditionalParents || canSurroundWithAlias(original))
        {
            extraParents.append(nextParent);
        }
    }

    void calcInheritedGuards();
    bool isCandidateThatMoves() const;
    bool usedOnMultiplePaths() const;

    inline bool hasDefinitions() const { return definitions.ordinality() != 0; }
    inline bool changesLocation() const { return moveTo != this; }
    inline void setFirstParent(ConditionalContextInfo * parent) { firstParent = parent; }

public:
    // use a pointer for the first match to avoid reallocation for unshared items
    ConditionalContextInfo * firstParent;  // NULL for the root item
    // if this expression is conditional, then which expression contains all uses of it?
    ConditionalContextInfo * commonLocation;
    // where will the definition be moved to.  NULL if it will not be moved.
    ConditionalContextInfo * moveTo;
    // First annotated expression seen (if any)
    IHqlExpression * firstAnnotatedExpr;
    //Which guarded expressions need to be created at this point?
    HqlExprArray definitions;
    //If conditional, a list of all the other places it is used.
    PointerArrayOf<ConditionalContextInfo> extraParents;
    // guards for all expressions that this expression contains
    Owned<CHqlExprMultiGuard> guards;
    //All guards that need to be passed on the container/parent expressions [excludes definitions]
    Owned<CHqlExprMultiGuard> inheritedGuards;
    //A sequence number use to ensure it is efficient to find a common parent
    unsigned seq;
    bool hasSharedParent;
    bool calcedCommonLocation;
    //Is this a candidate for evaluating in a single location with guards?
    bool isCandidateExpr;
    bool createAlias;
};

//---------------------------------------------------------------------------------------------------------------------

class ConditionalContextTransformer : public ConditionalHqlTransformer
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
    virtual void transformCandidate(ConditionalContextInfo * candidate) = 0;
    ConditionalContextTransformer(HqlTransformerInfo & info, bool _alwaysEvaluateGuardedTogether);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    bool hasUnconditionalCandidate() const;

    void analyseConditionalParents(IHqlExpression * expr);
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
    CIArrayOf<ConditionalContextInfo> candidates;
    ConditionalContextInfo * activeParent;
    OwnedHqlExpr rootExpr;
    CopyCIArrayOf<CHqlExprMultiGuard> childGuards;
    CopyCIArrayOf<ConditionalContextInfo> insertLocations;
    unsigned seq;
    bool alwaysEvaluateGuardedTogether;
    bool noteManyUnconditionalParents;
    bool hasConditionalCandidate;
    bool createRootGraph;
};

IHqlExpression * mapExternalToInternalResults(IHqlExpression * expr, IHqlExpression * graph);

#endif
