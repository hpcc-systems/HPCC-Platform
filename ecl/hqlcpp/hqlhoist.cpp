/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

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

#include "hqlexpr.hpp"
#include "hqlfold.hpp"
#include "hqlhoist.hpp"
#include "hqlutil.hpp"
#include "hqlcpputil.hpp"

bool canSurroundWithAlias(IHqlExpression * expr)
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
    case no_attr_expr:
    case no_transform:
    case no_newtransform:
        return false;
    case no_alias_scope:
        return canSurroundWithAlias(expr->queryChild(0));
    default:
        return true;
    }
}

//---------------------------------------------------------------------------------------------------------------------

static HqlTransformerInfo externalToInternalResultMapperInfo("ExternalToInternalResultMapper");
class ExternalToInternalResultMapper : public NewHqlTransformer
{
public:
    ExternalToInternalResultMapper(IHqlExpression * _graph) : NewHqlTransformer(externalToInternalResultMapperInfo), graph(_graph)
    {
    }

protected:
    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        OwnedHqlExpr transformed = NewHqlTransformer::createTransformed(expr);
        if (transformed->getOperator() == no_getgraphresult)
        {
            if (hasOperand(transformed, graph))
                return removeAttribute(transformed, externalAtom);
        }
        return transformed.getClear();
    }

protected:
    IHqlExpression * graph;
};

IHqlExpression * mapExternalToInternalResults(IHqlExpression * expr, IHqlExpression * graph)
{
    ExternalToInternalResultMapper mapper(graph);
    return mapper.transformRoot(expr);
}

//---------------------------------------------------------------------------------------------------------------------

void CHqlExprMultiGuard::addGuarded(IHqlExpression * original)
{
    addGuarded(queryBoolExpr(true), original, false);
}

void CHqlExprMultiGuard::addGuarded(IHqlExpression * cond, IHqlExpression * original, bool guardContainsCandidate)
{
    guarded.append(*new CHqlExprGuard(cond, original, guardContainsCandidate));
}

void CHqlExprMultiGuard::combine(CHqlExprMultiGuard & other)
{
    //Potentially O(N^2).  If there are vast numbers of candidates this could become problematic.
    ForEachItemIn(i, other.guarded)
    {
        CHqlExprGuard & cur = other.guarded.item(i);
        combine(cur);
    }
}

void CHqlExprMultiGuard::combine(CHqlExprGuard & other)
{
    ForEachItemIn(i, guarded)
    {
        CHqlExprGuard & cur = guarded.item(i);
        if (cur.original == other.original)
        {
            //condition is now (a || b)
            OwnedHqlExpr newCond;
            if (matchesBoolean(cur.guard, true) || matchesBoolean(other.guard, true))
                newCond.set(queryBoolExpr(true));
            else
                newCond.setown(createBoolExpr(no_or, LINK(cur.guard), LINK(other.guard)));

            //MORE: Could sometimes reuse the existing guard if this was created by this node
            Owned<CHqlExprGuard> newGuard = new CHqlExprGuard(newCond, cur.original, cur.guardContainsCandidate||other.guardContainsCandidate);
            guarded.replace(*newGuard.getClear(), i);
            return;
        }
    }
    guarded.append(OLINK(other));
}

void CHqlExprMultiGuard::gatherCandidates(HqlExprCopyArray & candidates) const
{
    ForEachItemIn(i, guarded)
    {
        CHqlExprGuard & cur = guarded.item(i);
        if (!candidates.contains(*cur.original))
            candidates.append(*cur.original);
    }
}

IHqlExpression * CHqlExprMultiGuard::queryGuardCondition(IHqlExpression * original) const
{
    ForEachItemIn(i, guarded)
    {
        CHqlExprGuard & cur = guarded.item(i);
        if (cur.original == original)
            return cur.guard;
    }
    return NULL;
}

bool CHqlExprMultiGuard::guardContainsCandidate(IHqlExpression * original) const
{
    ForEachItemIn(i, guarded)
    {
        CHqlExprGuard & cur = guarded.item(i);
        if (cur.original == original)
            return cur.guardContainsCandidate;
    }
    return false;
}

void gatherCandidates(HqlExprCopyArray & candidates, CHqlExprMultiGuard * guards)
{
    if (guards)
        guards->gatherCandidates(candidates);
}


IHqlExpression * queryGuardCondition(CHqlExprMultiGuard * guards, IHqlExpression * original)
{
    if (guards)
    {
        IHqlExpression * condition = guards->queryGuardCondition(original);
        if (condition)
            return condition;
    }
    return queryBoolExpr(false);
}



//---------------------------------------------------------------------------------------------------------------------

void ConditionalContextInfo::calcInheritedGuards()
{
    if (guards && definitions.ordinality() != 0)
    {
        Owned<CHqlExprMultiGuard> newGuards = new CHqlExprMultiGuard;
        ForEachItemIn(i, guards->guarded)
        {
            CHqlExprGuard & cur = guards->guarded.item(i);
            if (!definitions.contains(*cur.original))
                newGuards->guarded.append(OLINK(cur));
        }
        if (newGuards->guarded.ordinality())
            inheritedGuards.setown(newGuards.getClear());
    }
    else
        inheritedGuards.set(guards);
}


bool ConditionalContextInfo::isCandidateThatMoves() const
{
    if (!isCandidateExpr)
        return false;

    if (!changesLocation())
        return false;

    return true;
}

bool ConditionalContextInfo::usedOnMultiplePaths() const
{
    if (extraParents.ordinality() != 0)
        return true;
    if (firstParent)
        return firstParent->usedOnMultiplePaths();
    return false;
}

//---------------------------------------------------------------------------------------------------------------------


ConditionalContextTransformer::ConditionalContextTransformer(HqlTransformerInfo & info, bool _alwaysEvaluateGuardedTogether)
: ConditionalHqlTransformer(info, CTFnoteor|CTFnoteand|CTFnotemap|CTFnoteifall),
    alwaysEvaluateGuardedTogether(_alwaysEvaluateGuardedTogether)
{
    seq = 0;
    hasConditionalCandidate = false;
    noteManyUnconditionalParents = false;//true;
    createRootGraph = false;
    rootExpr.setown(createValue(no_null, makeVoidType(), createAttribute(_root_Atom)));
    activeParent = NULL;  // cannot call queryBodyExtra(rootExpr) since in constructor
}


ANewTransformInfo * ConditionalContextTransformer::createTransformInfo(IHqlExpression * expr)
{
    return CREATE_NEWTRANSFORMINFO(ConditionalContextInfo, expr);
}


void ConditionalContextTransformer::analyseExpr(IHqlExpression * expr)
{
    switch (pass)
    {
    case PassFindConditions:
        ConditionalHqlTransformer::analyseExpr(expr);
        return;
    case PassFindParents:
        analyseConditionalParents(expr);
        return;
    case PassGatherGuards:
        analyseGatherGuards(expr);
        break;
    default:
        ConditionalHqlTransformer::analyseExpr(expr);
        break;
    }
}

bool ConditionalContextTransformer::hasSingleConditionalCandidate() const
{
    if (candidates.ordinality() != 1)
        return false;
    ConditionalContextInfo & candidate = candidates.item(0);
    if (candidate.isUnconditional())
        return false;
    return !candidate.usedOnMultiplePaths();
}


// ---- pass 1 --------

void ConditionalContextTransformer::noteCandidate(ConditionalContextInfo * extra)
{
    extra->isCandidateExpr = true;
    //MORE: If is used unconditionally enough???? then mark as such.  (don't mark as unconditional)
    if (!extra->isUnconditional())
        hasConditionalCandidate = true;
    candidates.append(*LINK(extra));
}


void ConditionalContextTransformer::analyseConditionalParents(IHqlExpression * expr)
{
    ConditionalContextInfo * extra = queryBodyExtra(expr);
    if (extra->seq)
    {
        extra->addExtraParent(activeParent, noteManyUnconditionalParents);
        return;
    }

    if (!extra->firstAnnotatedExpr && (expr != expr->queryBody()))
        extra->firstAnnotatedExpr = expr;

    extra->setFirstParent(activeParent);
    extra->seq = ++seq;

    {
        ConditionalContextInfo * savedParent = activeParent;
        activeParent = extra;
        ConditionalHqlTransformer::analyseExpr(expr);
        activeParent = savedParent;
    }
}

// ---- pass 1b --------

void ConditionalContextTransformer::addDefinition(ConditionalContextInfo * location, ConditionalContextInfo * candidate)
{
    location->definitions.append(*LINK(candidate->original));
    candidate->moveTo = location;

    if (!insertLocations.contains(*location))
        insertLocations.append(*location);
}

void ConditionalContextTransformer::removeDefinition(ConditionalContextInfo * location, ConditionalContextInfo * candidate)
{
    location->definitions.zap(*candidate->original);
    candidate->moveTo = NULL;
}

bool ConditionalContextTransformer::findCommonLocations()
{
    bool changed = false;
    ForEachItemIn(idx, candidates)
    {
        ConditionalContextInfo& cur = candidates.item(idx);
        ConditionalContextInfo * candidateLocation = findCandidateLocation(&cur);
        //A null alias location forces an expression to stay where it is
        if (candidateLocation)
        {
            addDefinition(candidateLocation, &cur);
            changed = true;
        }
    }
    return changed;
}

//Find a single location to evaluate all the child items in
bool ConditionalContextTransformer::findSingleCommonLocation()
{
    ConditionalContextInfo * bestLocation = NULL;
    PointerArrayOf<ConditionalContextInfo> matched;
    ForEachItemIn(idx, candidates)
    {
        ConditionalContextInfo& cur = candidates.item(idx);
        ConditionalContextInfo * candidateLocation = findCandidateLocation(&cur);
        if (candidateLocation)
        {
            matched.append(&cur);
            if (!bestLocation)
                bestLocation = candidateLocation;
            else
                bestLocation = findCommonPath(bestLocation, candidateLocation);
        }
    }

    while (bestLocation)
    {
        if (canSurroundWithAlias(bestLocation->original))
            break;
        bestLocation = selectParent(bestLocation);
    }

    //If best location is NULL then all expressions are marked as staying where they are.
    if (!bestLocation)
        return false;

    ForEachItemIn(i, matched)
        addDefinition(bestLocation, matched.item(i));
    return true;
}


//Find a single location to evaluate all the child items in
bool ConditionalContextTransformer::associateCandidatesWithRoot()
{
    ConditionalContextInfo * rootLocation = queryBodyExtra(rootExpr);
    PointerArrayOf<ConditionalContextInfo> matched;
    ForEachItemIn(idx, candidates)
    {
        ConditionalContextInfo& cur = candidates.item(idx);
        ConditionalContextInfo * candidateLocation = findCandidateLocation(&cur);
        if (candidateLocation)
            addDefinition(rootLocation, &cur);
    }

    return rootLocation->hasDefinitions();
}


//What location in the tree should extra be evaluated?
//If it is unconditional, then if first use is unconditional, eval here, else in the root.
//If this expression is conditional and has a single use,
//   then evaluate here or globally - if always done globally.
//if conditional, and multiple uses, then find the common evaluation condition of the parents.
ConditionalContextInfo * ConditionalContextTransformer::calcCommonLocation(ConditionalContextInfo * extra)
{
    if (extra->calcedCommonLocation)
        return extra->commonLocation;

    if (!extra->firstParent)
    {
        dbgassertex(extra == queryBodyExtra(rootExpr));
        return extra;
    }

    ConditionalContextInfo * commonLocation = extra;
    if (extra->isUnconditional() && !alwaysEvaluateGuardedTogether)
    {
        if (!extra->isFirstUseUnconditional())
            commonLocation = queryBodyExtra(rootExpr);
    }
    else
    {
        commonLocation = calcCommonLocation(extra->firstParent);
        if (extra->hasSharedParent)
        {
            ForEachItemIn(i, extra->extraParents)
            {
                ConditionalContextInfo * curParent = extra->extraParents.item(i);
                ConditionalContextInfo * nextExtra = calcCommonLocation(curParent);

                //MORE: What should be done if some expressions can be guarded, and others are marked as unmovable?
                commonLocation = findCommonPath(commonLocation, nextExtra);
                if (!commonLocation)
                    break;
            }
        }
        else
        {
            if (commonLocation == extra->firstParent)
                commonLocation = extra;
        }

        //MORE commonLocation == NULL if you never want to move this - e.g., if parent is a choose/which/rejected/....
    }
    extra->calcedCommonLocation = true;
    extra->commonLocation = commonLocation;
    return commonLocation;
}

ConditionalContextInfo * ConditionalContextTransformer::findCandidateLocation(ConditionalContextInfo * extra)
{
    ConditionalContextInfo * best = calcCommonLocation(extra);
    loop
    {
        if (!best)
            return NULL;
        IHqlExpression * bestLocation = best->original;
        if (canSurroundWithAlias(bestLocation))
            return best;
        best = selectParent(best);
    }
}


ConditionalContextInfo * ConditionalContextTransformer::selectParent(ConditionalContextInfo * info)
{
    if (info->hasSharedParent)
        return calcCommonLocation(info);
    return info->firstParent;
}

ConditionalContextInfo * ConditionalContextTransformer::findCommonPath(ConditionalContextInfo * left, ConditionalContextInfo * right)
{
    loop
    {
        if (!left || !right)
            return NULL;
        if (left == right)
            return left;

        //Ensure if there is a child and a parent that the child's parent is selected
        if (left->seq > right->seq)
            left = selectParent(left);
        else
            right = selectParent(right);
    }
}


// ---- pass 2 --------

//Recursively walk the tree calculating guards, and gathering a list of guards for child expressions
//Must be called after the common locations have been calculated
void ConditionalContextTransformer::analyseGatherGuards(IHqlExpression * expr)
{
    ConditionalContextInfo * extra = queryBodyExtra(expr);
    if (!alreadyVisited(extra))
    {
        unsigned prevGuards = childGuards.ordinality();

        doAnalyseExpr(expr);

        extra->guards.setown(calcGuard(extra, prevGuards));
        extra->calcInheritedGuards();
        childGuards.trunc(prevGuards);
    }

    if (extra->inheritedGuards)
        childGuards.append(*extra->inheritedGuards);
}


//Calculate guards for all candidates - even those that will be defined here.
CHqlExprMultiGuard * ConditionalContextTransformer::calcGuard(ConditionalContextInfo * cur, unsigned firstGuard)
{
    bool needToAddCandidate = cur->isCandidateThatMoves();
    unsigned maxGuard = childGuards.ordinality();
    if ((firstGuard == maxGuard) && !needToAddCandidate)
        return NULL;

    IHqlExpression * original = cur->original;
    node_operator op = original->getOperator();
    switch (op)
    {
    case no_if:
        return createIfGuard(cur);
    case no_case:
    case no_map:
        return createCaseMapGuard(cur, op);
    case no_or:
    case no_and:
        return createAndOrGuard(cur);
    case no_which:
    case no_rejected:
    case no_choose:
        //MORE!!!
        break;
    }

    //Get the union of all guards present for the child expressions
    if ((firstGuard == maxGuard-1) && !needToAddCandidate)
        return LINK(&childGuards.item(firstGuard));

    Owned<CHqlExprMultiGuard> newGuard = new CHqlExprMultiGuard;
    for (unsigned i=firstGuard; i < maxGuard; i++)
        newGuard->combine(childGuards.item(i));
    if (needToAddCandidate)
        newGuard->addGuarded(original);
    return newGuard.getClear();
}

CHqlExprMultiGuard * ConditionalContextTransformer::createIfGuard(IHqlExpression * ifCond, CHqlExprMultiGuard * condGuard, CHqlExprMultiGuard * trueGuard, CHqlExprMultiGuard * falseGuard)
{
    //If you want to common up the conditions between the child query and the parent code you might achieve
    //it by forcing the condition into an alias.  E.g.,
    //queryBodyExtra(ifCond)->createAlias = true;

    Owned<CHqlExprMultiGuard> newGuards = new CHqlExprMultiGuard;
    HqlExprCopyArray candidates;
    gatherCandidates(candidates, trueGuard);
    gatherCandidates(candidates, falseGuard);
    ForEachItemIn(i, candidates)
    {
        IHqlExpression * candidate = &candidates.item(i);
        IHqlExpression * trueCond = queryGuardCondition(trueGuard, candidate);
        IHqlExpression * falseCond = queryGuardCondition(falseGuard, candidate);
        OwnedHqlExpr newCond;
        if (trueCond != falseCond)
            newCond.setown(createValue(no_if, makeBoolType(), LINK(ifCond), LINK(trueCond), LINK(falseCond)));
        else
            newCond.set(trueCond);
        newGuards->addGuarded(newCond, candidate, condGuard != NULL);
    }

    if (condGuard)
        newGuards->combine(*condGuard);
    return newGuards.getClear();
}

CHqlExprMultiGuard * ConditionalContextTransformer::createIfGuard(ConditionalContextInfo * cur)
{
    IHqlExpression * original = cur->original;
    IHqlExpression * ifCond = original->queryChild(0);
    CHqlExprMultiGuard * condGuard = queryGuards(ifCond);
    CHqlExprMultiGuard * trueGuard = queryGuards(original->queryChild(1));
    CHqlExprMultiGuard * falseGuard = queryGuards(original->queryChild(2));
    if (!trueGuard && !falseGuard && !cur->isCandidateThatMoves())
        return LINK(condGuard);

    Owned<CHqlExprMultiGuard> newGuards = createIfGuard(ifCond, condGuard, trueGuard, falseGuard);
    if (cur->isCandidateThatMoves())
        newGuards->addGuarded(original);
    return newGuards.getClear();
}

CHqlExprMultiGuard * ConditionalContextTransformer::createCaseMapGuard(ConditionalContextInfo * cur, node_operator op)
{
    IHqlExpression * original = cur->original;
    IHqlExpression * testExpr = (op == no_case) ? original->queryChild(0) : NULL;

    //MAP(k1=>v1,k2=>v2...,vn) is the same as IF(k1,v1,IF(k2,v2,...vn))
    //So walk the MAP operator in reverse applying the guard conditions.
    //Use queryLastNonAttribute() instead of max-1 to eventually cope with attributes
    IHqlExpression * defaultExpr = queryLastNonAttribute(original);
    Linked<CHqlExprMultiGuard> prevGuard = queryGuards(defaultExpr);
    bool createdNewGuard = false;
    unsigned first = (op == no_case) ? 1 : 0;
    unsigned max = original->numChildren();
    for (unsigned i= max-1; i-- != first; )
    {
        IHqlExpression * mapto = original->queryChild(i);
        //In the unlikely event there are attributes this ensures only maps are processed
        if (mapto->getOperator() != no_mapto)
            continue;

        IHqlExpression * testValue = mapto->queryChild(0);
        CHqlExprMultiGuard * condGuard = queryGuards(testValue);
        CHqlExprMultiGuard * trueGuard = queryGuards(mapto->queryChild(1));
        if (trueGuard || prevGuard)
        {
            OwnedHqlExpr ifCond = testExpr ? createBoolExpr(no_eq, LINK(testExpr), LINK(testValue)) : LINK(testValue);
            Owned<CHqlExprMultiGuard> newGuards = createIfGuard(ifCond, condGuard, trueGuard, prevGuard);
            prevGuard.setown(newGuards.getClear());
            createdNewGuard = true;
        }
        else
        {
            prevGuard.set(condGuard);
            assertex(!createdNewGuard);
        }
    }

    CHqlExprMultiGuard * testGuards = testExpr ? queryGuards(testExpr) : NULL;
    if (testGuards || cur->isCandidateThatMoves())
    {
        if (!createdNewGuard)
            prevGuard.setown(new CHqlExprMultiGuard(prevGuard));

        if (testGuards)
            prevGuard->combine(*testGuards);

        if (cur->isCandidateThatMoves())
            prevGuard->addGuarded(original);
    }
    return prevGuard.getClear();
}

CHqlExprMultiGuard * ConditionalContextTransformer::createAndOrGuard(ConditionalContextInfo * cur)
{
    IHqlExpression * original = cur->original;
    IHqlExpression * left = original->queryChild(0);
    CHqlExprMultiGuard * leftGuard = queryGuards(left);
    CHqlExprMultiGuard * rightGuard = queryGuards(original->queryChild(1));
    if (!rightGuard && !cur->isCandidateThatMoves())
        return LINK(leftGuard);

    node_operator op = original->getOperator();
    Owned<CHqlExprMultiGuard> newGuards = new CHqlExprMultiGuard;
    ForEachItemIn(i, rightGuard->guarded)
    {
        CHqlExprGuard & cur = rightGuard->guarded.item(i);
        OwnedHqlExpr cond;
        //(a || b):  b is evaluated if a is false.  => guard'(b,x) = !a && guard(b,x)
        //(a && b):  b is evaluated if a is true.   => guard'(b,x) = a && guard(b,x)
        if (op == no_or)
            cond.setown(createBoolExpr(no_and, getInverse(left), LINK(cur.guard)));
        else
            cond.setown(createBoolExpr(no_and, LINK(left), LINK(cur.guard)));
        newGuards->addGuarded(cond, cur.original, leftGuard != NULL);
    }

    //MORE: Is the reversal of the order going to matter?  E.g., instead of guard(a,x) || (a && guard(b,x))?
    if (leftGuard)
        newGuards->combine(*leftGuard);
    if (cur->isCandidateThatMoves())
        newGuards->addGuarded(cur->original);
    return newGuards.getClear();
}

CHqlExprMultiGuard * ConditionalContextTransformer::queryGuards(IHqlExpression * expr)
{
    if (!expr)
        return NULL;
    return queryBodyExtra(expr)->guards;
}

// ---- pass 3 --------


// ---- summary --------

bool ConditionalContextTransformer::hasUnconditionalCandidate() const
{
    ForEachItemIn(i, candidates)
    {
        if (candidates.item(i).isUnconditional())
            return true;
    }
    return false;
}

bool ConditionalContextTransformer::analyseNeedsTransform(const HqlExprArray & exprs)
{
    ConditionalContextInfo * rootInfo = queryBodyExtra(rootExpr);

    activeParent = rootInfo;
    rootInfo->setFirstUnconditional();

    // Spot conditional and unconditional expressions
    analyseArray(exprs, PassFindConditions);
    analyseArray(exprs, PassFindCandidates);
    if (candidates.ordinality() == 0)
        return false;

    // Tag expressions with their parent locations, and gather a list of candidates.
    analyseArray(exprs, PassFindParents);

    // Work out the locations conditional expressions need to be evaluated in
    if (createRootGraph)
        associateCandidatesWithRoot();
    else if (!alwaysEvaluateGuardedTogether)
        findCommonLocations();
    else
        findSingleCommonLocation();

    if (hasConditionalCandidate)
    {
        // Calculate the guard conditions for each condition
        analyseArray(exprs, PassGatherGuards);
        rootInfo->guards.setown(calcGuard(rootInfo, 0));
        childGuards.kill();

        // Any guard conditions that are always true should become unconditional if context is unconditional
        analyseArray(exprs, 3);
    }

    return hasConditionalCandidate || alwaysEvaluateGuardedTogether;
}

// --------------------------------------------------------------------------------------------------------------------

static IHqlExpression * createConditionalExpr(IHqlExpression * condition, IHqlExpression * expr, IHqlExpression * elseExpr)
{
    if (matchesBoolean(condition, true))
        return LINK(expr);
    if (matchesBoolean(condition, false))
        return LINK(elseExpr);

    //MORE: This code is currently disabled.
    //The intent is to ensure that in the condition IF (a || b) that b is only evaluated if a is false.
    //However there are complications and problems with nested guard conditions which need independently solving.
    if (false)
    {
        switch (condition->getOperator())
        {
        case no_and:
            if (expr->queryRecord())
            {
                //Use dataset if operators to ensure that the conditions are sortcircuited.
                // if (a && b, c, -) => if (a, if (b, c, -), -)
                OwnedHqlExpr second = createConditionalExpr(condition->queryChild(1), expr, elseExpr);
                return createConditionalExpr(condition->queryChild(0), second, elseExpr);
            }
            break;
        case no_or:
            if (expr->queryRecord())
            {
                // if (a || b, c, -) => if (a, c, if (b, c, -))
                OwnedHqlExpr second = createConditionalExpr(condition->queryChild(1), expr, elseExpr);
                return createConditionalExpr(condition->queryChild(0), expr, second);
            }
            break;
        case no_if:
            {
                OwnedHqlExpr trueExpr = createConditionalExpr(condition->queryChild(1), expr, elseExpr);
                OwnedHqlExpr falseExpr = createConditionalExpr(condition->queryChild(2), expr, elseExpr);
                return createIf(LINK(condition), trueExpr.getClear(), falseExpr.getClear());
            }
        }
    }

    return createIf(LINK(condition), LINK(expr), LINK(elseExpr));
}

IHqlExpression * ConditionalContextTransformer::getGuardCondition(ConditionalContextInfo * extra, IHqlExpression * expr)
{
    CHqlExprMultiGuard * guards = extra->guards;
    if (guards)
    {
        if (expr != extra->original)
            return foldHqlExpression(guards->queryGuardCondition(expr));
    }
    return LINK(queryBoolExpr(true));
}

IHqlExpression * ConditionalContextTransformer::createGuardedDefinition(ConditionalContextInfo * extra, IHqlExpression * expr)
{
    OwnedHqlExpr guard = getGuardCondition(extra, expr);
    assertex(guard);
    return createGuardedDefinition(extra, expr, guard);
}

IHqlExpression * ConditionalContextTransformer::createGuardedDefinition(ConditionalContextInfo * extra, IHqlExpression * expr, IHqlExpression * condition)
{
    if (matchesBoolean(condition, true))
        return LINK(expr);

    OwnedHqlExpr guard = transform(condition);
    OwnedHqlExpr defaultExpr = createNullExpr(expr);
    return createConditionalExpr(guard, expr, defaultExpr);
}

// --------------------------------------------------------------------------------------------------------------------

IHqlExpression * ConditionalContextTransformer::createTransformed(IHqlExpression * expr)
{
    OwnedHqlExpr transformed = ConditionalHqlTransformer::createTransformed(expr);
    updateOrphanedSelectors(transformed, expr);
    switch (transformed->getOperator())
    {
    case no_compound_indexread:
    case no_compound_indexcount:
    case no_compound_indexaggregate:
    case no_compound_indexgroupaggregate:
    case no_compound_indexnormalize:
        if (!queryPhysicalRootTable(transformed))
            return LINK(transformed->queryChild(0));
        break;
    }

    if (queryBodyExtra(expr)->createAlias)
    {
        if (expr == expr->queryBody(true))
            transformed.setown(createAlias(transformed, NULL));
    }
    return transformed.getClear();
}

void ConditionalContextTransformer::transformAllCandidates()
{
    //Process definitions in the order they were found - since this is a toplogical dependency ordering
    ForEachItemIn(i, candidates)
        transformCandidate(&candidates.item(i));
}

void ConditionalContextTransformer::transformAll(HqlExprArray & exprs, bool forceRootFirst)
{
    transformAllCandidates();
    OwnedHqlExpr rootDefinitions = createDefinitions(queryBodyExtra(rootExpr));
    HqlExprArray transformed;
    if (rootDefinitions && forceRootFirst)
        transformed.append(*rootDefinitions.getClear());

    ForEachItemIn(i, exprs)
    {
        IHqlExpression * cur = &exprs.item(i);
        OwnedHqlExpr mapped = transformRoot(cur);

        //Add the child query etc. before the first expression that changes.
        if (rootDefinitions && (mapped != cur))
            transformed.append(*rootDefinitions.getClear());

        transformed.append(*mapped.getClear());
    }
    assertex(!rootDefinitions);
    exprs.swapWith(transformed);
}
