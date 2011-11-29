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
#ifndef __HQLCSE_IPP_
#define __HQLCSE_IPP_

#include "hqlcpp.hpp"
#include "hqlcpp.ipp"

#include "hqltrans.ipp"

//#define NEW_CSE_PROCESSING

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
    CseSpotter();

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
};

class ConjunctionTransformer : public NewHqlTransformer
{
public:
    ConjunctionTransformer();

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
};

#ifdef NEW_CSE_PROCESSING
class CseScopeInfo : public NewTransformInfo
{
public:
    CseScopeInfo(IHqlExpression * expr) : NewTransformInfo(expr) 
    { 
        firstParent = NULL; 
        commonLocation = NULL; 
        seq = 0; 
        hasSharedParent = false;
        calcedCommonLocation = false;
        isUnconditional = false;
    }

    void addParent(IHqlExpression * nextParent)
    {
//      if (firstParent)
//      {
//          if (nextParent)
//          {
//              if ((nextParent != firstParent) && !parents.contains(nextParent))
                    parents.append(nextParent);
//          }
//          else
//              firstParent = NULL;
//      }
    }

    HqlExprArray aliasesToDefine;
    // use a pointer for the first match to avoid reallocation for unshared items
    IHqlExpression * firstParent;
    CseScopeInfo * commonLocation;
    PointerArrayOf<IHqlExpression> parents;
    unsigned seq;
    bool hasSharedParent;
    bool calcedCommonLocation;
    bool isUnconditional;
};


class CseScopeTransformer : public NewHqlTransformer
{
public:
    CseScopeTransformer();

    virtual void analyseExpr(IHqlExpression * expr);
            bool attachCSEs(IHqlExpression * root);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    CseScopeInfo * calcCommonLocation(CseScopeInfo * extra);
    IHqlExpression * findAliasLocation(CseScopeInfo * extra);
    CseScopeInfo * findCommonPath(CseScopeInfo * left, CseScopeInfo * right);
    CseScopeInfo * selectParent(CseScopeInfo * info);

    inline CseScopeInfo * queryExtra(IHqlExpression * expr) { return (CseScopeInfo*)NewHqlTransformer::queryTransformExtra(expr); }

protected:
    CIArrayOf<CseScopeInfo> allCSEs;
    IHqlExpression * activeParent;
    unsigned seq;
    unsigned conditionDepth;
};
#else
//NB: path isn't linked for efficiency.
typedef ICopyArrayOf<IHqlExpression> PathArray;

class CSEentry : public CInterface
{
public:
    CSEentry(IHqlExpression * _value, PathArray & _path);

    void findCommonPath(PathArray & otherPath);

protected:
    void ensurePathValid();

public:
    HqlExprAttr         value;
    PathArray           path;
    CIArrayOf<CSEentry> dependsOn;
};

    
class CseScopeInfo : public NewTransformInfo
{
public:
    CseScopeInfo(IHqlExpression * expr) : NewTransformInfo(expr) { }

    Owned<CSEentry>     cseUse;
    CIArrayOf<CSEentry> cseDefine;
};


class CseScopeTransformer : public NewHqlTransformer
{
public:
    CseScopeTransformer();

    virtual void analyseExpr(IHqlExpression * expr);
            bool attachCSEs(IHqlExpression * root);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

protected:
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    inline CseScopeInfo * queryExtra(IHqlExpression * expr) { return (CseScopeInfo*)NewHqlTransformer::queryTransformExtra(expr); }

protected:
    CIArrayOf<CSEentry> allCSEs;
    PathArray path;
    CIArrayOf<CSEentry> activeCSE;
};
#endif


IHqlExpression * spotScalarCSE(IHqlExpression * expr, IHqlExpression * limit = NULL);
void spotScalarCSE(SharedHqlExpr & expr, SharedHqlExpr & associated, IHqlExpression * limit, IHqlExpression * invariantSelector);
void spotScalarCSE(HqlExprArray & exprs, HqlExprArray & associated, IHqlExpression * limit, IHqlExpression * invariantSelector);

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
