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
