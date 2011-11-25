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
#ifndef __HQLTRANS_IPP_
#define __HQLTRANS_IPP_

//Optionto change all transform infos so they are allocated from an extending heap owned by the transformer
//and then thrown away in a single lump.  It would save lots of small allocations
//#define OPTIMIZE_TRANSFORM_ALLOCATOR
#ifdef OPTIMIZE_TRANSFORM_ALLOCATOR
#define CREATE_NEWTRANSFORMINFO(x, e)   new (transformerAlloc(sizeof(x))) x(e)
#define CREATE_NEWTRANSFORMINFO2(x, e, a1)  new (transformerAlloc(sizeof(x))) x(e, a1)
#else
#define CREATE_NEWTRANSFORMINFO(x, e)   new x(e)
#define CREATE_NEWTRANSFORMINFO2(x, e, a1)  new x(e, a1)
#endif

#include "hqlexpr.hpp"
#include "jset.hpp"
#ifdef USE_TBB
#include "tbb/scalable_allocator.h"
#endif

typedef MapOwnedToOwned<IHqlExpression, IHqlExpression> MapOwnedHqlToOwnedHql;

//#define TRANSFORM_STATS
//#define TRANSFORM_STATS_OPS
//#define TRANSFORM_STATS_TIME
//#define TRANSFORM_STATS_MEMORY
//#define ALLOW_TRANSFORM_TRACING

class HqlTransformStats
{
public:
    HqlTransformStats();

    void add(const HqlTransformStats & other);
    StringBuffer & getText(StringBuffer & out) const;

    void clear();
    void beginTransform();
    void endMatchTransform(IHqlExpression * expr, IHqlExpression * match);
    void endNewTransform(IHqlExpression * expr, IHqlExpression * transformed);

public:
    unsigned __int64 numAnalyse;
    unsigned __int64 numAnalyseCalls;
    unsigned __int64 numTransforms;
    unsigned __int64 numTransformsSame;
    unsigned __int64 numTransformCalls;
    unsigned __int64 numTransformCallsSame;
    unsigned __int64 numTransformSelects;
    unsigned __int64 numTransformSelectsSame;
#ifdef TRANSFORM_STATS_TIME
    unsigned __int64 totalTime;
    unsigned __int64 childTime;
    unsigned __int64 recursiveTime;
#endif
    unsigned depth;
    unsigned maxDepth;
    unsigned maxGlobalDepth;
#ifdef TRANSFORM_STATS_OPS
    unsigned transformCount[no_last_pseudoop];
#endif
    static unsigned globalDepth;
};

class HQL_API HqlTransformerInfo
{
public:
    HqlTransformerInfo(const char * _name);
    ~HqlTransformerInfo();

#ifdef TRANSFORM_STATS
    void tally(const HqlTransformStats & _other)
    {
        numInstances++;
        stats.add(_other);
    }
#endif
    bool getStatsText(StringBuffer & s) const;
    void resetStats();

public:
    const char * name;
#ifdef TRANSFORM_STATS
    unsigned numInstances;
    HqlTransformStats stats;
#endif
};

class HQL_API RecursionChecker
{
public:
    RecursionChecker()  { lockTransformMutex(); }
    ~RecursionChecker() { unlockTransformMutex(); }
    bool alreadyVisited(IHqlExpression * expr) const    { return (expr->queryTransformExtra() != NULL); }
    void setVisited(IHqlExpression * expr)              { expr->setTransformExtraUnlinked(expr); }
};


class HQL_API HqlExprMapAssociation
{
public:
    HqlExprMapAssociation()         { lockTransformMutex(); }
    ~HqlExprMapAssociation()        { unlockTransformMutex(); }

    IInterface * queryMapping(IHqlExpression * expr)            { return expr->queryTransformExtra(); }
    void setMapping(IHqlExpression * expr, IInterface * value)  { expr->setTransformExtra(value); }
};

//Here as a debugging aid to ensure an expression has been substituted correctly
extern void assertNoMatchingExpression(IHqlExpression * expr, IHqlExpression * search);

//---------------------------------------------------------------------------

extern HQL_API void setTransformTracing(bool ok);
extern HQL_API bool isTransformTracing();

class HQL_API HqlTransformerBase
{
public:
    inline HqlTransformerBase(HqlTransformerInfo & _info) : info(_info) 
    { 
        beginTime();
        lockTransformMutex(); 
#ifdef ALLOW_TRANSFORM_TRACING
        if (isTransformTracing())
            printf(">%d>%s\n", queryCurrentTransformDepth(), info.name);
#endif
    }
    inline ~HqlTransformerBase() 
    { 
#ifdef ALLOW_TRANSFORM_TRACING
        if (isTransformTracing())
            printf("<%d<%s\n", queryCurrentTransformDepth(), info.name);
#endif
        noteMemory();
        unlockTransformMutex(); 
        endTime();          // must come afetr unlockTransformMutex
#ifdef TRANSFORM_STATS
        info.tally(stats);
#endif
    }

#ifdef TRANSFORM_STATS_TIME
    void beginTime();
    void endTime();
    void noteChildTime(unsigned __int64 childTime, bool recursive)      
    { 
        stats.childTime += childTime; 
        if (recursive)
            stats.recursiveTime += childTime;
    }
#else
    inline void beginTime() {}
    inline void endTime() {}
#endif
    void noteMemory();

    virtual IHqlExpression * transform(IHqlExpression * expr) = 0;

    virtual ITypeInfo * transformType(ITypeInfo * type);

protected:
    inline IHqlExpression * completeTransform(IHqlExpression * expr, HqlExprArray & done)
    {
        if (!optimizedTransformChildren(expr, done))
            return expr->clone(done);
        return LINK(expr);
    }
    inline IHqlExpression * completeTransform(IHqlExpression * expr)
    {
        HqlExprArray done;
        return completeTransform(expr, done);
    }
    inline IHqlExpression * safeTransform(IHqlExpression * expr) { return expr ? transform(expr) : NULL; }
    inline ITypeInfo * safeTransformType(ITypeInfo * type) { return type ? transformType(type) : NULL; }

    IHqlExpression * createTransformedCall(IHqlExpression * call);
    bool optimizedTransformChildren(IHqlExpression * expr, HqlExprArray & children);
    IHqlExpression * transformAlienType(IHqlExpression * expr);
    IHqlExpression * transformField(IHqlExpression * expr);
    bool transformChildren(IHqlExpression * expr, HqlExprArray & children);
    bool transformScope(IHqlScope * newScope, IHqlScope * oldScope);
    bool transformScope(HqlExprArray & newSymbols, IHqlScope * scope);
    IHqlExpression * transformScope(IHqlExpression * expr);

protected:
    HqlTransformerInfo & info;
#ifdef TRANSFORM_STATS
    HqlTransformStats stats;
#endif
#ifdef TRANSFORM_STATS_TIME
    unsigned __int64 startTime;
    HqlTransformerBase * prev;
#endif
};

//---------------------------------------------------------------------------

// A simple raw transformer that doesn't handle selectors etc. differently.  Used for transforming the unnormalized trees.
// The queryTransformExtra() is only used to point to the transformed expression - there is no extra information
// annotations are processed separately from the bodies.
class HQL_API QuickHqlTransformer : public HqlTransformerBase
{
public:
    QuickHqlTransformer(HqlTransformerInfo & _info, IErrorReceiver * _errors);

    virtual void analyse(IHqlExpression * expr);
    virtual void doAnalyse(IHqlExpression * expr);
    virtual void doAnalyseBody(IHqlExpression * expr);
    void analyse(IHqlScope * scope);

    void analyseArray(const HqlExprArray & exprs);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr);
    virtual IHqlExpression * transform(IHqlExpression * expr);

    IHqlScope * transform(IHqlScope * scope);
    void transformArray(const HqlExprArray & in, HqlExprArray & out);

    inline IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr)
    { 
        return static_cast<IHqlExpression *>(expr->queryTransformExtra());
    }
    void setMapping(IHqlExpression * oldValue, IHqlExpression * newValue);

protected:
    IHqlExpression * doCreateTransformedScope(IHqlExpression * expr);


protected:
    IErrorReceiver * errors;
};


class HQL_API QuickExpressionReplacer : public QuickHqlTransformer
{
public:
    QuickExpressionReplacer();

    void setMapping(IHqlExpression * oldValue, IHqlExpression * newValue);
};


class HQL_API DebugDifferenceAnalyser : public QuickHqlTransformer
{
public:
    DebugDifferenceAnalyser(_ATOM _search);

    virtual void doAnalyse(IHqlExpression * expr);
    
protected:
    IHqlExpression * prev;
    _ATOM search;
};

//---------------------------------------------------------------------------

class HQL_API HqlSectionAnnotator : public QuickHqlTransformer
{
public:
    HqlSectionAnnotator(IHqlExpression * section);
    
    virtual IHqlExpression * createTransformedBody(IHqlExpression * expr);

    void noteInput(IHqlExpression * expr);
    
protected:
    OwnedHqlExpr sectionAttr;
};



#ifdef OPTIMIZE_TRANSFORM_ALLOCATOR
#ifdef new
#define SAVEDnew new
#undef new
#endif
#endif

enum
{
    NTFtransformOriginal    = 0x0001,
    NTFselectorOriginal     = 0x0002,
};

class HQL_API ANewTransformInfo : implements IInterface, public CInterface
{
public:
    ANewTransformInfo(IHqlExpression * _original);
    IMPLEMENT_IINTERFACE

    virtual IHqlExpression * queryTransformed() = 0;
    virtual IHqlExpression * queryTransformedSelector() = 0;
    virtual void setTransformed(IHqlExpression * expr) = 0;
    virtual void setTransformedSelector(IHqlExpression * expr) = 0;

    inline void setUnvisited() { lastPass = (byte)-1; }

#ifdef USE_TBB
    void *operator new(size32_t size) { return scalable_malloc(size); }
    void operator delete(void *ptr) { return scalable_free(ptr); }
#endif

#ifdef OPTIMIZE_TRANSFORM_ALLOCATOR
    void *operator new(size32_t size, void * ptr) { return ptr; }                                                   
    void *operator new(size32_t size);      // cause a link error if called.    
    void operator delete(void *ptr, void * x)   { }
    void operator delete(void *ptr) { }
#endif

public:
    //64bit these bytes will pack in after the unsigned counter in CInterface
    byte                lastPass;
    byte                flags;
    byte                spareByte1;     // some derived classes use these to minimise memory usage and alignment issues.
    byte                spareByte2;
    IHqlExpression *    original;       // original must stay live when processing
};

#ifdef OPTIMIZE_TRANSFORM_ALLOCATOR
#ifdef SAVEDnew
#define new SAVEDnew
//#undef SAVEDnew
#endif
#endif

class HQL_API NewTransformInfo : public ANewTransformInfo
{
public:
    NewTransformInfo(IHqlExpression * _original) : ANewTransformInfo(_original) {}

    inline IHqlExpression * inlineQueryTransformed()                
    { 
        if (flags & NTFtransformOriginal)
            return original;
        else
            return transformed; 
    }
    inline IHqlExpression * inlineQueryTransformedSelector()        
    {
        if (flags & NTFselectorOriginal)
            return original;
        else
            return transformedSelector; 
    }
    inline void inlineSetTransformed(IHqlExpression * expr)     
    { 
        if (expr == original)
        {
            flags |= NTFtransformOriginal;
            //transformed.clear() // never accessed if flag set, so don't bother.
        }
        else
        {
            flags &= ~NTFtransformOriginal;
            transformed.set(expr); 
        }
    }
    inline void inlineSetTransformedSelector(IHqlExpression * expr)
    { 
        if (expr == original)
        {
            flags |= NTFselectorOriginal;
            //transformedSelector.clear() // never accessed if flag set, so don't bother.
        }
        else
        {
            flags = (flags & ~NTFselectorOriginal);
            transformedSelector.set(expr); 
        }
    }

    virtual IHqlExpression * queryTransformed() { return inlineQueryTransformed(); }
    virtual IHqlExpression * queryTransformedSelector() { return inlineQueryTransformedSelector(); }
    virtual void setTransformed(IHqlExpression * expr)  { inlineSetTransformed(expr); }
    virtual void setTransformedSelector(IHqlExpression * expr) { inlineSetTransformedSelector(expr); }

private:
    HqlExprAttr         transformed;            // after spliiting etc.
    HqlExprAttr         transformedSelector;    // after spliiting etc.
};

//---------------------------------------------------------------------------

class HQL_API ANewHqlTransformer : public HqlTransformerBase
{
public:
    inline ANewHqlTransformer(HqlTransformerInfo & _info) : HqlTransformerBase(_info) {}

protected:
    //These are the virtual methods that need to be implemented by the transformer
    virtual void analyseExpr(IHqlExpression * expr) = 0;

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) = 0;
    virtual IHqlExpression * createTransformed(IHqlExpression * expr) = 0;

#ifdef OPTIMIZE_TRANSFORM_ALLOCATOR
    ANewHqlTransformer();
    ~ANewHqlTransformer();

protected:
    size32_t prevSize;
#endif
};


//---------------------------------------------------------------------------

//This class is used for applying transformations to trees that have already been normalized, or
//that need to assocaite extra information with the the expressions being transformed
//If the transformer inserts,swaps or hoists expressions then it should ensure that createTransformed()
//processes the annotations and the body at the same time - otherwise they can become separated.
class HQL_API NewHqlTransformer : public ANewHqlTransformer
{
public:
    enum { TFunconditional = 1, TFconditional = 2, TFsequential = 4 };
    enum { TCOtransformNonActive        = 0x0001,
         };

    NewHqlTransformer(HqlTransformerInfo & _info);

    void analyse(IHqlExpression * expr, unsigned pass);
    void analyseArray(const HqlExprArray & exprs, unsigned pass);

    void transformRoot(const HqlExprArray & in, HqlExprArray & out);
    IHqlExpression * transformRoot(IHqlExpression * expr) { return doTransformRootExpr(expr); }

protected:
            bool alreadyVisited(IHqlExpression * pass);
    virtual void analyseExpr(IHqlExpression * expr);
    virtual void analyseSelector(IHqlExpression * expr);
    virtual void analyseChildren(IHqlExpression * expr);
    virtual void analyseAssign(IHqlExpression * expr);

    virtual IHqlExpression * doTransformRootExpr(IHqlExpression * expr);                 // override if you need to do a different action for each root expression

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);
    virtual ANewTransformInfo * queryTransformExtra(IHqlExpression * expr);
    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed);
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual IHqlExpression * createTransformedAnnotation(IHqlExpression * expr);
    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr);    // return null if not transformed
    virtual IHqlExpression * transform(IHqlExpression * expr);

    virtual IHqlExpression * createTransformedSelector(IHqlExpression * expr);
    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);
    virtual IHqlExpression * transformSelector(IHqlExpression * expr);

    inline void updateOrphanedSelectors(SharedHqlExpr & transformed, IHqlExpression * expr)
    {
        if (expr != transformed)
            transformed.setown(doUpdateOrphanedSelectors(expr, transformed));
    }
    IHqlExpression * doUpdateOrphanedSelectors(IHqlExpression * expr, IHqlExpression * transformed);

    IHqlExpression * queryTransformed(IHqlExpression * expr);

    void quickAnalyseTransform(IHqlExpression * expr);
    IHqlExpression * quickTransformTransform(IHqlExpression * expr);
    IHqlExpression * transformAssign(IHqlExpression * expr);
    IHqlExpression * queryTransformAnnotation(IHqlExpression * expr);

//helper functions
    void doAnalyseChildren(IHqlExpression * expr, unsigned first);
    void doAnalyseChildren(IHqlExpression * expr, unsigned first, unsigned last);
    IHqlExpression *    createTransformedActiveSelect(IHqlExpression * expr);
    void initializeActiveSelector(IHqlExpression * expr, IHqlExpression * transformed);
    IHqlExpression *    getTransformedChildren(IHqlExpression * expr);
    void                stopDatasetTransform(IHqlExpression * expr);
    IHqlExpression *    transformCall(IHqlExpression * expr);
    IHqlExpression *    transformExternalCall(IHqlExpression * expr);
    IHqlExpression *    queryActiveSymbol() const;
    IHqlExpression *    queryActiveLocation() const;
    IHqlExpression *    queryActiveLocation(IHqlExpression * expr) const;
    void setMapping(IHqlExpression * oldValue, IHqlExpression * newValue);
    void setSelectorMapping(IHqlExpression * oldValue, IHqlExpression * newValue);

protected:
    void setMappingOnly(IHqlExpression * oldValue, IHqlExpression * newValue);

protected:
    unsigned            pass;
    unsigned            analyseFlags;
    unsigned            optimizeFlags;
private:
    HqlExprCopyArray    locations;
};

class HQL_API HqlMapTransformer : public NewHqlTransformer
{
public:
    HqlMapTransformer();

    IHqlExpression * queryMapping(IHqlExpression * oldValue);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        IHqlExpression * body = expr->queryBody(true);
        if (expr == body)
            return NewHqlTransformer::createTransformed(expr);
        OwnedHqlExpr transformed = transform(body);
        return expr->cloneAnnotation(transformed);
    }

    using NewHqlTransformer::setMapping;
    using NewHqlTransformer::setSelectorMapping;
};

class HQL_API HqlMapSelectorTransformer : public HqlMapTransformer
{
public:
    HqlMapSelectorTransformer(IHqlExpression * oldValue, IHqlExpression * newValue);

protected:
    OwnedHqlExpr oldSelector;
};


/*
Notes on transformations that move code to a different context....

Certain pieces of code - e.g. global(), code to spot simple global expressions, aggregates within activities etc. take an expression from its original context,
and move it to a different context.  There are several issues:

i) Expressions should never be hoisted outside of sequential items, or workflow actions.
ii) Sometimes the expressions are unconditionally hoisted out of conditional actions, other times only if they are used unconditionally elsewhere, otherwise they should be retained.
iii) For transformations that are context dependent - saving and restoring transformation lists doesn't work.

To cope with all these situations these transforms now work as follows:
a) Unless always unconditionally global, the expression tree is first analysed to work out whether an expression occurs outside of an unconditional context.
b) When analysing sequential/workflow are not recursed into.  IF actions are recursed, but noted as conditional
c) There is a new (pure virtual) method transformIndependent() which is called to independently process a sequential branch etc.
d) Expressions are only hoisted if used unconditionally (or always unconditionally hoisted)
e) IF() actions are handled by 
   i) transforming the children - (will hoist if used unconditionally elsewhere)
   ii) recursively calling transformIndependent() on children.
f) If no unconditional candidates are found when analysing, it should be possible to short-circuit the transform.
g) Always works on body, not on named symbol

This provides a mechanism for handling the code correctly, but also allowing the transform to be context independent.
Flags provided to the constructor indicate which options for transforming are used.
*/

class HoistingTransformInfo : public NewTransformInfo
{
public:
    HoistingTransformInfo(IHqlExpression * _original) : NewTransformInfo(_original) { spareByte1 = false; }
    
    inline bool isUnconditional() { return spareByte1 != 0; }
    inline void setUnconditional() { spareByte1 = true; }

private:
    using NewTransformInfo::spareByte1;             //prevent derived classes from also using this spare byte
};

//This allows expressions to be evaluated in a nested context, so that once that nested context is finished
//all the mapped expressions relating to that nested context are removed, and not commoned up.
//Useful for processing 
class HQL_API HoistingHqlTransformer : public NewHqlTransformer
{
public:
    enum { HTFnoteconditionalactions = 0x01, 
           HTFnoteconditionaldatasets= 0x02,
           HTFnoteconditionaldatarows= 0x04,
           HTFtraverseallnodes = 0x08,
           HTFnoteallconditionals = 0x10,
    };
    HoistingHqlTransformer(HqlTransformerInfo & _info, unsigned _flags);

    void appendToTarget(IHqlExpression & curOwned);
    void setFlags(unsigned _flags) { flags = _flags; }
    void transformRoot(const HqlExprArray & in, HqlExprArray & out);
    IHqlExpression * transformRoot(IHqlExpression * expr);

protected:
    bool analyseThis(IHqlExpression * expr);

    virtual void doAnalyseExpr(IHqlExpression * expr);

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);
    inline HoistingTransformInfo * queryExtra(IHqlExpression * expr)        { return static_cast<HoistingTransformInfo *>(queryTransformExtra(expr)); }
    inline bool isUsedUnconditionally(IHqlExpression * expr)                { return queryExtra(expr->queryBody())->isUnconditional(); }

    virtual void analyseExpr(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);
    virtual IHqlExpression * transformIndependent(IHqlExpression * expr) = 0;

    void transformArray(const HqlExprArray & in, HqlExprArray & out);
    IHqlExpression * transformEnsureResult(IHqlExpression * expr);

    inline bool checkConditional(IHqlExpression * expr)
    {
        return ((flags & HTFnoteallconditionals) ||
            ((flags & HTFnoteconditionalactions) && expr->isAction()) ||
            ((flags & HTFnoteconditionaldatasets) && expr->isDataset()) ||
            ((flags & HTFnoteconditionaldatarows) && expr->isDatarow()));
    }


private:
    HqlExprArray *      target;
    unsigned flags;

protected:
    unsigned conditionDepth;
    bool containsUnknownIndependentContents;
};


//This allows expressions to be evaluated in a nested context, so that once that nested context is finished
//all the mapped expressions relating to that nested context are removed, and not commoned up.
class HQL_API NestedHqlTransformer : public NewHqlTransformer
{
public:
    NestedHqlTransformer(HqlTransformerInfo & _info) : NewHqlTransformer(_info) { savedNull.setown(createAttribute(_empty_str_Atom)); }

    void beginNestedScope();
    void endNestedScope();

    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed);
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed);

    void transformArray(const HqlExprArray & in, HqlExprArray & out);

protected:
    HqlExprCopyArray savedSelectors;
    HqlExprCopyArray savedTransformed;
    HqlExprArray savedTransformedValue;
    UnsignedArray depthStack;
    OwnedHqlExpr savedNull;
};

class HQL_API NestedHqlMapTransformer : public NestedHqlTransformer, public CInterface
{
public:
    NestedHqlMapTransformer();

//  IHqlExpression * queryMapping(IHqlExpression * oldValue);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        IHqlExpression * body = expr->queryBody(true);
        if (expr == body)
            return NewHqlTransformer::createTransformed(expr);
        OwnedHqlExpr transformed = transform(body);
        return expr->cloneAnnotation(transformed);
    }

    using NestedHqlTransformer::setMapping;
    using NestedHqlTransformer::setSelectorMapping;
};


extern HQL_API IHqlExpression * replaceExpression(IHqlExpression * expr, IHqlExpression * original, IHqlExpression * replacement);


//------------------------------------------------------------------------

/*
The merging transformer is used whenever a parent dataset can be removed,swapped,or changed.  Examples are the optimizer,
the code for creating compound fetches.

It introduces another level of complication because a dataset selector needs to be transformed differently depending on where
it appears. E.g., take project2(filter(project1(ds))) and project3(project1(ds)).  If the filter is swapped with project1 then
have project2'(project1'(filter(ds)) and project3(project1(ds)).

It works as follows:
1) If introduces a dataset into scope (not left/right) then a new context is created.
   Evaluate is treated similarly.
   ** Currently only introduced if the parent dataset changes - is this correct? **
   ** Also could also do it only if the normalized selector didn't match new normalizedSelector **
   *Could do it always to flush out any bugs in the code *
2) The transformedSelectors() for the new dataset are set.  We also walk up the a.b.c.d selector tree, and initialise those.
3) If datasets are nested then a scope is created within the previous scope.


*/

class HQL_API AMergingTransformInfo : public NewTransformInfo
{
public:
    inline AMergingTransformInfo(IHqlExpression * _expr) : NewTransformInfo(_expr) {}

    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * childScope) = 0;
    virtual void setTransformed(IHqlExpression * childScope, IHqlExpression * value) = 0;

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * childScope) = 0;
    virtual void setTransformedSelector(IHqlExpression * childScope, IHqlExpression * value) = 0;

    inline bool recurseParentScopes()
    {
//#ifdef ENSURE_SELSEQ_UID
        switch (original->getOperator())
        {
        case no_select:
            return false;
        }
//#endif
        return true;
    }
};

class HQL_API MergingTransformSimpleInfo : public AMergingTransformInfo
{
public:
    MergingTransformSimpleInfo(IHqlExpression * _expr);

    virtual bool isOnlyTransformedOnce() const { return true; }

private:
    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * childScope);
    virtual void setTransformed(IHqlExpression * childScope, IHqlExpression * value);

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * childScope);
    virtual void setTransformedSelector(IHqlExpression * childScope, IHqlExpression * value);
};


#if 0
class HQL_API MergingTransformComplexCache
{
public:
    MergingTransformComplexCache();
    ~MergingTransformComplexCache() { delete mappedSelector; delete mapped; }

private:
    IHqlExpression * queryAlreadyTransformed(AMergingTransformInfo & baseInfo, IHqlExpression * childScope);
    void setTransformed(AMergingTransformInfo & baseInfo, IHqlExpression * childScope, IHqlExpression * value);

    IHqlExpression * queryAlreadyTransformedSelector(AMergingTransformInfo & baseInfo, IHqlExpression * childScope);
    void setTransformedSelector(AMergingTransformInfo & baseInfo, IHqlExpression * childScope, IHqlExpression * value);

public:
    MapOwnedToOwned<IHqlExpression, IHqlExpression> * mapped;
    MapOwnedToOwned<IHqlExpression, IHqlExpression> * mappedSelector;
    HqlExprAttr         transformedScope;
    HqlExprAttr         transformedSelectorScope;
};
#endif

class FastScopeMapping
{
public:
    FastScopeMapping() : map(4) {}

    inline IHqlExpression * getValue(IHqlExpression * key)
    {
        LinkedHqlExpr * match = map.getValue(key);
        if (match)
            return match->get();
        return NULL;
    }
    inline void setValue(IHqlExpression * key, IHqlExpression * value)
    {
        map.setValue(key, value);
    }

public:
    MapOwnedToOwned<IHqlExpression, IHqlExpression> map;
};

class SlowScopeMapping
{
public:
    inline IHqlExpression * getValue(IHqlExpression * key)
    {
        if (!key)
            return nullValue;
        unsigned match = find(key);
        if (match != NotFound)
            return &values.item(match);
        return NULL;
    }
    inline void setValue(IHqlExpression * key, IHqlExpression * value)
    {
        if (!key)
        {
            nullValue.set(value);
            return;
        }
        unsigned match = find(key);
        if (match == NotFound)
        {
            keys.append(OLINK(*key));
            values.append(OLINK(*value));
        }
        else
            values.replace(OLINK(*value), match);
    }
    inline unsigned find(IHqlExpression * key)
    {
        IHqlExpression * * head = keys.getArray();
        unsigned max = keys.ordinality();
        for (unsigned i=0; i < max; i++)
            if (head[i] == key)
                return i;
        return NotFound;
        for (unsigned i=0; i < max; i++)
            if (&keys.item(i) == key)
                return i;
        return NotFound;
    }

public:
    HqlExprAttr nullValue;
    HqlExprArray keys;
    HqlExprArray values;
};

class HQL_API MergingTransformInfo : public AMergingTransformInfo
{
    typedef FastScopeMapping MAPPINGCLASS;
public:
    MergingTransformInfo(IHqlExpression * _expr);
    ~MergingTransformInfo() { delete mappedSelector; delete mapped; }

protected:
    inline bool onlyTransformOnce() const { return spareByte1 != 0; }
    inline void setOnlyTransformOnce(bool _value) { spareByte1 = _value ? 1 : 0; }

private:
    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * childScope);
    virtual void setTransformed(IHqlExpression * childScope, IHqlExpression * value);

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * childScope);
    virtual void setTransformedSelector(IHqlExpression * childScope, IHqlExpression * value);

    virtual bool isOnlyTransformedOnce() const { return onlyTransformOnce(); }

public:
    MAPPINGCLASS * mapped;
    MAPPINGCLASS * mappedSelector;
    HqlExprAttr         transformedScope;
    HqlExprAttr         transformedSelectorScope;

private:
    using AMergingTransformInfo::spareByte1;                //prevent derived classes from also using this spare byte
};


class HQL_API MergingHqlTransformer : public NewHqlTransformer
{
public:
    MergingHqlTransformer(HqlTransformerInfo & _info) : NewHqlTransformer(_info) {}

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr);
    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed);

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed);

protected:
    virtual void pushChildContext(IHqlExpression * expr, IHqlExpression * transformed);
    virtual void popChildContext();
    inline MergingTransformInfo * queryExtra(IHqlExpression * expr) { return (MergingTransformInfo *)queryTransformExtra(expr); }

protected:
    HqlExprAttr childScope;
};

class HQL_API HqlScopedMapSelectorTransformer : public MergingHqlTransformer
{
public:
    HqlScopedMapSelectorTransformer (IHqlExpression * oldValue, IHqlExpression * newValue);
};


#if 0
class HQL_API SelectorReplacingTransformer : public MergingHqlTransformer
{
public:
    SelectorReplacingTransformer();

    void initSelectorMapping(IHqlExpression * oldValue, IHqlExpression * newValue);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    inline bool foundAmbiguity() const { return introducesAmbiguity; }

protected:
    void updateMapping();
    virtual void pushChildContext(IHqlExpression * expr, IHqlExpression * transformed);

protected:
    OwnedHqlExpr oldSelector;
    OwnedHqlExpr newSelector;
    OwnedHqlExpr oldDataset;
    OwnedHqlExpr newDataset;
    bool isHidden;
    bool introducesAmbiguity;
    IHqlExpression * savedNewDataset;
};
#endif


class HQL_API NewSelectorReplacingInfo : public NewTransformInfo
{
public:
    NewSelectorReplacingInfo(IHqlExpression * _original) : NewTransformInfo(_original) {}

    inline int b2i(bool b) { return b?1:0; }

    virtual IHqlExpression * queryTransformed(bool hidden)          { return transformed[b2i(hidden)]; }
    virtual IHqlExpression * queryTransformedSelector(bool hidden)  { return transformedSelector[b2i(hidden)]; }
    virtual void setTransformed(IHqlExpression * expr, bool hidden) { transformed[b2i(hidden)].set(expr); }
    virtual void setTransformedSelector(IHqlExpression * expr, bool hidden) { transformedSelector[b2i(hidden)].set(expr); }

protected:
    HqlExprAttr         transformed[2];         // after spliiting etc.
    HqlExprAttr         transformedSelector[2]; // after spliiting etc.
};


class HQL_API NewSelectorReplacingTransformer : public NewHqlTransformer
{
public:
    NewSelectorReplacingTransformer();

    void initSelectorMapping(IHqlExpression * oldValue, IHqlExpression * newValue);

    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(NewSelectorReplacingInfo, expr); }
    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed)
    {
        queryExtra(expr)->setTransformed(transformed, isHidden);
    }
        
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed)
    {
        queryExtra(expr)->setTransformedSelector(transformed, isHidden);
    }

    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr)
    {
        return queryExtra(expr)->queryTransformed(isHidden);
    }

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);

    inline NewSelectorReplacingInfo * queryExtra(IHqlExpression * expr) { return static_cast<NewSelectorReplacingInfo *>(queryTransformExtra(expr)); }

    inline bool foundAmbiguity() const { return introducesAmbiguity; }

protected:
    OwnedHqlExpr oldSelector;
    bool introducesAmbiguity;
    bool isHidden;
    IHqlExpression * savedNewDataset;
};


//------------------------------------------------------------------------

class HQL_API ScopeInfo : public CInterface
{
public:
    ScopeInfo() { isWithin = false; }
    inline void clear()                                             { dataset.clear(); left.clear(); right.clear(); }
    inline void setDataset(IHqlExpression * _dataset, IHqlExpression * transformed)             
                                                                    { dataset.set(_dataset); transformedDataset.set(transformed); }
    inline void setDatasetLeft(IHqlExpression * _dataset, IHqlExpression * transformed, IHqlExpression * _seq)              
                                                                    { dataset.set(_dataset); transformedDataset.set(transformed); left.set(_dataset); seq.set(_seq); }
    inline void setLeft(IHqlExpression * _left, IHqlExpression * _seq)
                                                                    { left.set(_left); seq.set(_seq); }
    inline void setLeftRight(IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * _seq) 
                                                                    { left.set(_left); right.set(_right); seq.set(_seq); }
    inline void setTopLeftRight(IHqlExpression * _dataset, IHqlExpression * transformed, IHqlExpression * _seq)         
                                                                    { dataset.set(_dataset); transformedDataset.set(transformed); left.set(_dataset); right.set(_dataset); seq.set(_seq); }

    inline bool isEmpty()                                           { return !dataset && !left; }

    IHqlDataset * queryActiveDataset();

public:
    HqlExprAttr     dataset;
    HqlExprAttr     transformedDataset;
    HqlExprAttr     left;
    HqlExprAttr     right;
    HqlExprAttr     seq;
    //NB: left and right are never transformed by this point, so no need to store transformed version
//  HqlExprAttr     evaluateScope;
    bool            isWithin;
};

struct ScopeSuspendInfo
{
    CIArrayOf<ScopeInfo> scope;
    CIArrayOf<ScopeInfo> saved;
    PointerIArrayOf<IHqlExpression> savedI;
};

class HQL_API ScopedTransformer : public NewHqlTransformer
{
public:
    ScopedTransformer(HqlTransformerInfo & _info);

protected:
    virtual void analyseChildren(IHqlExpression * expr);
    virtual void analyseAssign(IHqlExpression * expr);
    virtual IHqlExpression * createTransformed(IHqlExpression * expr);

    virtual IHqlExpression * doTransformRootExpr(IHqlExpression * expr);

//Functions for determining if something is in scope.
    bool isDatasetActive(IHqlExpression * expr);                // is it in an active dataset?
    bool isDatasetARow(IHqlExpression * expr);                  // can it be used as a row?
    bool isDatasetRelatedToScope(IHqlExpression * dataset);
    bool isNewDataset()                                             { return innerScope && innerScope->isEmpty(); }
    bool isTopDataset(IHqlExpression * selector);
    bool insideActivity();
    unsigned tableNesting();

    IHqlExpression * getScopeState();

//Functions for keeping track of what tables are currently in scope...
    virtual void pushScope();
    virtual void pushEvaluateScope(IHqlExpression * expr, IHqlExpression * transformed);
    virtual void popScope();
    virtual void popEvaluateScope();

    virtual void suspendScope();
    virtual void restoreScope();


    virtual void clearDataset(bool nested);
    virtual bool setDataset(IHqlExpression * _dataset, IHqlExpression * _transformed)               
                                                    { assertHasScope(); innerScope->setDataset(_dataset, _transformed); return true; }
    virtual bool setDatasetLeft(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * _seq)            
                                                    { assertHasScope(); innerScope->setDatasetLeft(_dataset, _transformed, _seq); return true; }
    virtual bool setLeft(IHqlExpression * _left, IHqlExpression * _seq)
                                                    { assertHasScope(); innerScope->setLeft(_left, _seq); return true; }
    virtual bool setLeftRight(IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * _seq)                       
                                                    { assertHasScope(); innerScope->setLeftRight(_left, _right, _seq); return true; }
    virtual bool setTopLeftRight(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * _seq)           
                                                    { assertHasScope(); innerScope->setTopLeftRight(_dataset, _transformed, _seq); return true; }

    virtual void suspendAllScopes(ScopeSuspendInfo & info);
    virtual void restoreScopes(ScopeSuspendInfo & info);

    inline void assertHasScope() { if (!innerScope) throwScopeError(); }
    void throwScopeError();

private:
    IHqlExpression * getEvaluateScope(IHqlExpression * scope);
    bool checkInScope(IHqlExpression * selector, bool allowCreate);

protected:
    CIArrayOf<ScopeInfo>    scopeStack;
    ScopeInfo *             innerScope;
    CIArrayOf<ScopeInfo>    savedStack;
    HqlExprAttr             emptyScopeMarker;
};


//Uses the MergingTransformInfo
class HQL_API ScopedDependentTransformer : public ScopedTransformer
{
protected:
    ScopedDependentTransformer(HqlTransformerInfo & _info);

    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr);

    virtual IHqlExpression * queryAlreadyTransformed(IHqlExpression * expr);
    virtual void setTransformed(IHqlExpression * expr, IHqlExpression * transformed);

    virtual IHqlExpression * queryAlreadyTransformedSelector(IHqlExpression * expr);
    virtual void setTransformedSelector(IHqlExpression * expr, IHqlExpression * transformed);

    virtual void pushEvaluateScope(IHqlExpression * expr, IHqlExpression * _transformed);
    virtual void popEvaluateScope();

    virtual void clearDataset(bool nested);
    virtual bool setDataset(IHqlExpression * _dataset, IHqlExpression * _transformed);
    virtual bool setDatasetLeft(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * _seq);
    virtual bool setTopLeftRight(IHqlExpression * _dataset, IHqlExpression * _transformed, IHqlExpression * _seq);
    virtual bool setLeft(IHqlExpression * _left, IHqlExpression * _seq);
    virtual bool setLeftRight(IHqlExpression * _left, IHqlExpression * _right, IHqlExpression * _seq);

    virtual void suspendAllScopes(ScopeSuspendInfo & info);
    virtual void restoreScopes(ScopeSuspendInfo & info);

#ifdef _DEBUG
    virtual IHqlExpression * transform(IHqlExpression * expr)
    {
        IHqlExpression * scope = childScope;
        IHqlExpression * ret = ScopedTransformer::transform(expr);
        if (childScope != scope)
            ret = createTransformed(expr);
        assertex(scope == childScope);
        return ret;
    }
#endif

protected:
    void pushChildContext(IHqlExpression * expr, IHqlExpression * transformed);
    void popChildContext();
    inline MergingTransformInfo * queryExtra(IHqlExpression * expr) { return (MergingTransformInfo *)queryTransformExtra(expr); }

protected:
    HqlExprAttr childScope;
    HqlExprAttr cachedLeft;
    HqlExprAttr cachedRight;
};


extern HQL_API unsigned activityHidesSelectorGetNumNonHidden(IHqlExpression * expr, IHqlExpression * selector);
inline bool activityHidesSelector(IHqlExpression * expr, IHqlExpression * selector)
{
    return activityHidesSelectorGetNumNonHidden(expr, selector) != 0;
}


class HQL_API HqlSelectorLocator : public NewHqlTransformer
{
public:
    HqlSelectorLocator(IHqlExpression * _selector);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual void analyseSelector(IHqlExpression * expr);

    bool containsSelector(IHqlExpression * expr);

protected:
    bool foundSelector;
    OwnedHqlExpr selector;
};

class HQL_API HqlSelectorAnywhereLocator : public NewHqlTransformer
{
public:
    HqlSelectorAnywhereLocator(IHqlExpression * _selector);

    virtual void analyseExpr(IHqlExpression * expr);
    virtual void analyseSelector(IHqlExpression * expr);

    bool containsSelector(IHqlExpression * expr);

protected:
    bool foundSelector;
    OwnedHqlExpr selector;
};

class SplitterVerifierInfo : public NewTransformInfo
{
public:
    SplitterVerifierInfo(IHqlExpression * _original) : NewTransformInfo(_original) { useCount = 0; }

public:
    unsigned    useCount;
};

class SplitterVerifier : public NewHqlTransformer
{
public:
    SplitterVerifier();

protected:
    virtual void             analyseExpr(IHqlExpression * expr);
    virtual ANewTransformInfo * createTransformInfo(IHqlExpression * expr) { return CREATE_NEWTRANSFORMINFO(SplitterVerifierInfo, expr); }
    inline SplitterVerifierInfo * queryExtra(IHqlExpression * expr)     { return static_cast<SplitterVerifierInfo *>(queryTransformExtra(expr)); }
};

extern HQL_API bool containsSelector(IHqlExpression * expr, IHqlExpression * selector);
extern HQL_API bool containsSelectorAnywhere(IHqlExpression * expr, IHqlExpression * selector);         // searches through nested "hidden" definitions

class HQL_API FieldAccessAnalyser : public NewHqlTransformer
{
public:
    FieldAccessAnalyser(IHqlExpression * selector);

    inline bool accessedAll() const { return numAccessed == fields.ordinality(); }
    IHqlExpression * queryLastFieldAccessed() const;

protected:
    virtual void analyseExpr(IHqlExpression * expr);
    virtual void analyseSelector(IHqlExpression * expr);

    inline void setAccessedAll() { numAccessed = fields.ordinality(); }

protected:
    LinkedHqlExpr selector;
    HqlExprCopyArray fields;
    Owned<IBitSet> accessed;
    unsigned numAccessed;
};
/*

If something can be transformed more than one way depending on the context then must either
i) don't call transform() on the children that are dependant, 
   [loses commoning up when transforming, and should not be done for fields or nything else
    that can be referred to]
ii) make it dependant on the thing that can change - e.g., 
    a) parent dataset
    b) parent table.
    c) containing activity kind.

*/

extern HQL_API IHqlExpression * newReplaceSelector(IHqlExpression * expr, IHqlExpression * oldSelector, IHqlExpression * newSelector);
extern HQL_API void newReplaceSelector(HqlExprArray & target, const HqlExprArray & source, IHqlExpression * oldSelector, IHqlExpression * newSelector);
extern HQL_API IHqlExpression * queryNewReplaceSelector(IHqlExpression * expr, IHqlExpression * oldSelector, IHqlExpression * newSelector);
extern HQL_API IHqlExpression * expandCreateRowSelectors(IHqlExpression * expr);
extern HQL_API void verifySplitConsistency(IHqlExpression * expr);
extern HQL_API IHqlExpression * convertWorkflowToImplicitParmeters(HqlExprArray & parameters, HqlExprArray & defaults, IHqlExpression * expr);
extern HQL_API unsigned getNumUniqueExpressions(IHqlExpression * expr);
extern HQL_API unsigned getNumUniqueExpressions(const HqlExprArray & exprs);
extern HQL_API unsigned getNumOccurences(HqlExprArray & exprs, IHqlExpression * search, unsigned limit);
extern HQL_API void logTreeStats(IHqlExpression * expr);
extern HQL_API void logTreeStats(const HqlExprArray & exprs);
extern HQL_API IHqlExpression * quickFullReplaceExpression(IHqlExpression * expr, IHqlExpression * oldValue, IHqlExpression * newValue);
extern HQL_API IHqlExpression * quickFullReplaceExpressions(IHqlExpression * expr, const HqlExprArray & oldValues, const HqlExprArray & newValues);
extern HQL_API void gatherSelectExprs(HqlExprArray & target, IHqlExpression * expr);
extern HQL_API void dbglogTransformStats(bool reset);

#ifdef OPTIMIZE_TRANSFORM_ALLOCATOR
size32_t beginTransformerAllocator();
void endTransformerAllocator(size32_t prevSize);
extern HQL_API void * transformerAlloc(size32_t size);
#endif

#endif
