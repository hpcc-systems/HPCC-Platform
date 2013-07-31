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
#ifndef HQLSTMT_IPP
#define HQLSTMT_IPP

#include "hqlstmt.hpp"

//Sometimes the number of definitions can get very very large.  
//To help with that situation the following option creates an array of "hashes" of the definitions
//It uses more memory, but has much better cache locality than accessing a member of the definition

#define CACHE_DEFINITION_HASHES
inline unsigned getSearchHash(IHqlExpression * search)
{
    unsigned hash = search->getHash();
    return (unsigned short)((hash >> 16) | hash);
}
//typedef UnsignedArray DefinitionHashArray;
typedef UnsignedShortArray DefinitionHashArray;

//---------------------------------------------------------------------------

class HqlStmts;
class PeepHoleOptimizer;

class HqlStmt : public CInterfaceOf<IHqlStmt>
{
public:
    HqlStmt(StmtKind _kind, HqlStmts * _container);
    
    virtual StmtKind                getStmt();
    virtual StringBuffer &          getTextExtra(StringBuffer & out);
    virtual bool                    isIncluded() const;
    virtual unsigned                numChildren() const;
    virtual IHqlStmt *              queryChild(unsigned index) const;
    virtual IHqlExpression *        queryExpr(unsigned index);
    virtual HqlStmts *              queryContainer();

            void                    addExpr(IHqlExpression * expr);
            void                    killExprs() { exprs.kill(); }
            bool                    isIncomplete()  { return incomplete; }
            unsigned                queryPriority() { return priority; }
    virtual void                    mergeScopeWithContainer()       {}
    virtual void                    setIncomplete(bool _incomplete) { incomplete = _incomplete; }
    virtual void                    setIncluded(bool _included) { included = _included; }
            void                    setPriority(unsigned _prio) { priority = _prio; }

protected:
    bool hasChildren() const;

protected:
    unsigned                            priority;       //64bit: pack with link count in CInterface
    HqlStmts *                          container;
    HqlExprArray                        exprs;
#ifdef _DEBUG
    StmtKind                            kind;
#else
    unsigned char                       kind;
#endif
    bool                                incomplete;
    bool                                included;
};

#ifdef _WIN32
#pragma warning( push )
#pragma warning( disable : 4275 ) // hope this warning not significant! (may get link errors I guess)
#endif

typedef IArrayOf<HqlStmt> HqlStmtArray;

class HQLCPP_API HqlStmts : public IArrayOf<HqlStmt>
{
    friend class BuildCtx;
    friend class PeepHoleOptimizer;
    friend class AssociationIterator;
public:
    HqlStmts(HqlStmt * _owner);

    void                            appendStmt(HqlStmt & stmt);
    void                            inheritDefinitions(HqlStmts & owwther);
    HqlStmt *                       queryStmt() { return owner; };

    void appendOwn(HqlExprAssociation & next);

    inline bool zap(HqlExprAssociation & next)
    {
        unsigned match = defs.find(next);
        if (match == NotFound)
            return false;
        defs.remove(match);
#ifdef CACHE_DEFINITION_HASHES
        exprHashes.remove(match);
#endif
        return true;
    }

protected:
    HqlStmt *                       owner;
    CIArrayOf<HqlExprAssociation>   defs;
    // A bit mask of which types of associations this contains.  Don't worry about false positives.
    unsigned                        associationMask;
#ifdef CACHE_DEFINITION_HASHES
    DefinitionHashArray             exprHashes;
#endif
};


#ifdef _WIN32
#pragma warning( pop ) 
#endif


class HqlCompoundStmt : public HqlStmt
{
    friend class BuildCtx;
    friend class PeepHoleOptimizer;
public:
    HqlCompoundStmt(StmtKind _kind, HqlStmts * _container);

    virtual unsigned                numChildren() const;
    virtual void                    mergeScopeWithContainer();
    virtual IHqlStmt *              queryChild(unsigned index) const;

protected:
    HqlStmts                         code;
};


class HqlQuoteStmt : public HqlCompoundStmt
{
public:
    HqlQuoteStmt(StmtKind _kind, HqlStmts * _container, const char * _text) : HqlCompoundStmt(_kind, _container), text(_text) {}

    virtual StringBuffer &          getTextExtra(StringBuffer & out);

protected:
  StringAttr text;
};


enum {
    PHOconvertReal = 0x0001,
};

class PeepHoleOptimizer
{
public:
    PeepHoleOptimizer(HqlCppTranslator & _translator);
    void optimize(HqlStmts & stmts);

protected:
    HqlCppTranslator & translator;
    size32_t combineStringLimit;
    unsigned peepholeOptions;
};

//---------------------------------------------------------------------------

struct HQLCPP_API HqlDefinedValue : public HqlExprAssociation
{
public:
    HqlDefinedValue(IHqlExpression * _original) : HqlExprAssociation(_original) {}

    virtual AssocKind   getKind()           { return AssocExpr; }
};

    
struct HQLCPP_API HqlSimpleDefinedValue : public HqlDefinedValue
{
public:
    HqlSimpleDefinedValue(IHqlExpression * _original, IHqlExpression * _expr) : HqlDefinedValue(_original)
    { expr.set(_expr); }

    virtual IHqlExpression * queryExpr() const              { return expr; }

public:
    HqlExprAttr         expr;
};


#endif
