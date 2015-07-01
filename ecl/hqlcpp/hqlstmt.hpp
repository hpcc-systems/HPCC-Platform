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
#ifndef HQLSTMT_HPP
#define HQLSTMT_HPP

#ifdef HQLCPP_EXPORTS
#define HQLCPP_API __declspec(dllexport)
#else
#define HQLCPP_API __declspec(dllimport)
#endif

interface IHqlStmt;
struct BuildOptions;
class BuildCtx;
class HqlStmt;
class HqlCompoundStmt;
class HqlStmts;
class HqlCppInstance;
class CHqlBoundExpr;
class all_result_types;

interface IFunctionInfo;

//---------------------------------------------------------------------------
// used to represent an association of something already calculated in the context
// e.g. a cursor, temporary value, or even dependency information.

//These are represented using unique bits, so that a mask can be stored to indicate which associations are held
enum AssocKind
{
    AssocExpr              = 0x0000001,
    AssocActivity          = 0x0000002,
    AssocCursor            = 0x0000004,
    AssocClass             = 0x0000008,
    AssocRow               = 0x0000010,
    AssocActivityInstance  = 0x0000020,
    AssocExtract           = 0x0000040,
    AssocExtractContext    = 0x0000080,
    AssocSubQuery          = 0x0000100,
    AssocGraphNode         = 0x0000200,
    AssocSubGraph          = 0x0000400,
    AssocStmt              = 0x0000800,
 };

class CHqlBoundExpr;
struct HQLCPP_API HqlExprAssociation : public CInterface
{
public:
    HqlExprAssociation(IHqlExpression * _represents) { represents.set(_represents); }

    virtual bool isAlias()                                  { return false; }
    virtual bool isRowAssociation()                         { return false; }
    virtual AssocKind getKind() = 0;
    virtual IHqlExpression * queryExpr() const              { UNIMPLEMENTED; }
    virtual void getBound(CHqlBoundExpr & result);

public:
    HqlExprAttr represents;
};

interface IAssociationVisitor
{
    virtual bool visit(HqlExprAssociation & assoc) = 0;         // return true if done
};
    
//---------------------------------------------------------------------------
// Class used to represent current location for generating source code statements.
    
class CHqlBoundExpr;
class HQLCPP_API BuildCtx : public CInterface
{
    friend class AssociationIterator;
public:
    BuildCtx(HqlCppInstance & _state, IAtom * section);
    BuildCtx(HqlCppInstance & _state);
    BuildCtx(BuildCtx & _owner);
    BuildCtx(BuildCtx & _owner, IHqlStmt * container);
    ~BuildCtx();

    IHqlStmt *                  addAssign(IHqlExpression * target, IHqlExpression * value);
    IHqlStmt *                  addAssignLink(IHqlExpression * target, IHqlExpression * value);
    IHqlStmt *                  addAssignIncrement(IHqlExpression * target, IHqlExpression * value);
    IHqlStmt *                  addAssignDecrement(IHqlExpression * target, IHqlExpression * value);
    IHqlStmt *                  addAlias(IHqlStmt * stmt);
    IHqlStmt *                  addBlock();
    IHqlStmt *                  addBreak();
    IHqlStmt *                  addCase(IHqlStmt * owner, IHqlExpression * condition);
    IHqlStmt *                  addCatch(IHqlExpression * caught);
    IHqlStmt *                  addConditionalGroup(IHqlStmt * stmt); // generated if stmt->isIncluded() is true
    IHqlStmt *                  addContinue();
    IHqlStmt *                  addDeclare(IHqlExpression * name, IHqlExpression * value=NULL);
    IHqlStmt *                  addDeclareExternal(IHqlExpression * name);
    IHqlStmt *                  addDeclareAssign(IHqlExpression * name, IHqlExpression * value);
    IHqlStmt *                  addDefault(IHqlStmt * owner);
    IHqlStmt *                  addExpr(IHqlExpression * condition);
    IHqlStmt *                  addExprOwn(IHqlExpression * condition);
    IHqlStmt *                  addReturn(IHqlExpression * value);
    IHqlStmt *                  addFilter(IHqlExpression * condition);
    IHqlStmt *                  addFunction(IHqlExpression * funcdef);
    IHqlStmt *                  addGoto(const char * labelText);
    IHqlStmt *                  addGroup(); // like a block but no {}
    IHqlStmt *                  addGroupPass(IHqlExpression * pass);
    IHqlStmt *                  addIndirection(const BuildCtx & _parent);       // pretend this is generated in the parent context (add a modified container)
    IHqlStmt *                  addLabel(const char * labelText);
    IHqlStmt *                  addLine(const char * filename = NULL, unsigned lineNum = 0);
    IHqlStmt *                  addLoop(IHqlExpression * cond, IHqlExpression * next, bool atEnd);
    IHqlStmt *                  addQuoted(const char * text);
    IHqlStmt *                  addQuotedLiteral(const char * text); // must only be used for constant C++ strings - avoids a memory clone
    IHqlStmt *                  addQuotedF(const char * text, ...) __attribute__((format(printf, 2, 3)));
    IHqlStmt *                  addQuotedCompound(const char * text, const char * extra = NULL);
    IHqlStmt *                  addQuotedCompoundOpt(const char * text, const char * extra = NULL);
    IHqlStmt *                  addQuoted(StringBuffer & text)              { return addQuoted(text.str()); }
    IHqlStmt *                  addQuotedCompound(StringBuffer & text, const char * extra = NULL){ return addQuotedCompound(text.str(), extra); }
    IHqlStmt *                  addSwitch(IHqlExpression * condition);
    IHqlStmt *                  addThrow(IHqlExpression * thrown);
    IHqlStmt *                  addTry();
    void                        associate(HqlExprAssociation & next);
    void                        associateOwn(HqlExprAssociation & next);
    HqlExprAssociation *        associateExpr(IHqlExpression * represents, IHqlExpression * expr);
    HqlExprAssociation *        associateExpr(IHqlExpression * represents, const CHqlBoundExpr & bound);
    bool                        hasAssociation(HqlExprAssociation & search, bool unconditional);
    HqlExprAssociation *        queryAssociation(IHqlExpression * dataset, AssocKind kind, HqlExprCopyArray * selectors);
    HqlExprAssociation *        queryFirstAssociation(AssocKind kind);
    HqlExprAssociation *        queryFirstCommonAssociation(AssocKind kind);
    HqlExprAssociation *        queryMatchExpr(IHqlExpression * expr);
    bool                        getMatchExpr(IHqlExpression * expr, CHqlBoundExpr & bound);
    IHqlExpression *            getTempDeclare(ITypeInfo * type, IHqlExpression * value);
    bool                        isOuterContext() const;
    void                        needFunction(IFunctionInfo & helper);
    void                        needFunction(IAtom * name);
    void                        removeAssociation(HqlExprAssociation * search);
    IHqlStmt *                  replaceExpr(IHqlStmt * stmt, IHqlExpression * expr);            // use with extreme care!
    IHqlStmt *                  selectBestContext(IHqlExpression * expr);
    void                        selectContainer();
    void                        selectElse(IHqlStmt * filter);
    void                        setNextConstructor()    { setNextPriority(ConPrio); }
    void                        setNextDestructor()     { setNextPriority(DesPrio); }
    void                        setNextNormal()         { setNextPriority(NormalPrio); }
    void                        setNextPriority(unsigned newPrio);
    unsigned                    setPriority(unsigned newPrio);

    void                        set(IAtom * section);
    void                        set(BuildCtx & _owner);

public:
    enum { ConPrio = 1, EarlyPrio =3000, NormalPrio = 5000, LatePrio = 7000, DesPrio = 9999, OutermostScopePrio };

protected:
    HqlStmt *                   appendCompound(HqlCompoundStmt * next);
    HqlStmt *                   appendSimple(HqlStmt * next);
    void                        appendToOutermostScope(HqlStmt * next);
    void                        init(HqlStmts * _root);
    bool                        isChildOf(HqlStmt * stmt, HqlStmts * stmts);
    void                        recordDefine(HqlStmt * declare);
    IHqlStmt *                  recursiveGetBestContext(HqlStmts * searchStmts, HqlExprCopyArray & required);
    void                        selectCompound(IHqlStmt * stmt);

private:
    BuildCtx(BuildCtx & owner, HqlStmts * _root);

protected:
    HqlStmts *                      root;
    HqlStmts *                      curStmts;
    bool                            ignoreInput;
    unsigned                        curPriority;
    unsigned                        nextPriority;
    HqlCppInstance &            state;
};


enum StmtKind { 
             null_stmt,
             assign_stmt, block_stmt, group_stmt, declare_stmt, 
             expr_stmt,
             return_stmt,
             quote_stmt,
             quote_compound_stmt,
             quote_compoundopt_stmt,
             filter_stmt,
             goto_stmt, label_stmt,
             switch_stmt, case_stmt, default_stmt,
             loop_stmt, break_stmt,
             pass_stmt, external_stmt,
             indirect_stmt,
             assigninc_stmt, assigndec_stmt,
             alias_stmt,
             line_stmt,
             continue_stmt,
             function_stmt,
             assign_link_stmt,
             try_stmt,
             catch_stmt,
             throw_stmt,
};


interface IHqlStmt : public IInterface
{
public:
    virtual StringBuffer &  getTextExtra(StringBuffer & out) const = 0;
    virtual bool            isIncluded() const = 0;
    virtual StmtKind        getStmt() const = 0;
    virtual unsigned        numChildren() const = 0;
    virtual IHqlStmt *      queryChild(unsigned index) const = 0;
    virtual IHqlExpression *queryExpr(unsigned index) const = 0;

//used when creating the statement graph
    virtual void            mergeScopeWithContainer() = 0;
    virtual void            setIncomplete(bool incomplete) = 0;
    virtual void            setIncluded(bool _included) = 0;
    virtual void            finishedFramework() = 0;
};

class HqlCppTranslator;
void peepholeOptimize(HqlCppInstance & instance, HqlCppTranslator & translator);

class AssociationIterator
{
public:
    AssociationIterator(BuildCtx & ctx);

    bool first();
    bool isValid()      { return curStmts != NULL; }
    inline bool next()  { return doNext(); }
    HqlExprAssociation & get();

protected:
    virtual bool doNext();

protected:
    HqlStmts * rootStmts;
    unsigned curIdx;
    unsigned searchMask;
    HqlStmts * curStmts;
};


class FilteredAssociationIterator : public AssociationIterator
{
public:
    FilteredAssociationIterator(BuildCtx & ctx, AssocKind _searchKind) : AssociationIterator(ctx)
    {
        searchKind = _searchKind;
        searchMask = searchKind;
    }

    virtual bool doNext();

protected:
    AssocKind searchKind;
};



class BoundRow;
class RowAssociationIterator : public AssociationIterator
{
public:
    RowAssociationIterator(BuildCtx & ctx) : AssociationIterator(ctx)
    {
        searchMask = AssocRow|AssocCursor;
    }

    virtual bool doNext();

    BoundRow & get()                                        { return (BoundRow &)AssociationIterator::get(); }
};


unsigned calcTotalChildren(const IHqlStmt * stmt);

IHqlExpression * stripTranslatedCasts(IHqlExpression * e);
IHqlExpression * peepholeAddExpr(IHqlExpression * left, IHqlExpression * right);
bool rightFollowsLeft(IHqlExpression * left, IHqlExpression * leftLen, IHqlExpression * right);
extern HQLCPP_API void outputSizeStmts();

#endif
