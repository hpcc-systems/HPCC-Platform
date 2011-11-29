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
#ifndef HQLGRAM_HPP_INCL
#define HQLGRAM_HPP_INCL

#include "platform.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "jstream.hpp"
#include "jmisc.hpp"
#include "hqlexpr.hpp"
#include "jprop.hpp"
#include "jexcept.hpp"
#include "hqlpmap.hpp"
#include "hqlutil.hpp"

#include "hqlxmldb.hpp"

#define DEFAULT_MAX_ERRORS 100
#define EXPORT_FLAG 1
#define VIRTUAL_FLAG 2
#define SHARED_FLAG 4

#define REC_FLD_ERR_STR "Need to supply a value for field '%s'"
#define ERR_WRONGSCOPING_ERR_STR "Value for field '%s' cannot be computed in this scope"

struct DefineIdSt
{
private: 
    ITypeInfo* type;
    IPropertyTree * doc;
public:
    _ATOM      id;
    int        scope;
    DefineIdSt() { type = NULL; id = NULL; scope = 0; doc = NULL; }
    ~DefineIdSt() { ::Release(type); ::Release(doc); }  
    void setDoc(IPropertyTree * _doc) { doc = _doc; }
    void setType(ITypeInfo* t) { type = t; }
    ITypeInfo* queryType() const { return type; }
    IPropertyTree* queryDoc() const { return doc; }
    ITypeInfo* getType() { ITypeInfo* rt = type; type = NULL; return rt; }
};

struct attribute
{
private:
    union 
    {
        IHqlExpression *expr;
        ITypeInfo      *type;
        _ATOM           name;
        char           *str_val;
        __int64         int_val;
        DefineIdSt     *defineid;
        IFileContents  *contents;
    };
    enum { t_none,t_expr,t_scope,t_atom,t_string,t_int,t_type,t_defineid,t_contents } atr_type;
public:
    ECLlocation pos;

public:
    void annotateExprWithLocation();
    
    inline void clearPosition()
    {
        pos.clear();
    }
    inline void setPosition(const attribute &from)
    {
        setPosition(from.pos);
    }
    inline void setPosition(const ECLlocation &from)
    {
        setPosition(from.lineno, from.column, from.position, from.sourcePath);
    }
    inline void querySetPosition(const attribute &from)
    {
        if (pos.column == 0 && pos.lineno == 0 && pos.position == 0)
            setPosition(from);
    }
    void setPosition(int _line, int _column, int _position, ISourcePath * _sourcePath);
    inline IHqlExpression *getExpr()
    {
        assertex(atr_type==t_expr); 
        atr_type = t_none;
        return expr; 
    }
    inline IHqlExpression *queryExpr() const
    {
        assertex(atr_type==t_expr); 
        return expr; 
    }
    inline ITypeInfo *queryExprType() const
    {
        assertex(atr_type==t_expr); 
        return expr->queryType(); 
    }
    inline DefineIdSt* queryDefineId() const
    {
        assertex(atr_type==t_defineid);
        return defineid;
    }
    
    /* getters */
    inline IFileContents * getContents() 
    {
        assertex(atr_type==t_contents);         
        atr_type = t_none;
        return contents; 
    }
    inline _ATOM getName() 
    {
        assertex(atr_type==t_atom);         
        atr_type = t_none;
        return name; 
    }
    inline char *getString()
    {
        assertex(atr_type==t_string);
        atr_type = t_none;
        return str_val; 
    }
    inline __int64 getInt() 
    { 
        assertex(atr_type==t_int); 
        atr_type = t_none;
        return int_val; 
    }
    inline ITypeInfo *getType() 
    { 
        assertex(atr_type==t_type); 
        atr_type = t_none;
        return type; 
    }
    inline DefineIdSt* getDefineId()
    {
        assertex(atr_type==t_defineid);
        atr_type = t_none;
        return defineid;
    }
    inline node_operator getOperator() const
    {
        return queryExpr()->getOperator();
    }
    inline bool isDataset() const
    {
        return queryExpr()->isDataset();
    }
    inline bool isDatarow() const
    {
        return queryExpr()->isDatarow();
    }

    /* setters */
    inline void inherit(attribute & other)
    {
        *this = other;
        other.clear();
    }

    bool isZero() const;

    inline void setNullExpr() 
    { 
        atr_type=t_expr; 
        expr = NULL; 
        clearPosition();
    }

    inline void setExpr(IHqlExpression *v) 
    { 
        atr_type=t_expr; 
        expr = v; 
    }

    inline void setType(ITypeInfo *t) 
    { 
        atr_type=t_type; 
        type = t; 
    }
    inline void setContents(IFileContents * _contents) 
    {
        atr_type = t_contents;
        contents = _contents; 
    }
    inline void setName(_ATOM v) 
    { 
        atr_type=t_atom; 
        name = v; 
    }
    inline void setString(char *v) 
    { 
        atr_type=t_string; 
        str_val = v; 
    }
    inline void setInt(__int64 v) 
    { 
        atr_type=t_int; 
        int_val = v; 
    }
    inline void setDefineId(DefineIdSt* defid)
    {
        atr_type = t_defineid;
        defineid = defid;
    }

    inline void unwindCommaList(HqlExprArray & tgt)
    {
        IHqlExpression * e = getExpr();
        if (e)
            flattenListOwn(tgt, e);
    }

    inline void setExpr(IHqlExpression *v, const attribute &from) 
    { 
        setExpr(v);
        setPosition(from);
    }

    inline void setType(ITypeInfo *t, const attribute &from) 
    { 
        setType(t);
        setPosition(from);
    }

    attribute & release() 
    { 
        switch(atr_type)
        {
        case t_expr: 
            ::Release(expr); 
            break;
        case t_string: 
            free(str_val); 
            break;
        case t_type: 
            ::Release(type); 
            break;
        case t_none:
            break;
        case t_int:
            break;
        case t_atom:
            break;
        case t_contents:
            ::Release(contents);
            break;
        case t_defineid:
            delete defineid; 
            break;
        default:
            assertex(false);
        }
        //Don't need to release pos because it will be cleaned up properly anyway since a real member.
        atr_type = t_none;
        return *this;
    }
    inline void clear()
    {
        atr_type = t_none;
    }
    inline void clear(const attribute &from) 
    {
        clear();
        setPosition(from);
    }
    inline attribute()
    {
        atr_type = t_none;
    }
};

#define YYSTYPE attribute

class HqlLex;

struct ActiveScopeInfo : public CInterface
{
public:
    ActiveScopeInfo()       { firstSideEffect = 0; isParametered = false; }

    void newPrivateScope();
    void resetParameters()  { isParametered = false; activeParameters.kill(); activeDefaults.kill(); }
    IHqlExpression * createDefaults();
    IHqlExpression * createFormals(bool oldSetFormat);
    IHqlExpression * queryParameter(_ATOM name);

public:
    Owned<IHqlScope> localScope;
    Owned<IHqlScope> privateScope;
    unsigned firstSideEffect;
    //The following are used for the symbol currently being defined....  Here so correctly scoped.
    bool isParametered;
    HqlExprArray activeParameters;
    HqlExprArray activeDefaults;
    Owned<IHqlScope> templateAttrContext;
};


class TransformSaveInfo : public CInterface
{
public:
    Owned<IHqlScope> transformScope;
    Owned<IHqlExpression> curTransform;
    bool selfUsedOnRhs;
};

class FunctionCallInfo : public CInterface
{
public:
    FunctionCallInfo(IHqlExpression * _funcdef);

    void fillWithOmitted(unsigned next);
    void flushPendingComponents();
    IHqlExpression * getFinalActuals();

public:
    LinkedHqlExpr funcdef;
    HqlExprArray actuals;
    HqlExprArray pendingComponents;
    unsigned numFormals;
    bool hasActiveTopDataset;
    bool hadNamed;
};

class HqlExprArrayItem : public CInterface
{
public:
    HqlExprArray array;
};
typedef CIArrayOf<HqlExprArrayItem> HqlExprArrayArray;


class HqlGramCtx : public CInterface
{
public:
    HqlGramCtx(HqlLookupContext & _lookupCtx) : lookupCtx(_lookupCtx) {}
    bool hasAnyActiveParameters();
public:
    CIArrayOf<ActiveScopeInfo> defineScopes;
    IEclRepository *dataServer;
    HqlScopeArray defaultScopes;
    Owned<IHqlScope> globalScope;
    Linked<ISourcePath> sourcePath;
    HqlLookupContext lookupCtx;
    HqlExprArray imports;
};

typedef const IAtom * const * AtomList;
struct TokenMap;

class HqlGram;

extern int eclyyparse(HqlGram * parser);
class HqlGram : public CInterface, implements IErrorReceiver
{
    friend class HqlLex;
    friend int eclyyparse(HqlGram * parser);

public:
    HqlGram(HqlGramCtx &parent, IHqlScope * containerScope, IFileContents * text, IXmlScope *xmlScope);
    HqlGram(IHqlScope * _globalScope, IHqlScope * _containerScope, IFileContents * text, HqlLookupContext & _ctx, IXmlScope *xmlScope, bool _hasFieldMap, bool loadImplicit);
    virtual ~HqlGram();
    IMPLEMENT_IINTERFACE

    IHqlExpression *yyParse(bool parsingTemplateAttribute, bool catchAbort);
    void setQuery(bool _value) { isQuery = _value; }

    void yySetLexer(HqlLex *LexObject);
    HqlLex* getLexer() { return lexObject; }

    void saveContext(HqlGramCtx & ctx, bool cloneScopes);
    IHqlScope * queryGlobalScope();

    bool canFollowCurrentState(int tok, const short * yyps);
    void syntaxError(const char *s, int token, int *expected);
    int mapToken(int lexToken) const;
    IHqlExpression *lookupSymbol(_ATOM name, const attribute& errpos);
    IHqlExpression *lookupSymbol(IHqlScope * scope, _ATOM name);
    void enableAttributes(int activityToken);

    IHqlExpression * recordLookupInTemplateContext(_ATOM name, IHqlExpression * expr, IHqlScope * templateScope);
    void processImportAll(attribute & modulesAttr);
    void processImport(attribute & modulesAttr, _ATOM as);
    void processImport(attribute & membersAttr, attribute & modulesAttr, _ATOM as);
    void applyDefaultPromotions(attribute &a1);
    unsigned checkCompatible(ITypeInfo * a1, ITypeInfo * t2, const attribute &ea, bool complain=true);
    void checkMaxCompatible(IHqlExpression * sortOrder, IHqlExpression * values, attribute & errpos);
    void checkCompatibleTransforms(HqlExprArray & values, IHqlExpression * record, attribute & errpos);
    void checkBoolean(attribute &atr);
    void checkBooleanOrNumeric(attribute &atr);
    void checkDatarow(attribute &atr);
    void checkDataset(attribute &atr);
    void checkFieldnameValid(const attribute &errpos, _ATOM name);
    void checkList(attribute &atr);
    void checkScalar(attribute &atr);
    void checkUseLocation(const attribute & errpos);
    void checkCosort(IHqlExpression * sortlist, IHqlExpression * partition, const attribute & ea);
    ITypeInfo * checkPromoteNumeric(attribute &a1);
    ITypeInfo * checkPromoteNumericType(attribute &a1, attribute &a2);
    ITypeInfo * checkPromoteType(attribute &a1, attribute &a2);
    ITypeInfo * checkPromoteIfType(attribute &a1, attribute &a2);
    void checkSameType(attribute &a1, attribute &a2);
    void checkType(attribute &a1, ITypeInfo *t2);
    ITypeInfo *checkType(attribute &e1, attribute &e2);
    bool checkAlienTypeDef(IHqlScope* scope, const attribute& errpos);
    IHqlExpression* checkServiceDef(IHqlScope* serviceScope,_ATOM name, IHqlExpression* attrs, const attribute& errpos);
    void checkConstant(attribute & attr);
    IHqlExpression * checkConstant(const attribute & errpos, IHqlExpression * expr);
    void checkConstantEvent(attribute & attr);
    IHqlExpression * checkConcreteModule(const attribute & errpos, IHqlExpression * expr);
    void checkFoldConstant(attribute & attr);
    void checkInteger(attribute &e1);
    void checkPositive(attribute &e1);
    bool checkString(attribute &e1);
    bool checkStringOrUnicode(attribute &e1);
    void checkIntegerOrString(attribute & e1);
    void checkNumeric(attribute &e1);
    ITypeInfo *checkNumericGetType(attribute &e1);
    void checkLibraryParametersMatch(const attribute & errpos, bool isParametered, const HqlExprArray & activeParameters, IHqlExpression * definition);
    void checkReal(attribute &e1);
    ITypeInfo *checkStringIndex(attribute & strAttr, attribute & idxAttr);
    void checkWorkflowScheduling(IHqlExpression * expr, attribute& errpos);
    void checkWorkflowMultiples(IHqlExpression * currentWorkflow, IHqlExpression * newWorkflow, attribute& errpos);
    void checkJoinFlags(const attribute & err, IHqlExpression * joinExpr);
    void checkLoopFlags(const attribute & err, IHqlExpression * loopExpr);
    IHqlExpression * checkIndexRecord(IHqlExpression * record, const attribute & errpos);
    void checkIndexFieldType(IHqlExpression * cur, bool isPayload, bool insideNestedRecord, const attribute & errpos);
    void checkIndexRecordType(IHqlExpression * record, unsigned numPayloadFields, bool insideNestedRecord, const attribute & errpos);
    void checkIndexRecordTypes(IHqlExpression * index, const attribute & errpos);
    void reportIndexFieldType(IHqlExpression * expr, bool isPayload, const attribute & errpos);
    void reportUnsupportedFieldType(ITypeInfo * type, const attribute & errpos);
    void checkCaseForDuplicates(HqlExprArray & exprs, attribute &err);
    void checkOnFailRecord(IHqlExpression * expr, attribute & errpos);
    void checkAggregateRecords(IHqlExpression * expr, IHqlExpression * record, attribute & errpos);
    void checkExportedModule(const attribute & errpos, IHqlExpression * scopeExpr);
    bool checkCompatibleSymbol(const attribute & errpos, IHqlExpression * prevValue, IHqlExpression * newValue);
    IHqlExpression * createAveList(const attribute & errpos, IHqlExpression * list);
    IHqlExpression * createIff(attribute & condAttr, attribute & leftAttr, attribute & rightAttr);
    IHqlExpression * createListFromExpressionList(attribute & attr);
    IHqlExpression * createListIndex(attribute & list, attribute & which, IHqlExpression * attr);
    IHqlExpression * createNullPattern();
    IHqlExpression * createLibraryInstance(const attribute & errpos, IHqlExpression * name, IHqlExpression * func, HqlExprArray & actuals);
    IHqlExpression * createLocationAttr(const attribute & errpos);
    IHqlExpression * createSortExpr(node_operator op, attribute & dsAttr, const attribute & orderAttr, HqlExprArray & args);
    IHqlExpression * createIffDataset(IHqlExpression * record, IHqlExpression * value);

    bool convertAllToAttribute(attribute &atr);
    IHqlExpression * convertToOutOfLineFunction(const ECLlocation & errpos, IHqlExpression  * expr);

    void ensureBoolean(attribute &a);
    void ensureDataset(attribute & attr);
    void ensureString(attribute &a);
    void ensureTypeCanBeIndexed(attribute &a);
    void ensureUnicode(attribute &a);
    void ensureData(attribute &a);
    void ensureTransformTypeMatch(attribute & tattr, IHqlExpression * ds);
    bool checkTransformTypeMatch(const attribute & errpos, IHqlExpression * ds, IHqlExpression * transform);
    void ensureDatasetTypeMatch(attribute & tattr, IHqlExpression * ds);
    _ATOM ensureCommonLocale(attribute &a, attribute &b);
    void ensureUnicodeLocale(attribute & a, char const * locale);
    void ensureType(attribute &atr, ITypeInfo * type);
    void inheritRecordMaxLength(IHqlExpression * dataset, SharedHqlExpr & record);

    void normalizeExpression(attribute & expr);
    void normalizeExpression(attribute & expr, type_t expectedType, bool isConstant);

    IHqlExpression * createListFromExprArray(const attribute & errpos, HqlExprArray & args);
    IHqlExpression * normalizeExprList(const attribute & errpos, const HqlExprArray & values);

    bool isDiskFile(IHqlExpression * expr);
    bool isFilteredDiskFile(IHqlExpression * expr);
    bool isSaved(IHqlExpression * failure);
    bool okToAddSideEffects(IHqlExpression * expr);
    void processUpdateAttr(attribute & attr);
    IHqlExpression * createArithmeticOp(node_operator op, attribute &a1, attribute &a2);
    ITypeInfo *promoteToSameType(attribute &a1, attribute &a2);
    ITypeInfo *promoteToSameType(HqlExprArray & exprs, const attribute &ea, ITypeInfo * otherType, bool allowVariableLength);
    void promoteToSameCompareType(attribute &a1, attribute &a2, node_operator op);
    ITypeInfo *promoteToSameCompareType(attribute &a1, attribute &a2);
    void promoteToSameCompareType(attribute &a1, attribute &a2, attribute &a3);
    void promoteToSameListType(attribute & leftAttr, attribute & rightAttr);
    ITypeInfo *promoteCaseToSameType(attribute &eTest, HqlExprArray & exprs, attribute &eElse);
    ITypeInfo *promoteMapToSameType(HqlExprArray & exprs, attribute &eElse);
    ITypeInfo *promoteSetToSameType(HqlExprArray & exprs, attribute &errpos);
    ITypeInfo * queryElementType(const attribute & errpos, IHqlExpression * list);
    IHqlExpression *createINExpression(node_operator op, IHqlExpression *expr, IHqlExpression *set, attribute &errpos);
    IHqlExpression * createLoopCondition(IHqlExpression * left, IHqlExpression * arg1, IHqlExpression * arg2, IHqlExpression * seq);
    void setTemplateAttribute();
    void warnIfFoldsToConstant(IHqlExpression * expr, const attribute & errpos);
    void warnIfRecordPacked(IHqlExpression * expr, const attribute & errpos);
    void warnIfRecordPacked(const attribute & errpos) { warnIfRecordPacked(errpos.queryExpr(), errpos); }
    void validateParseTerminate(IHqlExpression * e, attribute & errpos);
    void validateXPath(attribute & a);

    void beginFunctionCall(attribute & function);
    IHqlExpression * endFunctionCall();
    void addActual(const attribute & errpos, IHqlExpression * ownedExpr);
    void addNamedActual(const attribute & errpos, _ATOM name, IHqlExpression * ownedExpr);
    bool processParameter(FunctionCallInfo & call, _ATOM name, IHqlExpression * actualValue, const attribute& errpos);
    void checkActualTopScope(FunctionCallInfo & call, IHqlExpression * formal, IHqlExpression * actual);
    void leaveActualTopScope(FunctionCallInfo & call);

    bool doCheckValidFieldValue(const attribute &errpos, IHqlExpression *value, IHqlExpression * field);
    bool checkValidFieldValue(const attribute &errpos, IHqlExpression *value, IHqlExpression * field);
    IHqlExpression * createCheckMatchAttr(attribute & attr, type_t tc);

    bool extractConstantString(StringBuffer & text, attribute & attr);

    //Various grammar rule productions.
    void beginAlienType(const attribute & errpos);
    void beginDefineId(_ATOM name, ITypeInfo * type);

    IHqlExpression * processAlienType(const attribute & errpos);
    IHqlExpression * processIndexBuild(attribute & indexAttr, attribute * recordAttr, attribute * payloadAttr, attribute & filenameAttr, attribute & flagsAttr);
    IHqlExpression * processCompoundFunction(attribute & result, bool outOfLine);
    IHqlExpression * processCppBody(const attribute & errpos, IHqlExpression * cpp);
    void processEnum(attribute & idAttr, IHqlExpression * value);
    void processError(bool full);
    void processLoadXML(attribute & a1, attribute * a2);
    IHqlExpression * processModuleDefinition(const attribute & errpos);
    IHqlExpression * processRowset(attribute & selectorAttr);
    void processServiceFunction(const attribute & errpos, _ATOM name, IHqlExpression * thisAttrs, ITypeInfo * type);
    void processStartTransform(const attribute & errpos);
    IHqlExpression * processUserAggregate(const attribute & mainPos, attribute & dsAttr, attribute & recordAttr, attribute & transformAttr, attribute * mergeAttr,
                                      attribute *itemsAttr, attribute &rowsAttr, attribute &seqAttr);

    void enterEnum(ITypeInfo * type);
    void enterService(attribute & attrs);
    IHqlExpression * leaveEnum(const attribute & errpos);
    IHqlExpression * leaveService(const attribute & errpos);

    IHqlExpression * nextEnumValue();

// Error handling
    void doReportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos);
    void reportError(int errNo, const attribute& a, const char* format, ...) __attribute__((format(printf, 4, 5)));
    void reportError(int errNo, const ECLlocation & pos, const char* format, ...) __attribute__((format(printf, 4, 5)));
    void reportMacroExpansionPosition(int errNo, HqlLex * lexer, bool isError);
    void reportErrorUnexpectedX(const attribute & errpos, _ATOM unexpected);

    // Don't use overloading: va_list is the same as char*!!
    void reportErrorVa(int errNo, const ECLlocation & a, const char* format, va_list args);
    void reportError(int errNo, const char *msg, int lineno, int column, int position=0);
    void reportWarning(int warnNo, const ECLlocation & pos, const char* format, ...) __attribute__((format(printf, 4, 5)));
    void reportWarningVa(int errNo, const attribute& a, const char* format, va_list args);
    void reportWarning(int warnNo, const char *msg, int lineno, int column);
    void addResult(IHqlExpression *query, const attribute& errpos);
    bool okToReportError(const ECLlocation & pos);
// interface IErrorReceiver
    virtual void reportError(int errNo, const char *msg, const char *filename=NULL, int lineno=0, int column=0, int pos=0);
    virtual void report(IECLError*);
    virtual void reportWarning(int warnNo, const char *msg, const char *filename=NULL, int lineno=0, int column=0, int pos=0);
    virtual size32_t errCount();
    virtual size32_t warnCount();
    
    IHqlExpression * findAssignment(IHqlExpression *field);
    void addAssignment(attribute &field, attribute &source);
    void addAssignment(const attribute & errpos, IHqlExpression * targetExpr, IHqlExpression * srcExpr);
    void addAssignall(IHqlExpression * record, IHqlExpression * src,const attribute& errpos);
    void addConditionalAssign(const attribute & errpos, IHqlExpression * self, IHqlExpression * leftSelect, IHqlExpression * rightSelect, IHqlExpression * field);
    void addConditionalRowAssign(const attribute & errpos, IHqlExpression * self, IHqlExpression * leftSelect, IHqlExpression * rightSelect, IHqlExpression * record);
    void checkAllAssigned(IHqlExpression * record, const attribute &errpos);
    void checkGrouping(const attribute & errpos, HqlExprArray & parms, IHqlExpression* record, IHqlExpression* groups);
    void checkGrouping(const attribute & errpos, IHqlExpression * dataset, IHqlExpression* record, IHqlExpression* groups);
    void checkFieldMap(IHqlExpression* map, attribute& errpos);
    IHqlExpression * createDistributeCond(IHqlExpression * left, IHqlExpression * right, const attribute & err, const attribute & seqAttr);
    IHqlExpression * addSideEffects(IHqlExpression * expr);
    IHqlExpression * associateSideEffects(IHqlExpression * expr, const ECLlocation & errpos);
    void clearSideEffects();
    bool sideEffectsPending() const;

    void checkAssignedNormalizeTransform(IHqlExpression * record, const attribute &errpos);
    void doCheckAssignedNormalizeTransform(HqlExprArray * assigns, IHqlExpression* select, IHqlExpression* targetSelect, IHqlExpression * cur, const attribute& errpos, bool & modified);

    bool checkValidBaseModule(const attribute & attr, SharedHqlExpr & expr);
    IHqlExpression * implementInterfaceFromModule(attribute & iAttr, attribute & mAttr, IHqlExpression * flags);
    IHqlExpression * implementInterfaceFromModule(const attribute & modpos, const attribute & ipos, IHqlExpression * implementModule, IHqlExpression * projectInterface, IHqlExpression * flags);

    DefineIdSt * createDefineId(int scope, ITypeInfo * ownedType);
    void enterCompoundObject();
    void leaveCompoundObject();
    void enterScope(IHqlScope * scope, bool allowExternal);
    void enterScope(bool allowExternal);
    void enterVirtualScope();
    void leaveScope(const attribute & errpos);
    IHqlExpression * leaveLamdaExpression(attribute & exprattr);
    IHqlScope * closeLeaveScope(const YYSTYPE & errpos);
    void enterPatternScope(IHqlExpression * pattern);
    void leavePatternScope(const YYSTYPE & errpos);
    bool insideNestedScope() const;

    void beginTransform(ITypeInfo * type);
    IHqlExpression *endTransform(const attribute &errpos);
    void openTransform(ITypeInfo * type);
    IHqlExpression *closeTransform(const attribute &errpos);
    void appendTransformAssign(IHqlExpression * transform, IHqlExpression * to, IHqlExpression * from, const attribute& errpos);
    void beginRecord();
    void addRecordOption(attribute & attr);

    void attachPendingWarnings(attribute & exprAttr) { if (pendingWarnings.ordinality()) doAttachPendingWarnings(exprAttr); }
    void doAttachPendingWarnings(attribute & exprAttr);
    IHqlExpression * attachPendingWarnings(IHqlExpression * ownedExpr);
    IHqlExpression * attachMetaAttributes(IHqlExpression * ownedExpr, HqlExprArray & meta);

    void addDatasetField(const attribute &errpos, _ATOM name, IHqlExpression * record, IHqlExpression *value, IHqlExpression * attrs);
    void addField(const attribute &errpos, _ATOM name, ITypeInfo *type, IHqlExpression *value, IHqlExpression *attrs);
    void addFields(const attribute &errpos, IHqlExpression *record, IHqlExpression * dataset, bool clone);
    void addIfBlockToActive(const attribute &errpos, IHqlExpression * ifblock);
    void addToActiveRecord(IHqlExpression * newField);
    void beginIfBlock();
    IHqlExpression * endIfBlock();

    IHqlExpression * expandedSortListByReference(attribute * module, attribute & list);
    IHqlExpression *bindParameters(const attribute & errpos, IHqlExpression * function, HqlExprArray & ownedActuals);
    IHqlExpression *bindParameters(attribute &a, IHqlExpression *parms);
    IHqlExpression *bindParameters(const attribute & errpos, IHqlExpression * func, IHqlExpression *parms);
    IHqlExpression* bindTemplateFunctionParameters(IHqlExpression* origFunc, HqlExprArray& actuals, const attribute& errpos);
    IHqlExpression* bindDatasetParameter(IHqlExpression* expr, IHqlExpression* formal, IHqlExpression* actual, const attribute& errpos);
    IHqlExpression* bindConcreteDataset(IHqlExpression* expr, IHqlExpression* formal, IHqlExpression* actual, const attribute& errpos);
    IHqlExpression* bindAbstractDataset(IHqlExpression* expr, IHqlExpression* formal, IHqlExpression* actual, const attribute& errpos);
    IHqlExpression* processAbstractDataset(IHqlExpression* _expr, IHqlExpression* formal, IHqlExpression* actual, IHqlExpression * mapping, const attribute& errpos, bool errorIfNotFound, bool & hadError);

    void enterType(const attribute &errpos, bool isParameteried);
    void leaveType(const YYSTYPE & errpos);
    int checkRecordTypes(IHqlExpression *left, IHqlExpression *right, attribute &atr, unsigned maxFields = (unsigned)-1);
    bool checkRecordCreateTransform(HqlExprArray & assigns, IHqlExpression *leftExpr, IHqlExpression *leftSelect, IHqlExpression *rightExpr, IHqlExpression *rightSelect, attribute &atr);
    IHqlExpression * checkEnsureRecordsMatch(IHqlExpression * left, IHqlExpression * right, attribute & errpos, bool rightIsRow);
    void checkRecordIsValid(attribute &atr, IHqlExpression *record);
    void checkValidRecordMode(IHqlExpression * dataset, attribute & atr, attribute & modeatr);
    void checkValidCsvRecord(const attribute & errpos, IHqlExpression * record);
    void checkValidPipeRecord(const attribute & errpos, IHqlExpression * record, IHqlExpression * attrs, IHqlExpression * expr);

    void createAppendFiles(attribute & targetAttr, attribute & leftAttr, attribute & rightAttr, _ATOM kind);
    IHqlExpression * processIfProduction(attribute & condAttr, attribute & trueAttr, attribute * falseAttr);

    IHqlExpression * createSymbolFromValue(IHqlExpression * primaryExpr, IHqlExpression * value);
    unsigned getMaxErrorsAllowed() { return m_maxErrorsAllowed; }
    void setMaxErrorsAllowed(unsigned n) { m_maxErrorsAllowed = n; } 
    void setAssociateWarnings(bool value) { associateWarnings = value; }
    IHqlExpression* clearFieldMap(IHqlExpression* expr);
    void setExpectedAttribute(_ATOM _expectedAttribute)             { expectedAttribute = _expectedAttribute; current_id = _expectedAttribute; }
    void setCurrentToExpected()             { current_id = expectedAttribute; }
    IHqlScope * queryPrimaryScope(bool isPrivate);
    unsigned nextParameterIndex()               { return 0; } // not commoned up at moment{ return activeParameters.length()+savedParameters.length(); }
    void addActiveParameterOwn(const attribute & errpos, IHqlExpression * expr, IHqlExpression * defaultValue);
    void gatherActiveParameters(HqlExprCopyArray & target);


    IHqlExpression * createUniqueId();  
    IHqlExpression * doCreateUniqueSelectorSequence();

    void onOpenBra();
    void onCloseBra();

    int yyLex(attribute * yylval, const short * activeState);

protected:
    _ATOM createUnnamedFieldName();
    _ATOM createUnnamedFieldName(const char * prefix);
    _ATOM getNameFromExpr(attribute& attr);
    _ATOM createFieldNameFromExpr(IHqlExpression * expr);
    IHqlExpression * createAssert(attribute & cond, attribute * msg, attribute & flags);

    void defineImport(const attribute & errpos, IHqlExpression * imported, _ATOM newName);
    IHqlExpression * resolveImportModule(const attribute & errpos, IHqlExpression * expr);

    void setActiveAttrs(int activityToken, const TokenMap * attrs);

    IHqlExpression *doParse();
    IHqlExpression * checkBuildIndexRecord(IHqlExpression *record, attribute & errpos);
    void checkNotAlreadyDefined(_ATOM name, IHqlScope * scope, const attribute & idattr);
    void checkNotAlreadyDefined(_ATOM name, const attribute & idattr);
    void checkBuildIndexFilenameFlags(IHqlExpression * dataset, attribute & flags);
    IHqlExpression * createBuildFileFromTable(IHqlExpression * table, attribute & flagsAttr, IHqlExpression * filename, attribute & errpos);
    IHqlExpression * createBuildIndexFromIndex(attribute & indexAttr, attribute & flagsAttr, IHqlExpression * filename, attribute & errpos);
    void checkOutputRecord(attribute & errpos, bool outerLevel);
    void checkSoapRecord(attribute & errpos);
    IHqlExpression * checkOutputRecord(IHqlExpression *record, const attribute & errpos, bool & allConstant, bool outerLevel);
    void doAddAssignment(IHqlExpression * transform, IHqlExpression * field, IHqlExpression * rhs, const attribute& errpos);
    void doAddAssignall(IHqlExpression* assignall, IHqlExpression *tgt, IHqlExpression *src,const attribute& errpos);
    void doAddAssignSelf(IHqlExpression* assignall, IHqlExpression *tgt, IHqlExpression *src,const attribute& errpos);
    void doAddAssignCompound(IHqlExpression * assignall, IHqlExpression * target, IHqlExpression * src, IHqlExpression * record, const attribute& errpos);
    void doAddAssignCompoundOwn(IHqlExpression * assignall, IHqlExpression * target, IHqlExpression * src, IHqlExpression * record, const attribute& errpos);
    IHqlExpression * doFindAssignment(IHqlExpression *in, IHqlExpression *field);
    IHqlExpression * replaceSelfReferences(IHqlExpression * transform, IHqlExpression * rhs, IHqlExpression * self, const attribute& errpos);

    void appendToActiveScope(IHqlExpression * arg);
    bool isVirtualFunction(DefineIdSt * defineid, const attribute & errpos);
    
    IHqlExpression * processSortList(const attribute & errpos, node_operator op, IHqlExpression * dataset, HqlExprArray & items, OwnedHqlExpr *joinedClause, OwnedHqlExpr *attributes);
    void expandSortedAsList(HqlExprArray & args);
    bool expandWholeAndExcept(IHqlExpression * dataset, const attribute & errpos, HqlExprArray & parms);
    void expandWholeAndExcept(IHqlExpression * dataset, attribute & a);
    void cleanCurTransform();
    void unwindSelect(IHqlExpression* expr, HqlExprArray& r);
    void setSelfUsedOnRhs();
    void setDefaultString(attribute &a);

    void canNotAssignTypeError(ITypeInfo* expected, ITypeInfo* given, const attribute& errpos);
    void canNotAssignTypeWarn(ITypeInfo* expected, ITypeInfo* given, const attribute& errpos);
    void abortParsing();
    bool isExceptionalCase(attribute& defineid, attribute& object, attribute& failure);
    void checkSvcAttrNoValue(IHqlExpression* attr, const attribute& errpos);
    void checkFormals(_ATOM name, HqlExprArray & parms, HqlExprArray & defaults, attribute& object);
    IHqlExpression * checkParameter(const attribute * errpos, IHqlExpression * actual, IHqlExpression * formal, bool isDefault, IHqlExpression * funcdef);
    void checkDedup(IHqlExpression *ds, IHqlExpression *flags, attribute &errpos);
    void addParameter(const attribute & errpos, _ATOM name, ITypeInfo* type, IHqlExpression* defValue);
    void addFunctionParameter(const attribute & errpos, _ATOM name, ITypeInfo* type, IHqlExpression* defValue);
    void addFunctionProtoParameter(const attribute & errpos, _ATOM name, IHqlExpression * like, IHqlExpression* defValue);
    bool checkParameters(IHqlExpression* func, HqlExprArray& actuals, const attribute& errpos);
    bool checkTemplateFunctionParameters(IHqlExpression* func, HqlExprArray& actuals, const attribute& errpos);
    void checkSizeof(IHqlExpression* expr, attribute& errpos);
    void checkSizeof(ITypeInfo* expr, attribute& errpos, bool isDataset = false);
    void normalizeStoredNameExpression(attribute & a);
    void checkPatternFailure(attribute & attr);
    void checkDistributer(attribute & err, HqlExprArray & args);
    IHqlExpression * createScopedSequenceExpr();
    IHqlExpression * createPatternOr(HqlExprArray & args, const attribute & errpos);
    IHqlExpression * mapAlienArg(IHqlSimpleScope * scope, IHqlExpression * expr);
    ITypeInfo * mapAlienType(IHqlSimpleScope * scope, ITypeInfo * type, const attribute & errpos);

    void disableError() { errorDisabled = true; }
    void enableError() { errorDisabled = false; }
    bool isAborting() { return errorDisabled; }
    _ATOM fieldMapTo(IHqlExpression* expr, _ATOM name);
    _ATOM fieldMapFrom(IHqlExpression* expr, _ATOM name);
    bool requireLateBind(IHqlExpression* funcdef, Array& actuals);
    IHqlExpression* createDefJoinTransform(IHqlExpression* left,IHqlExpression* right,attribute& errpos, IHqlExpression * seq, IHqlExpression * flags);
    IHqlExpression * createRowAssignTransform(const attribute & srcAttr, const attribute & tgtAttr, const attribute & seqAttr);
    IHqlExpression * createClearTransform(IHqlExpression * record, const attribute & errpos);
    IHqlExpression * createDefaultAssignTransform(IHqlExpression * record, IHqlExpression * rowValue, const attribute & errpos);
    IHqlExpression * createDefaultProjectDataset(IHqlExpression * record, IHqlExpression * src, const attribute & errpos);
    IHqlExpression * createDatasetFromList(attribute & listAttr, attribute & recordAttr);

    void checkConditionalAggregates(_ATOM name, IHqlExpression * value, const attribute & errpos);
    void checkProjectedFields(IHqlExpression * e, attribute & errpos);
    IHqlExpression * createRecordFromDataset(IHqlExpression * ds);
    IHqlExpression * cleanIndexRecord(IHqlExpression * record);
    IHqlExpression * createRecordIntersection(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createRecordUnion(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createRecordDifference(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createRecordExcept(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createIndexFromRecord(IHqlExpression * record, IHqlExpression * attr, const attribute & errpos);
    IHqlExpression * createProjectRow(attribute & rowAttr, attribute & transformAttr, attribute & seqAttr);
    void doDefineSymbol(DefineIdSt * defineid, IHqlExpression * expr, IHqlExpression * failure, const attribute & idattr, int assignPos, int semiColonPos, bool isParametered);
    void defineSymbolInScope(IHqlScope * scope, DefineIdSt * defineid, IHqlExpression * expr, IHqlExpression * failure, const attribute & idattr, int assignPos, int semiColonPos, bool isParametered, HqlExprArray & parameters, IHqlExpression * defaults);
    void checkDerivedCompatible(_ATOM name, IHqlExpression * scope, IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, attribute const & errpos);
    void defineSymbolProduction(attribute & nameattr, attribute & paramattr, attribute & assignattr, attribute * valueattr, attribute * failattr, attribute & semiattr);
    void definePatternSymbolProduction(attribute & nameattr, const attribute & assignAttr, attribute & valueAttr, attribute & workflowAttr, const attribute & semiattr);
    void cloneInheritedAttributes(IHqlScope * scope, const attribute & errpos);

    IHqlExpression * createEvaluateOutputModule(const attribute & errpos, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, node_operator outputOp);
    IHqlExpression * createStoredModule(const attribute & errpos, IHqlExpression * scopeExpr);
    void processForwardModuleDefinition(const attribute & errpos);
    void checkNonGlobalModule(const attribute & errpos, IHqlExpression * scopeExpr);

    const HqlExprArray & queryActiveParameters()    { return defineScopes.tos().activeParameters; }
    bool queryParametered()                         { return defineScopes.tos().isParametered; }
    void resetParameters()                          { defineScopes.tos().resetParameters(); }
    void setParametered(bool value)                 { defineScopes.tos().isParametered = value; }
    inline void setDotScope(IHqlExpression * expr)  { dotScope.set(expr); }

    IHqlScope * queryTemplateContext();
    bool insideTemplateFunction() { return queryTemplateContext() != NULL; }
    inline const char * querySourcePathText()   { return sourcePath->str(); } // safe if null

    bool areSymbolsCompatible(IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, IHqlExpression * prevValue);
    IHqlExpression * extractBranchMatch(const attribute & errpos, IHqlExpression & curSym, HqlExprArray & values);
    ITypeInfo * extractBranchMatches(const attribute & errpos, IHqlExpression & curSym, HqlExprArrayArray & branches, HqlExprArray & extracted);
    void expandScopeEntries(HqlExprArrayArray & branches, IHqlExpression * scope);
    void processIfScope(const attribute & errpos, IHqlExpression * cond, IHqlExpression * trueScope, IHqlExpression * falseScope);

    void appendTransformOption(IHqlExpression * expr) 
    { 
        if (curTransform)
            curTransform->addOperand(expr);
        else 
            expr->Release();
    }
    void restoreTypeFromActiveTransform()
    {
        //Now restore the active "global" variables.
        current_type = curTransform->queryType();
    }

protected:
    bool errorDisabled;
    bool parsingTemplateAttribute;
    bool expectedUnknownId;
    bool insideEvaluate;
    bool fieldMapUsed;
    bool resolveSymbols;
    bool forceResult;
    bool associateWarnings;
    bool legacyEclSemantics;
    bool isQuery;
    unsigned m_maxErrorsAllowed;

    IECLErrorArray pendingWarnings;
    Linked<ISourcePath> sourcePath;
    _ATOM moduleName;
    _ATOM current_id;
    _ATOM expectedAttribute;
    int current_flags;
    IHqlScope *transformScope;
    PointerArray savedIds;
    UnsignedArray savedLastpos;
    unsigned lastpos;
    bool inType;
    Owned<IHqlScope> modScope;
    OwnedHqlExpr dotScope;
    unsigned outerScopeAccessDepth;
    IHqlScope* containerScope;
    IHqlScope* globalScope;
    ITypeInfo *current_type;
    HqlExprArray topScopes;
    HqlExprArray leftScopes;
    HqlExprArray rightScopes;
    HqlExprArray rowsScopes;
    HqlExprArray rowsIds;
    HqlExprArray selfScopes;
    HqlExprArray localeStack;
    HqlExprArray localFunctionCache;
    HqlExprArray curListStack;
    CIArrayOf<OwnedHqlExprItem> counterStack;
    CIArrayOf<TransformSaveInfo> transformSaveStack;
    CIArrayOf<ActiveScopeInfo> defineScopes;
    OwnedHqlExpr curList;
    BoolArray wasInEvaluate;
    HqlExprAttr curDatabase;
    unsigned curDatabaseCount;
    HqlExprCopyArray activeRecords;
    HqlExprArray activeIfBlocks;
    HqlLex *lexObject;
    HqlExprArray parseResults;
    IErrorReceiver *errorHandler;
    IHqlExpression *curTransform;
    ITypeInfo * defaultIntegralType;
    ITypeInfo * uint4Type;
    ITypeInfo * defaultRealType;
    ITypeInfo * boolType;
    IEclRepository *dataServer;
    HqlScopeArray defaultScopes;
    PointerArray savedType;
    HqlExprAttr curFeatureParams;
    HqlExprCopyArray implicitFeatureNames;
    HqlExprCopyArray implicitFeatureValues;
    HqlExprArray activeSelectorSequences;
    Owned<IHqlScope> parseScope;
    HqlExprAttr lastEnumValue;
    Owned<ITypeInfo> curEnumType;
    unsigned sortDepth;
    Owned<IHqlScope> serviceScope;
    HqlLookupContext lookupCtx;
    HqlExprAttr defaultServiceAttrs;
    CIArrayOf<FunctionCallInfo> activeFunctionCalls;
    OwnedHqlExpr serviceExtraAttributes;
    ConstPointerArray validAttributesStack;
    unsigned minimumScopeIndex;
    const TokenMap * pendingAttributes;
    bool selfUsedOnRhs;
    bool aborting;

    void setIdUnknown(bool expected) { expectedUnknownId = expected; }
    bool getIdUnknown() { return expectedUnknownId; }
    void init(IHqlScope * _globalScope, IHqlScope * _containerScope);
    void addProperty(const char *prop, const char *val);
    IHqlExpression * createSelect(IHqlExpression * lhs, IHqlExpression * rhs, const attribute & errpos);
    IHqlExpression * createIndirectSelect(IHqlExpression * lhs, IHqlExpression * rhs, const attribute & errpos);
    IHqlExpression * addDatasetSelector(IHqlExpression * lhs, IHqlExpression * rhs);

    void pushTopScope(IHqlExpression *);
    void pushLeftScope(IHqlExpression *);
    void pushRightScope(IHqlExpression *);
    void pushRowsScope(IHqlExpression *);
    void pushSelfScope(IHqlExpression *);
    void pushSelfScope(ITypeInfo * selfType);
    void pushSelectorSequence(IHqlExpression * left, IHqlExpression * right);
    void pushUniqueSelectorSequence();
    IHqlExpression * popSelectorSequence();
    IHqlExpression * createActiveSelectorSequence(IHqlExpression * left, IHqlExpression * right);

    IHqlExpression * getSelectorSequence();
    IHqlExpression * forceEnsureExprType(IHqlExpression * expr, ITypeInfo * type);

    void popTopScope();
    void popLeftScope();
    void popRightScope();
    IHqlExpression * popRowsScope();
    void popSelfScope();
    void swapTopScopeForLeftScope();

    void beginList();
    void addListElement(IHqlExpression * expr);
    void endList(HqlExprArray & args);

    void pushLocale(IHqlExpression *);
    void popLocale();
    IHqlExpression *queryDefaultLocale();

    IHqlExpression * getActiveCounter(attribute & errpos);
    void pushRecord(IHqlExpression *);
    IHqlExpression *popRecord();
    IHqlExpression *queryTopScope();
    ITypeInfo * getPromotedECLType(HqlExprArray & args, ITypeInfo * otherType, bool allowVariableLength);
    IHqlExpression *getTopScope();
    IHqlExpression *queryLeftScope();
    IHqlExpression *queryRightScope();
    IHqlExpression *queryRowsScope();
    IHqlExpression *getLeftScope();
    IHqlExpression *getRightScope();
    IHqlExpression *getSelfScope();
    IHqlExpression *getSelfDotExpr(const attribute & errpos);
    IHqlExpression *resolveRows(const attribute & errpos, IHqlExpression * ds);
    void releaseScopes();
    static void simplify(int *expected, int first, ...);
    static void simplifyExpected(int *expected);
    static bool isExplicitlyDistributed(IHqlExpression *e);
    void checkMergeSortOrder(attribute &errpos, IHqlExpression *ds1, IHqlExpression *ds2, IHqlExpression * sortOrder);
    void checkDistribution(attribute &errpos, IHqlExpression *input, bool localSpecified, bool ignoreGrouping);
    void checkDistribution(attribute &errpos, IHqlExpression *newExpr, bool ignoreGrouping);
    void checkMergeInputSorted(attribute &atr, bool isLocal);
    void checkGrouped(attribute & atr);
    void checkRegrouping(attribute & atr, HqlExprArray & args);
    void checkRecordsMatch(attribute & atr, HqlExprArray & args);

    IHqlExpression * transformRecord(IHqlExpression *dataset, _ATOM targetCharset, const attribute & errpos);
    IHqlExpression * transformRecord(IHqlExpression *record, _ATOM targetCharset, IHqlExpression * scope, bool & changed, const attribute & errpos);
    IHqlExpression * translateFieldsToNewScope(IHqlExpression * expr, IHqlSimpleScope * record, const attribute & err);

    ITypeInfo *queryCurrentRecordType();
    ITypeInfo *queryCurrentTransformType();
    IHqlExpression *queryCurrentTransformRecord();
    IHqlExpression* queryFieldMap(IHqlExpression* expr);
    IHqlExpression* bindFieldMap(IHqlExpression*, IHqlExpression*);
    void applyPayloadAttribute(const attribute & errpos, IHqlExpression * record, SharedHqlExpr & extra);
    void extractRecordFromExtra(SharedHqlExpr & record, SharedHqlExpr & extra);
    void transferOptions(attribute & filenameAttr, attribute & optionsAttr);
    IHqlExpression * extractTransformFromExtra(SharedHqlExpr & extra);
    void expandPayload(HqlExprArray & fields, IHqlExpression * payload, IHqlSimpleScope * scope, ITypeInfo * & lastFieldType, const attribute & errpos);
    void modifyIndexPayloadRecord(SharedHqlExpr & record, SharedHqlExpr & payload, SharedHqlExpr & extra, const attribute & errpos);

    bool haveAssignedToChildren(IHqlExpression * select);
    void checkPattern(attribute & pattern, bool isCompound);
    void checkSubPattern(attribute & pattern);
    void checkPattern(attribute & pattern, HqlExprArray & values);
    ITypeInfo * getCompoundRuleType(IHqlExpression * lhs);
    ITypeInfo * getCompoundRuleType(ITypeInfo * lType, ITypeInfo * rType);
    IHqlExpression * convertPatternToExpression(attribute & text);
    void checkProduction(const HqlExprArray & args, const attribute & errpos);
    IHqlExpression * processExprInPattern(attribute & attr);

    IHqlExpression * getFeatureParams();
    IHqlExpression * deduceGuardFeature(IHqlExpression * expr, attribute & errpos);
    void expandImplicitFeatures();
    void expandImplicitFeatures(IHqlExpression * feature, IHqlExpression * value);
    IHqlExpression * findFeature(IHqlExpression * value);
    void setFeatureParamsOwn(IHqlExpression * expr);
};



#ifndef YY_TYPEDEF_YY_SCANNER_T
#define YY_TYPEDEF_YY_SCANNER_T
typedef void* yyscan_t;
#endif

class HqlLex
{
    public:
        HqlLex(HqlGram *gram, IFileContents * _text, IXmlScope *xmlScope, IHqlExpression *macroExpr);
        ~HqlLex();   

        static int doyyFlex(YYSTYPE & returnToken, yyscan_t yyscanner, HqlLex * lexer, bool lookup, const short * activeState);
        static int lookupIdentifierToken(YYSTYPE & returnToken, HqlLex * lexer, bool lookup, const short * activeState, const char * tokenText);

        int yyLex(YYSTYPE & returnToken, bool lookup, const short * activeState);    /* lexical analyzer */

        bool assertNext(YYSTYPE & returnToken, int expected, unsigned code, const char * msg);
        bool assertNextOpenBra();
        bool assertNextComma();

        void set_yyLineNo(int lineno) { yyLineNo = lineno; }
        void set_yyColumn(int column) { yyColumn = column; }
        int get_yyLineNo(void) { return yyLineNo; }
        int get_yyColumn(void) { return yyColumn; }
        IFileContents* query_FileContents(void);

        // report error line/column for macro
        int getActualLineNo(void) { return (inmacro) ? inmacro->getActualLineNo() : yyLineNo; }
        int getActualColumn(void) { return (inmacro) ? inmacro->getActualColumn() : yyColumn; }

        // yyPosition handles directly to buffer. Don't mess with it!
        int get_yyPosition(void) { return yyPosition; }

        /* push back a string to the input */
        HqlLex*  getMacroLex() { return inmacro; }
        char *get_yyText(void);
        StringBuffer &getTokenText(StringBuffer &);
        HqlLex* getParentLex() { return parentLex; }
        void setParentLex(HqlLex* pLex) { parentLex = pLex; }
        const char* getMacroName() { return (macroExpr) ? macroExpr->queryName()->str() : "<param>"; }
        IPropertyTree * getClearJavadoc();

        void loadXML(const YYSTYPE & errpos, const char * value, const char * child = NULL);

        void getPosition(ECLlocation & pos)
        {
            if (inmacro)
                inmacro->getPosition(pos);
            else
                pos.set(yyLineNo, yyColumn, yyPosition, sourcePath);
        }
        inline IFileContents * queryFileContents() { return text; } 
        inline ISourcePath * querySourcePath() { return sourcePath; }
        inline int queryLastToken() const { return lastToken; }

        ISourcePath * queryActualSourcePath()
        {
            if (inmacro)
            {
                ISourcePath * macName = inmacro->queryActualSourcePath();
                if (macName)
                    return macName;
            }
            return sourcePath;
        }

        inline void setTokenPosition(YYSTYPE & returnToken)
        {
            returnToken.setPosition(yyLineNo, yyColumn, yyPosition, sourcePath);
        }

        inline void updatePosition(unsigned delta)
        {
            yyPosition += delta;
            yyColumn += delta;
        }

        inline void updateNewline()
        {
            yyColumn = 1;
            ++yyLineNo;
        }

        void pushText(const char *);

    protected:
        void init(IFileContents * _text);

    private:
        void declareXmlSymbol(const YYSTYPE & errpos, const char *name);
        StringBuffer &lookupXmlSymbol(const YYSTYPE & errpos, const char *name, StringBuffer &value);
        void setXmlSymbol(const YYSTYPE & errpos, const char *name, const char *value, bool append);
        IIterator *getSubScopes(const YYSTYPE & errpos, const char *name, bool doAll);
        IXmlScope *queryTopXmlScope();
        IXmlScope *ensureTopXmlScope(const YYSTYPE & errpos);

        IHqlExpression *lookupSymbol(_ATOM name, const attribute& errpos);
        void reportError(const YYSTYPE & returnToken, int errNo, const char *format, ...) __attribute__((format(printf, 4, 5)));
        void reportWarning(const YYSTYPE & returnToken, int warnNo, const char *format, ...) __attribute__((format(printf, 4, 5)));

        void beginNestedHash(unsigned kind) { hashendKinds.append(kind); hashendDepths.append(1); }
        unsigned endNestedHash() { hashendKinds.pop(); return hashendDepths.pop(); }
        void clearNestedHash() { hashendKinds.kill(); hashendDepths.kill(); }

        inline bool parserExpecting(int tok, const short * activeState)
        {
            return yyParser->canFollowCurrentState(tok, activeState);
        }
        inline int mapToken(int tok)
        {
            return yyParser->mapToken(tok);
        }
        inline void onOpenBra() { yyParser->onOpenBra(); }
        inline void onCloseBra() { yyParser->onCloseBra(); }
        inline ISourcePath * querySourcePath() const { return sourcePath; }

        bool isMacroActive(IHqlExpression *expr);
        bool isAborting();
        void pushMacro(IHqlExpression *expr);
        void pushText(const char *s, int startLineNo, int startColumn);
        bool getParameter(StringBuffer &curParam, const char* for_what, int* startLine=NULL, int* startCol=NULL);
        IValue *parseConstExpression(const YYSTYPE & errpos, StringBuffer &curParam, IXmlScope *xmlScope, int line, int col);
        IHqlExpression * parseECL(StringBuffer &curParam, IXmlScope *xmlScope, int startLine, int startCol);
        void setMacroParam(const YYSTYPE & errpos, IHqlExpression* funcdef, StringBuffer& curParam, _ATOM argumentName, unsigned& parmno,IProperties *macroParms);
        unsigned getTypeSize(unsigned lengthTypeName);

        void doPreprocessorLookup(const YYSTYPE & errpos, bool stringify, int extra);
        void doApply(YYSTYPE & returnToken);
        int doElse(YYSTYPE & returnToken, bool lookup, const short * activeState, bool isElseIf);
        void doExpand(YYSTYPE & returnToken);
        void doTrace(YYSTYPE & returnToken);
        void doError(YYSTYPE & returnToken, bool isError);
        void doExport(YYSTYPE & returnToken, bool toXml);
        void doFor(YYSTYPE & returnToken, bool doAll);
        int doHashText(YYSTYPE & returnToken);
        void doLoop(YYSTYPE & returnToken);
        void doIf(YYSTYPE & returnToken);
        void doSet(YYSTYPE & returnToken, bool _append);
        void doLine(YYSTYPE & returnToken);
        void doDeclare(YYSTYPE & returnToken);
        void doDefined(YYSTYPE & returnToken);
        void doGetDataType(YYSTYPE & returnToken);
        bool doIsDefined(YYSTYPE & returnToken);
        void doIsValid(YYSTYPE & returnToken);
        void doInModule(YYSTYPE & returnToken);
        void doMangle(YYSTYPE & returnToken, bool de);
        void doUniqueName(YYSTYPE & returnToken);
        void processEncrypted();

        void declareUniqueName(const char* name, const char * pattern);
        void checkNextLoop(const YYSTYPE & errpos, bool first,int startLine,int startCol);

        bool getDefinedParameter(StringBuffer &curParam, YYSTYPE & returnToken, const char* for_what, SharedHqlExpr & resolved);

        bool checkUnicodeLiteral(char const * str, unsigned length, unsigned & ep, StringBuffer & msg);

private:
        HqlGram *yyParser;
        Owned<IFileContents> text;
        Linked<ISourcePath> sourcePath;
        
        HqlLex *inmacro;

        /* to handle recursive macro */
        HqlLex *parentLex;

        IProperties *macroParms;
        IIterator *forLoop;
        IHqlExpression *macroExpr;
        StringBuffer forBody;
        StringBuffer forFilter;

        IXmlScope *xmlScope;

        enum { HashStmtNone, HashStmtFor, HashStmtForAll, HashStmtLoop, HashStmtIf };
        int lastToken;
        int macroGathering;
        int skipping;
        UnsignedArray hashendDepths;
        UnsignedArray hashendKinds;
        bool hasHashbreak;
        int loopTimes;

        bool inComment;
        bool inCpp;
        bool encrypted;
        StringBuffer javaDocComment;

        yyscan_t scanner;

        int yyLineNo;
        int yyColumn;
        int yyPosition;
        int yyStartPos;
        char *yyBuffer;

        static unsigned hex2digit(char c);
        static __int64 str2int64(unsigned len, const char * digits, unsigned base);
        static void hex2str(char * target, const char * digits, unsigned len);
};


IHqlExpression *reparseTemplateFunction(IHqlExpression * funcdef, IHqlScope *scope, HqlLookupContext & ctx, bool hasFieldMap);
extern HQL_API void resetLexerUniqueNames();        // to make regression suite consistent
extern HQL_API void testHqlInternals();

#endif
