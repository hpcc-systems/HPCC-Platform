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

#define EXPORT_FLAG 1
#define VIRTUAL_FLAG 2
#define SHARED_FLAG 4

#define REC_FLD_ERR_STR "Need to supply a value for field '%s'"
#define ERR_WRONGSCOPING_ERR_STR "Value for field '%s' cannot be computed in this scope"

//Not exported from hql.so
extern ITypeInfo * defaultIntegralType;
extern ITypeInfo * uint4Type;
extern ITypeInfo * defaultRealType;
extern ITypeInfo * boolType;

//The following flags control what processing is applied when the lexer matches a particular token.  Should
//it set up the associated type/expression, or should it only return the token type.
//Macro gathering and other processing only care about the span of the matched token, not the the meaning.
enum LexerFlags : unsigned
{
    LEXnone         = 0x00000,
    LEXresolve      = 0x00001,  // Should identifiers be resolved to expressions or keywords
    LEXidentifier   = 0x00002,  // Should identifiers create atoms or not
    LEXstring       = 0x00004,  // Do string constants create IHqlExpressions
    LEXnumeric      = 0x00008,  // Do numeric tokens evaluate their value?
    LEXembed        = 0x00010,  // Create strings for the body of embedded code
    LEXexpand       = 0x00020,  // process #expand

    LEXall          = 0xFFFFFFFF,
    LEXnoresolve    = (LEXall & ~LEXresolve)
};
BITMASK_ENUM(LexerFlags);


struct DefineIdSt
{
private: 
    ITypeInfo* type;
    IPropertyTree * doc;
public:
    IIdAtom *      id;
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
        IIdAtom *           name;
        IIdAtom *     cname;
        char           *str_val;
        __int64         int_val;
        DefineIdSt     *defineid;
        IFileContents  *contents;
    };
    enum { t_none,t_expr,t_scope,t_atom,t_catom,t_string,t_int,t_type,t_defineid,t_contents } atr_type;
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
    inline IIdAtom * queryId() const
    {
        assertex(atr_type==t_catom);
        return cname;
    }
    
    /* getters */
    inline IFileContents * getContents()
    {
        assertex(atr_type==t_contents);         
        atr_type = t_none;
        return contents; 
    }
    inline IIdAtom * getId()
    {
        assertex(atr_type==t_catom);
        atr_type = t_none;
        return cname;
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
    inline bool isDictionary() const
    {
        return queryExpr()->isDictionary();
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
    inline void setId(IIdAtom * v)
    {
        atr_type=t_catom;
        cname = v;
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
        case t_catom:
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

#if !defined(BISON_MAJOR_VER) || BISON_MAJOR_VER == 2
#define YYSTYPE attribute
#else
#define ECLYYSTYPE attribute
#endif

class HqlLex;

struct ActiveScopeInfo : public CInterface
{
public:
    ActiveScopeInfo()       { firstSideEffect = 0; isParametered = false; }

    void newPrivateScope();
    void resetParameters()  { isParametered = false; activeParameters.kill(); activeDefaults.kill(); }
    IHqlExpression * createDefaults();
    IHqlExpression * createFormals(bool oldSetFormat);
    IHqlExpression * queryParameter(IIdAtom * name);

public:
    Owned<IHqlScope> localScope;
    Owned<IHqlScope> privateScope;
    unsigned firstSideEffect;
    //The following are used for the symbol currently being defined....  Here so correctly scoped.
    bool isParametered;
    bool legacyOnly = false;
    HqlExprArray activeParameters;
    HqlExprArray activeDefaults;
    Owned<IHqlScope> templateAttrContext;
};

class SelfReferenceReplacer;
class TransformSaveInfo : public CInterface
{
public:
    Owned<IHqlScope> transformScope;
    Owned<IHqlExpression> curTransform;
    OwnedHqlExpr transformRecord;
    Owned<SelfReferenceReplacer> selfReplacer;
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
    HqlGramCtx(HqlLookupContext & _lookupCtx, bool _inSignedModule)
      : lookupCtx(_lookupCtx), inSignedModule(_inSignedModule)
    {
    }
    bool hasAnyActiveParameters();
    void clearParentContainer() { lookupCtx.clearParentContainer(); }
public:
    CIArrayOf<ActiveScopeInfo> defineScopes;
    HqlScopeArray defaultScopes;
    HqlScopeArray implicitScopes;
    Owned<IHqlScope> globalScope;
    Linked<ISourcePath> sourcePath;
    HqlLookupContext lookupCtx;
    HqlExprArray imports;
    bool inSignedModule;
    bool legacyImport = false;
    bool legacyWhen = false;
};

typedef const IAtom * const * AtomList;
struct TokenMap;

class HqlGram;

class LeftRightScope : public CInterface
{
public:
    OwnedHqlExpr left;
    OwnedHqlExpr right;
    OwnedHqlExpr selSeq;
    OwnedHqlExpr rowsScope;
    OwnedHqlExpr rowsId;
};

extern int eclyyparse(HqlGram * parser);
class HqlGram : implements IErrorReceiver, public CInterface
{
    friend class HqlLex;
    friend class SelfReferenceReplacer;
    friend int eclyyparse(HqlGram * parser);

public:
    HqlGram(HqlGram & container, IHqlScope * containerScope, IFileContents * text, IXmlScope *xmlScope); // parse a constant expression
    HqlGram(HqlGramCtx &parent, IHqlScope * containerScope, IFileContents * text, IXmlScope *xmlScope);  // parse a forward declaration
    HqlGram(IHqlScope * _globalScope, IHqlScope * _containerScope, IFileContents * text, HqlLookupContext & _ctx, IXmlScope *xmlScope, bool _hasFieldMap, bool loadImplicit);
    virtual ~HqlGram();
    IMPLEMENT_IINTERFACE

    IHqlExpression *yyParse(bool parsingTemplateAttribute, bool catchAbort);
    void setQuery(bool _value) { isQuery = _value; }

    void yySetLexer(HqlLex *LexObject);
    HqlLex* getLexer() { return lexObject; }

    void saveContext(HqlGramCtx & ctx, bool cloneScopes);
    IHqlScope * queryGlobalScope();
    IHqlScope * queryMacroScope(IEclPackage * & package);

    bool canFollowCurrentState(int tok, const short * yyps);
    int mapToken(int lexToken) const;
    IHqlExpression *lookupSymbol(IIdAtom * name, const attribute& errpos);
    IHqlExpression *lookupSymbol(IHqlScope * scope, IIdAtom * name);
    void enableAttributes(int activityToken);

    IHqlExpression * recordLookupInTemplateContext(IIdAtom * name, IHqlExpression * expr, IHqlScope * templateScope);
    void processImportAll(attribute & modulesAttr);
    void processImport(attribute & modulesAttr, IIdAtom * as);
    void processImport(attribute & membersAttr, attribute & modulesAttr, IIdAtom * as);
    void applyDefaultPromotions(attribute &a1, bool extendPrecision);
    unsigned checkCompatible(ITypeInfo * a1, ITypeInfo * t2, const attribute &ea, bool complain=true);
    void checkMaxCompatible(IHqlExpression * sortOrder, IHqlExpression * values, attribute & errpos);
    void checkCompatibleTransforms(HqlExprArray & values, IHqlExpression * record, attribute & errpos);
    ITypeInfo * checkCompatibleScopes(const attribute& left, const attribute& right);
    void checkBoolean(attribute &atr);
    void checkBooleanOrNumeric(attribute &atr);
    void checkDatarow(attribute &atr);
    void checkDataset(attribute &atr);
    void checkDictionary(attribute &atr);
    void checkFieldnameValid(const attribute &errpos, IIdAtom * name);
    void checkList(attribute &atr);
    void checkScalar(attribute &atr);
    void checkSensibleId(const attribute & attr, IIdAtom * id);
    void checkUseLocation(const attribute & errpos);
    void checkCosort(IHqlExpression * sortlist, IHqlExpression * partition, const attribute & ea);
    ITypeInfo * checkPromoteNumeric(attribute &a1, bool extendPrecision);
    ITypeInfo * checkPromoteType(attribute &a1, attribute &a2);
    ITypeInfo * checkPromoteIfType(attribute &a1, attribute &a2);
    void checkSameType(attribute &a1, attribute &a2);
    void checkType(attribute &a1, ITypeInfo *t2);
    ITypeInfo *checkType(attribute &e1, attribute &e2);
    bool checkAlienTypeDef(IHqlScope* scope, const attribute& errpos);
    void checkAlienType(const attribute &errpos, IHqlAlienTypeInfo * alien);
    IHqlExpression* checkServiceDef(IHqlScope* serviceScope,IIdAtom * name, IHqlExpression* attrs, const attribute& errpos);
    void checkConstant(attribute & attr, bool callAllowed);
    IHqlExpression * checkConstant(const attribute & errpos, IHqlExpression * expr, bool callAllowed);
    void checkConstantEvent(attribute & attr);
    IHqlExpression * checkConcreteModule(const attribute & errpos, IHqlExpression * expr);
    void checkFoldConstant(attribute & attr, bool callAllowed);
    void checkInteger(attribute &e1);
    void checkPositive(attribute &e1);
    bool checkString(attribute &e1);
    bool checkStringOrUnicode(attribute &e1);
    void checkIntegerOrString(attribute & e1);
    void checkNumeric(attribute &e1);
    ITypeInfo *checkNumericGetType(attribute &e1);
    void checkInlineDatasetOptions(const attribute & attr);
    void checkLibraryParametersMatch(const attribute & errpos, bool isParametered, const HqlExprArray & activeParameters, IHqlExpression * definition);
    void checkReal(attribute &e1);
    ITypeInfo *checkStringIndex(attribute & strAttr, attribute & idxAttr);
    void checkWorkflowScheduling(IHqlExpression * expr, attribute& errpos);
    void checkWorkflowMultiples(IHqlExpression * currentWorkflow, IHqlExpression * newWorkflow, attribute& errpos);
    void checkJoinFlags(const attribute & err, IHqlExpression * joinExpr);
    void checkLoopFlags(const attribute & err, IHqlExpression * loopExpr);
    IHqlExpression * checkIndexRecord(IHqlExpression * record, const attribute & errpos, OwnedHqlExpr & indexAttrs);
    void checkIndexFieldType(IHqlExpression * cur, bool isPayload, bool insideNestedRecord, const attribute & errpos);
    void checkIndexRecordType(IHqlExpression * record, unsigned numPayloadFields, bool insideNestedRecord, const attribute & errpos);
    void checkIndexRecordTypes(IHqlExpression * index, const attribute & errpos);
    void reportInvalidIndexFieldType(IHqlExpression * expr, bool isPayload, const attribute & errpos);
    void reportUnsupportedFieldType(ITypeInfo * type, const attribute & errpos);
    void checkCaseForDuplicates(HqlExprArray & exprs, attribute &err);
    void checkOnFailRecord(IHqlExpression * expr, attribute & errpos);
    void checkAggregateRecords(IHqlExpression * expr, IHqlExpression * record, attribute & errpos);
    void checkExportedModule(const attribute & errpos, IHqlExpression * scopeExpr);
    bool checkCompatibleSymbol(const attribute & errpos, IHqlExpression * prevValue, IHqlExpression * newValue);
    bool insideSignedMacro();
    bool checkAllowed(const attribute & errpos, const char *category, const char *description);
    void saveDiskAccessInformation(const attribute & errpos, HqlExprArray & options);
    void saveDiskAccessInformation(const attribute & errpos, OwnedHqlExpr & options);
    IHqlExpression * createAveList(const attribute & errpos, IHqlExpression * list);
    IHqlExpression * createIff(attribute & condAttr, attribute & leftAttr, attribute & rightAttr);
    IHqlExpression * createListFromExpressionList(attribute & attr);
    IHqlExpression * createListIndex(attribute & list, attribute & which, IHqlExpression * attr);
    IHqlExpression * createNullPattern();
    IHqlExpression * createLibraryInstance(const attribute & errpos, IHqlExpression * name, IHqlExpression * func, HqlExprArray & actuals, IHqlExpression * attrs);
    IHqlExpression * createLocationAttr(const attribute & errpos);
    IHqlExpression * createSortExpr(node_operator op, attribute & dsAttr, const attribute & orderAttr, HqlExprArray & args);
    IHqlExpression * createIffDataset(IHqlExpression * record, IHqlExpression * value);
    IHqlExpression * createSetRange(attribute & array, attribute & range);
    /**
     * Check that RECORDOF,LOOKUP finds a compatible record in dfs
     *
     * @param newRecord     The record retrieved from DFS
     * @param defaultRecord The record provided in the ECL code
     * @param errpos        Where to report errors
     */
    bool checkDFSfields(IHqlExpression *dfsRecord, IHqlExpression *defaultRecord, const attribute& errpos);
    /**
     * Check usage of RECORDOF,LOOKUP and create no_record expression
     *
     * @param errpos        Where to report errors
     * @param _name         The expression representing the logical filename
     * @param _default      The default record definition provided in the ECL code
     * @param _lookupAttr   The LOOKUP attribute expression
     * @param isOpt         Indicates whether ,OPT was present
     */
    IHqlExpression * lookupDFSlayout(const attribute &errpos, IHqlExpression *_name, IHqlExpression *_default, IHqlExpression *_lookupAttr, bool isOpt);

    bool isSingleValuedExpressionList(const attribute & attr);
    bool convertAllToAttribute(attribute &atr);
    IHqlExpression * convertToOutOfLineFunction(const ECLlocation & errpos, IHqlExpression  * expr);
    IHqlExpression * convertToInlineFunction(const ECLlocation & errpos, IHqlExpression  * expr);

    void ensureBoolean(attribute &a);
    void ensureDataset(attribute & attr);
    void ensureString(attribute &a);
    void ensureTypeCanBeIndexed(attribute &a);
    void ensureUnicode(attribute &a);
    void ensureUTF8(attribute &a);
    void ensureData(attribute &a);
    void ensureTransformTypeMatch(attribute & tattr, IHqlExpression * ds);
    bool checkTransformTypeMatch(const attribute & errpos, IHqlExpression * ds, IHqlExpression * transform);
    void ensureDatasetTypeMatch(attribute & tattr, IHqlExpression * ds);
    IAtom * ensureCommonLocale(attribute &a, attribute &b);
    void ensureUnicodeLocale(attribute & a, char const * locale);
    void ensureType(attribute &atr, ITypeInfo * type);
    void inheritRecordMaxLength(IHqlExpression * dataset, SharedHqlExpr & record);

    IHqlExpression * getTargetPlatformExpr();
    void normalizeExpression(attribute & expr);
    void normalizeExpression(attribute & expr, type_t expectedType, bool isConstant, bool callAllowed=true);
    void checkRegex(const attribute & pattern);

    IHqlExpression * createListFromExprArray(const attribute & errpos, HqlExprArray & args);
    IHqlExpression * normalizeExprList(const attribute & errpos, const HqlExprArray & values);

    bool isDiskFile(IHqlExpression * expr);
    bool isFilteredDiskFile(IHqlExpression * expr);
    bool isSaved(IHqlExpression * failure);
    bool isCritical(IHqlExpression * failure);
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
    IHqlExpression * createLoopCondition(IHqlExpression * left, IHqlExpression * arg1, IHqlExpression * arg2, IHqlExpression * seq, IHqlExpression * rowsid);
    void setTemplateAttribute();
    void warnIfFoldsToConstant(IHqlExpression * expr, const attribute & errpos);
    void warnIfRecordPacked(IHqlExpression * expr, const attribute & errpos);
    void warnIfRecordPacked(const attribute & errpos) { warnIfRecordPacked(errpos.queryExpr(), errpos); }
    void validateParseTerminate(IHqlExpression * e, attribute & errpos);
    void validateXPath(attribute & a);

    void beginFunctionCall(attribute & function);
    IHqlExpression * endFunctionCall();
    void addActual(const attribute & errpos, IHqlExpression * ownedExpr);
    void addNamedActual(const attribute & errpos, IIdAtom * name, IHqlExpression * ownedExpr);
    bool processParameter(FunctionCallInfo & call, IIdAtom * name, IHqlExpression * actualValue, const attribute& errpos);
    void checkActualTopScope(FunctionCallInfo & call, IHqlExpression * formal, IHqlExpression * actual);
    void leaveActualTopScope(FunctionCallInfo & call);

    bool doCheckValidFieldValue(const attribute &errpos, IHqlExpression *value, IHqlExpression * field);
    bool checkValidFieldValue(const attribute &errpos, IHqlExpression *value, IHqlExpression * field);
    IHqlExpression * createCheckMatchAttr(attribute & attr, type_t tc);

    bool extractConstantString(StringBuffer & text, attribute & attr);

    IHqlExpression *processPartitionBloomAttr(IHqlExpression *bloom, IHqlExpression *index, const attribute & errpos);
    void setIndexScope(IHqlExpression *index);
    void clearIndexScope();
    void pushIndexScope();

    //Various grammar rule productions.
    void beginAlienType(const attribute & errpos);
    void beginDefineId(IIdAtom * name, ITypeInfo * type);

    IHqlExpression * processAlienType(const attribute & errpos);
    IHqlExpression * processIndexBuild(const attribute &err, attribute & indexAttr, attribute * recordAttr, attribute * payloadAttr, attribute & filenameAttr, attribute & flagsAttr);
    IHqlExpression * processCompoundFunction(attribute & result, bool outOfLine);
    IHqlExpression * processEmbedBody(const attribute & errpos, IHqlExpression * embedText, IHqlExpression * language, IHqlExpression *attribs);
    IHqlExpression * checkEmbedBody(const attribute & errpos, DefineIdSt * defineid, IHqlExpression *body, HqlExprArray & params);
    IHqlExpression * getGpgSignature();
    void processEnum(attribute & idAttr, IHqlExpression * value);
    void processError(bool full);
    void processLoadXML(attribute & a1, attribute * a2);
    IHqlExpression * processModuleDefinition(const attribute & errpos);
    IHqlExpression * processRowset(attribute & selectorAttr);
    void processServiceFunction(const attribute & errpos, IIdAtom * name, IHqlExpression * thisAttrs, ITypeInfo * type);
    void processStartTransform(const attribute & errpos);
    IHqlExpression * processUserAggregate(const attribute & mainPos, attribute & dsAttr, attribute & recordAttr, attribute & transformAttr, attribute * mergeAttr,
                                      attribute *itemsAttr, attribute &rowsAttr, attribute &seqAttr);

    void enterEnum(const attribute & errpos, ITypeInfo * type);
    void setEnumType(const attribute & errpos, ITypeInfo * type);
    void enterService(attribute & attrs);
    IHqlExpression * leaveEnum(const attribute & errpos);
    IHqlExpression * leaveService(const attribute & errpos);

    IHqlExpression * nextEnumValue();

// Error handling
    void syntaxError(const char *s, int token, int *expected);
    bool checkErrorCountAndAbort();
    bool exceedsMaxCompileErrors();
    unsigned getMaxCompileErrors()
    {
        return lookupCtx.queryParseContext().maxErrors;
    }
    bool unsuppressImmediateSyntaxErrors()
    {
        return lookupCtx.queryParseContext().unsuppressImmediateSyntaxErrors;
    }
    void reportTooManyErrors();
    void doReportWarning(WarnErrorCategory category, ErrorSeverity severity, int warnNo, const char *msg, const char *filename, int lineno, int column, int pos);
    void reportError(int errNo, const attribute& a, const char* format, ...) __attribute__((format(printf, 4, 5)));
    void reportError(int errNo, const ECLlocation & pos, const char* format, ...) __attribute__((format(printf, 4, 5)));
    void reportMacroExpansionPosition(IError * warning, HqlLex * lexer);
    void reportErrorUnexpectedX(const attribute & errpos, IAtom * unexpected);

    // Don't use overloading: va_list is the same as char*!!
    void reportErrorVa(int errNo, const ECLlocation & a, const char* format, va_list args) __attribute__((format(printf,4,0)));
    void reportError(int errNo, const char *msg, int lineno, int column, int position=0);
    void reportWarning(WarnErrorCategory category, int warnNo, const ECLlocation & pos, const char* format, ...) __attribute__((format(printf, 5,6)));
    void reportWarning(WarnErrorCategory category, ErrorSeverity severity, int warnNo, const ECLlocation & pos, const char* format, ...) __attribute__((format(printf, 6, 7)));
    void reportWarningVa(WarnErrorCategory category, int errNo, const attribute& a, const char* format, va_list args) __attribute__((format(printf, 5,0)));
    void reportWarning(WarnErrorCategory category, int warnNo, const char *msg, int lineno, int column);
    void addResult(IHqlExpression *query, const attribute& errpos);

    // interface IErrorReceiver
    virtual void reportError(int errNo, const char *msg, const char *filename=NULL, int lineno=0, int column=0, int pos=0);
    virtual void report(IError * error);
    virtual IError * mapError(IError * error);
    virtual void exportMappings(IWorkUnit * wu) const;
    void reportWarning(WarnErrorCategory category, int warnNo, const char *msg, const char *filename=NULL, int lineno=0, int column=0, int pos=0);
    virtual size32_t errCount();
    virtual size32_t warnCount();
    
    IHqlExpression * queryAlreadyAssigned(IHqlExpression * select);
    bool checkAlreadyAssigned(const attribute & errpos, IHqlExpression * select);
    IHqlExpression * findAssignment(IHqlExpression *field);
    void addAssignment(attribute &field, attribute &source);
    void addAssignment(const attribute & errpos, IHqlExpression * targetExpr, IHqlExpression * srcExpr);
    void addAssignall(IHqlExpression * record, IHqlExpression * src,const attribute& errpos);
    void addConditionalAssign(const attribute & errpos, IHqlExpression * self, IHqlExpression * leftSelect, IHqlExpression * rightSelect, IHqlExpression * field);
    void addConditionalRowAssign(const attribute & errpos, IHqlExpression * self, IHqlExpression * leftSelect, IHqlExpression * rightSelect, IHqlExpression * record);
    void checkAllAssigned(IHqlExpression * originalRecord, IHqlExpression * unadornedRecord, const attribute &errpos);
    void checkFieldMap(IHqlExpression* map, attribute& errpos);
    IHqlExpression * createDistributeCond(IHqlExpression * left, IHqlExpression * right, const attribute & err, const attribute & seqAttr);
    IHqlExpression * addSideEffects(IHqlExpression * expr);
    IHqlExpression * associateSideEffects(IHqlExpression * expr, const ECLlocation & errpos);
    void clearSideEffects();
    bool sideEffectsPending() const;

    void checkAssignedNormalizeTransform(IHqlExpression * originalRecord, IHqlExpression * unadornedRecord, const attribute &errpos);
    void doCheckAssignedNormalizeTransform(HqlExprArray * assigns, IHqlExpression* select, IHqlExpression* targetSelect, IHqlExpression * original, IHqlExpression * unadorned, const attribute& errpos, bool & modified);

    bool checkValidBaseModule(const attribute & attr, SharedHqlExpr & expr);
    IHqlExpression * implementInterfaceFromModule(attribute & iAttr, attribute & mAttr, IHqlExpression * flags);
    IHqlExpression * implementInterfaceFromModule(const attribute & modpos, const attribute & ipos, IHqlExpression * implementModule, IHqlExpression * projectInterface, IHqlExpression * flags);

    DefineIdSt * createDefineId(int scope, ITypeInfo * ownedType);
    void enterCompoundObject();
    void leaveCompoundObject();
    void enterScope(IHqlScope * scope, bool allowExternal, bool legacyOnly);
    void enterScope(bool allowExternal);
    void enterVirtualScope();
    void leaveScope(const attribute & errpos);
    IHqlExpression * leaveLamdaExpression(attribute * modifierattr, attribute & exprattr);
    IHqlScope * closeLeaveScope(const attribute & errpos);
    void enterPatternScope(IHqlExpression * pattern);
    void leavePatternScope(const attribute & errpos);
    bool insideNestedScope() const;

    void beginTransform(ITypeInfo * recordType, IHqlExpression * unadornedRecord);
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

    void addDatasetField(const attribute &errpos, IIdAtom * name, ITypeInfo * type, IHqlExpression *value, IHqlExpression * attrs);
    void addDictionaryField(const attribute &errpos, IIdAtom * name, ITypeInfo * type, IHqlExpression *value, IHqlExpression * attrs);
    void addField(const attribute &errpos, IIdAtom * name, ITypeInfo *type, IHqlExpression *value, IHqlExpression *attrs);
    void addFieldFromValue(const attribute &errpos, attribute & valueAttr);
    void addFields(const attribute &errpos, IHqlExpression *record, IHqlExpression * dataset, bool clone);
    void addIfBlockToActive(const attribute &errpos, IHqlExpression * ifblock);
    void addToActiveRecord(IHqlExpression * newField);
    void beginIfBlock();
    IHqlExpression * endIfBlock();
    void beginPayload();
    IHqlExpression * endPayload();

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
    void leaveType(const attribute & errpos);
    void checkRecordTypesMatch(IHqlExpression *ds1, IHqlExpression *ds2, const attribute & errpos);
    int checkRecordTypesSimilar(IHqlExpression *left, IHqlExpression *right, const ECLlocation & errPos, unsigned maxFields = (unsigned)-1);
    bool checkRecordCreateTransform(HqlExprArray & assigns, IHqlExpression *leftExpr, IHqlExpression *leftSelect, IHqlExpression *rightExpr, IHqlExpression *rightSelect, const ECLlocation & errPos);
    IHqlExpression * checkEnsureRecordsMatch(IHqlExpression * left, IHqlExpression * right, const ECLlocation & errPos, type_t rightType);
    void ensureMapToRecordsMatch(OwnedHqlExpr & recordExpr, HqlExprArray & args, const attribute & errpos, type_t rightType);
    void checkRecordIsValid(const attribute &atr, IHqlExpression *record);
    void checkValidRecordMode(IHqlExpression * dataset, attribute & atr, attribute & modeatr);
    void checkValidCsvRecord(const attribute & errpos, IHqlExpression * record);
    void checkValidPipeRecord(const attribute & errpos, IHqlExpression * record, IHqlExpression * attrs, IHqlExpression * expr);
    void checkValidLookupFlag(IHqlExpression * dataset, IHqlExpression * filename, attribute & atr);

    void setPluggableModeExpr(attribute & targetAttr, attribute & pluginAttr, attribute & options);

    void createAppendDictionaries(attribute & targetAttr, attribute & leftAttr, attribute & rightAttr, IAtom * kind);
    void createAppendFiles(attribute & targetAttr, attribute & leftAttr, attribute & rightAttr, IAtom * kind);
    IHqlExpression * createAppendFiles(attribute & filesAttr, IHqlExpression * _attrs);
    IHqlExpression * processIfProduction(attribute & condAttr, attribute & trueAttr, attribute * falseAttr);

    IHqlExpression * createSymbolFromValue(IHqlExpression * primaryExpr, IHqlExpression * value);
    void setAssociateWarnings(bool value) { associateWarnings = value; }
    IHqlExpression* clearFieldMap(IHqlExpression* expr);
    void setExpectedAttribute(IIdAtom * _expectedAttribute)             { expectedAttribute = _expectedAttribute; current_id = _expectedAttribute; }
    void setCurrentToExpected()             { current_id = expectedAttribute; }
    void setPendingGlobalImport(bool _globalImportPending) { globalImportPending = _globalImportPending; }
    IHqlScope * queryPrimaryScope(bool isPrivate);
    unsigned nextParameterIndex()               { return 0; } // not commoned up at moment{ return activeParameters.length()+savedParameters.length(); }
    void addActiveParameterOwn(const attribute & errpos, IHqlExpression * expr, IHqlExpression * defaultValue);
    void gatherActiveParameters(HqlExprCopyArray & target);

    IHqlExpression * createVolatileId() { return ::createUniqueId(_volatileId_Atom); }
    IHqlExpression * createUniqueId() { return createUniqueId(_uid_Atom); }
    IHqlExpression * createUniqueId(IAtom * name);

    void onOpenBra();
    void onCloseBra();

    int yyLex(attribute * yylval, const short * activeState);

protected:
    IIdAtom * createUnnamedFieldId();
    IIdAtom * createUnnamedFieldId(const char * prefix);
    IIdAtom * getNameFromExpr(attribute& attr);
    IIdAtom * createFieldNameFromExpr(IHqlExpression * expr);
    IHqlExpression * createAssert(attribute & cond, attribute * msg, attribute & flags);

    void defineImport(const attribute & errpos, IHqlExpression * imported, IIdAtom * newName);
    IHqlExpression * doResolveImportModule(HqlLookupContext & importCtx, const attribute & errpos, IHqlExpression * expr);
    IHqlExpression * resolveImportModule(const attribute & errpos, IHqlExpression * expr);

    void setActiveAttrs(int activityToken, const TokenMap * attrs);

    IHqlExpression *doParse();
    IHqlExpression * checkBuildIndexRecord(IHqlExpression *record, attribute & errpos);
    void checkNotAlreadyDefined(IIdAtom * name, IHqlScope * scope, const attribute & idattr);
    void checkNotAlreadyDefined(IIdAtom * name, const attribute & idattr);
    void checkBuildIndexFilenameFlags(IHqlExpression * dataset, attribute & flags);
    IHqlExpression * createBuildFileFromTable(IHqlExpression * table, const HqlExprArray & createBuildFileFromTable, IHqlExpression * filename, attribute & errpos);
    IHqlExpression * createBuildIndexFromIndex(attribute & indexAttr, attribute & flagsAttr, attribute & errpos);
    void checkOutputRecord(attribute & errpos, bool outerLevel);
    void checkSoapRecord(attribute & errpos);
    IHqlExpression * processHttpMarkupFlag(__int64 op);
    IHqlExpression * processHttpMarkupFlag(__int64 op, IHqlExpression *flags);
    IHqlExpression * processHttpMarkupFlag(__int64 op, IHqlExpression *flags, IHqlExpression *p1);
    IHqlExpression * checkOutputRecord(IHqlExpression *record, const attribute & errpos, bool & allConstant, bool outerLevel);
    void checkDefaultValueVirtualAttr(const attribute &errpos, IHqlExpression * attrs);

    void doAddAssignment(IHqlExpression * transform, IHqlExpression * field, IHqlExpression * rhs, const attribute& errpos);
    void doAddAssignall(IHqlExpression* assignall, IHqlExpression *tgt, IHqlExpression *src,const attribute& errpos);
    void doAddAssignSelf(IHqlExpression* assignall, IHqlExpression *tgt, IHqlExpression *src,const attribute& errpos);
    void doAddAssignCompound(IHqlExpression * assignall, IHqlExpression * target, IHqlExpression * src, IHqlExpression * record, const attribute& errpos);
    void doAddAssignCompoundOwn(IHqlExpression * assignall, IHqlExpression * target, IHqlExpression * src, IHqlExpression * record, const attribute& errpos);
    IHqlExpression * replaceSelfReferences(IHqlExpression * transform, IHqlExpression * rhs, IHqlExpression * self, const attribute& errpos);

    void appendToActiveScope(IHqlExpression * arg);
    bool isVirtualFunction(DefineIdSt * defineid, const attribute & errpos);
    
    IHqlExpression * castIndexTypes(IHqlExpression *sortList);
    IHqlExpression * processSortList(const attribute & errpos, node_operator op, IHqlExpression * dataset, HqlExprArray & items, OwnedHqlExpr *joinedClause, OwnedHqlExpr *attributes);
    void expandSortedAsList(HqlExprArray & args);
    bool expandWholeAndExcept(IHqlExpression * dataset, const attribute & errpos, HqlExprArray & parms);
    void expandWholeAndExcept(IHqlExpression * dataset, attribute & a);
    void cleanCurTransform();
    void unwindSelect(IHqlExpression* expr, HqlExprArray& r);
    void setDefaultString(attribute &a);

    void canNotAssignTypeError(ITypeInfo* expected, ITypeInfo* given, const char * name, const attribute& errpos);
    void canNotAssignTypeWarn(ITypeInfo* expected, ITypeInfo* given, const char * name, const attribute& errpos);
    bool isExceptionalCase(attribute& defineid, attribute& object, attribute& failure);
    void checkSvcAttrNoValue(IHqlExpression* attr, const attribute& errpos);
    void checkFormals(IIdAtom * name, HqlExprArray & parms, HqlExprArray & defaults, attribute& object);
    IHqlExpression * checkParameter(const attribute * errpos, IHqlExpression * actual, IHqlExpression * formal, bool isDefault, IHqlExpression * funcdef);
    void checkDedup(IHqlExpression *ds, IHqlExpression *flags, attribute &errpos);
    void addParameter(const attribute & errpos, IIdAtom * name, ITypeInfo* type, IHqlExpression* defValue);
    void addFunctionParameter(const attribute & errpos, IIdAtom * name, ITypeInfo* type, IHqlExpression* defValue);
    void addFunctionProtoParameter(const attribute & errpos, IIdAtom * name, IHqlExpression * like, IHqlExpression* defValue);
    bool checkParameters(IHqlExpression* func, HqlExprArray& actuals, const attribute& errpos);
    bool checkTemplateFunctionParameters(IHqlExpression* func, HqlExprArray& actuals, const attribute& errpos);
    void checkSizeof(IHqlExpression* expr, attribute& errpos);
    void checkSizeof(ITypeInfo* expr, attribute& errpos, bool isDataset = false);
    void normalizeStoredNameExpression(attribute & a);
    void checkPatternFailure(attribute & attr);
    void checkDistributer(const ECLlocation & errPos, HqlExprArray & args);
    void checkConcreteRecord(attribute & cur);
    IHqlExpression * createScopedSequenceExpr();
    IHqlExpression * createPatternOr(HqlExprArray & args, const attribute & errpos);
    IHqlExpression * mapAlienArg(IHqlSimpleScope * scope, IHqlExpression * expr);
    ITypeInfo * mapAlienType(IHqlSimpleScope * scope, ITypeInfo * type, const attribute & errpos);
    IHqlExpression * lookupParseSymbol(IIdAtom * searchName);
    IHqlExpression * lookupContextSymbol(IIdAtom * searchName, const attribute& errpos, unsigned & scopeAccessDepth);

    void disableError() { errorDisabled = true; }
    void enableError() { errorDisabled = false; }
    void abortParsing();
    bool checkAborting()
    {
        if (lookupCtx.isAborting())
        {
            abortParsing();//Ensure a consistent abort by propagating the aborting state.
            return true;
        }
        return false;
    }

    IIdAtom * fieldMapTo(IHqlExpression* expr, IIdAtom * name);
    IIdAtom * fieldMapFrom(IHqlExpression* expr, IIdAtom * name);
    bool requireLateBind(IHqlExpression* funcdef, const HqlExprArray & actuals);
    IHqlExpression* createDefJoinTransform(IHqlExpression* left,IHqlExpression* right,attribute& errpos, IHqlExpression * seq, IHqlExpression * flags);
    IHqlExpression * createRowAssignTransform(const attribute & srcAttr, const attribute & tgtAttr, const attribute & seqAttr);
    IHqlExpression * createRowAssignTransform(const attribute & srcAttr, IHqlExpression * res_rec, const attribute & seqAttr);
    IHqlExpression * createRowAssignTransform(const attribute & srcAttr, const attribute & tgtAttr, IHqlExpression * res_rec, const attribute & seqAttr);
    IHqlExpression * createClearTransform(IHqlExpression * record, const attribute & errpos);
    IHqlExpression * createDefaultAssignTransform(IHqlExpression * record, IHqlExpression * rowValue, const attribute & errpos);
    IHqlExpression * createDefaultProjectDataset(IHqlExpression * record, IHqlExpression * src, const attribute & errpos);
    IHqlExpression * createDatasetFromList(attribute & listAttr, attribute & recordAttr, IHqlExpression * attrs);
    IHqlExpression * getUnadornedRecord(IHqlExpression * record);

    void checkConditionalAggregates(IIdAtom * name, IHqlExpression * value, const attribute & errpos);
    void checkProjectedFields(IHqlExpression * e, attribute & errpos);
    IHqlExpression * createRecordFromDataset(IHqlExpression * ds);
    IHqlExpression * cleanIndexRecord(IHqlExpression * record);
    IHqlExpression * createRecordIntersection(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createRecordUnion(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createRecordDifference(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createRecordExcept(IHqlExpression * left, IHqlExpression * right, const attribute & errpos);
    IHqlExpression * createIndexFromRecord(IHqlExpression * record, IHqlExpression * attr, const attribute & errpos);
    IHqlExpression * createProjectRow(attribute & rowAttr, attribute & transformAttr, attribute & seqAttr);
    void doDefineSymbol(DefineIdSt * defineid, IHqlExpression * expr, IHqlExpression * failure, const attribute & idattr, int assignPos, int semiColonPos, bool isParametered, IHqlExpression * modifiers);
    void defineSymbolInScope(IHqlScope * scope, DefineIdSt * defineid, IHqlExpression * expr, const attribute & idattr, int assignPos, int semiColonPos);
    void checkDerivedCompatible(IIdAtom * name, IHqlExpression * scope, IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, attribute const & errpos);
    void defineSymbolProduction(attribute & nameattr, attribute & paramattr, attribute & assignattr, attribute * valueattr, attribute * failattr, attribute & semiattr);
    void definePatternSymbolProduction(attribute & nameattr, attribute & paramattr, const attribute & assignAttr, attribute & valueAttr, attribute & workflowAttr, const attribute & semiattr);
    void cloneInheritedAttributes(IHqlScope * scope, const attribute & errpos);
    IHqlExpression * normalizeFunctionExpression(const attribute & idattr, DefineIdSt * defineid, IHqlExpression * expr, IHqlExpression * failure, bool isParametered, HqlExprArray & parameters, IHqlExpression * defaults, IHqlExpression * modifiers);

    IHqlExpression * createEvaluateOutputModule(const attribute & errpos, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, node_operator outputOp, IIdAtom *matchId);
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
    inline const char * querySourcePathText()   { return str(sourcePath); } // safe if null

    bool areSymbolsCompatible(IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, IHqlExpression * prevValue);
    IHqlExpression * extractBranchMatch(const attribute & errpos, IHqlExpression & curSym, HqlExprArray & values);
    ITypeInfo * extractBranchMatches(const attribute & errpos, IHqlExpression & curSym, HqlExprArrayArray & branches, HqlExprArray & extracted);
    void expandScopeEntries(HqlExprArrayArray & branches, IHqlExpression * scope);
    void processIfScope(const attribute & errpos, IHqlExpression * cond, IHqlExpression * trueScope, IHqlExpression * falseScope);

    unsigned getExtraLookupFlags(IHqlScope * scope);

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
    bool forceResult;
    bool associateWarnings;
    bool isQuery;
    bool parseConstantText = false;
    bool expandingMacroPosition;
    bool inSignedModule;
    bool globalImportPending = false;
    OwnedHqlExpr gpgSignature;

    IErrorArray pendingWarnings;
    HqlGram * containerParser = nullptr;
    Linked<ISourcePath> sourcePath;
    IIdAtom * moduleName;
    IIdAtom * current_id;
    IIdAtom * expectedAttribute;
    IHqlScope *transformScope;
    PointerArray savedIds;
    UnsignedArray savedLastpos;
    unsigned lastpos;
    bool inType;
    Owned<IHqlScope> modScope;
    OwnedHqlExpr dotScope;
    OwnedHqlExpr indexScope;
    unsigned outerScopeAccessDepth;
    IHqlScope* containerScope;
    IHqlScope* globalScope;
    ITypeInfo *current_type;
    HqlExprArray topScopes;
    CIArrayOf<LeftRightScope> leftRightScopes;
    HqlExprArray selfScopes;
    HqlExprArray localeStack;
    HqlExprArray localFunctionCache;
    HqlExprArray curListStack;
    CIArrayOf<OwnedHqlExprItem> counterStack;
    CIArrayOf<TransformSaveInfo> transformSaveStack;
    CIArrayOf<ActiveScopeInfo> defineScopes;
    OwnedHqlExpr curList;
    BoolArray wasInEvaluate;
    HqlExprCopyArray activeRecords;
    HqlExprArray activeIfBlocks;
    HqlLex *lexObject;
    HqlExprArray parseResults;
    IErrorReceiver *errorHandler;
    IHqlExpression *curTransform;
    OwnedHqlExpr curTransformRecord;
    Owned<SelfReferenceReplacer> curSelfReplacer;
    HqlScopeArray defaultScopes;
    HqlScopeArray implicitScopes;
    PointerArray savedType;
    HqlExprAttr curFeatureParams;
    HqlExprCopyArray implicitFeatureNames;
    HqlExprCopyArray implicitFeatureValues;
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

    void setIdUnknown(bool expected) { expectedUnknownId = expected; }
    bool getIdUnknown() { return expectedUnknownId; }
    void init(IHqlScope * _globalScope, IHqlScope * _containerScope);
    void addProperty(const char *prop, const char *val);
    IHqlExpression * createSelect(IHqlExpression * lhs, IHqlExpression * rhs, const attribute & errpos);
    IHqlExpression * createIndirectSelect(IHqlExpression * lhs, IHqlExpression * rhs, const attribute & errpos);
    IHqlExpression * addDatasetSelector(IHqlExpression * lhs, IHqlExpression * rhs);

    void pushTopScope(IHqlExpression *);
    void pushLeftRightScope(IHqlExpression * left, IHqlExpression * right);
    void pushPendingLeftRightScope(IHqlExpression * left, IHqlExpression * right);
    void setRightScope(IHqlExpression *);
    void beginRowsScope(node_operator side);

    void pushSelfScope(IHqlExpression *);
    void pushSelfScope(ITypeInfo * selfType);

    IHqlExpression * getSelector(const attribute & errpos, node_operator side);

    IHqlExpression * createActiveSelectorSequence(IHqlExpression * left, IHqlExpression * right);

    IHqlExpression * getSelectorSequence();
    IHqlExpression * forceEnsureExprType(IHqlExpression * expr, ITypeInfo * type);

    void popTopScope();
    IHqlExpression * endRowsScope();
    IHqlExpression * popLeftRightScope();
    void popSelfScope();

    void beginList();
    void addListElement(IHqlExpression * expr);
    void endList(HqlExprArray & args);

    void pushLocale(IHqlExpression *);
    void popLocale();
    IHqlExpression *queryDefaultLocale();

    IHqlExpression * getActiveCounter(attribute & errpos);
    void pushRecord(IHqlExpression *);
    IHqlExpression *endRecordDef();
    IHqlExpression *popRecord();
    IHqlExpression *queryTopScope();
    ITypeInfo * getPromotedECLType(HqlExprArray & args, ITypeInfo * otherType, bool allowVariableLength);
    IHqlExpression *getTopScope();
    IHqlExpression *queryLeftScope();
    IHqlExpression *queryRightScope();
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
    void checkRegrouping(const ECLlocation & errPos, HqlExprArray & args);
    void checkRecordsMatch(attribute & atr, HqlExprArray & args);

    IHqlExpression * transformRecord(IHqlExpression *dataset, IAtom * targetCharset, const attribute & errpos);
    IHqlExpression * transformRecord(IHqlExpression *record, IAtom * targetCharset, IHqlExpression * scope, bool & changed, const attribute & errpos);
    IHqlExpression * translateFieldsToNewScope(IHqlExpression * expr, IHqlSimpleScope * record, const attribute & err);

    ITypeInfo *queryCurrentRecordType();
    ITypeInfo *queryCurrentTransformType();
    IHqlExpression *queryCurrentTransformRecord();
    IHqlExpression* queryFieldMap(IHqlExpression* expr);
    IHqlExpression* bindFieldMap(IHqlExpression*, IHqlExpression*);
    void extractIndexRecordAndExtra(SharedHqlExpr & record, SharedHqlExpr & extra);
    void transferOptions(attribute & filenameAttr, attribute & optionsAttr);
    IHqlExpression * extractTransformFromExtra(SharedHqlExpr & extra);
    void expandPayload(HqlExprArray & fields, IHqlExpression * payload, IHqlSimpleScope * scope, ITypeInfo * & lastFieldType, const attribute & errpos);
    void mergeDictionaryPayload(OwnedHqlExpr & record, IHqlExpression * payload, const attribute & errpos);
    void modifyIndexPayloadRecord(SharedHqlExpr & record, SharedHqlExpr & payload, SharedHqlExpr & extra, const attribute & errpos);

    bool haveAssignedToChildren(IHqlExpression * select);
    bool haveAssignedToAllChildren(IHqlExpression * select);
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

enum
{
    HEFhadtrue = 0x0001,
    HEFhadelse = 0x0002,
};

class HqlLex
{
    public:
        HqlLex(HqlGram *gram, IFileContents * _text, IXmlScope *xmlScope, IHqlExpression *macroExpr);
        ~HqlLex();   

        void enterEmbeddedMode();
        static int doyyFlex(attribute & returnToken, yyscan_t yyscanner, HqlLex * lexer, LexerFlags lookupFlags, const short * activeState);
        static int lookupIdentifierToken(attribute & returnToken, HqlLex * lexer, LexerFlags lookupFlags, const short * activeState, const char * tokenText);

        int yyLex(attribute & returnToken, LexerFlags lookupFlags, const short * activeState);    /* lexical analyzer */

        bool assertNext(attribute & returnToken, int expected, unsigned code, const char * msg);
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
        const char* getMacroName() { return (macroExpr) ? str(macroExpr->queryName()) : "<param>"; }
        const char * queryMacroScopeName(IEclPackage * & package);

        IPropertyTree * getClearJavadoc();
        void doSlashSlashHash(attribute const & returnToken, const char * command);

        void loadXML(const attribute & errpos, const char * value, const char * child = NULL);

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

        inline void setMacroParams(IProperties * _macroParams) { macroParms.set(_macroParams); }
        inline void setTokenPosition(attribute & returnToken) const
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

        StringBuffer& doGetDataType(StringBuffer & type, const char * text, int lineno, int column);
        void pushText(const char *);
        bool hasLegacyImportSemantics() const;
        bool hasLegacyWhenSemantics() const;
        void setLegacyImport(bool _legacyImportMode)
        {
            legacyImportMode = _legacyImportMode;
        }
        void setLegacyWhen(bool _legacyWhenMode)
        {
            legacyWhenMode = _legacyWhenMode;
        }

        bool isImplicitlySigned();

    protected:
        void init(IFileContents * _text);

    private:
        static void doEnterEmbeddedMode(yyscan_t yyscanner);
        void stripSlashNewline(attribute & returnToken, StringBuffer & target, size_t len, const char * data);

        void declareXmlSymbol(const attribute & errpos, const char *name);
        bool lookupXmlSymbol(const attribute & errpos, const char *name, StringBuffer &value);
        void setXmlSymbol(const attribute & errpos, const char *name, const char *value, bool append);
        IIterator *getSubScopes(const attribute & errpos, const char *name, bool doAll);
        IXmlScope *queryTopXmlScope();
        IXmlScope *ensureTopXmlScope();

        IHqlExpression *lookupSymbol(IIdAtom * name, const attribute& errpos);
        void reportError(const attribute & returnToken, int errNo, const char *format, ...) __attribute__((format(printf, 4, 5)));
        void reportError(const ECLlocation & pos, int errNo, const char *format, ...) __attribute__((format(printf, 4, 5)));
        void reportWarning(WarnErrorCategory category, const attribute & returnToken, int warnNo, const char *format, ...) __attribute__((format(printf, 5, 6)));

        void beginNestedHash(unsigned kind) { hashendKinds.append(kind); hashendFlags.append(0); }
        void endNestedHash() { hashendKinds.pop(); hashendFlags.pop(); }
        void clearNestedHash() { hashendKinds.kill(); hashendFlags.kill(); }
        void setHashEndFlags(unsigned i) { if (hashendFlags.ordinality()) { hashendFlags.pop(); hashendFlags.append(i); } }

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
        bool checkAborting();
        void pushMacro(IHqlExpression *expr);
        void pushText(IFileContents * text, int startLineNo, int startColumn);
        void pushText(const char *s, int startLineNo, int startColumn);
        bool getParameter(StringBuffer &curParam, const char* directive, const ECLlocation & location);
        IValue *foldConstExpression(const ECLlocation & errpos, IHqlExpression * expr, IXmlScope *xmlScope);
        IValue *parseConstExpression(const ECLlocation & errpos, StringBuffer &curParam, IXmlScope *xmlScope);
        IValue *parseConstExpression(const ECLlocation & errpos, IFileContents * contents, IXmlScope *xmlScope);
        IHqlExpression * parseECL(IFileContents * contents, IXmlScope *xmlScope, int startLine, int startCol);
        IHqlExpression * parseECL(const char * curParam, IXmlScope *xmlScope, int startLine, int startCol);
        void setMacroParam(const attribute & errpos, IHqlExpression* funcdef, StringBuffer& curParam, IIdAtom * argumentName, unsigned& parmno,IProperties *macroParms);
        unsigned getTypeSize(unsigned lengthTypeName);
        static IHqlExpression * createIntegerConstant(__int64 value, bool isSigned);

        void doPreprocessorLookup(const attribute & errpos, bool stringify, int extra);
        void doApply(attribute & returnToken);
        int doElse(attribute & returnToken, LexerFlags lookupFlags, const short * activeState, bool isElseIf);
        int doEnd(attribute & returnToken, LexerFlags lookupFlags, const short * activeState);
        void doExpand(attribute & returnToken);
        void doTrace(attribute & returnToken);
        void doError(attribute & returnToken, bool isError);
        void doExport(attribute & returnToken, bool toXml);
        void doFor(attribute & returnToken, bool doAll);
        int doHashText(attribute & returnToken);
        void doLoop(attribute & returnToken);
        void doIf(attribute & returnToken, bool isElseIf);
        void doSet(attribute & returnToken, bool _append);
        void doLine(attribute & returnToken);
        void doDeclare(attribute & returnToken);
        void doDefined(attribute & returnToken);
        void doGetDataType(attribute & returnToken);
        bool doIsDefined(attribute & returnToken);
        void doIsValid(attribute & returnToken);
        void doInModule(attribute & returnToken);
        void doMangle(attribute & returnToken, bool de);
        void doUniqueName(attribute & returnToken);
        void doSkipUntilEnd(attribute & returnToken, const char* directive, const ECLlocation & location);

        void processEncrypted();
        void checkSignature(const attribute & dummyToken);

        void declareUniqueName(const char* name, const char * pattern);
        void checkNextLoop(bool first);

        bool getDefinedParameter(StringBuffer &curParam, attribute & returnToken, const char* directive, const ECLlocation & location, SharedHqlExpr & resolved);

        int processStringLiteral(attribute & returnToken, char *CUR_TOKEN_TEXT, unsigned CUR_TOKEN_LENGTH, int oldColumn, int oldPosition);

private:
        HqlGram *yyParser;
        Owned<IFileContents> text;
        Linked<ISourcePath> sourcePath;
        
        HqlLex *inmacro;

        /* to handle recursive macro */
        HqlLex *parentLex;

        Owned<IProperties> macroParms;
        IIterator *forLoop;
        IXmlScope *xmlScope;
        IHqlExpression *macroExpr;
        ECLlocation forLocation;
        Owned<IFileContents> forBody;
        Owned<IFileContents> forFilter;
        IAtom * hashDollar = nullptr;
        IEclPackage * hashDollarPackage = nullptr;

        enum { HashStmtNone, HashStmtFor, HashStmtForAll, HashStmtLoop, HashStmtIf };
        int lastToken;
        int macroGathering;
        int skipNesting;
        UnsignedArray hashendFlags;
        UnsignedArray hashendKinds;
        bool hasHashbreak;
        bool legacyImportMode = false;
        bool legacyWhenMode = false;
        int loopTimes;

        bool inComment;
        bool inSignature;
        bool inCpp;
        bool inMultiString;
        bool encrypted;
        StringBuffer javaDocComment;

        yyscan_t scanner;

        int yyLineNo;
        int yyColumn;
        int yyPosition;
        int yyStartPos;
        char *yyBuffer;

        static __uint64 str2uint64(unsigned len, const char * digits, unsigned base);
        static void hex2str(char * target, const char * digits, unsigned len);
};


IHqlExpression *reparseTemplateFunction(IHqlExpression * funcdef, IHqlScope *scope, HqlLookupContext & ctx, bool hasFieldMap);
extern HQL_API void resetLexerUniqueNames();        // to make regression suite consistent
extern HQL_API int testHqlInternals();
extern HQL_API int testReservedWords();
extern HQL_API IHqlExpression * normalizeSelects(IHqlExpression * expr);
extern HQL_API bool checkGroupExpression(HqlExprArray &groups, IHqlExpression *field);
#endif
