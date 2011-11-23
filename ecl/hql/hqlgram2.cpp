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
#include "platform.h"
#include <stdio.h>
#include <stdlib.h>
#include "hql.hpp"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jexcept.hpp"
#include "hqlerrors.hpp"
#include "jfile.hpp" 
#include "junicode.hpp"

#include "hqlgram.hpp"
#include "hqlgram.h"

#include "hqlgram.hpp"
#include "hqlfold.hpp"
#include "hqlpmap.hpp"

#include "eclrtl.hpp"
#include "hqlerrors.hpp"
#include "hqlexpr.ipp"
#include "hqlattr.hpp"
#include "hqlmeta.hpp"
#include "hqlerror.hpp"
#include "hqlplugins.hpp"
#include "hqlscope.hpp"
#include "hqlthql.hpp"
#include "hqlpregex.hpp"
#include "hqlutil.hpp"
#include "hqltrans.ipp"
#include "hqlvalid.hpp"
#include "hqlrepository.hpp"

#define FAST_FIND_FIELD
//#define USE_WHEN_FOR_SIDEEFFECTS
#define MANYFIELDS_THRESHOLD                        2000
#define MAX_SENSIBLE_FIELD_LENGTH                   1000000000

struct TokenMap
{
    int lexToken;
    int attrToken;
};

static int defaultTokenMap[YY_LAST_TOKEN];
static int attributeToTokenMap[YY_LAST_TOKEN];
static TokenMap * nestedAttributeMap[YY_LAST_TOKEN];
static IHqlExpression * alreadyAssignedNestedTag;

//Called if token is only ever used as an attribute
static void setAttribute(int attrToken)
{
    defaultTokenMap[attrToken] = 0;
    attributeToTokenMap[attrToken] = attrToken;
}

static void setKeyword(int attrToken)
{
    attributeToTokenMap[attrToken] = attrToken;
}

//called if the attribute is clashes with another reserved word
static void setAttribute(int attrToken, int lexToken)
{
    attributeToTokenMap[attrToken] = lexToken;
}

static void setAttributes(int lexToken, ...)
{
    unsigned numAttrs = 0;
    va_list args;
    va_start(args, lexToken);
    for (;;)
    {
        int attrToken = va_arg(args, int);
        if (!attrToken)
            break;
        numAttrs++;
    }
    va_end(args);
    assertex(numAttrs);

    TokenMap * map = new TokenMap[numAttrs+1];
    unsigned curAttr = 0;
    va_list args2;
    va_start(args2, lexToken);
    for (;;)
    {
        int attrToken = va_arg(args2, int);
        if (!attrToken)
            break;
        int lexToken = attributeToTokenMap[attrToken];
        assertex(lexToken);
        map[curAttr].lexToken = lexToken;
        map[curAttr].attrToken = attrToken;
        curAttr++;
    }
    va_end(args2);
    assertex(curAttr == numAttrs);
    map[curAttr].lexToken = 0;
    map[curAttr].attrToken = 0;
    nestedAttributeMap[lexToken] = map;
}

/*
Why dynamically map keywords?  There are several different reasons:

a) If an attribute has a different syntax from the non attribute form of the keyword then dynamically mapping the keyword can allow the
   grammar to disambiguate between the two.

b) If a keyword is only used as an attribute name then it allows us to only pollute the name space when that attribute would be valid.

c) If attributes have different forms in different places then using different mapped attributes can allow us to use a single shared
   production with all the possible attributes.

(a) Is required in a couple of cases at the moment, there may be other situations in the future.

(b) Generally this is implemented by looking ahead in the parser tables.  FORMAT_ATTR is an example that is handled this way.

(c) Unlikely to ever come to anything...

*/

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    for (unsigned i=0; i < YY_LAST_TOKEN; i++)
        defaultTokenMap[i] = i;

    setAttribute(FORMAT_ATTR);
    setAttribute(MERGE_ATTR, MERGE);
    setAttribute(PARTITION_ATTR, PARTITION);

    setAttributes(DISTRIBUTE, MERGE_ATTR, PARTITION_ATTR, 0);
    setAttributes(HEADING, FORMAT_ATTR, 0);
    alreadyAssignedNestedTag = createAttribute(_alreadyAssignedNestedTag_Atom);
    return true;
}
MODULE_EXIT()
{
    ::Release(alreadyAssignedNestedTag);
    for (unsigned i=0; i < YY_LAST_TOKEN; i++)
        delete [] nestedAttributeMap[i];
}

// An experiment in programming style.
template <class T>
class RestoreValueBlock
{
public:
    inline RestoreValueBlock(T & _variable) : variable(_variable), oldValue(_variable) {}
    inline RestoreValueBlock(T & _variable, const T newValue) : variable(_variable), oldValue(_variable) { _variable = newValue; }
    inline ~RestoreValueBlock() { variable = oldValue; }

protected:
    T & variable;
    const T oldValue;
};


/* This enables warning on a assignall which tries to reassign a field. */
//#define _WARN_ON_ASSIGNALL

void attribute::annotateExprWithLocation()
{
    if ((atr_type==t_expr) && expr && !queryLocation(expr))
        expr = createLocationAnnotation(expr, pos);
}

bool attribute::isZero() const
{
    IValue * value = queryExpr()->queryValue();
    return (value && (value->getIntValue() == 0));
}

void attribute::setPosition(int _line, int _column, int _position, ISourcePath * _sourcePath)
{
    pos.set(_line, _column, _position, _sourcePath);

    if ((atr_type==t_expr) && expr)
    {
        if (okToAddLocation(expr) && !queryLocation(expr))
            expr = createLocationAnnotation(expr, pos);
    }
}

IHqlExpression * ActiveScopeInfo::createDefaults()
{
    ForEachItemIn(i, activeDefaults)
    {
        if (activeDefaults.item(i).getOperator() != no_omitted)
            return createValueSafe(no_sortlist, makeSortListType(NULL), activeDefaults);
    }
    return NULL;
}

IHqlExpression * ActiveScopeInfo::createFormals(bool oldSetFormat)
{
    assertex(isParametered);
    if (oldSetFormat)
    {
        ForEachItemIn(i, activeParameters)
        {
            IHqlExpression & cur = activeParameters.item(i);
            assertex(cur.getOperator() == no_param);
            ITypeInfo * type = cur.queryType();
            if (type->getTypeCode() == type_set)
            {
                Owned<ITypeInfo> newType = makeAttributeModifier(LINK(type), createAttribute(oldSetFormatAtom));
                HqlExprArray args;
                unwindChildren(args, &cur);
                activeParameters.replace(*createParameter(cur.queryName(), (unsigned)cur.querySequenceExtra(), newType.getClear(), args), i);
            }
        }
    }
    return createValueSafe(no_sortlist, makeSortListType(NULL), activeParameters);
}


void ActiveScopeInfo::newPrivateScope()
{
    if (localScope)
        privateScope.setown(createPrivateScope(localScope));
}

IHqlExpression * ActiveScopeInfo::queryParameter(_ATOM name)
{
    ForEachItemIn(idx, activeParameters)
    {
        IHqlExpression &parm = activeParameters.item(idx);
        if (parm.queryName()==name)
            return &parm;
    }
    return NULL;
}

bool HqlGramCtx::hasAnyActiveParameters()
{
    ForEachItemIn(i2, defineScopes)
    {
        if (defineScopes.item(i2).activeParameters.ordinality())
            return true;
    }
    return false;
}

void HqlGram::gatherActiveParameters(HqlExprCopyArray & target)
{
    ForEachItemIn(i2, defineScopes)
    {
        appendArray(target, defineScopes.item(i2).activeParameters);
    }
}


/* In parm: scope not linked */
HqlGram::HqlGram(IHqlScope * _globalScope, IHqlScope * _containerScope, IFileContents * _text, HqlLookupContext & _ctx, IXmlScope *xmlScope, bool _hasFieldMap, bool loadImplicit)
: lookupCtx(_ctx)
{
    init(_globalScope, _containerScope);
    fieldMapUsed = _hasFieldMap;
    if (!lookupCtx.functionCache)
        lookupCtx.functionCache = &localFunctionCache;

    errorHandler = lookupCtx.errs;
    sourcePath.set(_text->querySourcePath());
    moduleName = _containerScope->queryName();
    forceResult = false;
    lexObject = new HqlLex(this, _text, xmlScope, NULL);

    if(lookupCtx.queryRepository() && loadImplicit && legacyEclSemantics)
    {
        HqlScopeArray scopes;
        getImplicitScopes(scopes, lookupCtx.queryRepository(), _containerScope, lookupCtx);
        ForEachItemIn(i, scopes)
            defaultScopes.append(OLINK(scopes.item(i)));
    }
}

HqlGram::HqlGram(HqlGramCtx & parent, IHqlScope * _containerScope, IFileContents * _text, IXmlScope *xmlScope)
: lookupCtx(parent.lookupCtx)
{
    //This is used for parsing a constant expression inside the preprocessor
    //And reprocessing FORWARD module definitions.

    ForEachItemIn(i2, parent.defineScopes)
        defineScopes.append(OLINK(parent.defineScopes.item(i2)));

    init(parent.globalScope, _containerScope);
    for (unsigned i=0;i<parent.defaultScopes.length();i++)
        defaultScopes.append(*LINK(&parent.defaultScopes.item(i)));
    sourcePath.set(parent.sourcePath);
    errorHandler = lookupCtx.errs;
    moduleName = containerScope->queryName();

    ForEachItemIn(i3, parent.imports)
        parseScope->defineSymbol(LINK(&parent.imports.item(i3)));
         
    //Clone parseScope
    lexObject = new HqlLex(this, _text, xmlScope, NULL);
    forceResult = true;
}

void HqlGram::saveContext(HqlGramCtx & ctx, bool cloneScopes)
{
    ForEachItemIn(i2, defineScopes)
    {
        ActiveScopeInfo & cur = defineScopes.item(i2);
        if (cloneScopes)
        {
            //parameters will be cleared as the rest of the file is parsed => need to preserve
            ActiveScopeInfo & clone = * new ActiveScopeInfo;
            clone.localScope.set(cur.localScope);
            clone.privateScope.set(cur.privateScope);
            clone.isParametered = cur.isParametered;
            appendArray(clone.activeParameters, cur.activeParameters);
            appendArray(clone.activeDefaults, cur.activeDefaults);
            ctx.defineScopes.append(clone);
        }
        else
            ctx.defineScopes.append(OLINK(cur));
    }

    ctx.globalScope.set(globalScope);
    appendArray(ctx.defaultScopes, defaultScopes);
    ctx.sourcePath.set(sourcePath);
    parseScope->getSymbols(ctx.imports);
};

IHqlScope * HqlGram::queryGlobalScope()
{
    return globalScope;
}

void HqlGram::init(IHqlScope * _globalScope, IHqlScope * _containerScope)
{
    minimumScopeIndex = 0;
    isQuery = false;
    legacyEclSemantics = queryLegacyEclSemantics();
    current_id = NULL;
    lexObject = NULL;
    expectedAttribute = NULL;
    pendingAttributes = NULL;

    outerScopeAccessDepth = 0;
    inType = false;
    aborting = false;
    errorHandler = NULL;
    moduleName = NULL;
    resolveSymbols = true;
    lastpos = 0;
    
    containerScope = _containerScope;
    globalScope = _globalScope;
    parseScope.setown(createPrivateScope(_containerScope));
    transformScope = NULL;
    if (globalScope->queryName() && legacyEclSemantics)
        parseScope->defineSymbol(globalScope->queryName(), NULL, LINK(queryExpression(globalScope)), false, false, ob_import);

    boolType = makeBoolType();
    defaultIntegralType = makeIntType(8, true);
    uint4Type = makeIntType(4, false);
    defaultRealType = makeRealType(DEFAULT_REAL_SIZE);

    current_type = NULL;
    curTransform = NULL;
    insideEvaluate = false;
    fieldMapUsed = false;
    associateWarnings = true;

    errorDisabled = false;
    setIdUnknown(false);
    m_maxErrorsAllowed = DEFAULT_MAX_ERRORS;
    sortDepth = 0;
    serviceScope.clear();
    selfUsedOnRhs = false;

    if (globalScope->isPlugin())
    {
        StringBuffer plugin, version;
        globalScope->getProp(pluginAtom, plugin);
        globalScope->getProp(versionAtom, version);
        serviceExtraAttributes.setown(createAttribute(pluginAtom, createConstant(plugin.str()), createConstant(version.str())));
    }
}

HqlGram::~HqlGram()
{
    errorHandler = NULL;
    delete lexObject;
    boolType->Release();
    defaultIntegralType->Release();
    uint4Type->Release();
    defaultRealType->Release();

    cleanCurTransform();
}                        

int HqlGram::yyLex(attribute * yylval, const short * activeState)
{
    if (aborting) return 0;
    return lexObject->yyLex(*yylval, resolveSymbols, activeState);
}


void HqlGram::cleanCurTransform()
{
    attribute pseudoErrPos;
    loop
    {
        IHqlExpression * ret = endTransform(pseudoErrPos);
        if (!ret)
            break;
        ret->Release();
    }
}

void HqlGram::pushTopScope(IHqlExpression *newScope)
{
    newScope->Link();
    topScopes.append(*newScope);
    wasInEvaluate.append(insideEvaluate);
    insideEvaluate = false;
}

void HqlGram::pushLeftScope(IHqlExpression *newScope)
{
    newScope->Link();
    leftScopes.append(*newScope);
}

void HqlGram::pushRightScope(IHqlExpression *newScope)
{
    newScope->Link();
    rightScopes.append(*newScope);
}

void HqlGram::pushRowsScope(IHqlExpression *newScope)
{
    newScope->Link();
    rowsScopes.append(*newScope);
    rowsIds.append(*createUniqueRowsId());
}

/* in: linked */
void HqlGram::pushSelfScope(IHqlExpression *newScope)
{
    selfScopes.append(*newScope);
}

void HqlGram::pushSelfScope(ITypeInfo * selfType)
{
    IHqlExpression * record = queryExpression(selfType);
    pushSelfScope(createSelector(no_self, record, NULL));
}

void HqlGram::popTopScope()
{
    if(topScopes.length() > 0)
    {
        topScopes.pop();
        insideEvaluate = wasInEvaluate.pop();
    }
}                                       

void HqlGram::popLeftScope()
{
    if(leftScopes.length() > 0)
        leftScopes.pop();
}  

void HqlGram::popRightScope()
{
    if(rightScopes.length() > 0)
        rightScopes.pop();
} 

IHqlExpression * HqlGram::popRowsScope()
{
    if(rowsScopes.length() > 0)
    {
        rowsScopes.pop();
        return &rowsIds.popGet();
    }
    return createAttribute(_rowsid_Atom, createConstant(0));
} 

void HqlGram::popSelfScope()
{
    if(selfScopes.length() > 0)
        selfScopes.pop();
} 

void HqlGram::swapTopScopeForLeftScope()
{
    assertex(topScopes.ordinality() > 0);
    leftScopes.append(topScopes.popGet());
}                                       

IHqlExpression * HqlGram::getSelectorSequence()
{
    if (activeSelectorSequences.ordinality())
        return LINK(&activeSelectorSequences.tos());
    //Can occur when LEFT is used in an invalid context
    return createDummySelectorSequence();
}

void HqlGram::pushLocale(IHqlExpression *newLocale)
{
    localeStack.append(*newLocale);
}

void HqlGram::popLocale()
{
    if(localeStack.length() > 0)
        localeStack.pop();
} 

IHqlExpression * HqlGram::queryDefaultLocale()
{
    if(localeStack.length() == 0)
        return NULL;
    return &localeStack.tos();
}

void HqlGram::pushRecord(IHqlExpression *newRecord)
{
    activeRecords.append(*newRecord);
}                                       

IHqlExpression* HqlGram::popRecord()
{
    return &activeRecords.pop();
}                                       


void HqlGram::beginFunctionCall(attribute & function)
{
    IHqlExpression * funcdef = function.queryExpr();
    //Check for strange situation where this isn't really a function (e.g., an alien type)
    //but the grammar allows optional parameters - so need a record on the stack
    if (!funcdef->isFunction()) 
        funcdef = NULL;     
    activeFunctionCalls.append(*new FunctionCallInfo(funcdef));
}

void HqlGram::addActual(const attribute & errpos, IHqlExpression * ownedExpr)
{
    assertex(ownedExpr);
    FunctionCallInfo & call = activeFunctionCalls.tos();
    OwnedHqlExpr releaseExpr = ownedExpr;
    processParameter(call, NULL, ownedExpr, errpos);
//  call.actuals.append(*LINK(ownedExpr));
}

void HqlGram::addNamedActual(const attribute & errpos, _ATOM name, IHqlExpression * ownedExpr)
{
    assertex(ownedExpr);
    FunctionCallInfo & call = activeFunctionCalls.tos();
    OwnedHqlExpr releaseExpr = ownedExpr;
    processParameter(call, name, ownedExpr, errpos);
//  IHqlExpression * named = createValue(no_namedactual, makeVoidType(), createAttribute(name), ownedExpr);
//  call.actuals.append(*named);
}

IHqlExpression * HqlGram::endFunctionCall()
{
    FunctionCallInfo & call = activeFunctionCalls.tos();
    leaveActualTopScope(call);
    OwnedHqlExpr ret = call.getFinalActuals();
    activeFunctionCalls.pop();
    return ret.getClear();
}

IHqlExpression * HqlGram::createUniqueId()
{
    HqlExprArray args;
    ForEachItemIn(i, defineScopes)
    {
        ActiveScopeInfo & curScope = defineScopes.item(i);
        appendArray(args, curScope.activeParameters);
    }

    if (args.ordinality())
    {
        args.add(*::createUniqueId(), 0);
        return createExprAttribute(_uid_Atom, args);
    }
    return ::createUniqueId();
}

IHqlExpression * HqlGram::doCreateUniqueSelectorSequence()
{
    HqlExprArray args;
    ForEachItemIn(i, defineScopes)
    {
        ActiveScopeInfo & curScope = defineScopes.item(i);
        appendArray(args, curScope.activeParameters);
    }

    if (args.ordinality())
    {
        args.add(*createUniqueId(), 0);
        return createAttribute(_selectorSequence_Atom, args);
    }
    return ::createUniqueSelectorSequence();
}



IHqlExpression * HqlGram::createActiveSelectorSequence(IHqlExpression * left, IHqlExpression * right)
{
#ifdef USE_SELSEQ_UID
    if (left || right)
    {
        HqlExprArray args;
        if (left) args.append(*LINK(left->queryNormalizedSelector()));
        if (right) args.append(*LINK(right->queryNormalizedSelector()));
        return createAttribute(_selectorSequence_Atom, args);
    }

    return ::createUniqueSelectorSequence();

    return doCreateUniqueSelectorSequence();
#else
    return createSelectorSequence();
#endif
}

void HqlGram::pushSelectorSequence(IHqlExpression * ds1, IHqlExpression * ds2)
{
    IHqlExpression * selSeq = createActiveSelectorSequence(ds1, ds2);   
    activeSelectorSequences.append(*selSeq);
}

void HqlGram::pushUniqueSelectorSequence()
{
    IHqlExpression * selSeq = ::createUniqueSelectorSequence();
    activeSelectorSequences.append(*selSeq);
}

IHqlExpression * HqlGram::popSelectorSequence()
{
    if (activeSelectorSequences.ordinality())
        return &activeSelectorSequences.popGet();
    else
        return createDummySelectorSequence();
}

void HqlGram::beginList()
{
    if (curList) 
        curListStack.append(*curList.getClear()); 
    curList.setown(createOpenValue(no_comma, makeNullType())); 
}

void HqlGram::addListElement(IHqlExpression * expr)
{
    if (expr)
        curList->addOperand(expr);
}

void HqlGram::endList(HqlExprArray & args)
{
    OwnedHqlExpr list = curList.getClear()->closeExpr();
    if (curListStack.ordinality())
        curList.setown(&curListStack.popGet());
    unwindChildren(args, list);
}

IHqlExpression * HqlGram::getActiveCounter(attribute & errpos)
{
    OwnedHqlExpr tempCounter;
    OwnedHqlExpr * ref;
    if (counterStack.empty())
    {
        reportError(ERR_COUNT_ILL_HERE, errpos, "COUNTER not valid in this context");
        ref = &tempCounter;
    }
    else
        ref = &counterStack.tos().value;

    if (!ref->get())
        ref->setown(createCounter());
    return LINK(*ref);
}


static StringBuffer& getFldName(IHqlExpression* field, StringBuffer& s)
{
    switch (field->getOperator())
    {
    case no_select:
        getFldName(field->queryChild(0), s).append(".");
        getFldName(field->queryChild(1), s);
        break;
    case no_field:
    default:
        s.append(field->queryName()->str());
        break;
    case no_self:
    case no_left:
    case no_right:
    case no_top:
    case no_activetable:
        s.append(getOpString(field->getOperator()));
        break;
    }
    return s;
}

IHqlExpression * HqlGram::translateFieldsToNewScope(IHqlExpression * expr, IHqlSimpleScope * record, const attribute & err)
{
    switch (expr->getOperator())
    {
    case no_field:
        return record->lookupSymbol(expr->queryName());
    case no_select:
        {
            OwnedHqlExpr newDs = translateFieldsToNewScope(expr->queryChild(0), record, err);
            IHqlExpression * lhsRecord = newDs->queryRecord();
            OwnedHqlExpr newField;
            _ATOM name = expr->queryChild(1)->queryName();
            if (lhsRecord)
                newField.setown(lhsRecord->querySimpleScope()->lookupSymbol(name));
            if (!newField)
                newField.setown(record->lookupSymbol(name));
            if (!newField)
            {
                reportError(ERR_FIELD_NOT_FOUND, err, "Could not find field %s", name->str());
                return LINK(expr);
            }
            return createSelectExpr(newDs.getClear(), newField.getClear());
        }
    }
    bool same = true;
    HqlExprArray args;
    ForEachChild(idx, expr)
    {
        IHqlExpression * cur = expr->queryChild(idx);
        IHqlExpression * updated = translateFieldsToNewScope(cur, record, err);
        if (cur != updated)
            same = false;
        args.append(*updated);
    }
    if (same)
        return LINK(expr);
    return expr->clone(args);
}

DefineIdSt * HqlGram::createDefineId(int scope, ITypeInfo * ownedType)
{ 
    DefineIdSt* defineid = new DefineIdSt();
    defineid->scope = scope;
    defineid->setType(ownedType);
    defineid->id   = current_id;
    defineid->setDoc(lexObject->getClearJavadoc());
    return defineid;
}


void HqlGram::beginAlienType(const attribute & errpos)
{
    if (inType)
        reportError(ERR_USRTYPE_NESTEDDECL, errpos, "Cannot nest TYPE declarations");

    inType = true;
    enterType(errpos, queryParametered());
}

void HqlGram::beginDefineId(_ATOM name, ITypeInfo * type)
{
    current_id = name;
    current_type = type;
}


IHqlExpression * HqlGram::processAlienType(const attribute & errpos)
{
    IHqlScope * alienScope = NULL;
    ActiveScopeInfo & cur = defineScopes.tos();
    if (checkAlienTypeDef(cur.localScope, errpos))
        alienScope = LINK(cur.localScope);
    leaveType(errpos);

    if (alienScope)
        return (createAlienType(current_id, closeScope(alienScope)))->closeExpr();
    return createConstant(false);
}


IHqlExpression * HqlGram::processCompoundFunction(attribute & resultAttr, bool outOfLine)
{
    OwnedHqlExpr resultExpr = resultAttr.getExpr();
    OwnedHqlExpr expr = associateSideEffects(resultExpr, resultAttr.pos);
    leaveScope(resultAttr);
    leaveCompoundObject();

    return expr.getClear();
}

IHqlExpression * HqlGram::convertToOutOfLineFunction(const ECLlocation & errpos, IHqlExpression  * expr)
{
    if (expr->getOperator() != no_outofline)
    {
        if (queryParametered())
        {
            OwnedHqlExpr mapped = convertWorkflowToImplicitParmeters(defineScopes.tos().activeParameters, defineScopes.tos().activeDefaults, expr);
            if (containsWorkflow(mapped))
            {
                reportError(ERR_USER_FUNC_NO_WORKFLOW, errpos, "Out of line user functions cannot contain workflow/stored");
                return mapped.getClear();
            }
            return createWrapper(no_outofline, mapped.getClear());
        }
    }
    return LINK(expr);
}

IHqlExpression * HqlGram::processCppBody(const attribute & errpos, IHqlExpression * cpp)
{
    HqlExprArray args;
    args.append(*LINK(cpp));
    Linked<ITypeInfo> type = current_type;
    if (!type)
        type.setown(makeVoidType());

    IHqlExpression * record = queryOriginalRecord(type);
    OwnedHqlExpr result;
    if (record)
    {
        args.append(*LINK(record));
        if (hasLinkCountedModifier(type))
            args.append(*getLinkCountedAttr());
        if (hasStreamedModifier(type))
            args.append(*getStreamedAttr());
        switch (type->getTypeCode())
        {
        case type_row:
        case type_record:
            result.setown(createRow(no_cppbody, args));
            break;
        case type_table:
        case type_groupedtable:
            result.setown(createDataset(no_cppbody, args));
            break;
        case type_transform:
            result.setown(createValue(no_cppbody, makeTransformType(LINK(record->queryType())), args));
            break;
        default:
            throwUnexpected();
        }
    }
    else
        result.setown(createValue(no_cppbody, LINK(type), args));

    result.setown(createLocationAnnotation(result.getClear(), errpos.pos));

    if (queryParametered())
        return createWrapper(no_outofline, result.getClear());
    return result.getClear();
}


IHqlExpression * HqlGram::processUserAggregate(const attribute & mainPos, attribute & dsAttr, attribute & recordAttr, attribute & transformAttr, attribute * mergeAttr,
                                      attribute *itemsAttr, attribute &rowsAttr, attribute &seqAttr)
{
    HqlExprArray sortItems;
    endList(sortItems);
    OwnedHqlExpr dataset = dsAttr.getExpr();
    OwnedHqlExpr record = recordAttr.getExpr();
    OwnedHqlExpr transform = transformAttr.getExpr();
    OwnedHqlExpr merge = mergeAttr ? mergeAttr->getExpr() : NULL;
    OwnedHqlExpr rowsid = rowsAttr.getExpr();
    OwnedHqlExpr selSeq = seqAttr.getExpr();
    //check the record match

    OwnedHqlExpr attrs;
    OwnedHqlExpr grouping = itemsAttr ? processSortList(*itemsAttr, no_aggregate, dataset, sortItems, NULL, &attrs) : NULL;

    if (grouping && (dataset->getOperator() == no_group) && isGrouped(dataset))
        reportWarning(WRN_GROUPINGIGNORED, dsAttr.pos, "Grouping of aggregate input will have no effect, was this intended?");

    HqlExprArray args;
    args.append(*LINK(dataset));
    args.append(*LINK(record));
    args.append(*LINK(transform));
    if (grouping)
        args.append(*LINK(grouping));
    if (merge)
        args.append(*createExprAttribute(mergeAtom, merge.getClear()));
    if (attrs)
        attrs->unwindList(args, no_comma);
    args.append(*LINK(rowsid));
    args.append(*LINK(selSeq));
    OwnedHqlExpr ret = createDataset(no_aggregate, args);
    checkAggregateRecords(ret, record, transformAttr);
    return ret.getClear();
}

IHqlExpression * HqlGram::processIndexBuild(attribute & indexAttr, attribute * recordAttr, attribute * payloadAttr, attribute & filenameAttr, attribute & flagsAttr)
{
    if (!recordAttr)
        warnIfRecordPacked(indexAttr);

    transferOptions(filenameAttr, flagsAttr);
    normalizeExpression(filenameAttr, type_string, false);

    OwnedHqlExpr dataset = indexAttr.getExpr();
    checkBuildIndexFilenameFlags(dataset, flagsAttr);

    LinkedHqlExpr inputDataset = dataset;
    OwnedHqlExpr flags = flagsAttr.getExpr();
    if (recordAttr)
    {
        OwnedHqlExpr record = recordAttr->getExpr();
        if (payloadAttr)
        {
            OwnedHqlExpr payload = payloadAttr->getExpr();
            checkIndexRecordType(record, 0, false, *recordAttr);
            checkIndexRecordType(payload, payload->numChildren(), false, *payloadAttr);
            modifyIndexPayloadRecord(record, payload, flags, indexAttr);
        }
        else
        {
            checkIndexRecordType(record, 1, false, *recordAttr);
        }
        record.setown(checkBuildIndexRecord(record.getClear(), *recordAttr));
        record.setown(checkIndexRecord(record, *recordAttr));
        inputDataset.setown(createDatasetF(no_selectfields, LINK(dataset), LINK(record), NULL));
        warnIfRecordPacked(inputDataset, *recordAttr);
    }
    else
    {
        checkIndexRecordType(dataset->queryRecord(), 1, false, indexAttr);
    }


    HqlExprArray args;
    args.append(*LINK(inputDataset));
    args.append(*filenameAttr.getExpr());
    if (flags)
        flags->unwindList(args, no_comma);
    checkDistributer(flagsAttr, args);
    return createValue(no_buildindex, makeVoidType(), args);
}

void HqlGram::processError(bool full)
{
    releaseScopes();
    sortDepth = 0;
    if (full)
    {
        resetParameters();
        current_id = NULL;
        current_type = NULL;
    }
}

void HqlGram::processEnum(attribute & idAttr, IHqlExpression * value)
{
    if (value)
        lastEnumValue.setown(ensureExprType(value, curEnumType));
    else
        lastEnumValue.setown(nextEnumValue());

    DefineIdSt * id = new DefineIdSt;
    id->id = idAttr.getName();
    id->scope = EXPORT_FLAG;
    doDefineSymbol(id, LINK(lastEnumValue), NULL, idAttr, idAttr.pos.position, idAttr.pos.position, false);
}

bool HqlGram::extractConstantString(StringBuffer & text, attribute & attr)
{
    normalizeExpression(attr, type_string, true);
    OwnedHqlExpr str = attr.getExpr();
    if (!str->queryValue())
    {
        reportError(999, attr, "Const-foldable string expression expected");
        return false;
    }

    str->queryValue()->getStringValue(text);
    return true;
}

void HqlGram::processLoadXML(attribute & a1, attribute * a2)
{
    StringBuffer s1, s2;
    if (extractConstantString(s1, a1))
    {
        if (a2)
        {
            if (extractConstantString(s2, *a2))
                lexObject->loadXML(a1, s1.str(), s2.str());
        }
        else
            lexObject->loadXML(a1, s1.str());
    }
}

IHqlExpression * HqlGram::processModuleDefinition(const attribute & errpos)
{
    Owned<IHqlScope> scope = defineScopes.tos().localScope;
    if (!scope)
        scope.setown(createScope());
    leaveScope(errpos);
    leaveCompoundObject();
    cloneInheritedAttributes(scope, errpos);
    OwnedHqlExpr newScope;
    try
    {
        newScope.setown(closeAndLink(queryExpression(scope)));
    }
    catch (IException * e)
    {
        StringBuffer s;
        reportError(e->errorCode(), errpos, "%s", e->errorMessage(s).str());
        e->Release();
        newScope.setown(createNullScope());
    }

    //Implements projects the module down to the implementation type.
    //It should also check the parameters match when a symbol is defined.
    IHqlExpression * libraryInterface = newScope->queryProperty(libraryAtom);
    if (libraryInterface)
        newScope.setown(implementInterfaceFromModule(errpos, errpos, newScope, libraryInterface->queryChild(0), libraryInterface));
    return newScope.getClear();
}

IHqlExpression * HqlGram::processRowset(attribute & selectorAttr)
{
    OwnedHqlExpr ds = selectorAttr.getExpr();
    unsigned match = rowsScopes.find(*ds);
    IHqlExpression * id = NULL;
    if (match == NotFound)
    {
        if (rowsScopes.ordinality() == 0)
            reportError(ERR_LEFT_ILL_HERE, selectorAttr, "ROWS not legal here");
        else
            reportError(ERR_LEFT_ILL_HERE, selectorAttr, "ROWS not legal on this dataset");

        OwnedHqlExpr selSeq = createDummySelectorSequence();
        ds.setown(createSelector(no_left, queryNullRecord(), selSeq));
    }
    else
        id = &OLINK(rowsIds.item(match));
    OwnedHqlExpr rows = createDataset(no_rows, LINK(ds), id);
    return createValue(no_rowset, makeSetType(rows->getType()), LINK(rows));
}

void HqlGram::processServiceFunction(const attribute & idAttr, _ATOM name, IHqlExpression * thisAttrs, ITypeInfo * type)
{
    setParametered(true);
    IHqlExpression *attrs = createComma(LINK(thisAttrs), LINK(defaultServiceAttrs));
    attrs = checkServiceDef(serviceScope,name,attrs,idAttr);
    bool oldSetFormat = queryPropertyInList(oldSetFormatAtom, attrs) != NULL;
    IHqlExpression *call = createExternalReference(name, LINK(type), attrs);
    IHqlExpression * formals = defineScopes.tos().createFormals(oldSetFormat);
    IHqlExpression * defaults = defineScopes.tos().createDefaults();
    IHqlExpression * func = createFunctionDefinition(name, call, formals, defaults, NULL);
    serviceScope->defineSymbol(name, NULL, func, true, false, 0, NULL, idAttr.pos.lineno, idAttr.pos.column, 0, 0, 0);
    resetParameters();
}

void HqlGram::processStartTransform(const attribute & errpos)
{
    Linked<ITypeInfo> transformType = queryCurrentRecordType();
    if (!transformType || !queryRecordType(transformType))
    {
        // bad type: hard to recover
        StringBuffer msg("TRANSFORM must have a record return type: ");
        if (!current_type)
            msg.append("<none> is given");
        else
            getFriendlyTypeStr(current_type, msg).append(" is given");
        reportError(ERR_TRANS_RECORDTYPE, errpos, "%s", msg.str());
        reportError(ERR_PARSER_CANNOTRECOVER,errpos,"Can not recover from previous error(s) - aborting compilation");

        abortParsing();
        transformType.set(queryNullRecord()->queryType());
    }
    
    if (false)
    {
        ITypeInfo * original = queryModifier(current_type, typemod_original);
        if (original && !queryModifier(transformType, typemod_original))
        {
            IHqlExpression * originalExpr = (IHqlExpression *)original->queryModifierExtra();
            transformType.setown(makeOriginalModifier(transformType.getClear(), LINK(originalExpr)));
        }
    }

    openTransform(transformType);
}

void HqlGram::enterEnum(ITypeInfo * type)
{
    enterScope(true);
    enterCompoundObject();
    curEnumType.set(type);
    lastEnumValue.setown(createConstant(curEnumType->castFrom(true, 0)));
}

IHqlExpression * HqlGram::leaveEnum(const attribute & errpos)
{
    IHqlScope * enumScope = LINK(defineScopes.tos().localScope);
    leaveScope(errpos);
    leaveCompoundObject();

    return createEnumType(curEnumType.getClear(), closeScope(enumScope));
}

IHqlExpression * HqlGram::nextEnumValue()
{
    IValue * value = lastEnumValue->queryValue();
    if (value)
        return createConstant(curEnumType->castFrom(true, value->getIntValue()+1));
    return createValue(no_add, LINK(curEnumType), LINK(lastEnumValue), createConstant(curEnumType->castFrom(true, 1)));
}


void HqlGram::enterService(attribute & attrs)
{
    enterScope(false);      // preserve parameters
    serviceScope.setown(createService());
    defaultServiceAttrs.setown(attrs.getExpr());
}

IHqlExpression * HqlGram::leaveService(const attribute & errpos)
{
    defaultServiceAttrs.clear();
    IHqlExpression* svc = QUERYINTERFACE(serviceScope.getClear(), IHqlExpression);
    if (svc)
        svc = svc->closeExpr();
    leaveScope(errpos);
    return svc;
}

//-----------------------------------------------------------------------------


/* this func does not affect linkage */
/* Assume: field is of the form: (((self.r1).r2...).rn). */
IHqlExpression * HqlGram::findAssignment(IHqlExpression *field)
{
//  assertex(field->getOperator() == no_select);
#ifdef FAST_FIND_FIELD
    IHqlExpression * match = (IHqlExpression *)field->queryTransformExtra();
    if (match && !match->isAttribute())
        return match;
    return NULL;
#endif

    unsigned kids = curTransform->numChildren();
    for (unsigned idx = 0; idx < kids; idx++)
    {
        IHqlExpression *kid = curTransform->queryChild(idx);
        IHqlExpression * ret = doFindAssignment(kid,field);
        if (ret)
            return ret;
    }

    return NULL;
}

IHqlExpression * HqlGram::doFindAssignment(IHqlExpression* in, IHqlExpression* field)
{
    switch (in->getOperator())
    {
    case no_assign:
        if (in->queryChild(0) == field)
            return in->queryChild(1);
        return NULL;
    case no_assignall:
        {
            unsigned kids = in->numChildren();
            for (unsigned idx = 0; idx < kids; idx++)
            {
                IHqlExpression *kid = in->queryChild(idx);
                IHqlExpression * ret = doFindAssignment(kid,field);
                if (ret)
                    return ret;
            }
        }
        break;
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        return NULL;
    default:
        assertex(false);
    }
    return NULL;
}

/* All in parms will be consumed by this function */
void HqlGram::addAssignment(attribute & target, attribute &source)
{
    OwnedHqlExpr targetExpr = target.getExpr();
    OwnedHqlExpr srcExpr = source.getExpr();

    if (!srcExpr) // something bad just happened.
        return; 
    
    node_operator targetOp = targetExpr->getOperator();
    if (targetOp ==no_self) // self := expr;
    {
        ITypeInfo* type = srcExpr->queryType();
        if (!type)
            type = queryCurrentTransformType();
        
        switch(type->getTypeCode())
        {
            case type_record:
            case type_row:
                addAssignall(targetExpr.getClear(), srcExpr.getClear(), target);
                break;
            default:
                {
                    StringBuffer msg("Can not assign non-record type ");
                    getFriendlyTypeStr(type, msg).append(" to self");
                    reportError(ERR_TRANS_ILLASSIGN2SELF, target, "%s", msg.str());
                }
        }
    }
    else if (targetOp == no_select)
    {
        // self.* := expr;      assertex(targetExpr->getOperator()==no_select);
        if (findAssignment(targetExpr))
        {
            StringBuffer s;
            reportError(ERR_VALUEDEFINED, target, "A value for \"%s\" has already been specified", getFldName(targetExpr,s).str());
        }
        else if (targetExpr->queryType()->getTypeCode() == type_row)
        {
            //assertex(srcExpr->getOperator() == no_null);
            addAssignall(targetExpr.getClear(), srcExpr.getClear(), target);
        }
        else
            doAddAssignment(curTransform, targetExpr.getClear(), srcExpr.getClear(), target);
    }
    //else error occurred somewhere else
}


void HqlGram::addAssignment(const attribute & errpos, IHqlExpression * targetExpr, IHqlExpression * srcExpr)
{
    if (!srcExpr) // something bad just happened.
        return; 
    
    node_operator targetOp = targetExpr->getOperator();
    if (targetOp ==no_self) // self := expr;
    {
        ITypeInfo* type = srcExpr->queryType();
        if (!type)
            type = queryCurrentTransformType();
        
        switch(type->getTypeCode())
        {
            case type_record:
            case type_row:
                addAssignall(LINK(targetExpr), LINK(srcExpr), errpos);
                break;
            default:
                {
                    StringBuffer msg("Can not assign non-record type ");
                    getFriendlyTypeStr(type, msg).append(" to self");
                    reportError(ERR_TRANS_ILLASSIGN2SELF, errpos, "%s", msg.str());
                }
        }
    }
    else if (targetOp == no_select)
    {
        // self.* := expr;      assertex(targetExpr->getOperator()==no_select);
        if (findAssignment(targetExpr))
        {
            StringBuffer s;
            reportError(ERR_VALUEDEFINED, errpos, "A value for \"%s\" has already been specified", getFldName(targetExpr,s).str());
        }
        else if (targetExpr->queryType()->getTypeCode() == type_row)
        {
            //assertex(srcExpr->getOperator() == no_null);
            addAssignall(LINK(targetExpr), LINK(srcExpr), errpos);
        }
        else
            doAddAssignment(curTransform, LINK(targetExpr), LINK(srcExpr), errpos);
    }
    //else error occurred somewhere else
}


class SelfReferenceReplacer
{
public:
    SelfReferenceReplacer(IHqlExpression * transform, IHqlExpression * _self) : self(_self)
    {
        lockTransformMutex();
        expandTransformAssigns(transform);
    }
    ~SelfReferenceReplacer()
    {
        unlockTransformMutex();
    }

    IHqlExpression * replaceExpression(IHqlExpression * expr)
    {
        ok = true;
        return recursiveReplaceExpression(expr);
    }

    inline bool allFieldsReplaced() { return ok; }

protected:
    IHqlExpression * recursiveReplaceExpression(IHqlExpression * expr)
    {
        IHqlExpression * mapped = (IHqlExpression *)expr->queryTransformExtra();
        if (mapped)
           return LINK(mapped);

        if (expr == self)
            ok = false;

        IHqlExpression * ret;
        unsigned max = expr->numChildren();
        if (max == 0)
            ret = LINK(expr);
        else
        {
            switch (expr->getOperator())
            {
            case no_attr:
            case no_attr_expr:
            case no_left:
            case no_right:
            case no_field:
            case no_record:
                ret = LINK(expr);
                break;
            default:
                {
                    bool same = true;
                    HqlExprArray args;
                    for (unsigned i=0; i< max; i++)
                    {
                        IHqlExpression * cur = expr->queryChild(i);
                        IHqlExpression * tr = recursiveReplaceExpression(cur);
                        args.append(*tr);
                        if (cur != tr)
                            same = false;
                    }

                    if (same)
                        ret = LINK(expr);
                    else
                        ret = expr->clone(args);
                }
                break;
            }
        }

        expr->setTransformExtra(ret);
        return ret;
    }

    void expandTransformAssigns(IHqlExpression * expr)
    {
        ForEachChild(i, expr)
        {
            IHqlExpression * cur = expr->queryChild(i);

            switch (cur->getOperator())
            {
            case no_transform:
            case no_newtransform:
            case no_assignall:
                expandTransformAssigns(cur);
                break;
            case no_assign:
                {
                    IHqlExpression * tgt = cur->queryChild(0);
                    OwnedHqlExpr castRhs = ensureExprType(cur->queryChild(1), tgt->queryType());
                    tgt->setTransformExtraOwned(castRhs.getClear());
                    break;
                }
            }
        }
    }


protected:
    LinkedHqlExpr self;
    bool ok;
};

IHqlExpression * HqlGram::replaceSelfReferences(IHqlExpression * transform, IHqlExpression * rhs, IHqlExpression * self, const attribute& errpos)
{
    //MORE: This could be done more efficiently by replacing all the self references in a single pass
    //would need to tag assigns in the transform, and process incrementally at the end.
    //Seems to be fast enough anyway at the moment.
    SelfReferenceReplacer replacer(transform, self);

    OwnedHqlExpr ret = replacer.replaceExpression(rhs);
    if (!replacer.allFieldsReplaced())
        reportError(ERR_SELF_ILL_HERE, errpos, "Reference to field in SELF that has not yet been defined");
    return ret.getClear();
}

/* all in parms: linked */
void HqlGram::doAddAssignment(IHqlExpression * transform, IHqlExpression * _field, IHqlExpression * _rhs, const attribute& errpos)
{
    //The arguments really shouldn't be linked
    OwnedHqlExpr field = _field;
    OwnedHqlExpr rhs = _rhs;
    
    assertex(field->getOperator()==no_select);
    if (containsSkip(rhs) && field->queryChild(0)->getOperator() != no_self)
        reportError(ERR_SKIP_IN_NESTEDCHILD, errpos, "SKIP in an assignment to a field in a nested record is not supported");

    if (selfUsedOnRhs)
    {
        OwnedHqlExpr self = getSelf(curTransform);
        rhs.setown(replaceSelfReferences(curTransform, rhs, self, errpos));
    }
    //
    // type checking
    ITypeInfo* fldType = field->queryType();
    Owned<ITypeInfo> rhsType = rhs->getType();
    if (!rhsType)           // this happens when rhs is no_null.
        rhsType.set(fldType);

    // handle alien type
    if (rhsType->getTypeCode() == type_alien)
    {
        IHqlAlienTypeInfo * alien = queryAlienType(rhsType);
        rhsType.setown(alien->getLogicalType());
    }

    if (!fldType->assignableFrom(rhsType))
    {
        StringBuffer msg("Can not assign ");
        getFriendlyTypeStr(rhsType,msg).append(" to ");
        getFriendlyTypeStr(fldType,msg).append(" (field ");
        getFldName(field,msg).append(")");
        reportError(ERR_TYPE_INCOMPATIBLE,errpos, "%s", msg.str()); 
    }

    appendTransformAssign(transform, field, rhs, errpos);
}


void HqlGram::appendTransformAssign(IHqlExpression * transform, IHqlExpression * to, IHqlExpression * _from, const attribute& errpos)
{
    LinkedHqlExpr from = _from;
    if (from->isConstant())
        from.setown(ensureExprType(from, to->queryType()));
    if (to->isDataset() && !recordTypesMatch(from, to))
    {
        //Fields are assignment compatible, but not the same => project down to the target field.
        if (from->queryRecord())        // project against cascading errors
            from.setown(createDefaultProjectDataset(to->queryRecord(), from, errpos));
    }

    IHqlExpression * assign = createAssign(LINK(to), LINK(from));
    if (okToAddLocation(assign))
        assign = createLocationAnnotation(assign, errpos.pos);
    transform->addOperand(assign);
#ifdef FAST_FIND_FIELD
    to->setTransformExtraOwned(from.getClear());

    IHqlExpression * parent = to->queryChild(0);
    while ((parent->getOperator() == no_select) && !parent->queryTransformExtra())
    {
        parent->setTransformExtra(alreadyAssignedNestedTag);
        parent = parent->queryChild(0);
    }
#endif
}

IHqlExpression * HqlGram::forceEnsureExprType(IHqlExpression * expr, ITypeInfo * type)
{
    //Ensure the no_outofline remains the top most node.  I suspect this casting should occur much earlier
    //or the out of line node should be added much later.
    if (expr->getOperator() == no_outofline)
    {
        OwnedHqlExpr ret = forceEnsureExprType(expr->queryChild(0), type);
        return createWrapper(no_outofline, LINK(ret));
    }
        
    OwnedHqlExpr ret = ensureExprType(expr, type);
    if (ret->queryType() == type)
        return ret.getClear();

    return createValue(no_implicitcast, LINK(type), LINK(ret));
}

//===================== Collective assignment ==================================//

/* All in parms: linked */
static bool containsSelect(IHqlExpression * expr, IHqlExpression * ds)
{
    loop
    {
        if (expr == ds)
            return true;
        if (expr->getOperator() != no_select)
            return false;
        expr = expr->queryChild(0);
    }
}

/*
static bool doHaveAssignedToChildren(IHqlExpression * select, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_ifblock:
            if (doHaveAssignedToChildren(select, cur->queryChild(1)))
                return true;
            break;
        case no_field:
            {
                OwnedHqlExpr selected = createSelectExpr(LINK(select), LINK(cur));
                if (findAssignment(selected))
                    return true;
                IHqlExpression * child = cur->queryRecord();
                if (selected->isDatarow() && doHaveAssignedToChildren(selected, cur->queryRecord()))
                    return true;
                break;
            }
        }
    }
}
                    
bool newhaveAssignedToChildren(IHqlExpression * select, IHqlExpression * transform)
{
    return doHaveAssignedToChildren(select, select->queryRecord());
}
*/  
bool haveAssignedToChildren(IHqlExpression * select, IHqlExpression * transform)
{
    ForEachChild(i, transform)
    {
        IHqlExpression * assign = transform->queryChild(i);
        switch (assign->getOperator())
        {
        case no_assign:
            if (containsSelect(assign->queryChild(0), select))
                return true;
            break;
        case no_assignall:
            if (haveAssignedToChildren(select, assign))
                return true;
            break;
            IHqlExpression * child0 = assign->queryChild(0);
            //Only need to check the first subfield, or field of originalAttr
            //if (haveAssignedToChildren(select, assign))
            if (child0 && containsSelect(child0->queryChild(0), select))
                return true;
            break;
        }
    }
    return false;
}

bool HqlGram::haveAssignedToChildren(IHqlExpression * select)
{
#ifdef FAST_FIND_FIELD
    return select->queryTransformExtra() == alreadyAssignedNestedTag;
#endif
    return ::haveAssignedToChildren(select, curTransform);
}

void HqlGram::addAssignall(IHqlExpression *tgt, IHqlExpression *src, const attribute& errpos)
{
    assertex(src);

    node_operator tgtOp = tgt->getOperator();
    if ((tgtOp == no_select) && findAssignment(tgt))
    {
        StringBuffer s;
        reportError(ERR_VALUEDEFINED, errpos, "A value for \"%s\" has already been specified", getFldName(tgt,s).str());
    }

    IHqlExpression * srcRecord = src->queryRecord();
    IHqlExpression * tgtRecord = tgt->queryRecord();

    if (srcRecord && tgtRecord && recordTypesMatch(srcRecord, tgtRecord) && (tgtOp != no_self) && !haveAssignedToChildren(tgt))
    {
        doAddAssignment(curTransform, tgt, src, errpos);
        return;
    }

    IHqlExpression *assignall = createOpenValue(no_assignall, NULL);
    //Should always add, but I don't want to cause persists to rebuild...
    unsigned firstAssign = 0;
    if (tgtOp != no_self)
    {
        assignall->addOperand(createAttribute(_original_Atom, LINK(tgt), LINK(src)));       //only used for regeneration
        firstAssign++;
    }

    doAddAssignCompoundOwn(assignall, tgt, src, NULL, errpos);

    assignall = assignall->closeExpr();
    if (assignall->numChildren() > firstAssign) 
        curTransform->addOperand(assignall);
    else // empty assignall.
    {
        //reportWarning(WRN_TRANX_EMPTYASSIGNALL, errpos.pos, "Assignment has no effect; ignored");
        assignall->Release();
    }
}


IHqlExpression * HqlGram::createDefaultAssignTransform(IHqlExpression * record, IHqlExpression * rowValue, const attribute & errpos)
{
    Owned<ITypeInfo> type = createRecordType(record);
    beginTransform(type);
    pushSelfScope(type);
    addAssignall(getSelfScope(), LINK(rowValue), errpos);
    return closeTransform(errpos);
}


IHqlExpression * HqlGram::createDefaultProjectDataset(IHqlExpression * record, IHqlExpression * src, const attribute & errpos)
{
    OwnedHqlExpr seq = createActiveSelectorSequence(src, NULL);
    OwnedHqlExpr left = createSelector(no_left, src, seq);
    OwnedHqlExpr transform = createDefaultAssignTransform(record, left, errpos);
    return createDatasetF(no_hqlproject, ::ensureDataset(src), LINK(transform), LINK(seq), NULL);
}


void HqlGram::doAddAssignCompound(IHqlExpression * assignall, IHqlExpression * target, IHqlExpression * src, IHqlExpression * record, const attribute& errpos)
{
    if (!record) record = target->queryRecord();
    if (!record)
    {
        StringBuffer name;
        reportError(ERR_TYPEMISMATCH_RECORD, errpos, "Cannot assign a row to field %s", getFldName(target, name).str());
        return;
    }

    IHqlExpression * srcRecord = src->queryRecord();
    if (!srcRecord)
    {
        if (src->getOperator() == no_null)
            srcRecord = record;
        else
        {
            StringBuffer name;
            reportError(ERR_TYPEMISMATCH_RECORD, errpos, "Cannot assign a field to row %s", getFldName(target, name).str());
            return;
        }
    }

    IHqlSimpleScope *srcScope = srcRecord->querySimpleScope();
    unsigned numChildren = record->numChildren();
    for (unsigned idx = 0; idx < numChildren; idx++)
    {
        IHqlExpression *subfield = record->queryChild(idx);

        switch (subfield->getOperator())
        {
        case no_ifblock:
            doAddAssignCompound(assignall, target, src, subfield->queryChild(1), errpos);
            break;
        case no_record:
            doAddAssignCompound(assignall, target, src, subfield, errpos);
            break;
        case no_field:
            {
                IHqlExpression *match = srcScope->lookupSymbol(subfield->queryName());
                if (!match) 
                    continue;

                OwnedHqlExpr lhs = createSelectExpr(LINK(target),LINK(subfield));
                OwnedHqlExpr rhs;
                if (src->getOperator() == no_null)
                {
                    rhs.set(src);
                    match->Release();
                }
                else
                    rhs.setown(createSelectExpr(LINK(src),match));
            
                if (!findAssignment(lhs))
                {
                    IHqlExpression * srcRecord = rhs->queryRecord();
                    IHqlExpression * tgtRecord = lhs->queryRecord();

                    type_t tc = subfield->queryType()->getTypeCode();
                    assertex(tc != type_record);
                    if (tc == type_row)
                    {
                        IHqlExpression * tgtRecord = lhs->queryRecord();
                        if (tgtRecord && !haveAssignedToChildren(lhs))
                        {
                            IHqlExpression * srcRecord = rhs->queryRecord();
                            if (false && (rhs->getOperator() == no_null) && !srcRecord)
                            {
                                OwnedHqlExpr nullRow = createRow(no_createrow, createClearTransform(tgtRecord, errpos));
//                              OwnedHqlExpr nullRow = createRow(no_null, LINK(tgtRecord));
                                doAddAssignment(assignall,LINK(lhs),LINK(nullRow),errpos);
                            }
                            else if (srcRecord && recordTypesMatch(srcRecord, tgtRecord))
                                doAddAssignment(assignall,LINK(lhs),LINK(rhs),errpos);
                            else
                                doAddAssignCompound(assignall,lhs,rhs,NULL,errpos);
                        }
                        else
                            doAddAssignCompound(assignall,lhs,rhs,NULL,errpos);
                    }
                    else if (tc == type_table || tc == type_groupedtable)
                    {
                        if (srcRecord && tgtRecord && !recordTypesMatch(srcRecord, tgtRecord))
                        {
                            OwnedHqlExpr project = createDefaultProjectDataset(tgtRecord, rhs, errpos);
                            if (project)
                                rhs.set(project);
                        }
                        
                        doAddAssignment(assignall,LINK(lhs),LINK(rhs),errpos);
                    }
                    else
                        doAddAssignment(assignall,LINK(lhs),LINK(rhs),errpos);
                }
                else
                {
    #ifdef _WARN_ON_ASSIGNALL
                    StringBuffer fldName;
                    reportWarning(WRN_TRANX_HASASSIGNEDVALUE, errpos.pos, "A value for \"%s\" has already been specified", getFldName(lhs,fldName).str());
    #endif
                }   
            }
        }
    }
}

void HqlGram::doAddAssignCompoundOwn(IHqlExpression * assignall, IHqlExpression * target, IHqlExpression * src, IHqlExpression * record, const attribute& errpos)
{
    doAddAssignCompound(assignall, target, src, record, errpos);
    ::Release(target);
    ::Release(src);
}


void HqlGram::doAddAssignSelf(IHqlExpression * assignall, IHqlExpression * field, IHqlExpression * src, const attribute& errpos)
{
    doAddAssignCompoundOwn(assignall, getSelf(curTransform), src, field, errpos);
}

IHqlExpression * HqlGram::createRowAssignTransform(const attribute & srcAttr, const attribute & tgtAttr, const attribute & seqAttr)
{
    IHqlExpression * src = srcAttr.queryExpr();
    IHqlExpression * res_rec = tgtAttr.queryExpr();
    
    // create transform
    beginTransform(res_rec->queryRecordType());
    
    // self := left;
    IHqlExpression *assignall = createOpenValue(no_assignall, NULL);
    doAddAssignSelf(assignall, res_rec->queryRecord(), createSelector(no_left, src, seqAttr.queryExpr()), tgtAttr);
    curTransform->addOperand(assignall->closeExpr());

    // close transform
    checkAllAssigned(res_rec, srcAttr);
    return endTransform(srcAttr);
}


IHqlExpression * HqlGram::createClearTransform(IHqlExpression * record, const attribute & errpos)
{
    OwnedHqlExpr null = createValue(no_null);
    return createDefaultAssignTransform(record, null, errpos);
}


void HqlGram::setSelfUsedOnRhs()
{
    selfUsedOnRhs = true;
}


ITypeInfo *HqlGram::queryCurrentRecordType()
{
    return ::queryRecordType(current_type);
}

ITypeInfo *HqlGram::queryCurrentTransformType()
{
    if (curTransform)
        return curTransform->queryRecordType();
    return NULL;
}

IHqlExpression *HqlGram::queryCurrentTransformRecord()
{
    ITypeInfo *t = queryCurrentTransformType();
    if (t)
        return queryExpression(t);
    return NULL;
}

/* Linkage: not affected */
void HqlGram::checkAssignedNormalizeTransform(IHqlExpression * record, const attribute &errpos)
{
    OwnedHqlExpr self = getSelf(record);
    bool modified = false;
    if (recordContainsNestedRecord(record))
    {
        HqlExprArray assigns;
        doCheckAssignedNormalizeTransform(&assigns, self, self, record, errpos, modified);

        if (modified)
        {
            IHqlExpression * newTransform = createOpenValue(no_transform, curTransform->getType());
            ForEachChild(i, curTransform)
            {
                IHqlExpression * cur = curTransform->queryChild(i);
                switch (cur->getOperator())
                {
                case no_assign:
                case no_assignall:
                    break;
                default:
                    assigns.append(*LINK(cur));
                    break;
                }
            }

            ForEachItemIn(i2, assigns)
                newTransform->addOperand(LINK(&assigns.item(i2)));

            curTransform->closeExpr()->Release();
            curTransform = newTransform;
        }
    }
    else
        doCheckAssignedNormalizeTransform(NULL, self, self, record, errpos, modified);
}

static bool isNullDataset(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_null:
        return true;
    case no_temptable:
        {
            IHqlExpression * child = expr->queryChild(0);
            return isNullList(child);
        }
    case no_inlinetable:
        return (expr->queryChild(0)->numChildren() == 0);
    }
    return false;
}

void HqlGram::doCheckAssignedNormalizeTransform(HqlExprArray * assigns, IHqlExpression* select, IHqlExpression* targetSelect, IHqlExpression * cur, const attribute& errpos, bool & modified)
{
    switch (cur->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, cur)
                doCheckAssignedNormalizeTransform(assigns, select, targetSelect, cur->queryChild(i), errpos, modified);
            break;
        }
    case no_ifblock:
        doCheckAssignedNormalizeTransform(assigns, select, targetSelect, cur->queryChild(1), errpos, modified);
        break;
    case no_field:
        {
            OwnedHqlExpr selected = createSelectExpr(LINK(select), LINK(cur));
            OwnedHqlExpr targetSelected = createSelectExpr(LINK(targetSelect), LINK(cur));
            IHqlExpression * match = findAssignment(selected);
            if (match)
            {
                if (assigns)
                    assigns->append(*createAssign(LINK(targetSelected), LINK(match)));
            }
            else
            {
                type_t tc = cur->queryType()->getTypeCode();
                assertex(tc != type_record);
                if (tc == type_row)
                {
                    IHqlExpression * record = cur->queryRecord();
                    OwnedHqlExpr self = getSelf(record);
                    if (assigns)
                    {
                        //create a new nested project.
                        HqlExprArray subAssigns;
                        doCheckAssignedNormalizeTransform(&subAssigns, selected, self, record, errpos, modified);

                        OwnedHqlExpr newTransform = createValue(no_transform, makeTransformType(record->getType()), subAssigns);
                        OwnedHqlExpr newValue = createRow(no_createrow, newTransform.getClear());
                        assigns->append(*createAssign(LINK(targetSelected), LINK(newValue)));
                        modified = true;
                    }
                    else
                        doCheckAssignedNormalizeTransform(NULL, selected, self, record, errpos, modified);
                }
                else
                {
                    IHqlExpression * child0 = queryRealChild(cur, 0);
                    if (child0 && (child0->isConstant() || isNullDataset(child0)))
                    {
                        OwnedHqlExpr castChild = ensureExprType(child0, targetSelected->queryType());
                        if (assigns)
                            assigns->append(*createAssign(LINK(targetSelected), LINK(castChild)));
                        else 
                            appendTransformAssign(curTransform, targetSelected, castChild, errpos);
                        modified = true;
                    }
                    else if (!insideTemplateFunction())
                    {
                        StringBuffer fldName;
                        getFldName(selected,fldName);

                        //Not very nice - only ok in some situations....
                        if (cur->hasProperty(virtualAtom))
                        {
                            reportWarning(ERR_TRANS_NOVALUE4FIELD, errpos.pos, "Transform does not supply a value for field \"%s\"", fldName.str());
                            OwnedHqlExpr null = createNullExpr(cur->queryType());
                            if (assigns)
                                assigns->append(*createAssign(LINK(targetSelected), LINK(null)));
                            else
                                appendTransformAssign(curTransform, targetSelected, null, errpos);
                        }
                        else
                            reportError(ERR_TRANS_NOVALUE4FIELD, errpos, "Transform does not supply a value for field \"%s\"", fldName.str());
                    }
                }
            }
            break;
        }
    case no_attr:
    case no_attr_link:
    case no_attr_expr:
        break;
    }
}


void HqlGram::checkAllAssigned(IHqlExpression * record, const attribute &errpos)
{
    checkAssignedNormalizeTransform(record, errpos);
}

void HqlGram::checkFoldConstant(attribute & attr)
{
    Owned<IHqlExpression> expr = attr.getExpr();
    attr.setExpr(foldHqlExpression(expr), attr);

    checkConstant(attr);
}

IHqlExpression * HqlGram::checkConstant(const attribute & errpos, IHqlExpression * expr)
{
    if (expr->isConstant())
        return LINK(expr);

    return createValue(no_assertconstant, expr->getType(), LINK(expr), createLocationAttr(errpos));
}

void HqlGram::checkConstant(attribute & attr)
{
    OwnedHqlExpr value = attr.getExpr();
    attr.setExpr(checkConstant(attr, value));
}


IHqlExpression * HqlGram::checkConcreteModule(const attribute & errpos, IHqlExpression * expr)
{
    return checkCreateConcreteModule(this, expr, errpos.pos);
}

void HqlGram::checkConstantEvent(attribute & attr)
{
    IHqlExpression * event = attr.queryExpr();
    if (!event->queryChild(0)->isConstant())
        reportError(ERR_EXPECTED_CONST, attr, "Expected a constant event name");
    IHqlExpression * filter = event->queryChild(1);
    if (filter && !filter->isConstant())
        reportError(ERR_EXPECTED_CONST, attr, "Expected a constant event filter");
}


void HqlGram::checkUseLocation(const attribute & errpos)
{
    if (!current_type || current_type->getTypeCode() != type_rule)
        reportError(ERR_USEONLYINRULE, errpos, "USE can only be used inside a rule");
}

void HqlGram::openTransform(ITypeInfo * type)
{
    beginTransform(type);
    pushSelfScope(type);
    enterCompoundObject();
}

IHqlExpression *HqlGram::closeTransform(const attribute &errpos)
{
    // make sure all fields are covered
    IHqlExpression *record = queryOriginalRecord(curTransform->queryType());

    checkAllAssigned(record, errpos);

    popSelfScope();

    IHqlExpression *ret = endTransform(errpos);

    return ret;
}

IHqlExpression * HqlGram::transformRecord(IHqlExpression *record, _ATOM targetCharset, IHqlExpression * scope, bool & changed, const attribute & errpos)
{
    Owned<ICharsetInfo> charset = getCharset(targetCharset);
    IHqlExpression *newrec = createRecord();
    unsigned kids = record->numChildren();
    if (!scope) scope = newrec;
    for (unsigned i = 0; i < kids; i++)
    {
        IHqlExpression *src = record->queryChild(i);
        switch (src->getOperator())
        {
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            newrec->addOperand(LINK(src));
            break;
        case no_ifblock:
            {
                IHqlExpression * newRecord = transformRecord(src->queryChild(1), targetCharset, scope, changed, errpos);
                IHqlExpression * newCond = translateFieldsToNewScope(src->queryChild(0), scope->querySimpleScope(), errpos);
                newrec->addOperand(createValue(no_ifblock, makeNullType(), newCond, newRecord));
                break;
            }
        case no_record:
            newrec->addOperand(transformRecord(src, targetCharset, scope, changed, errpos));
            break;
        case no_field:
            {
                Linked<ITypeInfo> srcType = src->queryType();
                type_t tc = srcType->getTypeCode();
                assertex(tc != type_record);
                if (tc == type_row)
                {
                    OwnedHqlExpr newRecord = transformRecord(src->queryRecord(), targetCharset, NULL, changed, errpos);
                    srcType.setown(replaceChildType(srcType, newRecord->queryType()));
                }
                else
                {
                    if ((tc ==type_string) && (srcType->queryCharset() != charset))
                    {
                        srcType.setown(makeStringType(srcType->getStringLen(), LINK(charset), NULL));
                        changed = true;
                    }
                    //MORE, should copy some attributes (cardinality) but not others (virtual)
                }
                IHqlExpression *newField = createField(src->queryName(), srcType.getClear(), NULL);
                newrec->addOperand(newField);
                break;
            }
        default:
            UNIMPLEMENTED;
        }
    }

    return newrec->closeExpr();
}

IHqlExpression *HqlGram::transformRecord(IHqlExpression *dataset, _ATOM targetCharset, const attribute & errpos)
{
    bool changed = false;
    IHqlExpression * newRec = transformRecord(dataset->queryRecord(), targetCharset, NULL, changed, errpos);
    if (changed)
        return newRec;
    newRec->Release();
    return NULL;
}

ITypeInfo * HqlGram::mapAlienType(IHqlSimpleScope * scope, ITypeInfo * type, const attribute & errpos)
{
    IHqlExpression * alienExpr = queryExpression(type);
    OwnedHqlExpr mappedAlien = translateFieldsToNewScope(alienExpr, scope, errpos);
    return makeModifier(mappedAlien->getType(), typemod_indirect, LINK(mappedAlien));
}

/* In parm e: not linked */
void HqlGram::addFields(const attribute &errpos, IHqlExpression *e, IHqlExpression * dataset, bool clone)
{
    if (e->getOperator() != no_record)
    {
        //dataset.childrecord or similar!
        clone = true;
        e = e->queryRecord();
    }

    //If inside an ifblock this may not match activeRecord.tos()..
    if (selfScopes.ordinality() == 0)
        return;
    IHqlSimpleScope * topScope = selfScopes.tos().querySimpleScope();

    assertex(e->getOperator()==no_record);
    ForEachChild(nkid, e)
    {
        IHqlExpression *field = e->queryChild(nkid);
        _ATOM name = field->queryName();    
        OwnedHqlExpr match = topScope->lookupSymbol(name);
        if (match)
        {
            if (!clone)
                reportWarning(ERR_REC_DUPFIELD, errpos.pos, "A field called %s is already defined in this record",name->str());
            continue;
        }

        if (!clone)
            addToActiveRecord(LINK(field));
        else
        {
            //MORE: Fields that are referenced in user defined types need to be resolved to the new cloned field names.  No idea how to fix it...
            switch (field->getOperator())
            {
            case no_attr:
            case no_attr_expr:
            case no_attr_link:
                break;
            case no_field:
                {
                    IHqlExpression * attrs = extractFieldAttrs(field);
                    Owned<ITypeInfo> type = field->getType();
                    _ATOM name = field->queryName();
                    if (type->getTypeCode() == type_alien)
                        type.setown(mapAlienType(activeRecords.tos().querySimpleScope(), type, errpos));
                    if (dataset)
                    {
                        OwnedHqlExpr value = createSelectExpr(LINK(dataset), LINK(field));
                        OwnedHqlExpr match = activeRecords.tos().querySimpleScope()->lookupSymbol(name);
                        //Ignore identical fields that are already present, so ds can be used to mean all fields not already added.
                        bool ignore = false;
                        if (match)
                        {
                            IHqlExpression * matchValue = match->queryChild(0);
                            if (matchValue && matchValue->queryNormalizedSelector() == value->queryNormalizedSelector())
                                ignore = true;
                        }
                        if (!ignore)
                            addField(errpos, name, type.getClear(), createSelectExpr(LINK(dataset), LINK(field)), attrs);
                    }
                    else
                        //We either cope with fields being able to be shared between records in folder etc.,
                        //or we have to create a new field at this point...
                        //addToActiveRecord(LINK(field));
                        addField(errpos, name, type.getClear(), LINK(queryRealChild(field, 0)), attrs); 
                    break;
                }
            case no_ifblock:
                {
                    beginIfBlock();
                    addFields(errpos, field->queryChild(1), dataset, clone);
                    IHqlExpression * record = endIfBlock();

                    //The condition in the if block needs translating to the new fields....
                    IHqlExpression * cond = translateFieldsToNewScope(field->queryChild(0), activeRecords.tos().querySimpleScope(), errpos);
                    IHqlExpression * expr = createValue(no_ifblock, makeNullType(), cond, record);
                    activeRecords.tos().addOperand(expr);
                    break;
                }
            case no_record:
                addFields(errpos, field, dataset, clone);
                break;
            }
        }
    }
}                                       

void HqlGram::addToActiveRecord(IHqlExpression * newField)
{
    IHqlExpression & topRecord = activeRecords.tos();
    topRecord.addOperand(newField);

    //This is horrible, but if a field is within an ifblock we need to immediately add it to the parent record's scope table,
    //rather than waiting for the ifblock to be added, otherwise the field can't be found in scope.
    //it means fields from ifblocks are inserted twice into the symbol table - and we still need to expose this internal function
    OwnedHqlExpr self = getSelfScope();
    assertex(self);
    CHqlRecord *currentRecord = QUERYINTERFACE(self.get(), CHqlRecord);
    if (currentRecord != &topRecord)
        currentRecord->insertSymbols(newField);
}

_ATOM HqlGram::createUnnamedFieldName(const char * prefix)
{
    StringBuffer s;
    s.append(prefix).append(activeRecords.tos().numChildren()+1);
    return createIdentifierAtom(s.str());
}

_ATOM HqlGram::createUnnamedFieldName()
{
    return createUnnamedFieldName("_unnamed_");
}



/* In parms: type, value: linked */
void HqlGram::addField(const attribute &errpos, _ATOM name, ITypeInfo *_type, IHqlExpression *value, IHqlExpression *attrs)
{
    Owned<ITypeInfo> fieldType = _type;
    Linked<ITypeInfo> expectedType = fieldType;
    if (expectedType->getTypeCode() == type_alien)
    {
        IHqlAlienTypeInfo * alien = queryAlienType(fieldType);
        expectedType.setown(alien->getLogicalType());
    }

    if (value && !isSameBasicType(value->queryType(), expectedType))
    {
        ITypeInfo * valueType = value->queryType();
        // MORE - is this implicit or explicit?
        if (!expectedType->assignableFrom(valueType->queryPromotedType()))
            canNotAssignTypeWarn(fieldType,valueType,errpos);
        if (expectedType->getTypeCode() != type_row)
        {
            IHqlExpression * newValue = ensureExprType(value, expectedType);
            value->Release();
            value = newValue;
        }
    }

    switch (fieldType->getTypeCode())
    {
    case type_any:
        if (!queryTemplateContext())
        {
            reportUnsupportedFieldType(fieldType, errpos);
            fieldType.set(defaultIntegralType);
        }
        break;
    case type_decimal:
        if (fieldType->getSize() == UNKNOWN_LENGTH)
        {
            reportWarning(ERR_BAD_FIELD_TYPE, errpos.pos, "Fields of unknown length decimal not currently supported");
            fieldType.setown(makeDecimalType(MAX_DECIMAL_DIGITS, MAX_DECIMAL_PRECISION, fieldType->isSigned()));
        }
        break;
    }

    if (queryPropertyInList(virtualAtom, attrs) && !fieldType->isScalar())
        reportError(ERR_BAD_FIELD_ATTR, errpos, "Virtual can only be specified on a scalar field");

    if (!name)
        name = createUnnamedFieldName();

    checkFieldnameValid(errpos, name);
    if(isUnicodeType(fieldType) && (*fieldType->queryLocale()->str() == 0))
    {
        StringBuffer locale;
        _ATOM localeAtom = createLowerCaseAtom(queryDefaultLocale()->queryValue()->getStringValue(locale));
        switch (fieldType->getTypeCode())
        {
        case type_varunicode:
            fieldType.setown(makeVarUnicodeType(fieldType->getStringLen(), localeAtom));
            break;
        case type_unicode:
            fieldType.setown(makeUnicodeType(fieldType->getStringLen(), localeAtom));
            break;
        case type_utf8:
            fieldType.setown(makeUtf8Type(fieldType->getStringLen(), localeAtom));
            break;
        default:
            throwUnexpectedType(fieldType);
        }
    }

    if ((fieldType->getSize() != UNKNOWN_LENGTH) && (fieldType->getSize() > MAX_SENSIBLE_FIELD_LENGTH))
        reportError(ERR_BAD_FIELD_SIZE, errpos, "Field %s is too large", name->str());

    IHqlExpression *newField = createField(name, fieldType.getClear(), value, attrs);
    addToActiveRecord(newField);
}

void HqlGram::addDatasetField(const attribute &errpos, _ATOM name, IHqlExpression * record, IHqlExpression *value, IHqlExpression * attrs)
{
    if (!name)
        name = createUnnamedFieldName();
    checkFieldnameValid(errpos, name);
    if (queryPropertyInList(virtualAtom, attrs))
        reportError(ERR_BAD_FIELD_ATTR, errpos, "Virtual can only be specified on a scalar field");
    if (!attrs)
        attrs = extractAttrsFromExpr(value);

    ITypeInfo * type = makeTableType(makeRowType(createRecordType(record)), NULL, NULL, NULL);
    IHqlExpression *newField = createField(name, type, value, attrs);
    addToActiveRecord(newField);
    record->Release();
}

void HqlGram::addIfBlockToActive(const attribute &errpos, IHqlExpression * ifblock)
{
    activeRecords.tos().addOperand(LINK(ifblock));
}

void HqlGram::beginIfBlock()
{
    IHqlExpression * record = createRecord();
    activeIfBlocks.append(*record);             // stop a leak if crashed whilst processing
    pushRecord(record);
}

IHqlExpression * HqlGram::endIfBlock()
{
    activeIfBlocks.pop(true);
    return popRecord()->closeExpr();
}

void HqlGram::checkFieldnameValid(const attribute &errpos, _ATOM name)
{
    OwnedHqlExpr self = getSelfScope();
    IHqlSimpleScope *recordScope = self->querySimpleScope();
    OwnedHqlExpr t(recordScope->lookupSymbol(name));
    if (t.get())
        reportError(ERR_REC_DUPFIELD, errpos, "A field called %s is already defined in this record",name->str());
}

/* Linkage: not affected */
void HqlGram::checkCosort(IHqlExpression * sortlist, IHqlExpression * joined, const attribute & ea)
{
    IHqlExpression * partition = joined->queryChild(0);
    IHqlExpression * othersortlist = partition->queryChild(1);
    unsigned numCpts = sortlist->numChildren();
    //should this be > instead of !=
    if (sortlist->numChildren() != othersortlist->numChildren())
    {
        reportError(ERR_JOINED_DIFFNOFIELDS, ea, "JOINED data set has different number of sort fields: %d vs %d", sortlist->numChildren(), othersortlist->numChildren());
    }
    else
    {
        unsigned idx;
        for (idx = 0; idx < numCpts; ++idx)
        {
            IHqlExpression * cur = sortlist->queryChild(idx);
            IHqlExpression * other = othersortlist->queryChild(idx);
            if (cur->queryType() != other->queryType())
            {
                StringBuffer x, y;
                reportError(ERR_JOINED_DIFFTYPE, ea, "Component of JOINED has different type in sort field %d: %s vs %s",
                    idx+1, getFriendlyTypeStr(cur, x).str(), getFriendlyTypeStr(other, y).str());
            }
        }
    }
}

void HqlGram::enterType(const attribute &errpos, bool isParametered)
{
    enterScope(true);
    enterCompoundObject();
}

void HqlGram::enterCompoundObject()
{
    savedIds.append(current_id);
    savedLastpos.append(lastpos);
}

void HqlGram::leaveCompoundObject()
{
    current_id = (_ATOM)savedIds.pop();
    lastpos = savedLastpos.pop();
}

void HqlGram::leaveType(const YYSTYPE & errpos)
{
    leaveCompoundObject();
    leaveScope(errpos);
    inType = false;
}


void HqlGram::appendToActiveScope(IHqlExpression * arg)
{
    IHqlScope * scope = defineScopes.tos().localScope;
    if (scope)
        queryExpression(scope)->addOperand(LINK(arg));
}


void HqlGram::enterScope(IHqlScope * scope, bool allowExternal)
{
    ActiveScopeInfo & next = * new ActiveScopeInfo;
    if (allowExternal)
    {
        next.localScope.set(scope);
        next.privateScope.setown(createPrivateScope(scope));
    }
    else
        next.privateScope.set(scope);
    next.firstSideEffect = parseResults.ordinality();
    defineScopes.append(next);
}

void HqlGram::enterScope(bool allowExternal)
{
    ActiveScopeInfo & next = * new ActiveScopeInfo;
    if (allowExternal)
        next.localScope.setown(createScope());
    next.privateScope.setown(createPrivateScope());
    next.firstSideEffect = parseResults.ordinality();
    defineScopes.append(next);
}

void HqlGram::enterVirtualScope()
{
    //This isn't perfect, but it is the best I can do.
    StringBuffer fullName;
    fullName.append(globalScope->queryFullName());
    ForEachItemIn(i, savedIds)
    {
        _ATOM name = (_ATOM)savedIds.item(i);
        if (name)
        {
            if (fullName.length())
                fullName.append(".");
            fullName.append(name);  // wrong...case insensitive
        }
    }
    if (current_id)
    {
        if (fullName.length())
            fullName.append(".");
        fullName.append(current_id);    // wrong...case insensitive
    }

    ActiveScopeInfo & next = * new ActiveScopeInfo;
    next.localScope.setown(createVirtualScope(current_id, fullName));
    next.privateScope.setown(createPrivateScope());
    next.firstSideEffect = parseResults.ordinality();
    defineScopes.append(next);
}

bool HqlGram::insideNestedScope() const
{
    return defineScopes.ordinality() > minimumScopeIndex;
}

bool HqlGram::sideEffectsPending() const
{
    unsigned first = defineScopes.tos().firstSideEffect;
    return (parseResults.ordinality() != first);
}

void HqlGram::clearSideEffects()
{
    unsigned first = defineScopes.tos().firstSideEffect;
    parseResults.popn(parseResults.ordinality() - first);
}

void HqlGram::leaveScope(const YYSTYPE & errpos)
{
    if (sideEffectsPending())
    {
        clearSideEffects();
        reportError(ERR_RESULT_IGNORED, errpos, "Action side effect is not associated with a definition");
    }
    if (defineScopes.ordinality() > minimumScopeIndex)
        defineScopes.pop();
}


IHqlScope * HqlGram::closeLeaveScope(const YYSTYPE & errpos)
{
    IHqlScope * scope = defineScopes.tos().localScope;
    if (!scope)
        scope = defineScopes.tos().privateScope;
    LINK(scope);
    leaveScope(errpos);
    return closeScope(scope);
}

IHqlExpression * HqlGram::leaveLamdaExpression(attribute & exprattr)
{
    OwnedHqlExpr resultExpr = exprattr.getExpr();
    OwnedHqlExpr expr = associateSideEffects(resultExpr, exprattr.pos);

    if (queryParametered())
    {
        ActiveScopeInfo & activeScope = defineScopes.tos();
        OwnedHqlExpr formals = activeScope.createFormals(false);
        OwnedHqlExpr defaults = activeScope.createDefaults();
        expr.setown(createFunctionDefinition(atAtom, expr.getClear(), formals.getClear(), defaults.getClear(), NULL));
    }

    leaveScope(exprattr);
    leaveCompoundObject();

    return expr.getClear();
}

//---------------------------------------------------------------------------------------------------------------------

#ifdef _DEBUG
#define PSEUDO_UNIMPLEMENTED
//#define PSEUDO_UNIMPLEMENTED  UNIMPLEMENTED
#else
#define PSEUDO_UNIMPLEMENTED
#endif
class PseudoPatternScope : public CHqlScope
{
public:
    PseudoPatternScope(IHqlExpression * _patternList);
    IMPLEMENT_IINTERFACE

    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags, IFileContents *fc, int bodystart, int lineno, int column) { ::Release(value); PSEUDO_UNIMPLEMENTED; }
    virtual void defineSymbol(_ATOM name, _ATOM moduleName, IHqlExpression *value, bool isExported, bool isShared, unsigned flags) { ::Release(value); PSEUDO_UNIMPLEMENTED; }
    virtual void defineSymbol(IHqlExpression * value) { PSEUDO_UNIMPLEMENTED; ::Release(value); }
    virtual IHqlExpression *lookupSymbol(_ATOM name, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual void removeSymbol(_ATOM name) { PSEUDO_UNIMPLEMENTED; }

    virtual void    getSymbols(HqlExprArray& exprs) const { PSEUDO_UNIMPLEMENTED; }
    virtual _ATOM   queryName() const { PSEUDO_UNIMPLEMENTED; return NULL; }
    virtual const char * queryFullName() const { PSEUDO_UNIMPLEMENTED; return NULL; }
    virtual ISourcePath * querySourcePath() const { PSEUDO_UNIMPLEMENTED; return NULL; }
    virtual bool hasBaseClass(IHqlExpression * searchBase) { return false; }

    virtual void ensureSymbolsDefined(HqlLookupContext & ctx) { }

    virtual bool isImplicit() const { return false; }
    virtual bool isPlugin() const { return false; }
    virtual int getPropInt(_ATOM, int dft) const { PSEUDO_UNIMPLEMENTED; return dft; }
    virtual bool getProp(_ATOM, StringBuffer &) const { PSEUDO_UNIMPLEMENTED; return false; }

    virtual IHqlScope * clone(HqlExprArray & children, HqlExprArray & symbols) { throwUnexpected(); }
    virtual IHqlScope * queryConcreteScope() { return this; }
    virtual IHqlScope * queryResolvedScope(HqlLookupContext * context) { return this; }

protected:
    IHqlExpression * patternList;   // NB: Not linked.
};

void HqlGram::enterPatternScope(IHqlExpression * pattern)
{
    Owned<IHqlScope> scope = new PseudoPatternScope(pattern);
    enterScope(scope, false);
}


void HqlGram::leavePatternScope(const YYSTYPE & errpos)
{
    leaveScope(errpos);
}

//---------------------------------------------------------------------------------------------------------------------

void HqlGram::releaseScopes()
{
    while(topScopes.length()>0)
        popTopScope();

    leftScopes.kill();
    rightScopes.kill();
    rowsScopes.kill();
    
    while (selfScopes.length()>0)
        popSelfScope();
    modScope.clear();
    outerScopeAccessDepth = 0;

    dotScope.clear();
}                          
 

void HqlGram::processForwardModuleDefinition(const attribute & errpos)
{
    //called when FORWARD is the token just returned from yyLex(), this consumes up until the last END
    ActiveScopeInfo & activeScope = defineScopes.tos();
    IHqlScope * scope = activeScope.localScope;
    if (!scope)
        return;

    IHqlExpression * scopeExpr = queryExpression(scope);
    if (scopeExpr->hasProperty(virtualAtom))
    {
        reportError(ERR_NO_FORWARD_VIRTUAL, errpos, "Cannot use FORWARD in combination with a VIRTUAL module ");
        return;
    }

    HqlGramCtx * parentCtx = new HqlGramCtx(lookupCtx);
    saveContext(*parentCtx, true);
    Owned<IHqlScope> newScope = createForwardScope(queryGlobalScope(), parentCtx, lookupCtx.queryParseContext());
    IHqlExpression * newScopeExpr = queryExpression(newScope);

    ForEachChild(i, scopeExpr)
        newScopeExpr->addOperand(LINK(scopeExpr->queryChild(i)));

    unsigned endNesting = 0;
    unsigned braNesting = 0;
    _ATOM prevId = NULL;
    _ATOM sharedSymbolName  = NULL;
    int sharedSymbolKind = 0;
    ECLlocation start;
    lexObject->getPosition(start);
    int prev = 0;
    YYSTYPE nextToken;
    loop
    {
        int next = lexObject->yyLex(nextToken, false, NULL);
        switch (next)
        {
        case ASSIGN:
            if ((sharedSymbolKind != 0) && prevId && (endNesting == 0))
            {
                sharedSymbolName = prevId;
            }
            break;
        case UNKNOWN_ID:
            {
                _ATOM id = nextToken.getName();
                if ((braNesting == 0) && (endNesting == 0))             // last identifier seen, but don't include parameters, or record members
                    prevId = id;
                if (id == ruleAtom)
                    next = RULE;
                else if (id == wholeAtom)
                    next = WHOLE;
                else if (id == typeAtom)
                {
                    // ":= TYPE" Beginning of a user type definition
                    if (prev == ASSIGN)
                    {
                        prevId = NULL;
                        endNesting++;
                    }
                }
                break;
            }
        case SHARED:
        case EXPORT:
            if (endNesting == 0)
            {
                braNesting = 0;     // in case something else has confused us.
                sharedSymbolKind = next;
            }
            break;
        case '(':
            //Functional version of IF, and inline version of transform doesn't have END
            if ((prev == IF) || (prev == TRANSFORM))
                endNesting--;
            braNesting++;
            break;
        case ')':
            braNesting--;
            break;
        case ';':
            if (sharedSymbolName && braNesting == 0 && endNesting == 0)
            {
                ECLlocation end;
                lexObject->getPosition(end);
                checkNotAlreadyDefined(sharedSymbolName, newScope, errpos);

                unsigned symbolFlags = 0;
                _ATOM moduleName = NULL;
                Owned<IFileContents> contents = createFileContentsSubset(lexObject->queryFileContents(), start.position, end.position - start.position);
                addForwardDefinition(newScope, sharedSymbolName, moduleName, contents,
                                     symbolFlags, (sharedSymbolKind == EXPORT), start.lineno, start.column);

                //Looks like the end of the shared symbol => define it
                start.set(end);
                sharedSymbolKind = 0;
                prevId = NULL;
                sharedSymbolName = NULL;
            }
            break;
        case RECORD:
            //Don't increment nesting when [WHOLE] RECORD used to indicate which fields to dedup by
            if ((prev != ',') && (prev != WHOLE) && (prev != VIRTUAL))
                endNesting++;
            break;
        case IF:
            //increment nesting, but probably decrement it straight away when we see the '('
            endNesting++;
            break;
        //MORE: These are now hard reserved....
        case FUNCTION:
        case IFBLOCK:
        case INTERFACE:
        case MODULE:
        case SERVICE:
        case TRANSFORM:
            endNesting++;
            break;
        case END:
            if (endNesting == 0)
            {
                lexObject->pushText("END");
                closeScope(activeScope.localScope.getClear())->Release();
                activeScope.localScope.setown(newScope.getClear());
                return;
            }
            endNesting--;
            break;
        case EOF:
        case 0:
            reportError(ERR_EXPECTED, errpos, "Missing END in FORWARD module definition");
            abortParsing();
            return;
        case COMPLEX_MACRO: 
        case MACRO:
        case SIMPLE_TYPE:
        case CPPBODY:
        case STRING_CONST:
        case REAL_CONST:
        case UNICODE_CONST:
        case DATA_CONST:
            break;
        }
        nextToken.release();
        prev = next;
    }
}
             
IHqlExpression *HqlGram::queryTopScope()
{
    IHqlExpression *top = NULL;
    if (topScopes.length())
    {
        top = (IHqlExpression *) &topScopes.item(topScopes.length()-1);
    }
    return top;
}

IHqlExpression *HqlGram::getTopScope()
{
    if (!topScopes.length())
        return NULL;
    IHqlExpression * ret = (IHqlExpression *) &topScopes.item(topScopes.length()-1);
    ret->Link();
    return ret;
}

IHqlExpression *HqlGram::getLeftScope()
{
    if (!leftScopes.length())
        return NULL;
    return &OLINK(leftScopes.tos());
}

IHqlExpression *HqlGram::getRightScope()
{
    if (!rightScopes.length())
        return NULL;
    return &OLINK(rightScopes.tos());
}

IHqlExpression *HqlGram::getSelfScope()
{
    if (!selfScopes.length())
        return NULL;
    IHqlExpression * ret = &selfScopes.tos();
    ret->Link();
    return ret;
}

IHqlExpression *HqlGram::queryLeftScope()
{
    if (!leftScopes.length())
        return NULL;
    return &leftScopes.tos();
}

IHqlExpression *HqlGram::queryRightScope()
{
    if (!rightScopes.length())
        return NULL;
    return &rightScopes.tos();
}

IHqlExpression *HqlGram::queryRowsScope()
{
    if (!rowsScopes.length())
        return NULL;
    return &rowsScopes.tos();
}

IHqlExpression *HqlGram::resolveRows(const attribute & errpos, IHqlExpression * ds)
{
    unsigned match = rowsScopes.find(*ds);
    if (match == NotFound)
    {
        if (rowsScopes.ordinality() == 0)
            reportError(ERR_LEFT_ILL_HERE, errpos, "ROWS not legal here");
        else
            reportError(ERR_LEFT_ILL_HERE, errpos, "ROWS not legal on this dataset");

        return createDataset(no_null, LINK(ds->queryRecord()));
    }

    IHqlExpression * id = &OLINK(rowsIds.item(match));
    return createDataset(no_rows, LINK(ds), id);
}


IHqlExpression * HqlGram::getSelfDotExpr(const attribute & errpos)
{
    OwnedHqlExpr self = getSelfScope();
    setDotScope(self);
    if (!self)
        reportError(ERR_SELF_ILL_HERE, errpos, "SELF not legal here");
    if (curTransform && dotScope && recordTypesMatch(dotScope, curTransform))
    {
        setSelfUsedOnRhs();
        return getSelf(curTransform);
    }
    return LINK(querySelfReference());
}

bool HqlGram::checkValidBaseModule(const attribute & attr, SharedHqlExpr & expr)
{
    node_operator op = expr->getOperator();
    if ((op == no_virtualscope) || (op == no_libraryscopeinstance) || (op == no_param))
        return true;

    if (op == no_forwardscope)
    {
        IHqlScope * scope = expr->queryScope();
        IHqlScope * resolved = scope->queryResolvedScope(&lookupCtx);
        expr.set(queryExpression(resolved));
        return true;
    }

    if (op == no_param)
        reportError(ERR_EXPECTED_MODULE, attr, "Cannot derive a module from a parameter");
    else
        reportError(ERR_EXPECTED_MODULE, attr, "Expected the name of a module definition");
    return false;
}



bool extractSymbolParameters(HqlExprArray & parameters, IHqlExpression * symbol)
{
    if (symbol->isFunction())
    {
        unwindChildren(parameters, queryFunctionParameters(symbol));
        return true;
    }
    return false;
}



//Given a previous symbol definition and a new value, create a attribute with the same structure
IHqlExpression * HqlGram::createSymbolFromValue(IHqlExpression * primaryExpr, IHqlExpression * value)
{
    return primaryExpr->cloneAllAnnotations(value);
}

IHqlExpression * HqlGram::implementInterfaceFromModule(const attribute & modpos, const attribute & ipos, IHqlExpression * implementModule, IHqlExpression * _projectInterface, IHqlExpression * flags)
{
    //MORE: What about multiple base interfaces?  Shouldn't really be needed, but would probably be easy enough to implement.
    HqlExprArray selectedFields;
    bool optional = false;
    if (flags)
    {
        flags->unwindList(selectedFields, no_comma);

        IHqlExpression * optionalAttr = queryProperty(optAtom, selectedFields);
        if (optionalAttr)
        {
            optional = true;
            selectedFields.zap(*optionalAttr);
        }
    }

    IHqlExpression * libraryAttr = queryProperty(libraryAtom, selectedFields);
    if (libraryAttr)
        selectedFields.zap(*libraryAttr);

    LinkedHqlExpr projectInterface = _projectInterface;
    if (projectInterface->getOperator() == no_funcdef)
        projectInterface.set(projectInterface->queryChild(0));
    IHqlScope * implementScope = implementModule->queryScope()->queryConcreteScope();
    if (!implementScope)
    {
        if (projectInterface->queryScope()->queryConcreteScope())
            reportError(ERR_ABSTRACT_MODULE, modpos, "PROJECT(interface, module) - module is abstract.  (Parameters round the wrong way?)");
        else
            reportError(ERR_ABSTRACT_MODULE, modpos, "Cannot PROJECT an abstract module to a new interface");
        return LINK(projectInterface);
    }

    Owned<IHqlScope> newScope = createVirtualScope();
    IHqlExpression * newScopeExpr = queryExpression(newScope);

    if (!checkValidBaseModule(ipos, projectInterface))
        return projectInterface.getClear();

    newScopeExpr->addOperand(LINK(projectInterface));
    IHqlScope * base = projectInterface->queryScope();
    if (base)
    {
        HqlExprArray syms;
        if (selectedFields.ordinality())
        {
            ForEachItemIn(i, selectedFields)
            {
                _ATOM name = selectedFields.item(i).queryName();
                OwnedHqlExpr match = base->lookupSymbol(name, LSFpublic, lookupCtx);
                if (match)
                    syms.append(*match.getClear());
                else
                    reportError(ERR_EXPECTED_ATTRIBUTE, ipos, "Interface does not define %s", name->str());
            }
        }
        else
            base->getSymbols(syms);
        syms.sort(compareSymbolsByName);
        ForEachItemIn(iSym, syms)
        {
            IHqlExpression & baseSym = syms.item(iSym);
            _ATOM name = baseSym.queryName();
            OwnedHqlExpr match  = implementScope->lookupSymbol(name, LSFpublic, lookupCtx);
            if (match)
            {
                HqlExprArray parameters;
                bool isParametered = extractSymbolParameters(parameters, &baseSym);

                checkDerivedCompatible(name, newScopeExpr, match, isParametered, parameters, modpos);
                newScope->defineSymbol(LINK(match));
            }
            else if (!optional)
                reportError(ERR_EXPECTED_ATTRIBUTE, modpos, "Module does not define %s", name->str());
        }
    }

    //Keep the library property so can check prototypes match later.
    if (libraryAttr)
        newScopeExpr->addOperand(LINK(libraryAttr));

    cloneInheritedAttributes(newScope, ipos);
    return closeAndLink(newScopeExpr);
}


IHqlExpression * HqlGram::implementInterfaceFromModule(attribute & mAttr, attribute & iAttr, IHqlExpression * flags)
{
    OwnedHqlExpr projectInterface = iAttr.getExpr();
    OwnedHqlExpr implementModule = mAttr.getExpr();
    return implementInterfaceFromModule(mAttr, iAttr, implementModule, projectInterface, flags);
}

void HqlGram::setActiveAttrs(int activityToken, const TokenMap * attrs)
{
    //Nasty special case because valid attribute handling is a side effect of the lexer rather than
    //the grammar reductions.
    //This function is typically called before the '(', or before the first comma of the attributes
    //At that point the next token will have already have been lexed (and saved in lastToken)
    //If is is a close bracket then the valid attribute stack will have already been popped 
    //so make sure we don't update otherwise it will mess up the parent's scope.
    if (lexObject->queryLastToken() == ')')
        return;

    int lastToken = lexObject->queryLastToken();
    switch (lastToken)
    {
    case '(':
    case ',':
        {
            unsigned max = validAttributesStack.ordinality();
            if (max)
                validAttributesStack.replace(attrs, max-1);
            break;
        }
    default:
        if (lastToken == activityToken)
            pendingAttributes = attrs;
        else
        {
            //Here as a debugging aid - invalid source means this shouldn't remain
            throwUnexpected();
        }
    }
}

void HqlGram::enableAttributes(int activityToken)
{
    assertex(activityToken >= 0 && activityToken < YY_LAST_TOKEN);
    TokenMap * map = nestedAttributeMap[activityToken];
//  assertex(map);
    if (map)
        setActiveAttrs(activityToken, map);
}

int HqlGram::mapToken(int lexToken) const
{
    if (validAttributesStack.ordinality() != 0)
    {
        const TokenMap * activeMap = static_cast<const TokenMap *>(validAttributesStack.tos());
        if (activeMap)
        {
            for (unsigned i=0;;i++)
            {
                int curLexToken = activeMap[i].lexToken;
                if (curLexToken == 0)
                    break;
                if (curLexToken == lexToken)
                    return activeMap[i].attrToken;
            }
        }
    }
    return defaultTokenMap[lexToken];
}

void HqlGram::onOpenBra()
{
    //This is called as a side-effect from the lexer, rather than as a production in the
    //grammar since it is simpler, significantly reduces the grammar production tables, 
    //and also avoids some potential r/r errors.  See also onCloseBra()
    //However that can cause interesting interaction between productions and lexer side effects,
    //see setActiveAttrs for more details.
    validAttributesStack.append(pendingAttributes);
    pendingAttributes = NULL;
}

void HqlGram::onCloseBra()
{
    if (validAttributesStack.ordinality())
        validAttributesStack.pop();
}

IHqlExpression *HqlGram::lookupSymbol(IHqlScope * scope, _ATOM searchName)
{
    return scope->lookupSymbol(searchName, LSFpublic, lookupCtx);
}

IHqlExpression *HqlGram::lookupSymbol(_ATOM searchName, const attribute& errpos)
{
#if 0
    if (stricmp(searchName->getAtomNamePtr(), "gh2")==0)
        searchName = searchName;
#endif
    if (expectedUnknownId)
        return NULL;

    try
    {
        // If there is a temporary scope, we only look up in that (and it must exist!).
        if (dotScope) 
        {
            IHqlExpression *ret = NULL;
            if (dotScope->getOperator() == no_enum)
            {
                ret = dotScope->queryScope()->lookupSymbol(searchName, LSFrequired, lookupCtx);
            }
            else
            {
                IHqlExpression * dotRecord = dotScope->queryRecord();
                if(!dotRecord) 
                    return NULL;

                IHqlExpression* map = queryFieldMap(dotScope);
                if (map)
                {
                    searchName = fieldMapTo(map, searchName);
                    IHqlExpression* ds = dotScope->queryChild(0);
                    ret = ds->queryRecord()->querySimpleScope()->lookupSymbol(searchName);
                    if (!ret)
                        reportError(ERR_OBJ_NOSUCHFIELD, errpos, "Object '%s' does not have a field named '%s'", ds->queryName()->str(), searchName->str());
                }
                else
                {
                    ret = dotRecord->querySimpleScope()->lookupSymbol(searchName);
                    if (!ret)
                    {
                        StringBuffer s;
                        getExprECL(dotScope, s);
                        reportError(ERR_OBJ_NOSUCHFIELD, errpos, "Object '%s' does not have a field named '%s'", s.str(), searchName->str());
                    }
                }
            }
            
            // dotScope only works once
            dotScope.clear();
            return ret;
        }

        if (modScope)
        {
            return modScope->lookupSymbol(searchName, LSFrequired, lookupCtx);
        }

        // Then come implicitly defined fields...
        IHqlExpression *top = queryTopScope();
        if (top)
        {
            if (outerScopeAccessDepth == 0)
            {
                IHqlExpression* ret;
                IHqlExpression* map = queryFieldMap(top);
                if (map)
                {
                    searchName = fieldMapTo(map, searchName);
                    IHqlExpression* ds = top->queryChild(0);
                    ret = ds->queryRecord()->querySimpleScope()->lookupSymbol(searchName);
                }
                else
                    ret = top->queryRecord()->querySimpleScope()->lookupSymbol(searchName);

                if (ret)
                {
                    if (top->getOperator() != no_record)
                    {
                        //more: Should probably return createDataset(no_anon, record)
                        //      or something similar, but need to watch evaluate(dataset[1], datatset.x) and SQL generation.
                        if (insideEvaluate)
                            return addDatasetSelector(getActiveTableSelector(), ret);

                        IHqlExpression * topSelect = top->queryNormalizedSelector(true);
                        return addDatasetSelector(LINK(topSelect), ret);
                    }
                    else
                        return addDatasetSelector(getActiveTableSelector(), ret);
                }
            }
            else
                outerScopeAccessDepth--;
        }

        //Slightly strange... The parameters for the current nested object are stored in the previous ActiveScopeInfo record.
        //This means outerScopeDepth is decremented after looking at the parameters.  It also means we need to increment by
        //one before we start.
        //It does mean
        //export anotherFunction(integer SomeValue2) := SomeValue2 * ^.SomeValue2; Doesn't quite work as expected, but 
        //it serves the user right for choosing a parameter name that clashes.  Otherwise you'd generally need one more ^ than you'd expect.
        if (outerScopeAccessDepth)
            outerScopeAccessDepth++;

        //Also note, if we're inside a template function then we need to record all access to symbols that occur at an outer level
        IHqlScope * templateScope = NULL;
        ForEachItemInRev(scopeIdx, defineScopes)
        {
            ActiveScopeInfo & cur = defineScopes.item(scopeIdx);
            if (cur.templateAttrContext)
                templateScope = cur.templateAttrContext;
            if (outerScopeAccessDepth == 0)
            {
                IHqlExpression * match = cur.queryParameter(searchName);
                if (match)
                {
    //                  PrintLog("Lookup %s got parameter %s", searchName->getAtomNamePtr(), searchName->getAtomNamePtr());
                    return LINK(match);
                }
            }
            else
                outerScopeAccessDepth--;

            if (outerScopeAccessDepth == 0)
            {
                IHqlExpression *ret = cur.privateScope->lookupSymbol(searchName, LSFsharedOK, lookupCtx);
                if (ret)
                    return recordLookupInTemplateContext(searchName, ret, templateScope);

                if (cur.localScope)
                {
                    ret = cur.localScope->lookupSymbol(searchName, LSFsharedOK, lookupCtx);
                    if (ret)
                    {
                        return recordLookupInTemplateContext(searchName, ret, templateScope);
                    }
                }
            }
        }

        //Now look up imports
        IHqlExpression *ret = parseScope->lookupSymbol(searchName, LSFsharedOK, lookupCtx);
        if (ret)
            return recordLookupInTemplateContext(searchName, ret, templateScope);

        // finally comes the local scope
        if (legacyEclSemantics && searchName==globalScope->queryName())
            return LINK(recordLookupInTemplateContext(searchName, queryExpression(globalScope), templateScope));

        ForEachItemIn(idx2, defaultScopes)
        {
            IHqlScope &plugin = defaultScopes.item(idx2);
            IHqlExpression *ret = plugin.lookupSymbol(searchName, LSFpublic, lookupCtx);
            if (ret)
            {
                recordLookupInTemplateContext(searchName, ret, templateScope);
                return ret;
            }
        }
        return NULL;
    }
    catch(IECLError* error)
    {
        if(errorHandler && !errorDisabled)
            errorHandler->report(error);
        error->Release();
        // recover: to avoid reload the definition again and again
        return createSymbol(searchName, createConstant(0), ob_private);
    }
    return NULL;
}


IHqlExpression * HqlGram::recordLookupInTemplateContext(_ATOM name, IHqlExpression * expr, IHqlScope * templateScope)
{
    if (expr && templateScope)
        templateScope->defineSymbol(name,NULL,expr,true,false,0);
    return expr;
}


unsigned HqlGram::checkCompatible(ITypeInfo * t1, ITypeInfo * t2, const attribute &ea, bool complain)
{
    if (t1 && t2)
    {
        if (t1->assignableFrom(t2))
            return 1;
        if (t2->assignableFrom(t1))
            return 2;
    }

    if (complain)
    {
        StringBuffer msg("Type mismatch - expected ");
        getFriendlyTypeStr(t1,msg).append(" value, given ");
        getFriendlyTypeStr(t2,msg);
        reportError(ERR_EXPECTED, ea, "%s", msg.str());
    }

    return 0;
}

ITypeInfo *HqlGram::checkType(attribute &a1, attribute &a2)
{
    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();
    switch (checkCompatible(t1, t2, a2))
    {
    case 1:
        ::Link(t1);
        return t1;
    case 2:
        ::Link(t2);
        return t2;
    }
    ::Link(t1);
    return t1;
}

void HqlGram::checkType(attribute &a1, ITypeInfo *t2)
{
    checkCompatible(a1.queryExprType(), t2, a1);
}

void HqlGram::checkMaxCompatible(IHqlExpression * sortOrder, IHqlExpression * values, attribute & errpos)
{
    if (sortOrder->numChildren() != values->numChildren())
    {
        reportError(ERR_MAX_MISMATCH, errpos, "MAX() must specify a value for each sort order element");
        return;
    }

    ForEachChild(i, sortOrder)
    {
        if (checkCompatible(sortOrder->queryChild(i)->queryType(), values->queryChild(i)->queryType(), errpos, false) == 0)
        {
            reportError(ERR_MAX_MISMATCH, errpos, "Value for MAX() element %d is not compatible with the sort order", i);
            return;
        }
    }
}

void HqlGram::checkSvcAttrNoValue(IHqlExpression* attr, const attribute& errpos)
{
    if (attr->numChildren()>0)
        reportWarning(WRN_SVC_ATTRNEEDNOVALUE, errpos.pos,"Service attribute '%s' requires no value; ignored",attr->queryName()->str());
}

void cleanupService(IHqlScope*& serviceScope)
{
    IHqlExpression* svc = queryExpression(serviceScope);
    if (svc)
    {
        if (!svc->isExprClosed())
            svc = svc->closeExpr();
        svc->Release();
    }
    serviceScope = NULL;
}

IHqlExpression* HqlGram::checkServiceDef(IHqlScope* serviceScope,_ATOM name, IHqlExpression* attrs, const attribute& errpos)
{
    // already defined?
    OwnedHqlExpr def = serviceScope->lookupSymbol(name, LSFsharedOK|LSFignoreBase, lookupCtx);
    if (def)
        reportError(ERR_SVC_FUNCDEFINED,errpos, "Function is already defined in service: %s",name->str());

    // gather the attrs
    HqlExprArray attrArray;
    if (attrs)
        attrs->unwindList(attrArray,no_comma);
    
    bool hasEntrypoint = false;
    unsigned count = attrArray.length();
    if (count>0)
    {
        // check attr one by one
        bool bcdApi = false, rtlApi = false, cApi = false;

        for (unsigned i=0; i<count; i++)
        {
            IHqlExpression* attr = &attrArray.item(i);
            _ATOM name = attr->queryName();

            // check duplication
            unsigned j;
            for (j=0; j<i; j++)
            {
                IHqlExpression & cur = attrArray.item(j);
                if (cur.queryName()==name)
                {
                    if (&cur != attr)
                        reportError(ERR_SVC_ATTRDEFINED, errpos, "Service has duplicate attribute with different value: %s", name->str());
                    break;
                }
            }

            if (name == entrypointAtom || name == initfunctionAtom)
            {
                hasEntrypoint = true;
                bool invalid = false;
                StringBuffer buf;
                if (attr->numChildren()==0)
                    invalid = true;
                else
                {
                    attr->queryChild(0)->queryValue()->getStringValue(buf);
                    if (!isCIdentifier(buf.str()))
                        invalid = true;
                }

                if (invalid)
                {
                    if (name == entrypointAtom)
                        reportError(ERR_SVC_INVALIDENTRYPOINT, errpos, "Invalid entrypoint '%s': must be valid C identifier", buf.str());
                    else 
                        reportError(ERR_SVC_INVALIDINITFUNC, errpos, "Invalid initFunction '%s': must be valid C identifier", buf.str());
                }
            }
            else if (name == pseudoentrypointAtom)
            {
                HqlExprArray args;
                unwindChildren(args, attr);
                attrs = createComma(attrs, createAttribute(entrypointAtom, args));
                hasEntrypoint = true;
            }
            else if (name == libraryAtom)
            {
                bool invalid = false;
                if (attr->numChildren()==0)
                    invalid = true;
                else
                {
                    StringBuffer buf;
                    attr->queryChild(0)->queryValue()->getStringValue(buf);
                    
                    // can we do better?
                    if (*buf.str() == 0)
                        invalid = true;
                }

                if (invalid)
                    reportError(ERR_SVC_INVALIDLIBRARY,errpos,"Invalid library: can not be empty");
            }
            else if (name == includeAtom)
            {
                bool invalid = false;
                //no parameters on include stops definition being generated - used internally.
                if (attr->numChildren()!=0)
                {
                    StringBuffer buf;
                    attr->queryChild(0)->queryValue()->getStringValue(buf);
                    
                    // can we do better?
                    if (*buf.str() == 0)
                        invalid = true;
                }

                /* should be really an error */
                if (invalid)
                    reportWarning(ERR_SVC_INVALIDINCLUDE,errpos.pos,"Invalid include: can not be empty");
            }
            else if (name == eclrtlAtom)
            {
                rtlApi = true;
                checkSvcAttrNoValue(attr, errpos);
            }
            else if (name == cAtom)
            {
                cApi = true;
                checkSvcAttrNoValue(attr, errpos);
            }
            else if (name == bcdAtom)
            {
                bcdApi = true;
                checkSvcAttrNoValue(attr, errpos);
            }
            else if (name == pureAtom || name == templateAtom || name == volatileAtom || name == onceAtom || name == actionAtom)
            {
                checkSvcAttrNoValue(attr, errpos);
            }
            else if ((name == gctxmethodAtom) || (name == ctxmethodAtom) || (name == contextAtom) || (name == globalContextAtom) || (name == sysAtom) ||
                     (name == methodAtom) || (name == newSetAtom) || (name == omethodAtom) || (name == oldSetFormatAtom) || (name == contextSensitiveAtom))
            {
                checkSvcAttrNoValue(attr, errpos);
            }
            else if ((name == userMatchFunctionAtom) || (name == costAtom) || (name == allocatorAtom))
            {
            }
            else if (name == holeAtom)
            {
                //backward compatibility
            }
            else // unsupported
                reportWarning(WRN_SVC_UNSUPPORTED_ATTR, errpos.pos, "Unsupported service attribute: '%s'; ignored", name->str());
        }

        // check attribute conflicts
        int apiAttrs = 0;
        if (rtlApi) apiAttrs++;
        if (cApi)   apiAttrs++;
        if (bcdApi) apiAttrs++;
        if (apiAttrs>1)
            reportWarning(ERR_SVC_ATTRCONFLICTS, errpos.pos, "Attributes eclrtl, bcd, c are conflict: only 1 can be used at a time");
    }

    if (!hasEntrypoint)
    {
        // may change from warning to error in the future
        reportWarning(ERR_SVC_NOENTRYPOINT, errpos.pos, "Entrypoint is not defined; default to %s", name->str());

        IHqlExpression *nameAttr = createAttribute(entrypointAtom, createConstant(name->str()));
        attrs = createComma(attrs, nameAttr);
    }
    attrs = createComma(attrs, LINK(serviceExtraAttributes));
    return attrs;
}

bool HqlGram::checkAlienTypeDef(IHqlScope* scope, const attribute& errpos)
{
    bool hasError = false;

    if (!scope)
        return false;

    // load
    OwnedHqlExpr load = scope->lookupSymbol(loadAtom, LSFpublic, lookupCtx);
    if (!load) 
    {
        reportError(ERR_USRTYPE_NOLOAD,errpos,"Load function is not defined for user type");
        hasError = true;
    }
    else
    {
        if (!load->isFunctionDefinition())
        {
            reportError(ERR_USRTYPE_NOTDEFASFUNC, errpos, "Load is not defined as a function for user type");
            hasError = true;
        }
        else if (load->numChildren()!=2)
        {
            reportError(ERR_PARAM_WRONGNUMBER, errpos, "Load must have exactly 1 parameter");
            hasError = true;
        }
    }

    // store
    OwnedHqlExpr store = scope->lookupSymbol(storeAtom, LSFpublic, lookupCtx);
    if (!store) 
    {
        reportError(ERR_USRTYPE_NOSTORE,errpos,"Store function is not defined for alien type");
        hasError = true;
    }
    else
    {
        if (!store->isFunctionDefinition())
        {
            reportError(ERR_USRTYPE_NOTDEFASFUNC, errpos, "Store is not defined as a function for user type");
            hasError = true;
        }
        else if (store->numChildren()!=2)
        {
            reportError(ERR_PARAM_WRONGNUMBER, errpos,"Store must have exactly 1 parameter");
            hasError = true;
        }
    }


    // types
    if (!hasError) /* only do the type check when no error, otherwise, crash may occur */
    {
        IHqlExpression * loadParam = load->queryChild(1)->queryChild(0);
        IHqlExpression * storeParam = store->queryChild(1)->queryChild(0);
        
        ITypeInfo * storeType = store->queryType()->queryChildType();
        ITypeInfo* logical= load->queryType()->queryChildType();
        ITypeInfo* physical = loadParam->queryType();

        if (logical != storeParam->queryType())
            reportError(ERR_USRTYPE_BADLOGTYPE, errpos, "User type has inconsistent logical types");

        else if (physical != storeType)
            reportError(ERR_USRTYPE_BADPHYTYPE, errpos,"User type has inconsistent physical types");
        else 
        {
            // check whether we need a physicalLength()
            bool phylenNeeded = physical->getSize()==UNKNOWN_LENGTH;
            OwnedHqlExpr phyLen = scope->lookupSymbol(physicalLengthAtom, LSFpublic, lookupCtx);

            // physicalLength
            if (phylenNeeded)
            {
                if (!phyLen)
                    reportError(ERR_USRTYPE_NOPHYLEN, errpos, "Need physicalLength since physical type size is unknown");
                else
                {
                    if (phyLen->isFunctionDefinition())
                    {
                        unsigned numArgs = phyLen->queryChild(1)->numChildren();
                        if (numArgs == 1)
                        {
                            IHqlExpression * phylenParam = phyLen->queryChild(1)->queryChild(0);
                            if (physical == storeType && phylenParam->queryType() != physical)
                                reportError(ERR_USRTYPE_BADPHYLEN, errpos, "physicalLength need to take physical type as parameter");
                        }
                        else if (numArgs > 1)
                            reportError(ERR_PARAM_WRONGNUMBER, errpos, "physicalLength can have at most 1 parameter");

                        if (phyLen->queryType()->queryChildType()->getTypeCode() != type_int)
                            reportError(ERR_TYPEERR_INT, errpos, "physicalLength needs to return integer");
                    }
                    else
                    {
                        // must be defined as an attribute.
                        // How to check??
                        // MORE: should disallow this: 
                        //     export physicalLength := MACRO 3x ENDMACRO; 
                        // and 
                        //     export physicalLength(String physical) := MACRO 3x ENDMACRO; 
                    
                        if (phyLen->isMacro() || !phyLen->queryType() || phyLen->queryType()->getTypeCode()!=type_int)
                            reportError(ERR_TYPEERR_INT,errpos, "physicalLength needs to be type integer");
                    }                   
                }
            } 
            else
            {
                if (phyLen)
                    reportWarning(WRN_USRTYPE_EXTRAPHYLEN,errpos.pos,"physicalLength not needed since the type size is known");
            }
        }

    }

    return !hasError;
}

ITypeInfo *HqlGram::checkPromoteType(attribute &a1, attribute &a2)
{
    checkCompatible(a1.queryExprType(), a2.queryExprType(), a2);
    return promoteToSameType(a1, a2);
}

ITypeInfo *HqlGram::checkPromoteIfType(attribute &a1, attribute &a2)
{
    if (a1.isDataset() || a2.isDataset())
    {
        OwnedHqlExpr right = a2.getExpr();
        a2.setExpr(checkEnsureRecordsMatch(a1.queryExpr(), right, a2, false));
        ensureDataset(a1);
        ensureDataset(a2);
        return NULL;
    }
    if (a1.isDatarow() || a2.isDatarow())
    {
        OwnedHqlExpr right = a2.getExpr();
        a2.setExpr(checkEnsureRecordsMatch(a1.queryExpr(), right, a2, true));
        checkDatarow(a1);
        checkDatarow(a2);
        return NULL;
    }

    checkCompatible(a1.queryExprType(), a2.queryExprType(), a2);
    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();

    Owned<ITypeInfo> type = ::getPromotedECLType(t1, t2);
    if (isStringType(type) && (t1->getStringLen() != t2->getStringLen()))
        type.setown(getStretchedType(UNKNOWN_LENGTH, type));

    ensureType(a1, type);
    ensureType(a2, type);
    return type.getClear();
}

ITypeInfo *HqlGram::checkPromoteNumericType(attribute &a1, attribute &a2)
{
    checkNumeric(a1);
    checkNumeric(a2);

    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();

    applyDefaultPromotions(a1);
    applyDefaultPromotions(a2);
    return promoteToSameType(a1, a2);
}


ITypeInfo * HqlGram::checkStringIndex(attribute & strAttr, attribute & idxAttr)
{
    IHqlExpression * src = strAttr.queryExpr();
    SubStringHelper info(strAttr.queryExpr(), idxAttr.queryExpr());
    unsigned strSize = getBestLengthEstimate(src);
    unsigned startIndex = info.fixedStart;
    unsigned endIndex = info.fixedEnd;

    if (info.knownStart() && (startIndex < 1 || ((strSize != UNKNOWN_LENGTH) && startIndex > strSize)))
    {
        if (startIndex<1)
            reportWarning(ERR_SUBSTR_INVALIDRANGE, idxAttr.pos,"Invalid substring range: start index %d must >= 1", startIndex);
        else  /* assert: strSize != UNKNOWN_LENGTH */
            reportWarning(ERR_SUBSTR_INVALIDRANGE, idxAttr.pos,"Invalid substring range: index %d out of bound: 1..%d", startIndex, strSize);
    }
    else if (info.knownEnd() && (endIndex < 1 || ((strSize != UNKNOWN_LENGTH) && endIndex > strSize)))
    {
        if (endIndex < 1)
            reportWarning(ERR_SUBSTR_INVALIDRANGE, idxAttr.pos, "Invalid substring range: end index %d must >= 1", endIndex);
        else
            reportWarning(ERR_SUBSTR_INVALIDRANGE, idxAttr.pos, "Invalid substring range: index %d out of bound: 1..%d", endIndex, strSize);
    }
    else if (info.knownStart() && info.knownEnd() && startIndex > endIndex)
        reportWarning(ERR_SUBSTR_INVALIDRANGE, idxAttr.pos, "Invalid substring range: start index %d > end index %d", startIndex, endIndex);

    unsigned resultSize = UNKNOWN_LENGTH;
//  if (strSize != UNKNOWN_LENGTH)
    {
        if (info.knownStart() && info.knownEnd() && endIndex >= startIndex)
            resultSize = endIndex - startIndex + 1;
        else if (info.from == info.to)
            resultSize = 1;
    }

    ITypeInfo * subType;
    ITypeInfo *type = src->queryType();
    if (type->getTypeCode() == type_varstring)
        subType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
    else if (type->getTypeCode() == type_varunicode)
        subType = makeUnicodeType(UNKNOWN_LENGTH, type->queryLocale());
    else
        subType = getStretchedType(resultSize, type);
    return subType;
}

void HqlGram::checkAggregateRecords(IHqlExpression * expr, IHqlExpression * record, attribute & errpos)
{
    if (!recordTypesMatch(expr->queryChild(1), record))
        reportError(ERR_ONFAIL_MISMATCH, errpos, "Type of the transform does not match the aggregate record");

    if (containsIfBlock(record))
        reportError(ERR_NO_IFBLOCKS, errpos, "IFBLOCKS not supported in the aggregate target record");

    IHqlExpression * mergeTransform = queryPropertyChild(expr, mergeAtom, 0);
    if (mergeTransform)
    {
        if (!recordTypesMatch(mergeTransform, expr))
            reportError(ERR_ONFAIL_MISMATCH, errpos, "Type of the MERGE transform does not match the aggregate record");
    }
}

void HqlGram::checkOnFailRecord(IHqlExpression * expr, attribute & errpos)
{
    IHqlExpression * onFail = expr->queryProperty(onFailAtom);
    if (onFail)
    {
        IHqlExpression * transform = onFail->queryChild(0);
        if (transform->getOperator() != no_skip)
        {
            if (!recordTypesMatch(transform, expr))
                reportError(ERR_ONFAIL_MISMATCH, errpos, "Type of the ONFAIL transform does not match the type of the result");
        }
    }
}

void HqlGram::applyDefaultPromotions(attribute &a1)
{
    ITypeInfo *t1 = a1.queryExprType();
    type_t tc = t1->getTypeCode();
    if ((tc == type_swapint) || (tc == type_packedint) || ((tc == type_int) && (t1->getSize() < 8)) || (tc == type_bitfield))
        ensureType(a1, defaultIntegralType);
}

void HqlGram::checkSameType(attribute &a1, attribute &a2)
{
    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();
    if (t1 != t2)
    {
        StringBuffer s1, s2;
        reportError(ERR_TYPE_DIFFER, a2, "Expressions must have the same type: %s vs %s",getFriendlyTypeStr(t1, s1).str(), getFriendlyTypeStr(t2, s2).str());
    }
}

void HqlGram::normalizeStoredNameExpression(attribute & a)
{
    normalizeExpression(a, type_string, true);
    IHqlExpression * name = a.queryExpr();
    if (name && name->queryValue())
    {
        StringBuffer nameText;
        name->queryValue()->getStringValue(nameText);
        if (!isCIdentifier(nameText))
            reportError(ERR_NAME_NOT_VALID_ID, a, "Name '%s' must be a valid identifier.", nameText.str());
    }
}

IHqlExpression * HqlGram::addDatasetSelector(IHqlExpression * lhs, IHqlExpression * rhs)
{
    if (rhs->getOperator() != no_select)
        return createSelectExpr(lhs, rhs);

    IHqlExpression * ret = addDatasetSelector(lhs, LINK(rhs->queryChild(0)));
    ret = createSelectExpr(ret, LINK(rhs->queryChild(1)));
    rhs->Release();
    return ret;
}

IHqlExpression * HqlGram::createListFromExprArray(const attribute & errpos, HqlExprArray & args)
{
    ITypeInfo * retType = promoteToSameType(args, errpos, NULL, true);
    return createValue(no_list, makeSetType(retType), args);
}

IHqlExpression * HqlGram::createListFromExpressionList(attribute & attr)
{
    HqlExprArray args;
    attr.unwindCommaList(args);
    return normalizeExprList(attr, args);
}

void HqlGram::normalizeExpression(attribute & exprAttr)
{
    if (exprAttr.getOperator() == no_sortlist)
    {
        HqlExprArray args;
        unwindChildren(args, exprAttr.queryExpr());
        assertex(args.ordinality());
        exprAttr.release().setExpr(normalizeExprList(exprAttr, args));
    }
}

void HqlGram::normalizeExpression(attribute & exprAttr, type_t expectedType, bool isConstant)
{
    normalizeExpression(exprAttr);
    switch (expectedType)
    {
    case type_boolean:
        checkBoolean(exprAttr);
        break;
    case type_string:
        checkString(exprAttr);
        break;
    case type_int:
        checkInteger(exprAttr);
        break;
    case type_any:
        //No checking
        break;
    case type_numeric:
        checkNumeric(exprAttr);
        break;
    case type_scalar:
        checkScalar(exprAttr);
        break;
    case type_real:
        checkReal(exprAttr);
        break;
    case type_stringorunicode:
        checkStringOrUnicode(exprAttr);
        break;
    case type_set:
        checkList(exprAttr);
        break;
    case type_table:
        ensureDataset(exprAttr);
        break;
    case type_data:
        //MORE: Complain if not assign compatible.
        break;
    case type_unicode:
        ensureUnicode(exprAttr);
        break;
    default:
        throwUnexpected();
    }
    if (isConstant)
        checkFoldConstant(exprAttr);
}

IHqlExpression * HqlGram::normalizeExprList(const attribute & errpos, const HqlExprArray & values)
{
    if ((values.ordinality() == 1) && values.item(0).isList())
        return &OLINK(values.item(0));

    HqlExprArray lists;
    HqlExprArray thisList;
    IHqlExpression * emptyList = NULL;
    ForEachItemIn(i, values)
    {
        IHqlExpression & cur = values.item(i);
        if (cur.isList())
        {
            switch (cur.getOperator())
            {
            case no_list:
                unwindChildren(thisList, &cur);
                if (cur.numChildren() == 0)
                    emptyList = &cur;
                break;
            case no_null:
                emptyList = &cur;
                break;
            default:
                if (thisList.ordinality())
                {
                    lists.append(*createListFromExprArray(errpos, thisList));
                    thisList.kill();
                }
                lists.append(OLINK(cur));
                break;
            }
        }
        else
            thisList.append(OLINK(cur));
    }

    if (thisList.ordinality())
        lists.append(*createListFromExprArray(errpos, thisList));

    if (lists.ordinality() == 0)
    {
        if (emptyList)
            return LINK(emptyList);
        return createValue(no_list, makeSetType(NULL));
    }

    Owned<ITypeInfo> combinedType = promoteToSameType(lists, errpos, NULL, true);
    return createUnbalanced(no_addsets, combinedType, lists);
}

IHqlExpression * HqlGram::createIndirectSelect(IHqlExpression * lhs, IHqlExpression * rhs, const attribute & errpos)
{
    OwnedHqlExpr releaseRhs = rhs;
    IHqlExpression * record = lhs->queryRecord();
    assertex(record);
    IHqlExpression * field = rhs;
    while (rhs->getOperator() == no_indirect)
        rhs = rhs->queryChild(0);

    if (rhs->getOperator() == no_select)
        field = rhs->queryChild(1);

    if (!isValidFieldReference(field))
    {
        reportError(ERR_EXPECTED_FIELD, errpos, "Expected a field reference inside <>");
        lhs->Release();
        return createNullExpr(rhs);
    }

    if (record->hasProperty(abstractAtom) || (field->getOperator() != no_field))
        return ::createSelectExpr(lhs, createValue(no_indirect, field->getType(), LINK(field)));

    OwnedHqlExpr match = record->querySimpleScope()->lookupSymbol(field->queryName());
    if (match)
    {
        if (match == field)
            return createSelect(lhs, LINK(match), errpos);

        OwnedHqlExpr normalizedMatch = getUnadornedExpr(match);
        OwnedHqlExpr normalizedField = getUnadornedExpr(field);
        if (normalizedMatch == normalizedField)
            return createSelect(lhs, LINK(match), errpos);
    }

    if (rhs->getOperator() == no_select)
    {
        IHqlExpression * selector = rhs->queryChild(0);
        if (selector->getOperator() == no_select)
        {
            OwnedHqlExpr selected = createIndirectSelect(lhs, LINK(selector), errpos);
            return createIndirectSelect(selected.getClear(), field, errpos);
        }
    }

    reportError(ERR_DATASET_NOT_CONTAIN_X, errpos, "Dataset doesn't contain a field %s", field->queryName()->str());
    lhs->Release();
    return createNullExpr(rhs);
}

IHqlExpression * HqlGram::createSelect(IHqlExpression * lhs, IHqlExpression * rhs, const attribute & errpos)
{
    if (rhs->getOperator() == no_indirect)
    {
        OwnedHqlExpr releaseRhs = rhs;
        return createIndirectSelect(lhs, LINK(rhs->queryChild(0)), errpos);
    }

    if (rhs->getOperator() != no_field)
    {
        rhs->Release();
        rhs = ::createField(unnamedAtom, LINK(defaultIntegralType), NULL);
    }
    return ::createSelectExpr(lhs, rhs);
}

                            

IHqlExpression * HqlGram::createAveList(const attribute & errpos, IHqlExpression * list)
{
    ITypeInfo * elemType = queryElementType(errpos, list);
    Owned<ITypeInfo> sumType = getSumAggType(elemType);
    OwnedHqlExpr sum = createValue(no_sumlist, LINK(sumType), LINK(list));
    OwnedHqlExpr count = createValue(no_countlist, LINK(defaultIntegralType), LINK(list));
    return createValue(no_div, LINK(defaultRealType), ensureExprType(sum, defaultRealType), ensureExprType(count, defaultRealType));
}


IHqlExpression * HqlGram::createSortExpr(node_operator op, attribute & dsAttr, const attribute & orderAttr, HqlExprArray & args)
{
    IHqlExpression *input = dsAttr.getExpr();
    OwnedHqlExpr joinedClause;
    OwnedHqlExpr attrs;
    IHqlExpression *sortOrder = processSortList(orderAttr, no_sort, input, args, &joinedClause, &attrs);
    if (!sortOrder)
    {
        reportError(ERR_SORT_EMPTYLIST, orderAttr, "The list to be sorted on is empty");
        return input;
    }

    bool isLocal = (queryPropertyInList(localAtom, attrs)!=NULL);
    checkDistribution(dsAttr, input, isLocal, false);

    if (joinedClause)
    {
        checkCosort(sortOrder, joinedClause, orderAttr);
        sortOrder = createComma(sortOrder, joinedClause.getClear());
        op = no_cosort;
    }
    sortOrder = createComma(sortOrder, attrs.getClear());
    return createDataset(op, input, sortOrder);
}


ITypeInfo * HqlGram::queryElementType(const attribute & errpos, IHqlExpression * list)
{
    ITypeInfo * elemType = list->queryType()->queryChildType();
    if (elemType)
        return elemType;

    reportError(ERR_CANNOT_DEDUCE_TYPE, errpos, "Can't deduce type of elements in list");
    return defaultIntegralType;
}


void HqlGram::setDefaultString(attribute &a)
{
    a.release();
    a.setExpr(createConstant(""));
}

void HqlGram::ensureString(attribute &a)
{
    ITypeInfo *t1 = a.queryExprType();
    if (t1 && !isSimpleStringType(t1))
    {
        if (!t1->isScalar())    // is this test sufficient
        {
            StringBuffer s;
            reportError(ERR_TYPE_INCOMPATIBLE, a, "Incompatible types: expected String, given %s", getFriendlyTypeStr(t1, s).str());
            setDefaultString(a);
        }
        else if (isStringType(t1))
        {
            t1 = makeStringType(t1->getStringLen(), NULL, NULL);
            a.setExpr(createValue(no_implicitcast, t1, a.getExpr() ));
        }
        else
        {
            t1 = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
            a.setExpr(createValue(no_implicitcast, t1, a.getExpr() ));
        }
    }
}

void HqlGram::validateXPath(attribute & a)
{
    IHqlExpression * expr = a.queryExpr();
    IValue * value = expr->queryValue();
    if (value)
    {
        StringBuffer s, error;
        value->getStringValue(s);
        if (!validateXMLParseXPath(s.str(), &error))
            reportError(ERR_INVALID_XPATH, a, "Invalid XPATH syntax: %s", error.str());
    }
}

void HqlGram::ensureTypeCanBeIndexed(attribute &a)
{
    ITypeInfo *t1 = a.queryExprType();
    if (!t1)
        setDefaultString(a);
    else
    {
        switch (t1->getTypeCode())
        {
        case type_string:
        case type_varstring:
        case type_qstring:
        case type_data:
        case type_unicode:
        case type_varunicode:
        case type_utf8:
            break;
        default:
            ensureString(a);
            break;
        }
    }
}

void HqlGram::ensureUnicode(attribute &a)
{
    ITypeInfo *t1 = a.queryExprType();
    if (t1 && !isUnicodeType(t1))
    {
        if (isStringType(t1))
        {
            Owned<ITypeInfo> unicodeType = makeUnicodeType(UNKNOWN_LENGTH, NULL);
            OwnedHqlExpr value = a.getExpr();
            a.setExpr(ensureExprType(value, unicodeType));
        }
        else
        {
            StringBuffer s;
            reportError(ERR_TYPE_INCOMPATIBLE, a, "Incompatible types: expected Unicode, given %s", getFriendlyTypeStr(t1, s).str());
        }
    }
}

void HqlGram::ensureData(attribute &a)
{
    ITypeInfo *t1 = a.queryExprType();
    if (t1 && (t1->getTypeCode() != type_data))
    {
        StringBuffer s;
        reportError(ERR_TYPE_INCOMPATIBLE, a, "Incompatible types: expected Data, given %s", getFriendlyTypeStr(t1, s).str());
    }
}

_ATOM HqlGram::ensureCommonLocale(attribute &a, attribute &b)
{
    ITypeInfo * t1 = a.queryExprType();
    ITypeInfo * t2 = b.queryExprType();
    if(t1 && t2)
    {
        if(haveCommonLocale(t1, t2))
            return getCommonLocale(t1, t2);
        
        reportError(ERR_LOCALES_INCOMPATIBLE, b, "Incompatible locales in unicode arguments of binary operation");
    }
    return _empty_str_Atom;
}

void HqlGram::ensureUnicodeLocale(attribute & a, char const * locale)
{
    Owned<ITypeInfo> type = a.queryExpr()->getType();
    if(strcmp(locale, type->queryLocale()->str()) != 0)
    {
        switch (type->getTypeCode())
        {
        case type_varunicode:
            type.setown(makeVarUnicodeType(type->getStringLen(), createLowerCaseAtom(locale)));
            break;
        case type_unicode:
            type.setown(makeUnicodeType(type->getStringLen(), createLowerCaseAtom(locale)));
            break;
        case type_utf8:
            type.setown(makeUtf8Type(type->getStringLen(), createLowerCaseAtom(locale)));
            break;
        }
        a.setExpr(createValue(no_implicitcast, LINK(type), a.getExpr()));
    }
}

void HqlGram::checkPattern(attribute & pattern, bool isCompound)
{
    type_t ptc = pattern.queryExprType()->getTypeCode();
    if (current_type)
    {
        switch (current_type->getTypeCode())
        {
        case type_pattern:
            if (ptc != type_pattern)
                reportError(ERR_TOKEN_IN_PATTERN, pattern, "Only patterns are valid inside pattern definitions");
            break;
        case type_token:
            if (isCompound && ptc != type_pattern)
                reportError(ERR_TOKEN_IN_TOKEN, pattern, "Combinations of tokens are not valid inside a token definition");
            else if (ptc == type_rule)
                reportError(ERR_TOKEN_IN_TOKEN, pattern, "Rules are not valid inside a token definition");
            break;
        case type_rule:
            if (ptc == type_pattern)
                pattern.setExpr(createValue(no_pat_imptoken, makeTokenType(), pattern.getExpr()));
            break;
        }
    }
    else
    {
        //Must be an separator attribute - allow anything
    }
}

IHqlExpression * HqlGram::createPatternOr(HqlExprArray & args, const attribute & errpos)
{
    if (args.ordinality() == 1)
        return &OLINK(args.item(0));

    Owned<ITypeInfo> type = makePatternType();
    ForEachItemIn(idx, args)
    {
        ITypeInfo * argType = args.item(idx).queryType();
        type_t argtc = argType->getTypeCode();
        if (type->getTypeCode() == type_rule)
        {
            ITypeInfo * rRecord = type->queryChildType();
            ITypeInfo * aRecord = argType->queryChildType();
            if (aRecord != rRecord)
            {
                if (!rRecord || !aRecord || !recordTypesMatch(rRecord, aRecord))
                    reportError(ERR_PATTERN_TYPE_MATCH, errpos, "Productions must return the same record");
            }
        }
        if (argtc == type_rule)
        {
            if (idx == 0)
                type.set(argType);
        }
        else if ((argtc == type_token) && (type->getTypeCode() == type_pattern))
            type.set(argType);
    }
    if (type->getTypeCode() != type_pattern)
    {
        ForEachItemIn(idx, args)
        {
            IHqlExpression & cur = args.item(idx);
            ITypeInfo * argType = cur.queryType();
            if (argType->getTypeCode() == type_pattern)
                args.replace(*createValue(no_pat_imptoken, makeTokenType(), LINK(&cur)), idx);
        }
    }

    return createValue(no_pat_or, type.getClear(), args);
}

void HqlGram::checkSubPattern(attribute & subpattern)
{
    if (subpattern.queryExprType()->getTypeCode() == type_rule)
    {
        //MORE: May want to extend this eventually!
        reportError(ERR_PATTEN_SUBPATTERN, subpattern, "Expected a pattern for the check pattern");
    }
}

IHqlExpression * HqlGram::createNullPattern()
{
    ITypeInfo * type = NULL;
    if (current_type && (current_type->getTypeCode() == type_rule))
        type = makeTokenType();
    else
        type = makePatternType();
    return createValue(no_null, type);
}


void HqlGram::checkPattern(attribute & pattern, HqlExprArray & values)
{
    if (current_type)
    {
        type_t currentTC = current_type->getTypeCode();

        if (currentTC == type_rule)
        {
            ForEachItemIn(idx, values)
            {
                IHqlExpression & cur = values.item(idx);
                if (cur.queryType()->getTypeCode() == type_pattern)
                    values.replace(*createValue(no_pat_imptoken, makeTokenType(), LINK(&cur)), idx);
            }
        }
        else
        {
            ForEachItemIn(idx, values)
            {
                IHqlExpression & cur = values.item(idx);
                if (cur.queryType()->getTypeCode() != type_pattern)
                {
                    if (currentTC == type_pattern)
                        reportError(ERR_TOKEN_IN_PATTERN, pattern, "Only patterns are valid inside pattern definitions");
                    else
                        reportError(ERR_TOKEN_IN_TOKEN, pattern, "Combinations of rules/tokens are not valid inside a token definition");
                }
            }
        }
    }
}

void HqlGram::checkProduction(const HqlExprArray & args, const attribute & errpos)
{
    bool matched = false;
    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        IHqlExpression * record = cur.queryRecord();

        if (record)
        {
            if (matched)
            {
                reportError(ERR_AMBIGUOUS_PRODUCTION, errpos, "Cannot create implicit production - more than one pattern with record type");
                return;
            }
            matched = true;
        }
    }
}


IHqlExpression * HqlGram::convertPatternToExpression(attribute & text)
{
    IHqlExpression * expr = text.queryExpr();
    try
    {
        IValue * value = expr->queryValue();
        unsigned len = value->queryType()->getStringLen();
        const void * data = value->queryValue();
        switch (expr->queryType()->getTypeCode())
        {
        case type_unicode:
        case type_varunicode:
            return ::convertPatternToExpression(len, (const UChar *)data);
        case type_utf8:
            return ::convertUtf8PatternToExpression(len, (const char *)data);
        default:
            return ::convertPatternToExpression(len, (const char *)data);
        }
    }       
    catch (IException * e)
    {
        StringBuffer s;
        e->errorMessage(s);
        reportError(ERR_BAD_PATTERN, text, "%s", s.str());
        e->Release();
    }
    return NULL;
}


ITypeInfo * HqlGram::getCompoundRuleType(ITypeInfo * lType, ITypeInfo * rType)
{
    if ((lType->getTypeCode() == type_pattern) && (lType == rType))
        return LINK(lType);

    //preserve the record for a rule if only one of the arguments has a record type
    if ((lType->getTypeCode() == type_rule) && lType->queryChildType())
    {
        if (!rType->queryChildType())
            return LINK(lType);
    }
    else if ((rType->getTypeCode() == type_rule) && rType->queryChildType())
    {
        return LINK(rType);
    }

    return makeRuleType(NULL);
}

ITypeInfo * HqlGram::getCompoundRuleType(IHqlExpression * lhs)
{
    ITypeInfo * lType = lhs->queryType();
    switch (lType->getTypeCode())
    {
    case type_pattern:
    case type_rule:                 //NB: preserve the record, if specified
        return LINK(lType);
    }

    return makeRuleType(NULL);
}


IHqlExpression * HqlGram::getFeatureParams()
{
    implicitFeatureNames.kill();
    implicitFeatureValues.kill();
    return curFeatureParams.getClear();
}

IHqlExpression * HqlGram::deduceGuardFeature(IHqlExpression * value, attribute & errpos)
{
    if (implicitFeatureNames.ordinality() == 0)
        expandImplicitFeatures();

    IHqlExpression * match = NULL;
    switch (value->getOperator())
    {
    case no_constant:
        {
            ITypeInfo * searchType;
            if (isStringType(value->queryType()))
                searchType = makeStringType(UNKNOWN_LENGTH, NULL, NULL);
            else
                searchType = makeIntType(DEFAULT_INT_SIZE, true);

            OwnedHqlExpr search = createValue(no_null, makeFeatureType(), createValue(no_featuretype, searchType));
            match = findFeature(search);
            break;
        }
    case no_pat_or:
        {
            HqlExprArray args;
            value->unwindList(args, no_pat_or);
            match = findFeature(&args.item(0));
            break;
        }
    default:
        match = findFeature(value);
        break;
    }

    if (match)
        return LINK(match);
    reportError(ERR_BADGUARD, errpos, "Could not deduce the feature being guarded");
    return createValue(no_null, makeFeatureType());
}


void HqlGram::expandImplicitFeatures(IHqlExpression * feature, IHqlExpression * value)
{
    assertex(value->getOperator() == no_pat_featuredef);
    implicitFeatureNames.append(*feature);  // not linked
    implicitFeatureValues.append(*value);   // not linked
    value = value->queryChild(0);
    switch (value->getOperator())
    {
    case no_null:
        break;
    case no_pat_featuredef:
        expandImplicitFeatures(feature, value);
        break;
    case no_pat_or:
        {
            HqlExprArray args;
            value->unwindList(args, no_pat_or);
            ForEachItemIn(idx, args)
                expandImplicitFeatures(feature, &args.item(idx));
            break;
        }
    default:
        UNIMPLEMENTED;
    }
}

IHqlExpression * HqlGram::findFeature(IHqlExpression * value)
{
    unsigned match = implicitFeatureValues.find(*value);
    if (match == NotFound)
        return NULL;
    return &implicitFeatureNames.item(match);
}
    

void HqlGram::expandImplicitFeatures()
{
    HqlExprArray features;
    if (curFeatureParams)
    {
        curFeatureParams->unwindList(features, no_comma);
        ForEachItemIn(idx, features)
        {
            IHqlExpression & cur = features.item(idx);
            expandImplicitFeatures(&cur, &cur);
        }
    }
}


void HqlGram::setFeatureParamsOwn(IHqlExpression * expr)
{
    curFeatureParams.setown(expr);
}


static IHqlExpression * translateExprToPattern(IHqlExpression * expr, bool insideRule);
static IHqlExpression * doTranslateExprToPattern(IHqlExpression * expr, bool insideRule)
{
    switch (expr->getOperator())
    {
    case no_constant:
        switch (expr->queryType()->getTypeCode())
        {
        case type_string:
        case type_unicode:
        case type_utf8:
            return createValue(no_pat_const, makePatternType(), LINK(expr));
        }
        break;
    case no_list:
        {
            HqlExprArray args;
            ForEachChild(idx, expr)
            {
                IHqlExpression * next = translateExprToPattern(expr->queryChild(idx), insideRule);
                if (!next)
                    return NULL;
                args.append(*next);
            }
            return createValue(no_pat_or, insideRule ? makeRuleType(NULL) : makePatternType(), args);
        }
    }
    return NULL;
}

static IHqlExpression * translateExprToPattern(IHqlExpression * expr, bool insideRule)
{
    IHqlExpression * pattern = doTranslateExprToPattern(expr, insideRule);
    if (pattern && insideRule && pattern->queryType()->getTypeCode() == type_pattern)
        pattern = createValue(no_pat_imptoken, makeTokenType(), pattern);
    return pattern;
}


IHqlExpression * HqlGram::processExprInPattern(attribute & attr)
{
    OwnedHqlExpr expr = attr.getExpr();
    OwnedHqlExpr folded = foldHqlExpression(expr);
    bool insideRule = current_type && (current_type->getTypeCode() == type_rule);;
    OwnedHqlExpr translated = translateExprToPattern(folded, insideRule);
    if (!translated)
    {
        reportError(ERR_EXPR_IN_PATTERN, attr, "This expression cannot be included in a pattern");
        translated.setown(createValue(no_pat_anychar, makePatternType()));
    }
    return translated.getClear();
}

void HqlGram::ensureBoolean(attribute &a)
{
    ensureType(a, boolType);
}


void HqlGram::ensureType(attribute &a, ITypeInfo * type)
{
    IHqlExpression *expr = a.queryExpr(); 
    ITypeInfo * exprType = expr->queryType();
    if (!isSameBasicType(exprType, type))
    {
        if (!type->queryPromotedType()->assignableFrom(exprType->queryPromotedType()))
        {
            StringBuffer msg("Incompatible types: expected ");
            getFriendlyTypeStr(type, msg).append(", given ");
            getFriendlyTypeStr(expr->queryType(),msg);
            reportError(ERR_TYPE_INCOMPATIBLE, a, "%s", msg.str());
        }
        expr = a.getExpr();
        a.setExpr(ensureExprType(expr, type));
        expr->Release();
    }
}

ITypeInfo *HqlGram::promoteToSameType(attribute &a1, attribute &a2)
{
    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();
    ITypeInfo * type = ::getPromotedECLType(t1, t2);
    ensureType(a1, type);
    ensureType(a2, type);
    return type;
}

ITypeInfo *HqlGram::promoteToSameCompareType(attribute &a1, attribute &a2)
{
    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();
    ITypeInfo * type = ::getPromotedECLCompareType(t1, t2);
    ensureType(a1, type);
    ensureType(a2, type);
    return type;
}

IHqlExpression * HqlGram::createArithmeticOp(node_operator op, attribute &a1, attribute &a2)
{
    normalizeExpression(a1, type_numeric, false);
    normalizeExpression(a2, type_numeric, false);

    switch (op)
    {
    case no_add:
    case no_sub:
    case no_mul:
    case no_div:
        applyDefaultPromotions(a1);
        applyDefaultPromotions(a2);
        break;
    }

    ITypeInfo *t1 = a1.queryExprType();
    ITypeInfo *t2 = a2.queryExprType();
    Owned<ITypeInfo> type;
    switch (op)
    {
    case no_add:
    case no_sub:
        type.setown(getPromotedAddSubType(t1, t2));
        break;
    case no_mul:
    case no_div:
        type.setown(getPromotedMulDivType(t1, t2));
        break;
    }

    if (!type)
        type.setown(getPromotedType(t1, t2));

    if (!isDecimalType(type))
    {
        ensureType(a1, type);
        ensureType(a2, type);
    }

    return createValue(op, type.getClear(), a1.getExpr(), a2.getExpr());
}


void HqlGram::promoteToSameCompareType(attribute &a1, attribute &a2, node_operator op)
{
    if ((a1.queryExpr()->getOperator() == no_constant) && (a2.queryExpr()->getOperator() != no_constant) && (op != no_between))
    {
        promoteToSameCompareType(a2, a1, getReverseOp(op));
        return;
    }

    ITypeInfo * t1 = a1.queryExprType();
    IHqlExpression * e2 = a2.queryExpr();

    //Check for comparisons that are always true/false....
    IValue * value = e2->queryValue();
    if (value)
    {
        bool alwaysFalse = false;
        bool alwaysTrue = false;
        int rc = value->rangeCompare(t1);
        if (rc != 0)
        {
            switch (op)
            {
            case no_eq: 
                alwaysFalse = true;
                break;
            case no_ne: 
                alwaysTrue = true; 
                break;
            case no_le:
            case no_lt:
                if (rc > 0)
                    alwaysTrue = true;
                else
                    alwaysFalse = true;
                break;
            case no_gt:
            case no_ge:
                if (rc < 0) // value underflows => test field will always be less than
                    alwaysTrue = true;
                else
                    alwaysFalse = true;
                break;
            case no_between:
                //Don't do anything 
                break;

            }
        }
        else
        {
            //This is worth doing for between as well.
            //Integer comparisons are done as int64 unless we are careful.  This ensures that
            //comparisons against constants are done at non-constant width if appropriate.
            ITypeInfo * t2 = e2->queryType();
            type_t ttc1 = t1->getTypeCode();
            if (((ttc1==type_int) && t2->isInteger()) || (ttc1==type_decimal))
            {
                OwnedIValue castValue = value->castTo(t1);
                if (castValue)
                {
                    OwnedITypeInfo promoted = ::getPromotedECLType(t1, t2);
                    OwnedIValue promotedCastValue = castValue->castTo(promoted);
                    OwnedIValue promotedValue = value->castTo(promoted);
                    if (promotedCastValue->compare(promotedValue) == 0)
                    {
                        //Value is represented in t1 type so do comparison as type t1.
                        a2.release();
                        a2.setExpr(createConstant(LINK(castValue)));
                    }
                }
            }
#if 0
            //Check if comparison value can be represented in the other type - if not, then it's not going to compare equal
            OwnedIValue uncastValue(value->castTo(t1));
            if (!uncastValue)
            {
                if (op == no_eq)
                    alwaysFalse = true;
                else if (op == no_ne)
                    alwaysTrue = true;
            }
#endif
        }

        if (alwaysTrue)
            reportWarning(WRN_COND_ALWAYS_TRUE, a2.pos, "Condition is always true");
        if (alwaysFalse)
            reportWarning(WRN_COND_ALWAYS_FALSE, a2.pos, "Condition is always false");
    }

#if 0
    //The following improves the generated code, but causes a few problems at the moment
    if (op != no_between && !e2->isConstant())
    {
        switch (t1->getTypeCode())
        {
        case type_string:
        case type_unicode:
        case type_utf8:
            {
                Owned<ITypeInfo> otherType = getStretchedType(t1->getStringLen(), e2->queryType());
                if (t1 == otherType)
                    return;
            }
            break;
        }
    }
#endif          

//  ::Release(promoteToSameCompareType(a1, a2));
    ::Release(promoteToSameType(a1, a2));
}

void HqlGram::warnIfFoldsToConstant(IHqlExpression * expr, const attribute & errpos)
{
    if (expr && !expr->queryValue())
    {
        OwnedHqlExpr folded = foldExprIfConstant(expr);
        if (folded->queryValue())
        {
            if (folded->queryValue()->getBoolValue())
                reportWarning(WRN_COND_ALWAYS_TRUE, errpos.pos, "Condition is always true");
            else
                reportWarning(WRN_COND_ALWAYS_FALSE, errpos.pos, "Condition is always false");
        }
    }
}


void HqlGram::warnIfRecordPacked(IHqlExpression * expr, const attribute & errpos)
{
    IHqlExpression * record = expr->queryRecord();
    if (record && record->hasProperty(packedAtom))
        reportWarning(WRN_PACKED_MAY_CHANGE, errpos.pos, "Packed record used for external input or output, packed formats may change");
}


void HqlGram::promoteToSameCompareType(attribute &a1, attribute &a2, attribute &a3)
{
    promoteToSameCompareType(a1, a2, no_between);
    promoteToSameCompareType(a1, a3, no_between);
    promoteToSameCompareType(a1, a2, no_between);
}


ITypeInfo * HqlGram::getPromotedECLType(HqlExprArray & exprs, ITypeInfo * _promoted, bool allowVariableLength)
{
    unsigned start = 0;
    Linked<ITypeInfo> promoted = _promoted;
    if (!promoted)
        promoted.set(exprs.item(start++).queryType());
    Linked<ITypeInfo> initial = promoted;

    unsigned max = exprs.ordinality();
    unsigned idx;
    bool allSame = true;
    for (idx = start; idx < max; idx++)
    {
        ITypeInfo * curType = exprs.item(idx).queryType();
        promoted.setown(::getPromotedECLType(promoted, curType));
        if (promoted != curType)
            allSame = false;
    }

    if (allowVariableLength)
    {
        //Really, maps/cases that return different length strings should have variable length string returns, but
        //that would cause differences in HOLe.  I suspect we should enable this and report errors in hole if the lengths
        //mismatch, rather than hobbling the code generator.
        if ((promoted != initial) || !allSame)
        {
            switch (promoted->getTypeCode())
            {
            case type_string:
            case type_varstring:
            case type_qstring:
            case type_unicode:
            case type_varunicode:
            case type_utf8:
                promoted.setown(getStretchedType(UNKNOWN_LENGTH, promoted));
                break;
            }
        }
    }

    return promoted.getClear();
}

ITypeInfo *HqlGram::promoteToSameType(HqlExprArray & exprs, const attribute &ea, ITypeInfo * otherType, bool allowVariableLength)
{
    if (exprs.ordinality() == 0)
        return LINK(otherType);

    ITypeInfo * promoted = getPromotedECLType(exprs, otherType, allowVariableLength);

    ForEachItemIn(idx, exprs)
    {
        IHqlExpression & cur = exprs.item(idx);
        checkCompatible(cur.queryType(), promoted, ea);
        IHqlExpression * cast = ensureExprType(&cur, promoted);
        exprs.replace(*cast, idx);
    }

    return promoted;
}

ITypeInfo *HqlGram::promoteMapToSameType(HqlExprArray & exprs, attribute &eElse)
{
    bool differentLengthStringsVariableLengthRatherThanLongest = false;
    ITypeInfo * promoted = getPromotedECLType(exprs, eElse.queryExprType(), differentLengthStringsVariableLengthRatherThanLongest);

    ForEachItemIn(idx, exprs)
    {
        IHqlExpression & cur = exprs.item(idx);
        checkCompatible(cur.queryType(), promoted, eElse);

        IHqlExpression * cast = ensureExprType(cur.queryChild(1), promoted);
        IHqlExpression * cond = cur.queryChild(0);
        IHqlExpression * map = createValue(no_mapto, LINK(promoted), LINK(cond), cast);
        exprs.replace(*map, idx);
    }

    checkType(eElse, promoted);
    ensureType(eElse, promoted);
    return promoted;
}

void HqlGram::promoteToSameListType(attribute & leftAttr, attribute & rightAttr)
{
    ITypeInfo * leftType = leftAttr.queryExprType();
    ITypeInfo * rightType = rightAttr.queryExprType();
    if (leftType != rightType)
    {
        IHqlExpression * left= leftAttr.getExpr();
        IHqlExpression * right = rightAttr.getExpr();
        if ((left->getOperator() == no_list) && (left->numChildren() == 0))
        {
            left->Release();
            left = createValue(no_list, LINK(rightType));
        }
        else if ((right->getOperator() == no_list) && (right->numChildren() == 0))
        {
            right->Release();
            right = createValue(no_list, LINK(leftType));
        }
        else
        {
            assertex(leftType->getTypeCode() == type_set && rightType->getTypeCode() == type_set);
            ITypeInfo * promoted = ::getPromotedECLType(leftType, rightType);
            IHqlExpression * newLeft = ensureExprType(left, promoted);
            left->Release();
            left = newLeft;
            IHqlExpression * newRight = ensureExprType(right, promoted);
            right->Release();
            right = newRight;
            promoted->Release();
        }

        leftAttr.setExpr(left);
        rightAttr.setExpr(right);
    }
}


ITypeInfo * HqlGram::promoteSetToSameType(HqlExprArray & exprs, attribute &errpos)
{
    Owned<ITypeInfo> promoted = exprs.item(0).getType();

    unsigned max = exprs.ordinality();
    for (unsigned idx1 = 1; idx1 < max; idx1++)
    {
        ITypeInfo * type = exprs.item(idx1).queryType();

        if (isStringType(promoted) && isStringType(type))
        {
            if (promoted->getStringLen() != type->getStringLen())
            {
                promoted.setown(::getPromotedECLType(promoted, type));
                promoted.setown(getStretchedType(UNKNOWN_LENGTH, promoted));
            }
        }

        promoted.setown(::getPromotedECLType(promoted, type));
    }

    ForEachItemIn(idx, exprs)
    {
        IHqlExpression & cur = exprs.item(idx);
        checkCompatible(cur.queryType(), promoted, errpos);
        IHqlExpression * cast = ensureExprType(&cur, promoted);
        exprs.replace(*cast, idx);
    }

    return promoted.getClear();
}

IHqlExpression *HqlGram::createINExpression(node_operator op, IHqlExpression *expr, IHqlExpression *set, attribute &errpos)
{
    ITypeInfo * exprType = expr->queryType();
    ITypeInfo * elementType = set->queryType()->queryChildType();
    if (elementType)
        checkCompatible(exprType, elementType, errpos);

    OwnedHqlExpr normalized = normalizeListCasts(set);
    if (normalized->getOperator()==no_list)
    {
        HqlExprArray args;
        unwindChildren(args, normalized);

        bool differentLengthStringsVariableLengthRatherThanLongest = false;
        Owned<ITypeInfo> retType = promoteToSameType(args, errpos, exprType, differentLengthStringsVariableLengthRatherThanLongest);
        if (retType != elementType)
            normalized.setown(createValue(no_list, makeSetType(LINK(retType)), args));
    }
    elementType = normalized->queryType()->queryChildType();
    if (elementType)
    {
        Owned<ITypeInfo> promotedType = ::getPromotedECLType(exprType, elementType);
        if (!isSameBasicType(exprType, promotedType))
            expr = createValue(no_implicitcast, LINK(promotedType), expr);
    }

    set->Release();
    return createBoolExpr(op, expr, normalized.getClear());
}

void HqlGram::checkCaseForDuplicates(HqlExprArray & exprs, attribute &err)
{
    TransformMutexBlock lock;
    unsigned max = exprs.ordinality();
    for (unsigned idx1 = 0; idx1 < max; idx1++)
    {
        IHqlExpression * e1 = exprs.item(idx1).queryChild(0)->queryBody();
        if (e1->queryTransformExtra())
        {
            StringBuffer s;
            s.append("Duplicate case entry: ");
            toECL(e1, s, false);
            reportWarning(WRN_DUPLICATECASE, err.pos, "%s", s.str());
        }
        else
            e1->setTransformExtraUnlinked(e1);
    }
}
        
/* Linkage: not affected */
ITypeInfo *HqlGram::promoteCaseToSameType(attribute &eTest, HqlExprArray & exprs, attribute &eElse)
{
    Owned<ITypeInfo> promotedTest = eTest.queryExpr()->getType();
    Owned<ITypeInfo> promotedResult = eElse.queryExpr()->getType();

    ForEachItemIn(idx, exprs)
    {
        IHqlExpression & cur = exprs.item(idx);
        promotedTest.setown(::getPromotedECLType(promotedTest, cur.queryChild(0)->queryType()));
        promotedResult.setown(::getPromotedECLType(promotedResult, cur.queryChild(1)->queryType()));
    }

    ForEachItemIn(idx2, exprs)
    {
        IHqlExpression & cur = exprs.item(idx2);
        IHqlExpression * cond = cur.queryChild(0);
        IHqlExpression * cast = cur.queryChild(1);

        checkCompatible(cond->queryType(), promotedTest, eTest);
        checkCompatible(cast->queryType(), promotedResult, eTest);

        cond = ensureExprType(cond, promotedTest);
        cast = ensureExprType(cast, promotedResult);

        IHqlExpression * map = createValue(no_mapto, LINK(promotedResult), cond, cast);
        exprs.replace(*map, idx2);
    }

    checkType(eTest, promotedTest);
    ensureType(eTest, promotedTest);
    checkType(eElse, promotedResult);
    ensureType(eElse, promotedResult);
    return promotedResult.getClear();
}

void HqlGram::checkReal(attribute &a1)
{
    ITypeInfo *t1 = a1.queryExprType();
    // MORE - do we need to put in a cast for the integer case?
    if (t1 && t1->getTypeCode() != type_real && t1->getTypeCode() != type_int)
    {
        Owned<ITypeInfo> realType = makeRealType(DEFAULT_REAL_SIZE);
        if (t1->getTypeCode() == type_decimal)
        {
            reportWarning(ERR_TYPEMISMATCH_REAL, a1.pos, "Decimal implicitly converted to a real");
        }
        else
        {
            StringBuffer msg("Type mismatch - Real value expected (");
            getFriendlyTypeStr(t1,msg).append(" was given)");
            reportError(ERR_TYPEMISMATCH_REAL, a1, "%s", msg.str());
        }

        OwnedHqlExpr value = a1.getExpr();
        a1.setExpr(ensureExprType(value, realType));
    }
}

void HqlGram::checkInteger(attribute &a1)
{
    ITypeInfo *t1 = a1.queryExprType();
    if (!t1 || !isIntegralType(t1))
    {
        if (t1 && isNumericType(t1))
        {
            OwnedHqlExpr value = a1.getExpr();
            a1.setExpr(ensureExprType(value, defaultIntegralType));
        }
        else
        {
            StringBuffer s;
            reportError(ERR_TYPEMISMATCH_INT, a1, "Type mismatch - Integer value expected (%s was given)", t1 ? getFriendlyTypeStr(t1, s).str() : "?");
            a1.release().setExpr(createConstant(I64C(0)));
        }
    }
}

void HqlGram::checkPositive(attribute &a1)
{
    IValue * value = a1.queryExpr()->queryValue();
    if (value && value->queryType()->isSigned())
    {
        //MORE: Recode to allow decimal and other types
        Owned<IValue> zero = value->queryType()->castFrom(0, (const char *)NULL);
        if (value->compare(zero) < 0)
            reportError(ERR_TYPEMISMATCH_INT, a1, "Type mismatch - the value must be positive");
    }   
}

bool HqlGram::checkString(attribute &a1)
{
    ITypeInfo *t1 = a1.queryExprType();
    if (t1)
    {
        t1 = t1->queryPromotedType();
        if (!isSimpleStringType(t1))
        {
            if (isStringType(t1))
            {
                ensureString(a1);
            }
            else
            {
                StringBuffer msg("Type mismatch - String value expected (");
                getFriendlyTypeStr(t1, msg).append(" was given)");
                reportError(ERR_TYPEMISMATCH_STRING, a1, "%s", msg.str());
                return false;
            }
        }
    }
    return true;
}


bool HqlGram::checkStringOrUnicode(attribute & exprAttr)
{
    ITypeInfo * type = exprAttr.queryExprType()->queryPromotedType();
    switch (type->getTypeCode())
    {
    case type_string:
    case type_varstring:
    case type_data:
    case type_qstring:
    case type_unicode:
    case type_varunicode:
    case type_utf8:
        return true;
    }

    StringBuffer msg("Type mismatch - String or unicode value expected (");
    getFriendlyTypeStr(type, msg).append(" was given)");
    reportError(ERR_TYPEMISMATCH_STRING, exprAttr, "%s", msg.str());
    return false;
}

void HqlGram::checkIntegerOrString(attribute & a1)
{
    ITypeInfo *t1 = a1.queryExprType();
    if (t1 && !isIntegralType(t1) && !isStringType(t1))
    {
        StringBuffer msg("Type mismatch - Integer or string value expected (");
        getFriendlyTypeStr(t1, msg).append(" was given)");
        reportError(ERR_TYPEMISMATCH_INTSTRING, a1, "%s", msg.str());
    }
}

void HqlGram::checkNumeric(attribute &a1)
{
    ITypeInfo *t1 = a1.queryExprType();
    if (!t1 || (!isNumericType(t1) && !isAnyType(t1)))
    {
        StringBuffer msg("Type mismatch - numeric expression expected");
        if (t1)
        {
            msg.append("(");
            getFriendlyTypeStr(t1, msg).append(" was given)");
        }
        reportError(ERR_EXPECTED_NUMERIC, a1, "%s", msg.str());
        a1.release().setExpr(getSizetConstant(0));
    }
}

ITypeInfo *HqlGram::checkNumericGetType(attribute &a1)
{
    ITypeInfo *t1 = a1.queryExpr()->getType();
    if (!t1)
    {
        reportError(ERR_TYPEMISMATCH_INTREAL, a1, "Type mismatch - unknown type! Integer or real value expected");
        t1 = makeIntType(DEFAULT_INT_SIZE, true);
    }
    if (!isNumericType(t1))
    {
        StringBuffer msg("Type mismatch - Integer or real value expected (");
        getFriendlyTypeStr(t1, msg).append(" was given)");
        reportError(ERR_TYPEMISMATCH_INTREAL, a1, "%s", msg.str());
        t1->Release();
        t1 = makeIntType(DEFAULT_INT_SIZE, true);
    }
    return t1;
}


IHqlExpression * HqlGram::createDatasetFromList(attribute & listAttr, attribute & recordAttr)
{
    OwnedHqlExpr list = listAttr.getExpr();
    OwnedHqlExpr record = recordAttr.getExpr();
    if (list->getOperator() == no_comma)
    {
        reportErrorUnexpectedX(listAttr, dynamicAtom);
        list.set(list->queryChild(0));  // should really complain about dynamic
    }

    if ((list->getOperator() == no_list) && (list->numChildren() == 0))
    {
        OwnedHqlExpr list = createValue(no_null);
        OwnedHqlExpr table = createDataset(no_temptable, LINK(list), record.getClear());
        return convertTempTableToInlineTable(errorHandler, listAttr.pos, table);
        return createDataset(no_null, LINK(record));
    }

    IHqlExpression * listRecord = list->queryRecord();
    if (listRecord)
    {
        if (list->getOperator() != no_list)
        {
            reportError(ERR_EXPECTED, listAttr, "Expected a list of rows");
            return createDataset(no_null, LINK(record));
        }

        HqlExprArray args;
        unwindChildren(args, list);

        if (args.item(0).queryRecord() != record->queryRecord())
            reportError(ERR_TYPEMISMATCH_RECORD, recordAttr, "Datarow must match the record definition, try using ROW()");
        
        OwnedHqlExpr combined;
        ForEachItemIn(i, args)
        {
            OwnedHqlExpr ds = ::ensureDataset(&args.item(i));
            if (combined)
                combined.setown(createDataset(no_addfiles, LINK(combined), LINK(ds)));
            else
                combined.set(ds);
        }
        return combined.getClear();
    }

    ITypeInfo * listType = list->queryType();
    assertex(listType);
    ITypeInfo * childType = listType->queryChildType();
    IHqlExpression * field = queryOnlyField(record);
    if (!field)
        reportError(ERR_EXPECT_SINGLE_FIELD, recordAttr, "Expected a single field in the dataset parameter");
    else if (childType && !field->queryType()->assignableFrom(childType))
        reportError(ERR_RECORD_NOT_MATCH_SET, recordAttr, "The field in the record does not match the type of the set elements");
    
    OwnedHqlExpr table = createDataset(no_temptable, LINK(list), record.getClear());
    return convertTempTableToInlineTable(errorHandler, listAttr.pos, table);
}


ITypeInfo *HqlGram::checkPromoteNumeric(attribute &a1)
{
    applyDefaultPromotions(a1);
    return checkNumericGetType(a1);
}


void expandRecord(HqlExprArray & fields, IHqlExpression * selector, IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                expandRecord(fields, selector, expr->queryChild(i));
            break;
        }
    case no_field:
        {
            OwnedHqlExpr subSelector = createSelectExpr(LINK(selector), LINK(expr));
            if (expr->queryRecord() && !expr->isDataset())
                expandRecord(fields, subSelector, expr->queryRecord());
            else
            {
                if (fields.find(*subSelector) == NotFound)
                    fields.append(*subSelector.getClear());
            }
            break;
        }
    case no_ifblock:
        expandRecord(fields, selector, expr->queryChild(1));
        break;
    }
}

bool HqlGram::expandWholeAndExcept(IHqlExpression * dataset, const attribute & errpos, HqlExprArray & parms)
{
    HqlExprArray results;

    //Process record first because it needs to be added before the EXCEPT.
    bool expandPending = false;
    bool hadExcept = false;
    bool hadField = false;
    ForEachItemIn(idx1, parms)
    {
        IHqlExpression &e = parms.item(idx1);
        if (e.isAttribute())
        {
            if (e.queryName() == recordAtom)
            {
                expandPending = true;
            }
            else if (e.queryName() == exceptAtom)
            {
                if (!hadField)
                    expandPending = true;
                hadExcept = true;
                break;
            }
        }
        else
        {
            if (e.queryType())
                hadField = true;
            results.append(OLINK(e));
        }
    }
    if (!expandPending && !hadExcept)
        return false;

    if (expandPending)
    {
        if (dataset)
            expandRecord(results, dataset->queryNormalizedSelector(), dataset->queryRecord());
        else
            reportError(ERR_NO_WHOLE_RECORD, errpos, "WHOLE RECORD is not valid here");
    }

    hadExcept = false;
    ForEachItemIn(idx2, parms)
    {
        IHqlExpression &e = parms.item(idx2);
        OwnedHqlExpr except;
        if (e.isAttribute())
        {
            _ATOM attr = e.queryName();
            if (attr == exceptAtom)
            {
                hadExcept = true;
                except.set(e.queryChild(0));
            }
            else if (attr != recordAtom)
                results.append(OLINK(e));
        }
        else 
        {
            if (hadExcept)
            {
                if (e.queryType())
                    except.set(&e);
                else
                    results.append(OLINK(e));
            }
        }

        if (except)
        {
            IHqlExpression * search = except->queryNormalizedSelector();
            unsigned match = results.find(*search);
            if (match != NotFound)
                results.remove(match);
            else
            {
                StringBuffer s;
                toECL(search, s, false);
                reportError(ERR_EXCEPT_NOT_FOUND, errpos, "EXCEPT: %s not found in the incoming record", s.str());
            }
        }
    }
    parms.swapWith(results);
    return true;
}

void HqlGram::expandWholeAndExcept(IHqlExpression * dataset, attribute & a)
{
    OwnedHqlExpr list = a.getExpr();

    HqlExprArray parms;
    if (list)
        list->unwindList(parms, no_comma);

    if (!expandWholeAndExcept(dataset, a, parms))
    {
        a.setExpr(list.getClear());
        return;
    }

    a.setExpr(createComma(parms));
}


void HqlGram::expandSortedAsList(HqlExprArray & args)
{
    LinkedHqlExpr sorted = queryProperty(sortedAtom, args);
    if (!sorted)
        return;

    args.zap(*sorted);
    unwindChildren(args, sorted);
}


IHqlExpression * HqlGram::createAssert(attribute & condAttr, attribute * msgAttr, attribute & flagsAttr)
{
    if (queryPropertyInList(constAtom, flagsAttr.queryExpr()))
    {
        checkConstant(condAttr);
        if (msgAttr)
            checkConstant(*msgAttr);
    }

    OwnedHqlExpr cond = condAttr.getExpr();

    HqlExprArray args;
    args.append(*LINK(cond));
    if (msgAttr)
    {
        normalizeExpression(*msgAttr, type_string, false);
        args.append(*msgAttr->getExpr());
    }
    else
    {
        args.append(*createDefaultAssertMessage(cond));
        args.append(*createAttribute(_default_Atom));
    }
    flagsAttr.unwindCommaList(args);

    args.append(*condAttr.pos.createLocationAttr());

    return createValue(no_assert, makeVoidType(), args);
}


IHqlExpression * HqlGram::processSortList(const attribute & errpos, node_operator op, IHqlExpression * dataset, HqlExprArray & items, OwnedHqlExpr * joinedClause, OwnedHqlExpr * attributes)
{
    expandWholeAndExcept(dataset, errpos, items);

    if (items.ordinality() == 0)
    {
        if (op == no_list)
            return createValue(no_sortlist, makeSortListType(NULL));
        return NULL;
    }

    bool expandRows = false;
    switch (op)
    {
    case no_hash:
        expandRows = true;
        break;
    }

    ForEachItemInRev(idx, items)
    {
        IHqlExpression &e = items.item(idx);
        node_operator eop = e.getOperator();
        if (e.isAttribute())
        {
            _ATOM attr = e.queryName();
            bool ok = false;
            if (attributes)
            {
                if (attr == hintAtom) ok = true;

                switch (op)
                {
                case no_aggregate:
                    if (attr == keyedAtom) ok = true;
                    if (attr == localAtom) ok = true;
                    if (attr == fewAtom) ok = true;
                    if (attr == manyAtom) ok = true;
                    if (attr == sortedAtom) ok = true;
                    if (attr == unsortedAtom) ok = true;
                    if (attr == skewAtom) ok = true;
                    if (attr == thresholdAtom) ok = true;
                    break;
                case no_usertable:
                    if (attr == keyedAtom) ok = true;
                    if (attr == prefetchAtom) ok = true;
                    if (attr == mergeAtom) ok = true;
                    //fall through
                case no_group:
                    if (attr == allAtom) ok = true;
                    if (attr == localAtom) ok = true;
                    if (attr == fewAtom) ok = true;
                    if (attr == manyAtom) ok = true;
                    if (attr == sortedAtom) ok = true;
                    if (attr == unsortedAtom) ok = true;
                    if (attr == skewAtom) ok = true;
                    if (attr == thresholdAtom) ok = true;
                    break;
                case no_topn:
                    if (attr == bestAtom) ok = true;
                    //fall through
                case no_sort:
                    if (attr == localAtom) ok = true;
                    if (attr == skewAtom) ok = true;
                    if (attr == thresholdAtom) ok = true;
                    if (attr == manyAtom) ok = true;
                    if (attr == fewAtom) ok = true;
                    if (attr == assertAtom) ok = true;
                    if (attr == stableAtom) ok = true;
                    if (attr == unstableAtom) ok = true;
                    break;
                case no_nwaymerge:
                    if (attr == dedupAtom) ok = true;
                    break;
                case no_mergejoin:
                    if (attr == dedupAtom) ok = true;
                    if (attr == assertAtom) ok = true;
                    //fall through
                case no_nwayjoin:
                    if (attr == mofnAtom) ok = true;
                    if (attr == leftonlyAtom) ok = true;
                    if (attr == leftouterAtom) ok = true;
                    if (attr == assertAtom) ok = true;
                    if (attr == skewAtom) ok = true;
                    if (attr == internalFlagsAtom) ok = true;
                    //if (attr == sortedAtom) ok = true;        - already converted
                    break;
                }
            }
            if (!ok)
            {
                StringBuffer msg;
                msg.append(attr).append(" is not valid here");
                reportError(ERR_ILL_HERE, errpos, "%s", msg.str());
            }
            else
                attributes->setown(createComma(LINK(&e), attributes->getClear()));
            items.remove(idx);
        }
        else if (eop == no_joined)
        {
            if ((op != no_sort && op != no_topn) || !joinedClause)
                reportError(ERR_JOINED_ILL_HERE, errpos, "JOINED is not valid here");
            else if (*joinedClause)
            {
                reportError(ERR_JOINED_TOOMANY, errpos, "Too many joined clauses: only 1 can be used");
            }
            else
                joinedClause->set(&e);
            items.remove(idx);
        }
        else if (eop == no_constant)
        {
            if ((op != no_hash) && (op != no_hash32) && (op != no_hash64) && (op != no_crc) && (op != no_hashmd5) && (op != no_list))
                reportWarning(ERR_CONSTANT_DAFT, errpos.pos, "Constant group/sort clauses make no sense");
        }
        else if (!containsAnyDataset(&e))
        {
            if ((op != no_hash) && (op != no_hash32) && (op != no_hash64) && (op != no_crc) && (op != no_hashmd5) && (op != no_list))
                reportWarning(WRN_SORT_INVARIANT, errpos.pos, "Sort/Group element is not related to the dataset");
        }
        else if (e.isDatarow() && expandRows)
        {
            //Expanding the row selectors at this point generates better code if the hash is done inside a compound read activity
            LinkedHqlExpr row = &e;
            RecordSelectIterator iter(e.queryRecord(), row);
            items.remove(idx);
            unsigned i=0;
            ForEach(iter)
            {
                items.add(*iter.get(), idx+i);
                i++;
            }
        }
    }
    if (items.ordinality())
        return createValue(no_sortlist, makeSortListType(NULL), items);
    if (op == no_list)
        return createValue(no_sortlist, makeSortListType(NULL), items);
    return NULL;
}

IHqlExpression * HqlGram::createDistributeCond(IHqlExpression * leftDs, IHqlExpression * rightDs, const attribute & err, const attribute & seqAttr)
{
    IHqlSimpleScope * leftScope = leftDs->queryRecord()->querySimpleScope();
    IHqlExpression * rightRecord = rightDs->queryRecord();

    OwnedHqlExpr left = createSelector(no_left, leftDs, seqAttr.queryExpr());
    OwnedHqlExpr right = createSelector(no_right, rightDs, seqAttr.queryExpr());
    IHqlExpression * cond = NULL;
    unsigned numFields = rightRecord->numChildren()-numPayloadFields(rightDs);
    for (unsigned i =0; i < numFields; i++)
    {
        IHqlExpression * rightField = rightRecord->queryChild(i);
        if (rightField->getOperator() == no_field)
        {
            IHqlExpression * leftField = leftScope->lookupSymbol(rightField->queryName());
            if (!leftField)
            {
                reportError(ERR_FIELD_NOT_FOUND, err, "Could not find a field %s in the dataset", rightField->queryName()->str());
                leftField = LINK(rightField);
            }
            IHqlExpression * test = createBoolExpr(no_eq, createSelectExpr(LINK(left), leftField), createSelectExpr(LINK(right), LINK(rightField)));
            cond = extendConditionOwn(no_and, cond, test);
        }
    }
    return cond;
}

IHqlExpression * HqlGram::createLoopCondition(IHqlExpression * leftDs, IHqlExpression * arg1, IHqlExpression * arg2, IHqlExpression * seq)
{
    IHqlExpression * count = NULL;
    IHqlExpression * filter = NULL;
    IHqlExpression * loopCond = NULL;

    if (arg1->queryType() == boolType)
    {
        if (arg2)
        {
            filter = arg1;
            loopCond = arg2;
        }
        else
        {
            //if refers to fields in LEFT then must be a row filter
            OwnedHqlExpr left = createSelector(no_left, leftDs, seq);
            if (containsSelector(arg1, left))
            {
                filter = arg1;
            }
            else
            {
                loopCond = arg1;
            }
        }
    }
    else
    {
        count = arg1;
        if (arg2)
            filter = arg2;
    }

    if (!count) count = createAttribute(_omitted_Atom);
    if (!filter) filter = createAttribute(_omitted_Atom);
    if (!loopCond) loopCond= createAttribute(_omitted_Atom);

    return createComma(count, filter, loopCond);
}

void HqlGram::reportError(int errNo, const attribute& a, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    reportErrorVa(errNo, a.pos, format, args);
    va_end(args);
}

void HqlGram::reportError(int errNo, const ECLlocation & pos, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    reportErrorVa(errNo, pos, format, args);
    va_end(args);
}

void HqlGram::reportErrorUnexpectedX(const attribute& errpos, _ATOM unexpected)
{
    reportError(ERR_UNEXPECTED_ATTRX, errpos, "Unexpected attribute %s", unexpected->str());
}

bool HqlGram::okToReportError(const ECLlocation & pos)
{
    if (errorHandler && !errorDisabled)
    {
        if (getMaxErrorsAllowed()>0 && errorHandler->errCount() >= getMaxErrorsAllowed())
        {
            errorHandler->reportError(ERR_ERROR_TOOMANY,"Too many errors; parsing aborted",pos.sourcePath->str(),pos.lineno,pos.column,pos.position);
            abortParsing();
            return false;
        }
        return true;
    }
    return false;
}

void HqlGram::doReportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
{
    if (associateWarnings)
        pendingWarnings.append(*createECLWarning(warnNo, msg, filename, lineno, column, pos));
    else
        errorHandler->reportWarning(warnNo, msg, filename, lineno, column, pos);
}

void HqlGram::reportMacroExpansionPosition(int errNo, HqlLex * lexer, bool isError)
{
    HqlLex * macro = lexer->getMacroLex();
    if (!macro)
        return; 
    reportMacroExpansionPosition(errNo, macro, isError);
    StringBuffer s;
    s.appendf("While expanding macro %s", macro->getMacroName());
    if (isError)
        errorHandler->reportError(errNo, s.str(), lexer->querySourcePath()->str(), lexer->get_yyLineNo(), lexer->get_yyColumn(), 0);
    else
        doReportWarning(errNo, s.str(), lexer->querySourcePath()->str(), lexer->get_yyLineNo(), lexer->get_yyColumn(), 0);
}
    
void HqlGram::reportErrorVa(int errNo, const ECLlocation & pos, const char* format, va_list args)
{
    if (okToReportError(pos))
    {
        StringBuffer msg;
        msg.valist_appendf(format, args);
        errorHandler->reportError(errNo, msg.str(), pos.sourcePath->str(), pos.lineno, pos.column, pos.position);
        reportMacroExpansionPosition(errNo, lexObject, true);
    }
}

void HqlGram::reportError(int errNo, const char *msg, int lineno, int column, int position)
{
    if (errorHandler && !errorDisabled)
    {
        if (getMaxErrorsAllowed()>0 && errorHandler->errCount() >= getMaxErrorsAllowed())
        {
            errorHandler->reportError(ERR_ERROR_TOOMANY,"Too many errors; parsing aborted",querySourcePathText(),lineno,column,position);
            abortParsing();
        }
        else
        {
            errorHandler->reportError(errNo, msg, lexObject->queryActualSourcePath()->str(), lineno, column, position);
            reportMacroExpansionPosition(errNo, lexObject, true);
        }
    }
}

void HqlGram::reportWarning(int warnNo, const ECLlocation & pos, const char* format, ...)
{
    if (errorHandler && !errorDisabled)
    {
        StringBuffer msg;
        va_list args;
        va_start(args, format);
        msg.valist_appendf(format, args);
        va_end(args);

        doReportWarning(warnNo, msg.str(), pos.sourcePath->str(), pos.lineno, pos.column, pos.position);
        reportMacroExpansionPosition(warnNo, lexObject, false);
    }
}

void HqlGram::reportWarningVa(int warnNo, const attribute& a, const char* format, va_list args)
{
    const ECLlocation & pos = a.pos;
    if (errorHandler && !errorDisabled)
    {
        StringBuffer msg;
        msg.valist_appendf(format, args);
        doReportWarning(warnNo, msg.str(), pos.sourcePath->str(), pos.lineno, pos.column, pos.position);
        reportMacroExpansionPosition(warnNo, lexObject, false);
    }
}

void HqlGram::reportWarning(int warnNo, const char *msg, int lineno, int column)
{
    if (errorHandler && !errorDisabled)
    {
        doReportWarning(warnNo, msg, querySourcePathText(), lineno, column, 0);
        reportMacroExpansionPosition(warnNo, lexObject, false);
    }
}


//interface IErrorReceiver
void HqlGram::reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
{
    Owned<ISourcePath> sourcePath = createSourcePath(filename);
    ECLlocation loc(lineno, column, pos, sourcePath);
    if (okToReportError(loc))
        errorHandler->reportError(errNo, msg, filename, lineno, column, pos);
}

void HqlGram::report(IECLError* error) { expandReportError(this, error); }

void HqlGram::reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
{
    if (errorHandler && !errorDisabled)
        doReportWarning(warnNo, msg, filename, lineno, column, pos);
}

size32_t HqlGram::errCount()
{
    return errorHandler ? errorHandler->errCount() : 0;
}

size32_t HqlGram::warnCount()
{
    return errorHandler ? errorHandler->warnCount() : 0;
}

IHqlExpression * HqlGram::createLocationAttr(const attribute & a)
{
    return a.pos.createLocationAttr();
}

void HqlGram::addResult(IHqlExpression *_query, const attribute& errpos)
{
    if (!_query)
        return;

    OwnedHqlExpr query = createLocationAnnotation(_query, errpos.pos);
    if (query->getOperator() == no_loadxml)
        return;

    parseResults.append(*query.getClear());
}

void HqlGram::checkFormals(_ATOM name, HqlExprArray& parms, HqlExprArray& defaults, attribute& object)
{
    node_operator op = object.queryExpr()->getOperator();
    bool isMacro = (op == no_macro);

    for (unsigned idx = 0; idx < parms.length(); idx++) // Can NOT use ForEachItemIn.
    {
        IHqlExpression* parm = (IHqlExpression*)&parms.item(idx);

        if (isMacro && !parm->hasProperty(noTypeAtom))
            reportError(ERR_MACRO_NOPARAMTYPE, object, "Type is not allowed for macro: parameter %d of %s", idx+1, name->str());

        //
        // check default value
        if (isMacro)
        {
            IHqlExpression* def = &defaults.item(idx);
            
            if ((def->getOperator() != no_omitted) && !def->isConstant()) 
            {
                if (def->queryType()->getTypeCode() != type_string)                 
                    reportError(ERR_MACRO_CONSTDEFPARAM, object, "Default parameter to macro must be constant string: parameter %d of %s",idx+1,name->str());
            }   
        } 
    }   
}

void HqlGram::addParameter(const attribute & errpos, _ATOM name, ITypeInfo* type, IHqlExpression* defValue)
{
    HqlExprArray attrs;
    endList(attrs);
    if (hasProperty(fieldsAtom, attrs))
    {
        type = makeSortListType(type);
        if (!defValue && hasProperty(optAtom, attrs))
            defValue = createValue(no_sortlist, LINK(type));
    }
    addActiveParameterOwn(errpos, createParameter(name, nextParameterIndex(), type, attrs), defValue);
}

void HqlGram::addFunctionParameter(const attribute & errpos, _ATOM name, ITypeInfo* type, IHqlExpression* defValue)
{
    ActiveScopeInfo & activeScope = defineScopes.tos();
    OwnedHqlExpr formals = activeScope.createFormals(false);
    OwnedHqlExpr defaults = activeScope.createDefaults();
    //MORE default values for these parameters are currently lost...

    leaveScope(errpos);     // Restores previous active parameters

    HqlExprArray attrs;
    endList(attrs);
    Owned<ITypeInfo> funcType = makeFunctionType(type, LINK(formals), defaults.getClear());
    addActiveParameterOwn(errpos, createParameter(name, nextParameterIndex(), LINK(funcType), attrs), defValue);
}

void HqlGram::addFunctionProtoParameter(const attribute & errpos, _ATOM name, IHqlExpression * donor, IHqlExpression* defValue)
{
    assertex(donor->isFunction());

    HqlExprArray attrs;
    endList(attrs);
    addActiveParameterOwn(errpos, createParameter(name, nextParameterIndex(), donor->getType(), attrs), defValue);
    donor->Release();
}

IHqlScope * HqlGram::queryTemplateContext()
{
    ForEachItemIn(i, defineScopes)
    {
        IHqlScope * scope = defineScopes.item(i).templateAttrContext;
        if (scope)
            return scope;
    }
    return false;
}


IHqlExpression *HqlGram::bindParameters(const attribute & errpos, IHqlExpression * function, HqlExprArray & actuals)
{
    assertex(function->isFunction());

    // something bad happened
    if (checkParameters(function, actuals, errpos)) 
    {
        try
        {
            const bool expandCallsWhenBound = lookupCtx.queryExpandCallsWhenBound();
            if (function->getOperator() != no_funcdef)
                return createBoundFunction(this, function, actuals, lookupCtx.functionCache, expandCallsWhenBound);

            IHqlExpression * body = function->queryChild(0);
            if (body->getOperator() == no_template_context)
            {
                if (requireLateBind(function, actuals))
                {
                    IHqlExpression * ret = NULL;
                    if (!expandCallsWhenBound)
                    {
                        HqlExprArray args;
                        args.append(*LINK(body));
                        unwindChildren(args, function, 1);
                        OwnedHqlExpr newFunction = createFunctionDefinition(function->queryName(), args);
                        OwnedHqlExpr boundExpr = createBoundFunction(this, newFunction, actuals, lookupCtx.functionCache, expandCallsWhenBound);
                        
                        // get rid of the wrapper
                        //assertex(boundExpr->getOperator()==no_template_context);
                        ret = LINK(boundExpr);//->queryChild(0));
                    }
                    else
                    {
                        OwnedHqlExpr boundExpr = createBoundFunction(this, function, actuals, lookupCtx.functionCache, expandCallsWhenBound);
                        
                        // get rid of the wrapper
                        assertex(boundExpr->getOperator()==no_template_context);
                        ret = LINK(boundExpr->queryChild(0));
                    }

                    IHqlExpression * formals = function->queryChild(1);
                    // bind fields
                    ForEachItemIn(idx, actuals)
                    {
                        IHqlExpression *formal = formals->queryChild(idx);

                        if (isAbstractDataset(formal))
                        {
                            IHqlExpression *actual = &actuals.item(idx);
                            ret = bindDatasetParameter(ret, formal, actual, errpos);
                        }
                    }
                    
                    return ret;
                }           
                else
                    return bindTemplateFunctionParameters(function, actuals, errpos);
            }
            else
            {
                bool expandCall = insideTemplateFunction() ? false : expandCallsWhenBound;
                // do the actual binding
                return createBoundFunction(this, function, actuals, lookupCtx.functionCache, expandCall);
            }
        }
        catch (IException * e)
        {
            if (e->errorCode() == ERR_ABORT_PARSING)
                throw;
            StringBuffer msg;
            e->errorMessage(msg);
            reportError(e->errorCode(), errpos, "%s", msg.str());
            e->Release();
        }
    }

    //Stop parameter mismatches causing failures/seh later because a dataset is expected to be returned.
    if (function->isDataset())
    {
        IHqlExpression * record = function->queryRecord();
        if (!record)
            record = queryNullRecord();
        return createDataset(no_null, LINK(record));
    }
    return createNullExpr(stripFunctionType(function->queryType()));
}


IHqlExpression *HqlGram::bindParameters(const attribute & errpos, IHqlExpression * func, IHqlExpression *parms)
{
    HqlExprArray actuals;
    if (parms)
        parms->unwindList(actuals, no_comma);

    return bindParameters(errpos, func, actuals);
}


IHqlExpression *HqlGram::bindParameters(attribute &a, IHqlExpression *parms)
{
    HqlExprArray actuals;
    if (parms)
    {
        parms->unwindList(actuals, no_comma);
        parms->Release();
    }

    OwnedHqlExpr origFunc = a.getExpr();
    return bindParameters(a, origFunc, actuals);
}


bool areFunctionsCompatible(IHqlExpression * arg1, IHqlExpression * arg2)
{
    ITypeInfo * type1 = arg1->queryType();
    ITypeInfo * type2 = arg2->queryType();
    if (!arg1->isFunction())
    {
        if (!arg2->isFunction())
            return (type1 == type2);
        return false;
    }
    if (!arg2->isFunction())
        return false;

    ITypeInfo * returnType1 = type1->queryChildType();
    ITypeInfo * returnType2 = type2->queryChildType();
    if (arg1->isDataset())
    {
        if (!returnType1->assignableFrom(returnType2->queryPromotedType()))
            return false;
    }
    else if (returnType1 != returnType2)
        return false;

    IHqlExpression * formals1 = queryFunctionParameters(type1);
    IHqlExpression * formals2 = queryFunctionParameters(type2);
    unsigned max = formals1->numChildren();
    if (max != formals2->numChildren())
        return false;
    for (unsigned i=0; i < max; i++)
    {
        if (!areFunctionsCompatible(formals1->queryChild(i), formals2->queryChild(i)))
            return false;
    }
    return true;
}


FunctionCallInfo::FunctionCallInfo(IHqlExpression * _funcdef) 
: funcdef(_funcdef) 
{ 
    hadNamed = false;
    hasActiveTopDataset = false; 
    numFormals = 0;
    if (funcdef)
    {
        IHqlExpression * formals = queryFunctionParameters(funcdef);
        numFormals = formals->numChildren();
        //Don't allow implicit hidden parameters to be specified
        while (numFormals && formals->queryChild(numFormals-1)->hasProperty(_hidden_Atom))
            numFormals--;
    }
}

IHqlExpression * FunctionCallInfo::getFinalActuals()
{
    assertex(!hasActiveTopDataset);
    flushPendingComponents();
    if (actuals.ordinality() == 1)
    {
        if (actuals.item(0).getOperator() == no_omitted)
            actuals.pop();
    }

    return createComma(actuals);
}

void FunctionCallInfo::flushPendingComponents()
{
    if (pendingComponents.ordinality() != 0)
    {
        IHqlExpression * formals = queryFunctionParameters(funcdef);
        IHqlExpression * formal = formals->queryChild(actuals.ordinality());
        assertex(formal && formal->hasProperty(fieldsAtom));
        actuals.append(*createValue(no_sortlist, formal->getType(), pendingComponents));
        pendingComponents.kill();
    }
}

void FunctionCallInfo::fillWithOmitted(unsigned next)
{
    while (actuals.ordinality() < next)
        actuals.append(*createOmittedValue());
}

/**
 * This handles parameter type checking and defvalue.
 *
 * Return: true:  binding is needed
 *         false: binding is not needed (either not a func, or FATAL error happened). 
*/
IHqlExpression * HqlGram::checkParameter(const attribute * errpos, IHqlExpression * actual, IHqlExpression * formal, bool isDefault, IHqlExpression * funcdef)
{
    if (actual->getOperator() == no_omitted)
        return LINK(actual);

    ITypeInfo * actualType = actual->queryType();
    _ATOM formalName = formal->queryName();
    if (actualType==NULL || 
        ((actualType->getTypeCode() == type_void) && !actual->isFunction()))
    {
        if (errpos)
            reportError(ERR_PARAM_NOTYPEORVOID, *errpos, "Non-typed or void type expression can not used: parameter %s", formalName->str());
        return NULL;
    }

    ITypeInfo * formalType = formal->queryType();
    if (formal->isFunction() || actual->isFunction())
    {
        if (formal->isFunction() && actual->isFunction())
        {
            if (!areFunctionsCompatible(formal, actual))
            {
                if (errpos)
                {
                    if (isGrouped(formal) != isGrouped(actual))
                        reportError(ERR_PARAM_TYPEMISMATCH, *errpos, "Grouping for functional parameter %s does not match", formalName->str());
                    else
                        reportError(ERR_PARAM_TYPEMISMATCH, *errpos, "Types for functional parameter %s does not match exactly", formalName->str());
                }
                return NULL;
            }
            return LINK(actual);
        }
        
        if (formal->isFunction())
        {
            if (errpos)
                reportError(ERR_PARAM_TYPEMISMATCH, *errpos, "Parameter %s requires an unbound functional argument", formalName->str());
            return NULL;
        }

        if (errpos)
            reportError(ERR_PARAM_TYPEMISMATCH, *errpos, "Cannot pass a functional definition to parameter %s", formalName->str());
        return NULL;
    }

    type_t actualTC = actualType->getTypeCode();
    type_t formalTC = formalType->getTypeCode();
    LinkedHqlExpr ret = actual;
    switch (formalTC)
    {
    case type_rule:
    case type_token:
        if (actualTC == type_pattern)
            ret.setown(createValue(no_pat_imptoken, makeTokenType(), LINK(actual)));
        else if ((actualTC != type_rule) && (actualTC != type_token))
        {
            if (errpos)
                reportError(ERR_PARAM_TYPEMISMATCH, *errpos, "Expression passed to pattern parameter");
            return NULL;
        }
        break;
    case type_pattern:
        if (actualTC != type_pattern)
        {
            if (errpos)
                reportError(ERR_TOKEN_IN_PATTERN, *errpos, "Only patterns can be passed to pattern parameters");
            return NULL;
        }
        break;
    case type_table:
    case type_groupedtable:
        {
            if (actual->isDatarow() && errpos)
                reportError(ERR_EXPECTED_DATASET, *errpos, "Expected a dataset instead of a row");
            IHqlExpression * record = formal->queryRecord();
            if (record->numChildren() == 0)
                break;
            // fallthrough
        }
    default:
        if (formalTC == type_row)
            formalType = formalType->queryChildType();
        if (!formalType->assignableFrom(actualType->queryPromotedType()))
        {
            if (errpos)
            {
                StringBuffer s,tp1,tp2;
                if (isDefault)
                {
                    s.appendf("Default for parameter %s type mismatch - expected %s, given %s",formalName->str(),
                            getFriendlyTypeStr(formal,tp1).str(),
                            getFriendlyTypeStr(actual,tp2).str());
                }
                else
                {
                    s.appendf("Parameter %s type mismatch - expected %s, given %s",formalName->str(),
                            getFriendlyTypeStr(formal,tp1).str(),
                            getFriendlyTypeStr(actual,tp2).str());
                }
                reportError(ERR_PARAM_TYPEMISMATCH, *errpos, "%s", s.str());
            }
            return NULL;
        }
    }

//  if (formal->hasProperty(constAtom) && funcdef && !isExternalFunction(funcdef))
    if (errpos && formal->hasProperty(assertConstAtom))
        ret.setown(checkConstant(*errpos, ret));

    if (formal->hasProperty(fieldAtom))
    {
        if (!isValidFieldReference(actual))
        {
            if (errpos)
                reportError(ERR_EXPECTED_FIELD, *errpos, "Expected a field reference to be passed to parameter %s", formalName->str());
            return NULL;
        }
        if (ret->getOperator() != no_indirect)
            ret.setown(createValue(no_indirect, ret->getType(), LINK(ret)));
    }
    else
    {
        if (isFieldSelectedFromRecord(ret))
                reportError(ERR_EXPECTED, *errpos, "Expression expected for parameter %s.  Fields from records can only be passed to field references", formalName->str());
    }

    if (formal->hasProperty(fieldsAtom))
    {
        if (!isValidFieldReference(actual) && actualTC != type_sortlist)
        {
            if (errpos)
                reportError(ERR_EXPECTED_FIELD, *errpos, "Expected a field reference to be passed to parameter %s", formalName->str());
            return NULL;
        }
    }

    return ret.getClear();
}

static unsigned findParameterByName(IHqlExpression * formals, _ATOM searchName)
{
    ForEachChild(i, formals)
    {
        if (formals->queryChild(i)->queryName() == searchName)
            return i;
    }
    return (unsigned)-1;
}

void HqlGram::checkActualTopScope(FunctionCallInfo & call, IHqlExpression * formal, IHqlExpression * actual)
{
    if (isAbstractDataset(formal))
    {
        leaveActualTopScope(call);
        if (!isOmitted(actual))
        {
            pushTopScope(actual);
            call.hasActiveTopDataset = true;
        }
    }
}

void HqlGram::leaveActualTopScope(FunctionCallInfo & call)
{
    if (call.hasActiveTopDataset)
    {
        popTopScope();
        call.hasActiveTopDataset = false;
    }
}

bool HqlGram::processParameter(FunctionCallInfo & call, _ATOM name, IHqlExpression * actualValue, const attribute& errpos)
{
    IHqlExpression * func = call.funcdef;
    if (!func)
    {
        reportError(ERR_TYPE_NOPARAMNEEDED, errpos, "Type does not require parameters");
        return false;
    }

    _ATOM funcName = func->queryName();
    IHqlExpression * formals = queryFunctionParameters(func);

    unsigned targetActual = call.actuals.ordinality();
    LinkedHqlExpr actual = actualValue;
    if (name)
    {
        call.flushPendingComponents();
        targetActual = findParameterByName(formals, name);
        if (targetActual == (unsigned)-1)
        {
            reportError(ERR_NAMED_PARAM_NOT_FOUND, errpos, "Could not find a parameter named %s", name->str());
            return false;
        }

        if (call.actuals.isItem(targetActual) && call.actuals.item(targetActual).getOperator() != no_omitted)
        {
            reportError(ERR_NAMED_ALREADY_HAS_VALUE, errpos, "Parameter %s already has a value supplied", name->str());
            return false;
        }
        call.hadNamed = true;
    }
    else
    {
        if (call.hadNamed)
        {
            reportError(ERR_NON_NAMED_AFTER_NAMED, errpos, "Named parameters cannot be followed by unnamed parameters");
            return false;
        }

        //opt parameters are skipped if the value passed is not compatible with the formal
        while (targetActual < call.numFormals)
        {
            IHqlExpression * formal = formals->queryChild(targetActual);
            if (!formal->hasProperty(optAtom))
            {
                //Non opt <?> may skip this argument if we have already had one that is compatible
                if (!formal->hasProperty(fieldsAtom))
                    break;
                if (call.pendingComponents.ordinality() == 0)
                    break;
            }
            //Just check - don't report any errors.
            OwnedHqlExpr checked = checkParameter(NULL, actual, formal, false, func);
            if (checked)
                break;
            targetActual++;
            call.flushPendingComponents();
        }
    }

    if (targetActual >= call.numFormals)
    {
        if (isOmitted(actual))
            return true;

        reportError(ERR_PARAM_TOOMANY, errpos, "Too many parameters passed to function %s (expected %d)", funcName->str(), call.numFormals);
        return false;
    }

    IHqlExpression * formal = formals->queryChild(targetActual);
    OwnedHqlExpr checked = checkParameter(&errpos, actual, formal, false, func);
    if (!checked)
        return false;

    call.fillWithOmitted(targetActual);

    //Process <??>.  Append to a pending list as many actual parameters as we can that are compatible, and later create a no_sortlist in its place.
    if (!name && formal->hasProperty(fieldsAtom) && (checked->queryType()->getTypeCode() != type_sortlist))
    {
        call.pendingComponents.append(*LINK(checked));
        return true;
    }

    if (call.actuals.isItem(targetActual))
        call.actuals.replace(*LINK(checked), targetActual);
    else
        call.actuals.append(*LINK(checked));

    if (!name)
        checkActualTopScope(call, formal, checked);

    return true;
}


bool HqlGram::checkParameters(IHqlExpression* func, HqlExprArray& actuals, const attribute& errpos)
{
    _ATOM funcName = func->queryName();
    if (!func->isFunction())
    {
        if (actuals.length())
            reportError(ERR_TYPE_NOPARAMNEEDED, errpos, "Type does not require parameters: %s", funcName->str());
        return false; 
    }

    IHqlExpression * formals = queryFunctionParameters(func);
    IHqlExpression * defaults = queryFunctionDefaults(func);
    ForEachChild(idx, formals)
    {
        IHqlExpression *formal = formals->queryChild(idx);
        if (!actuals.isItem(idx))
            actuals.append(*createOmittedValue());
        IHqlExpression *actual = &actuals.item(idx);

        if (actual->getOperator()==no_omitted)
        {
            IHqlExpression * defvalue = queryDefaultValue(defaults, idx);
            if (!defvalue)
            {
                _ATOM formalName = formal->queryName();
                reportError(ERR_PARAM_NODEFVALUE, errpos, "Omitted parameter %s has no default value", formalName->str());
                return false;
            }
//          actuals.replace(*LINK(defvalue), idx);
        }
    }
    
    return true;
}

void HqlGram::addActiveParameterOwn(const attribute & errpos, IHqlExpression * param, IHqlExpression * defaultValue)
{
    ActiveScopeInfo & activeScope = defineScopes.tos();
    activeScope.activeParameters.append(*param);
    activeScope.activeDefaults.append(*ensureNormalizedDefaultValue(defaultValue));

    if (defaultValue && !param->hasProperty(noTypeAtom))
    {
        OwnedHqlExpr newActual = checkParameter(&errpos, defaultValue, param, true, NULL);
    }
}

IHqlExpression * HqlGram::checkBuildIndexRecord(IHqlExpression *record, attribute & errpos)
{
    bool allConstant;
    IHqlExpression * newRecord = checkOutputRecord(record, errpos, allConstant, true);
    record->Release();
    return newRecord;

    // MORE - check that there is a file position, no constants, that kind of thing
    // MORE - check all fields are big_endian?
}


void HqlGram::checkBuildIndexFilenameFlags(IHqlExpression * dataset, attribute & flags)
{
    IHqlExpression * flagExpr = flags.queryExpr();
    HqlExprArray args;
    if (flagExpr)
        flagExpr->unwindList(args, no_comma);

    IHqlDataset * table = dataset->queryDataset()->queryRootTable();
    IHqlExpression * tableExpr = NULL;
    if (table)
       tableExpr = queryExpression(table);

}


IHqlExpression * HqlGram::createBuildFileFromTable(IHqlExpression * table, attribute & flagsAttr, IHqlExpression * filename, attribute & errpos)
{
    IHqlExpression * originAttr = table->queryProperty(_origin_Atom);
    IHqlExpression * ds = originAttr->queryChild(0);
    IHqlExpression * mode = table->queryChild(2);
    OwnedHqlExpr flags=flagsAttr.getExpr();
    if (!filename) filename = table->queryChild(0);

    HqlExprArray args;
    args.append(*LINK(ds));
    args.append(*LINK(filename));
    switch (mode->getOperator())
    {
    case no_csv:
        args.append(*createAttribute(csvAtom, LINK(mode->queryChild(0))));
        break;
    case no_xml:
        args.append(*createAttribute(xmlAtom, LINK(mode->queryChild(0))));
        break;
    }
    if (flags)
    {
        HqlExprArray expandedFlags;
        flags->unwindList(expandedFlags, no_comma);
        ForEachItemIn(i, expandedFlags)
        {
            IHqlExpression & cur = expandedFlags.item(i);
            _ATOM name = cur.queryName();
            if ((name == overwriteAtom) ||(name == backupAtom) || (name == namedAtom) || (name == updateAtom) || (name == expireAtom))
                args.append(OLINK(cur));
            else if (name == persistAtom)
                args.append(*createAttribute(persistAtom, LINK(ds)));       // preserve so changes in representation don't affect crc.
        }
    }
    return createValue(no_output, makeVoidType(), args);
}

IHqlExpression * HqlGram::createBuildIndexFromIndex(attribute & indexAttr, attribute & flagsAttr, IHqlExpression * filename, attribute & errpos)
{
    OwnedHqlExpr index = indexAttr.getExpr();
    loop
    {
        node_operator op = index->getOperator();
        if (op == no_compound)
            index.set(index->queryChild(1));
        else if (op == no_executewhen)
            index.set(index->queryChild(0));
        else
            break;
    }

    if (!isKey(index))
    {
        if (index->getOperator() == no_table && index->hasProperty(_origin_Atom))
            return createBuildFileFromTable(index, flagsAttr, filename, errpos);
        flagsAttr.release();
        reportError(ERR_EXPECTED_INDEX,indexAttr,"Expected an index as the first parameter");
        return createDataset(no_null, LINK(queryNullRecord()));
    }

    IHqlExpression *dataset = LINK(index->queryChild(0));
    IHqlExpression *record = index->queryChild(1);
    IHqlExpression *transform = NULL;
    if (index->getOperator() == no_keyindex)
    {
        if (!filename)
            filename = index->queryChild(2);
    }
    else
    {
        transform = index->queryChild(2);
        if (!filename)
            filename = index->queryChild(3);
    }

    //need to tag record scope in this case so it generates no_activetable as top selector
    OwnedHqlExpr distribution;

    checkBuildIndexFilenameFlags(dataset, flagsAttr);
    bool allConstant = true;
    bool someMissing = false;
    ForEachChild(idx, record)
    {
        IHqlExpression * field = record->queryChild(idx);
        if (field->isAttribute())
            continue;
        IHqlExpression * value = field->queryChild(0);
        if (!value)
            someMissing = true;
        else if (!value->isAttribute() && !value->isConstant())
            allConstant = false;
    }
    if (someMissing)
        reportError(ERR_KEYEDINDEXINVALID,indexAttr,"The index record contains fields with no mappings - cannot build an index on it");
    else if (allConstant)
        reportError(ERR_KEYEDINDEXINVALID,indexAttr,"The index record has no mappings from the dataset - cannot build an index on it");

    IHqlExpression * newRecord = LINK(record);
    if (!transform)
        newRecord = checkBuildIndexRecord(newRecord, errpos);
    IHqlExpression * select;
    if (transform)
        select = createDatasetF(no_newusertable, dataset, newRecord, LINK(transform), NULL); //createUniqueId(), NULL);
    else
        select = createDatasetF(no_selectfields, dataset, newRecord, NULL); //createUniqueId(), NULL);
    HqlExprArray args;
    args.append(*select);
    args.append(*LINK(filename));
    OwnedHqlExpr flags=flagsAttr.getExpr();
    if (flags)
    {
        HqlExprArray extra;
        flags->unwindList(extra, no_comma);
        ForEachItemIn(i, extra)
        {
            IHqlExpression & cur = extra.item(i);
            _ATOM name = cur.queryName();
            if (name == distributedAtom)
                distribution.setown(&cur);
            else if (name == persistAtom)
                args.append(*createAttribute(persistAtom, LINK(index)));        // preserve so changes in representation don't affect crc.
            else
                args.append(OLINK(cur));
        }
    }
    //Clone flags from the index that are required.
    ForEachChild(iflag, index)
    {
        IHqlExpression * cur = index->queryChild(iflag);
        if (cur->isAttribute())
        {
            _ATOM name = cur->queryName();
            if ((name == sort_AllAtom) || (name == sort_KeyedAtom) || (name == fixedAtom) || (name == compressedAtom) || (name == dedupAtom))
                args.append(*LINK(cur));
            else if (name == distributedAtom)
            {
                args.append(*createAttribute(noRootAtom));
                if (cur->queryChild(0))
                    distribution.setown(replaceSelector(cur, queryActiveTableSelector(), select));
                args.append(*createLocalAttribute());
            }
        }
    }
    IHqlExpression * payload = index->queryProperty(_payload_Atom);
    if (payload)
        args.append(*LINK(payload));
    if (distribution)
        args.append(*distribution.getClear());

    checkDistributer(flagsAttr, args);
    return createValue(no_buildindex, makeVoidType(), args);
}

bool HqlGram::doCheckValidFieldValue(const attribute &errpos, IHqlExpression *value, IHqlExpression * field)
{
    if (value->queryTransformExtra())
        return true;
    value->setTransformExtraUnlinked(value);
    switch (value->getOperator())
    {
    case NO_AGGREGATE:
        return true;
    case no_select:
        {
            do
            {
                value = value->queryChild(0);
            } while (value->getOperator() == no_select);
            if (value->getOperator() == no_selfref)
            {
                reportError(ERR_BAD_FIELD_ATTR, errpos, "SELF cannot be used to provide a value for field '%s'", field->queryName()->str());
                return false;
            }
            return true;
        }
    }

    ITypeInfo * type = value->queryType();
    if (!type || !type->isScalar())
        return true;
    ForEachChild(i, value)
    {
        if (!doCheckValidFieldValue(errpos, value->queryChild(i), field))
            return false;
    }
    return true;
}

bool HqlGram::checkValidFieldValue(const attribute &errpos, IHqlExpression *value, IHqlExpression *field)
{
    TransformMutexBlock lock;
    return doCheckValidFieldValue(errpos, value, field);
}
    

IHqlExpression * HqlGram::checkOutputRecord(IHqlExpression *record, const attribute & errpos, bool & allConstant, bool outerLevel)
{
    assertex(record->getOperator()==no_record||record->getOperator()==no_null);

    HqlExprArray children;
    unsigned numkids = record->numChildren();
    for (unsigned i = 0; i < numkids; i++)
    {
        IHqlExpression *field = record->queryChild(i);
        switch(field->getOperator())
        {
        case no_ifblock:
            {
                HqlExprArray args;
                args.append(*LINK(field->queryChild(0)));
                args.append(*checkOutputRecord(field->queryChild(1), errpos, allConstant, outerLevel));
                children.append(*cloneOrLink(field, args));
                break;
            }
        case no_record:
            children.append(*checkOutputRecord(field, errpos, allConstant, outerLevel));
            break;
        case no_field:
            {
                LinkedHqlExpr newField = field;
                IHqlExpression * child0 = field->queryChild(0);
                if (child0 && !child0->isAttribute())
                {
                    checkValidFieldValue(errpos, child0, field);
                    if (!child0->isConstant())
                        allConstant = false;
                }
                else if (field->isDatarow())
                {
                    IHqlExpression * oldRecord = field->queryRecord();
                    OwnedHqlExpr newRecord = checkOutputRecord(oldRecord, errpos, allConstant, outerLevel);

                    if (newRecord != oldRecord)
                    {
                        HqlExprArray args;
                        unwindChildren(args, field);
                        newField.setown(createField(field->queryName(), newRecord->getType(), args));
                    }
                }
                else
                {

                    _ATOM name = field->queryName();
                    reportError(ERR_REC_FIELDNODEFVALUE, errpos, REC_FLD_ERR_STR, name ? name->str() : "?");
                    allConstant = false;                                        // no point reporting this as well.

                    HqlExprArray args;
                    args.append(*createNullExpr(field->queryType()));
                    newField.setown(field->clone(args));
                }

                children.append(*newField.getClear());
                break;
            }
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            children.append(*LINK(field));
            break;
        default:
            PrintLogExprTree(field, "This is not a field! :");
            assertex(field->getOperator()==no_field);
            break;
        }
    }
    return cloneOrLink(record, children);
}


//Add the CRC of the original expression to as an argument to the update attribute so it gets preserved throughout the transformations.
void HqlGram::processUpdateAttr(attribute & attr)
{
    OwnedHqlExpr expr = attr.getExpr();
    HqlExprArray args;
    unwindChildren(args, expr);

    if (expr->queryProperty(updateAtom))
    {
        removeProperty(args, updateAtom);
        args.append(*createAttribute(updateAtom, createConstant((__int64)getExpressionCRC(expr))));
    }
    else
        args.append(*createAttribute(updateAtom, createConstant((__int64)getExpressionCRC(expr)), createAttribute(alwaysAtom)));
    expr.setown(expr->clone(args));

    attr.setExpr(expr.getClear());
}

void HqlGram::checkOutputRecord(attribute & errpos, bool outerLevel)
{
    OwnedHqlExpr record = errpos.getExpr();
    bool allConstant = true;
    errpos.setExpr(checkOutputRecord(record, errpos, allConstant, outerLevel));
    if (allConstant && (record->getOperator() != no_null) && (record->numChildren() != 0))
        reportWarning(WRN_OUTPUT_ALL_CONSTANT,errpos.pos,"All values for OUTPUT are constant - is this the intention?");
}

void HqlGram::checkSoapRecord(attribute & errpos)
{
    IHqlExpression * record = errpos.queryExpr();
    bool allConstant = true;
    OwnedHqlExpr mapped = checkOutputRecord(record, errpos, allConstant, true);
}


IHqlExpression * HqlGram::checkIndexRecord(IHqlExpression * record, const attribute & errpos)
{
    unsigned numFields = record->numChildren();
    if (numFields)
    {
        // if not, implies some error (already reported)
        if (numFields == 1)
            reportError(ERR_INDEX_COMPONENTS, errpos, "Record for index should have at least one component and a fileposition");
        else
        {
            IHqlExpression * lastField = record->queryChild(numFields-1);
            ITypeInfo * fileposType = lastField->queryType();
            if (!isIntegralType(fileposType))
                reportError(ERR_INDEX_FILEPOS_EXPECTED_LAST, errpos, "Expected last field to be an integral fileposition field");
//          else if (fileposType->getSize() != 8)
//              reportWarning(ERR_INDEX_FILEPOS_UNEXPECTED_SIZE, errpos.pos, "Expected fileposition field to be 8 bytes");
        }
    }
    return LINK(record);
}


void HqlGram::reportUnsupportedFieldType(ITypeInfo * type, const attribute & errpos)
{
    StringBuffer s;
    getFriendlyTypeStr(type, s);
    reportError(ERR_INDEX_BADTYPE, errpos, "Fields of type %s are not currently supported", s.str());
}

void HqlGram::reportIndexFieldType(IHqlExpression * expr, bool isKeyed, const attribute & errpos)
{
    StringBuffer s;
    getFriendlyTypeStr(expr, s);
    if (isKeyed)
        reportError(ERR_INDEX_BADTYPE, errpos, "INDEX does not support keyed fields of type %s", s.str());
    else
        reportError(ERR_INDEX_BADTYPE, errpos, "INDEX does not support fields of type %s", s.str());
}

void HqlGram::checkIndexFieldType(IHqlExpression * expr, bool isPayload, bool insideNestedRecord, const attribute & errpos)
{
    bool variableOk = isPayload;
    switch (expr->getOperator())
    {
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            _ATOM name = expr->queryName();
            switch (type->getTypeCode())
            {
            case type_real:
                if (!isPayload)
                    reportIndexFieldType(expr, true, errpos);
                break;
            case type_decimal:
                if (!isPayload && type->isSigned())
                    reportIndexFieldType(expr, true, errpos);
                break;
            case type_bitfield:
            case type_any:
                reportIndexFieldType(expr, false, errpos);
                break;
            case type_record:
                throwUnexpected();
            case type_row:
                {
                    IHqlExpression * record = expr->queryRecord();
                    unsigned numPayload = isPayload ? record->numChildren() : 0;
                    checkIndexRecordType(record, numPayload, true, errpos);
                    break;
                }
            case type_table:
            case type_groupedtable:
                if (!variableOk)
                    reportError(ERR_INDEX_BADTYPE, errpos, "Datasets (%s) are not supported inside indexes", name->str());
                break;
            case type_packedint:
                if (!isPayload)
                    reportError(ERR_INDEX_BADTYPE, errpos, "PACKED integers (%s) are not supported inside indexes", name->str());
                break;
            case type_set:
                if (!variableOk)
                    reportError(ERR_INDEX_BADTYPE, errpos, "SETS (%s) are not supported inside indexes", name->str());
                break;
            case type_int:
            case type_swapint:
                if (!isPayload && insideNestedRecord)
                {
                    if (type->isSigned() ||
                        ((type->getTypeCode() == type_littleendianint) && (type->getSize() != 1)))
                        reportWarning(ERR_INDEX_BADTYPE, errpos.pos, "Signed or little-endian field %s is not supported inside a keyed record field ", name->str());
                }
                break;
            default:
                if (!type->isScalar())
                    reportIndexFieldType(expr, false, errpos);
                else if ((type->getSize() == UNKNOWN_LENGTH) && !variableOk)
                {
                    reportError(ERR_INDEX_BADTYPE, errpos, "Variable size fields (%s) are not supported inside indexes", name->str());
                    break;
                }
            }
            break;
        }
    case no_ifblock:
        {
            if (!isPayload)
                reportError(ERR_INDEX_BADTYPE, errpos, "IFBLOCKS not supported inside indexes");

            IHqlExpression * record = expr->queryChild(1);
            unsigned numPayload = isPayload ? record->numChildren() : 0;
            checkIndexRecordType(record, numPayload, insideNestedRecord, errpos);
            break;
        }
    case no_record:
        {
            unsigned numPayload = isPayload ? expr->numChildren() : 0;
            checkIndexRecordType(expr, numPayload, insideNestedRecord, errpos);
            break;
        }
    }
}

void HqlGram::checkIndexRecordType(IHqlExpression * record, unsigned numPayloadFields, bool insideNestedRecord, const attribute & errpos)
{
    unsigned max = record->numChildren();
    for (unsigned i=0;i < max; i++)
        checkIndexFieldType(record->queryChild(i), i >= max-numPayloadFields, insideNestedRecord, errpos);
}

void HqlGram::checkIndexRecordTypes(IHqlExpression * index, const attribute & errpos)
{
    IHqlExpression * record = index->queryChild(1);
    checkIndexRecordType(record, numPayloadFields(index), false, errpos);
}

IHqlExpression * HqlGram::createRecordFromDataset(IHqlExpression * ds)
{
    IHqlExpression * record = ds->queryRecord();

    HqlExprArray fields;
    ForEachChild(idx, record)
    {
        IHqlExpression * field = record->queryChild(idx);
        if (field->isAttribute())
            fields.append(*LINK(field));
        else
        {
            //MORE: This should clone all attributes - not just extractFieldAttrs(), but if so it would also need to map the
            //fields referenced in COUNT(SELF.x) as well.
            fields.append(*createField(field->queryName(), field->getType(), createSelectExpr(LINK(ds), LINK(field)), extractFieldAttrs(field)));
        }
    }

    return createRecord(fields);
}


IHqlExpression * HqlGram::cleanIndexRecord(IHqlExpression * record)
{
    HqlExprArray fields;
    bool same = true;
    ForEachChild(idx, record)
    {
        IHqlExpression * field = record->queryChild(idx);
        if (field->getOperator() == no_field && hasUninheritedAttribute(field))
        {
            IHqlExpression * value = queryRealChild(field, 0);
            fields.append(*createField(field->queryName(), field->getType(), LINK(value), extractFieldAttrs(field)));
            same = false;
        }
        else
            fields.append(*LINK(field));
    }

    if (same)
        return LINK(record);
    return createRecord(fields);
}


interface IRecordFieldCompare
{
    virtual bool include(IHqlExpression * field) = 0;
};

class RecordFieldDifference : implements IRecordFieldCompare
{
public:
    RecordFieldDifference(IHqlExpression * record, HqlGram & _gram, const attribute & _errpos) : gram(_gram), errpos(_errpos)   
    {
         scope = record->querySimpleScope();
    }
    virtual bool include(IHqlExpression * field) 
    { 
        _ATOM name = field->queryName();
        OwnedHqlExpr match = scope->lookupSymbol(name);
        if (!match)
            return true;
        if (field->queryType() != match->queryType())
            gram.reportError(ERR_TYPEMISMATCH_RECORD, errpos, "Field %s has different type in records", name->str());
        return false;
    }
protected:
    HqlGram & gram;
    const attribute & errpos;
    IHqlSimpleScope * scope;
};


class RecordFieldExcept : implements IRecordFieldCompare
{
public:
    RecordFieldExcept(IHqlExpression * list, HqlGram & _gram, const attribute & _errpos) : gram(_gram), errpos(_errpos)
    {
        expand(list);
    }
    void checkAllUsed()
    {
        ForEachItemIn(i, names)
        {
            if (!matchedName.item(i))
                gram.reportError(ERR_OBJ_NOSUCHFIELD, errpos, "Record doesn't contain a field called %s", names.item(i).str());
        }
    }
    void expand(IHqlExpression * list)
    {
        while (list->getOperator() == no_comma)
        {
            expand(list->queryChild(0));
            list = list->queryChild(1);
        }
        names.append(*list->queryName());
        matchedName.append(false);
    }
    virtual bool include(IHqlExpression * field) 
    { 
        unsigned match = names.find(*field->queryName());
        if (match == NotFound)
            return true;
        matchedName.replace(true, match);
        return false;
    }

protected:
    HqlGram & gram;
    const attribute & errpos;
    AtomArray names;
    BoolArray matchedName;
};


class RecordFieldIntersect : implements IRecordFieldCompare
{
public:
    RecordFieldIntersect(IHqlExpression * record, HqlGram & _gram, const attribute & _errpos) : gram(_gram), errpos(_errpos)
    {
         scope = record->querySimpleScope();
    }
    virtual bool include(IHqlExpression * field) 
    { 
        _ATOM name = field->queryName();
        OwnedHqlExpr match = scope->lookupSymbol(name);
        if (match)
        {
            if (field->queryType() != match->queryType())
                gram.reportError(ERR_TYPEMISMATCH_RECORD, errpos, "Field %s has different type in records", name->str());
            return true;
        }
        return false;
    }
protected:
    HqlGram & gram;
    const attribute & errpos;
    IHqlSimpleScope * scope;
};


static bool ifblockFieldsIncluded(IHqlExpression * expr, IRecordFieldCompare & other)
{
    if (expr->getOperator() == no_select)
    {
        IHqlExpression * ds = expr->queryChild(0);
        while (ds->getOperator() == no_select)
        {
            expr = ds;
            ds = expr->queryChild(0);
        }
        return other.include(expr->queryChild(1));
    }

    ForEachChild(i, expr)
        if (!ifblockFieldsIncluded(expr->queryChild(i), other))
            return false;
    return true;
}

static IHqlExpression * createRecordExcept(IHqlExpression * left, IRecordFieldCompare & other)
{
    HqlExprArray fields;
    ForEachChild(i, left)
    {
        IHqlExpression * cur = left->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            if (other.include(cur))
                fields.append(*LINK(cur));
            break;
        case no_record:
            {
                OwnedHqlExpr newRecord = createRecordExcept(cur, other);
                if (newRecord->numChildren() != 0)
                    fields.append(*newRecord.getClear());
                break;
            }
        case no_ifblock:
            {
                IHqlExpression * cond = cur->queryChild(0);
                if (ifblockFieldsIncluded(cond, other))
                {
                    IHqlExpression * record = cur->queryChild(1);
                    OwnedHqlExpr newRecord = createRecordExcept(record, other);
                    if (newRecord->numChildren() != 0)
                    {
                        if (newRecord == record)
                            fields.append(*LINK(cur));
                        else
                            fields.append(*createValue(no_ifblock, makeNullType(), LINK(cond), newRecord.getClear()));
                    }
                }
                break;
            }
        default:
            fields.append(*LINK(cur));
            break;
        }
    }
    return cloneOrLink(left, fields);
}



IHqlExpression * HqlGram::createRecordIntersection(IHqlExpression * left, IHqlExpression * right, const attribute & errpos)
{
    RecordFieldIntersect compare(right, *this, errpos);

    return ::createRecordExcept(left, compare);
}


static void unwindExtraFields(HqlExprArray & fields, IHqlExpression * expr)
{
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            if (!queryProperty(cur->queryName(), fields))
                fields.append(*LINK(cur));
            break;
        case no_record:
            unwindExtraFields(fields, cur);
            break;
        default:
            fields.append(*LINK(cur));
        }
    }
}


IHqlExpression * HqlGram::createRecordUnion(IHqlExpression * left, IHqlExpression * right, const attribute & errpos)
{
    //MORE: maxlength should be handled better - but should really be specified on the fields.
    RecordFieldDifference compare(left, *this, errpos);
    OwnedHqlExpr extra = ::createRecordExcept(right, compare);

    HqlExprArray args;
    unwindChildren(args, left);
    unwindExtraFields(args, extra);
    return cloneOrLink(left, args);
}

IHqlExpression * HqlGram::createRecordDifference(IHqlExpression * left, IHqlExpression * right, const attribute & errpos)
{
    RecordFieldDifference compare(right, *this, errpos);
    return ::createRecordExcept(left, compare);
}

IHqlExpression * HqlGram::createRecordExcept(IHqlExpression * left, IHqlExpression * right, const attribute & errpos)
{
    RecordFieldExcept compare(right, *this, errpos);
    OwnedHqlExpr ret = ::createRecordExcept(left, compare);
    compare.checkAllUsed();
    return ret.getClear();
}


IHqlExpression * HqlGram::createIndexFromRecord(IHqlExpression * record, IHqlExpression * attr, const attribute & errpos)
{
    IHqlExpression * ds = createDataset(no_null, LINK(record), NULL);
    OwnedHqlExpr finalRecord = checkIndexRecord(record, errpos);
    finalRecord.setown(cleanIndexRecord(finalRecord));

    OwnedHqlExpr transform = createClearTransform(finalRecord, errpos);
    return createDataset(no_newkeyindex, ds, createComma(LINK(finalRecord), transform.getClear(), LINK(attr)));
}


void HqlGram::inheritRecordMaxLength(IHqlExpression * dataset, SharedHqlExpr & record)
{
    IHqlExpression * maxLength = queryRecordProperty(dataset->queryRecord(), maxLengthAtom);
//  if (maxLength && isVariableSizeRecord(record) && !queryRecordProperty(record, maxLengthAtom))
    if (maxLength && !queryRecordProperty(record, maxLengthAtom))
    {
        HqlExprArray fields;
        unwindChildren(fields, record);
        fields.add(*LINK(maxLength), 0);
        record.setown(record->clone(fields));
    }
}


static HqlTransformerInfo groupExpressionCheckerInfo("GroupExpressionChecker");
class GroupExpressionChecker : public QuickHqlTransformer
{
public:
    GroupExpressionChecker(const HqlExprArray & _groups)
        : QuickHqlTransformer(groupExpressionCheckerInfo, NULL)
    {
        ok = true;
        ForEachItemIn(i, _groups)
            groups.append(*_groups.item(i).queryBody());
    }

    virtual void doAnalyseBody(IHqlExpression * expr)
    {
        if (!ok)
            return;

        if (expr->isAggregate() || expr->isConstant())
            return;

        if (groups.contains(*expr))
            return;

        // No dice - check kids
        switch (expr->getOperator())
        {
        case no_select:
            ok = false;
            return;
        }

        QuickHqlTransformer::doAnalyseBody(expr);
    }

    bool isOk() const { return ok; }

protected:
    HqlExprCopyArray groups;
    bool ok;
};



bool checkGroupExpression(HqlExprArray &groups, IHqlExpression *field)
{
    GroupExpressionChecker checker(groups);
    checker.analyse(field);
    return checker.isOk();
}


static HqlTransformerInfo quickSelectNormalizerInfo("QuickSelectNormalizer");
class HQL_API QuickSelectNormalizer : public QuickHqlTransformer
{
public:
    QuickSelectNormalizer() : QuickHqlTransformer(quickSelectNormalizerInfo, NULL)
    {
    }

    virtual IHqlExpression * createTransformed(IHqlExpression * expr)
    {
        switch (expr->getOperator())
        {
        case no_constant:
        case no_colon:
        case no_cluster:
            return LINK(expr);
        case no_globalscope:
            if (!expr->hasProperty(optAtom))
                return LINK(expr);
            break;
        case NO_AGGREGATE:
            //These aren't strictly correct, but will only go wrong if the same
            //obscure expression is written out twice differently.
            //you really should be projecting or using the same attribute
            return LINK(expr);

        case no_select:
            return LINK(expr->queryNormalizedSelector());
        }
        return QuickHqlTransformer::createTransformed(expr);
    }
};



static IHqlExpression * normalizeSelects(IHqlExpression * expr)
{
    QuickSelectNormalizer transformer;
    return transformer.transform(expr);
}


void HqlGram::checkGrouping(const attribute& errpos, HqlExprArray & parms, IHqlExpression* record, IHqlExpression* groups)
{
    unsigned reckids = record->numChildren();
    for (unsigned i = 0; i < reckids; i++)
    {
        IHqlExpression *field = record->queryChild(i);

        switch(field->getOperator())
        {
        case no_record:
            checkGrouping(errpos, parms, field, groups);
            break;
        case no_ifblock:
            reportError(ERR_GROUP_BADSELECT, errpos, "IFBLOCKs are not supported inside grouped aggregates");
            break;
        case no_field:              
            {
                IHqlExpression * rawValue = field->queryChild(0);
                if (rawValue)
                {
                    OwnedHqlExpr value = normalizeSelects(rawValue);
                    bool ok = checkGroupExpression(parms, value);

                    if (!ok)
                    {
                        _ATOM name = NULL;
                        
                        switch(field->getOperator())
                        {
                        case no_select:
                            name = field->queryChild(1)->queryName();
                            break;
                        case no_field:  
                            name = field->queryName();
                            break;
                        default:
                            name = field->queryName();
                            break;
                        }

                        StringBuffer msg("Field ");
                        if (name)
                            msg.append("'").append(name).append("' ");
                        msg.append("in TABLE does not appear to be properly defined by grouping conditions");
                        reportWarning(ERR_GROUP_BADSELECT,errpos.pos, "%s", msg.str());
                    }
                }
                else if (field->isDatarow())
                {
                    checkGrouping(errpos, parms, field->queryRecord(), groups);
                }
                else
                    throwUnexpected();
            }
            break;
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            break;
        default:
            assertex(false);
        }
    }   
}



// MORE: how about child dataset?
void HqlGram::checkGrouping(const attribute & errpos, IHqlExpression * dataset, IHqlExpression* record, IHqlExpression* groups)
{
    if (!groups) return;
    assertex(record->getOperator()==no_record);

    //match should be by structure!!
    HqlExprArray parms1;
    HqlExprArray parms;
    groups->unwindList(parms1, no_sortlist);

    //The expressions need normalizing because the selectors need to be normalized before checking for matches.
    //The problem is that before the tree is tagged replaceSelector() doesn't work.  So have to use
    //an approximation instead.
    ForEachItemIn(idx, parms1)
    {
        IHqlExpression * cur = &parms1.item(idx);
        if (cur->getOperator() == no_field)
            reportError(ERR_GROUP_BADSELECT, errpos, "cannot use field of result record as a grouping parameter");
        else
        {
            IHqlExpression * mapped = normalizeSelects(cur);
            parms.append(*mapped);
        }
    }

    checkGrouping(errpos, parms, record, groups);
}


void HqlGram::checkConditionalAggregates(_ATOM name, IHqlExpression * value, const attribute & errpos)
{
    if (!value->isGroupAggregateFunction())
        return;

    IHqlExpression * cond;
    switch (value->getOperator())
    {
    case no_sumgroup:
    case no_maxgroup:
    case no_mingroup:
    case no_avegroup:
    case no_vargroup: 
        cond = queryRealChild(value, 1);
        break;
    case no_existsgroup:
    case no_countgroup:
    case no_notexistsgroup:
        cond = queryRealChild(value, 0);
        break;
    case no_covargroup:
    case no_corrgroup:
        cond = queryRealChild(value, 1);
        break;
    default:
        {
            ForEachChild(i, value)
                checkConditionalAggregates(name, value->queryChild(i), errpos);
            return;
        }
    }

    if (cond)
        reportError(ERR_AGG_FIELD_AFTER_VAR, errpos, "Field %s: Conditional aggregates cannot follow a variable length field", name ? name->str() : "");
}


void HqlGram::checkProjectedFields(IHqlExpression * e, attribute & errpos)
{
    if (!isAggregateDataset(e))
        return;
    IHqlExpression * record = e->queryRecord();
    bool hadVariableAggregate = false;
    bool isVariableOffset = false;
    ForEachChild(idx, record)
    {
        IHqlExpression * field = record->queryChild(idx);
        if (field->getOperator() == no_field)
        {
            IHqlExpression * value = field->queryChild(0);
            if (value)
            {
                _ATOM name = field->queryName();
                bool isVariableSize = (value->queryType()->getSize() == UNKNOWN_LENGTH);
                if (value->getOperator() == no_implicitcast)
                    value = value->queryChild(0);
                if (isVariableSize)
                {
                    if (hadVariableAggregate)
                        reportError(ERR_AGG_FIELD_AFTER_VAR, errpos, "Field %s: Fields cannot follow a variable length aggregate in the record", name ? name->str() : "");

                    if (value->isGroupAggregateFunction())
                        hadVariableAggregate = true;
                }

                
                if (isVariableOffset)
                    checkConditionalAggregates(name, value, errpos);

                if (isVariableSize)
                    isVariableOffset = true;
            }
        }
    }
}

void HqlGram::validateParseTerminate(IHqlExpression * expr, attribute & errpos)
{
    switch (expr->getOperator())
    {
    case no_list:
        {
            ForEachChild(idx, expr)
                validateParseTerminate(expr->queryChild(idx), errpos);
            break;
        }
    case no_constant:
        {
            ITypeInfo * type = expr->queryType();
            const void * value = expr->queryValue()->queryValue();
            switch (type->getTypeCode())
            {
            case type_data:
            case type_string:
            case type_unicode:
                if (type->getStringLen() != 1)
                    reportError(ERR_EXPECTED_CHARLIST, errpos, "Expected a list of single character strings");
                break;
            case type_utf8:
                if (type->getStringLen() != 1)
                    reportError(ERR_EXPECTED_CHARLIST, errpos, "Expected a list of single character strings");
                else if (rtlUtf8Size(1, value) != 1)
                    reportError(ERR_EXPECTED_CHARLIST, errpos, "Expected a list of single byte character strings");
                break;
            default:
                reportError(ERR_EXPECTED_CHARLIST, errpos, "Expected a list of single character strings");
                break;
            }
            break;
        }
    default:
        reportError(ERR_EXPECTED_CHARLIST, errpos, "Expected a list of single character strings");
        break;
    }
}

bool HqlGram::isExplicitlyDistributed(IHqlExpression *e)
{
    if (e->getOperator()==no_distribute || e->getOperator()==no_keyeddistribute)
        return true;
    return false;
    for (unsigned i = 0; i < getNumChildTables(e); i++)
    {
        if (isExplicitlyDistributed(e->queryChild(i)))
            return true;
    }
    return false;
}

static bool isFromFile(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_table:
            return true;
        case no_usertable:
            if (isAggregateDataset(expr))
                return false;
            //fallthrough...
        case no_filter:
        case no_hqlproject:
            expr = expr->queryChild(0);
            break;
        default:
            return false;
        }
    }
}


static const char * getName(IHqlExpression * e)
{
    if (e->queryName())
        return e->queryName()->str();
    return "";
}

void HqlGram::checkDistribution(attribute &errpos, IHqlExpression *input, bool localSpecified, bool ignoreGrouping)
{
    IInterface *distribution = input->queryType()->queryDistributeInfo();
    bool inputIsGrouped = isGrouped(input);
    if (localSpecified)
    {
        if (inputIsGrouped && !ignoreGrouping)
            reportError(WRN_LOCALIGNORED,errpos,"LOCAL specified on a grouped dataset %s - ungroup first", getName(input));
    }
    else
    {
        if (distribution && isExplicitlyDistributed(input))
        {
            if (!inputIsGrouped || ignoreGrouping)
            {
                const char * name = getName(input);
                reportWarning(WRN_LOCALONEXPLICITDIST,errpos.pos,"Input %s is explicitly DISTRIBUTEd but LOCAL not specified", name);
            }
        }
    }
}

void HqlGram::checkDistribution(attribute &errpos, IHqlExpression * newExpr, bool ignoreGrouping)
{
    checkDistribution(errpos, newExpr->queryChild(0), newExpr->hasProperty(localAtom), ignoreGrouping);
}

bool HqlGram::isDiskFile(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_table:
        return true;
    case no_cluster:
        return !expr->hasProperty(fewAtom);
    case no_colon:
        {
            ForEachChild(i, expr)
            {
                if (i && isSaved(expr->queryChild(i)))
                    return true;
            }
            break;
        }
    case no_keyed:
        return isDiskFile(expr->queryChild(0));
    }
    return false;
}

bool HqlGram::isFilteredDiskFile(IHqlExpression * expr)
{
    loop
    {
        switch (expr->getOperator())
        {
        case no_filter:
            expr = expr->queryChild(0);
            break;
        default:
            return isDiskFile(expr);
        }
    }
}

void HqlGram::checkJoinFlags(const attribute &err, IHqlExpression * join)
{
    bool lonly = join->hasProperty(leftonlyAtom);
    bool ronly = join->hasProperty(rightonlyAtom);
    bool fonly = join->hasProperty(fullonlyAtom);
    bool lo = join->hasProperty(leftouterAtom) || lonly;
    bool ro = join->hasProperty(rightouterAtom) || ronly;
    bool fo = join->hasProperty(fullouterAtom) || fonly;
    bool keep = join->hasProperty(keepAtom);
    IHqlExpression * rowLimit = join->queryProperty(rowLimitAtom);

    IHqlExpression * keyed = join->queryProperty(keyedAtom);
    if (keyed)
    {
        if (join->hasProperty(allAtom) || join->hasProperty(lookupAtom))
            reportError(ERR_KEYEDINDEXINVALID, err, "LOOKUP/ALL not compatible with KEYED");

        IHqlExpression * index = keyed->queryChild(0);
        if (index)
        {
            if (isKey(index))
            {
                IHqlExpression * rhs = join->queryChild(1);
                IHqlExpression * indexDataset = index->queryChild(0)->queryNormalizedSelector();

                if (indexDataset != rhs->queryNormalizedSelector())
                    reportWarning(ERR_KEYEDNOTMATCHDATASET,err.pos,"Parameter to KEYED is not an index on the RIGHT dataset");
                else if (!isFilteredDiskFile(rhs))
                    reportError(ERR_KEYEDNOTMATCHDATASET,err,"RIGHT side of a full keyed join must be a disk file");
                else
                {
                    if (indexDataset->getOperator() == no_table)
                    {
                        switch (indexDataset->queryChild(2)->getOperator())
                        {
                        case no_csv:
                        case no_xml:
                            reportError(ERR_KEYEDNOTMATCHDATASET,err,"RIGHT side of a full keyed join must be a THOR disk file (CSV/XML) not currently supported");
                            break;
                        }
                    }
                }

                IHqlExpression * indexRecord = index->queryRecord();
                bool hadMapping = false;
                if (indexRecord)
                {
                    unsigned max = indexRecord->numChildren()-numPayloadFields(index);
                    for (unsigned i = 0; i < max; i++)
                    {
                        IHqlExpression * cur = indexRecord->queryChild(i);
                        if (!cur->isAttribute() && queryRealChild(cur, 0))
                            hadMapping = true;
                    }
                }
                if (!hadMapping)
                    reportError(ERR_KEYEDINDEXINVALID, err, "Record of KEYED index does not contain references to the dataset");
            }
        }
    }
    if (join->hasProperty(lookupAtom))
    {
        bool isMany = join->hasProperty(manyAtom);
        if (ro || fo)
            reportError(ERR_BADKIND_LOOKUPJOIN, err, "JOIN(LOOKUP) only supports INNER, LEFT OUTER, and LEFT ONLY joins");
        if (join->hasProperty(partitionRightAtom))
            reportError(ERR_BADKIND_LOOKUPJOIN, err, "Lookup joins do not support PARTITION RIGHT");
        if (keep && !isMany)
            reportError(ERR_BADKIND_LOOKUPJOIN, err, "Lookup joins do not support KEEP");
        if (join->hasProperty(atmostAtom) && !isMany)
            reportError(ERR_BADKIND_LOOKUPJOIN, err, "Lookup joins do not support ATMOST");
        if (rowLimit && !isMany)
            reportError(ERR_BADKIND_LOOKUPJOIN, err, "Lookup joins do not support LIMIT (they can only match 1 entry)");
        if (isKey(join->queryChild(1)))
            reportWarning(ERR_BADKIND_LOOKUPJOIN, err.pos, "Lookup specified on an unfiltered keyed join - was this intended?");
    }
    else if (isKeyedJoin(join))
    {
        if (ro || fo)
            reportError(ERR_INVALIDKEYEDJOIN, err, "Keyed joins only support LEFT OUTER/ONLY");
        if (join->hasProperty(partitionRightAtom))
            reportError(ERR_INVALIDKEYEDJOIN, err, "Keyed joins do not support PARTITION RIGHT");
    }
    if (join->hasProperty(allAtom))
    {
        if (join->hasProperty(partitionRightAtom))
            reportError(ERR_INVALIDALLJOIN, err, "JOIN(,ALL) does not support PARTITION RIGHT");
        if (ro || fo)
            reportError(ERR_INVALIDALLJOIN, err, "JOIN(ALL) only supports INNER, LEFT OUTER, and LEFT ONLY joins");
        if (join->hasProperty(atmostAtom))
            reportError(ERR_INVALIDALLJOIN, err, "JOIN(ALL) does not support ATMOST");
        if (rowLimit)
            reportError(ERR_INVALIDALLJOIN, err, "JOIN(ALL) does not support LIMIT");
    }
    if (join->hasProperty(atmostAtom))
    {
        if (fo || ro)
            reportError(ERR_BAD_JOINFLAG, err, "ATMOST cannot be used with FULL or RIGHT ONLY/OUTER");

        if (rowLimit)// && getIntValue(rowLimit->queryChild(0), 1) != 0)
            reportError(ERR_BAD_JOINFLAG, err, "LIMIT and ATMOST can't be used in combination");
    }
    if (keep)
    {
        if (lonly || ro || fo)
            reportError(ERR_BAD_JOINFLAG, err, "KEEP can only be used with INNER, LEFT OUTER");
    }
    if (lonly || ronly || fonly)
    {
        if (rowLimit)
            reportError(ERR_BAD_JOINFLAG, err, "LIMIT cannot be used in combination ONLY");
        if (join->hasProperty(onFailAtom))
            reportError(ERR_BAD_JOINFLAG, err, "ONFAIL cannot be used in combination ONLY");
    }

    IHqlExpression * rhs = join->queryChild(1);
    if (!isKeyedJoin(join) && (rhs->getOperator() == no_filter) && (rhs != join->queryChild(0)))
    {
        IHqlExpression * cur = rhs;
        while (cur->getOperator() == no_filter)
            cur = cur->queryChild(0);
        if (isKey(cur))
            reportWarning(ERR_BAD_JOINFLAG, err.pos, "Filtered RIGHT prevents a keyed join being used.  Consider including the filter in the join condition.");
    }
}


void HqlGram::checkLoopFlags(const attribute &err, IHqlExpression * loopExpr)
{
    if (loopExpr->hasProperty(parallelAtom))
    {
        unsigned base = (loopExpr->getOperator() == no_loop ? 1 : 2);
        if (!queryRealChild(loopExpr, base))
            reportWarning(WRN_BAD_LOOPFLAG, err.pos, "PARALLEL is currently only supported with a defined number of iterations");
        if (queryRealChild(loopExpr, base+2))
            reportWarning(WRN_BAD_LOOPFLAG, err.pos, "PARALLEL is not supported with dataset loop termination condition");
    }
}


bool HqlGram::checkTransformTypeMatch(const attribute & errpos, IHqlExpression * ds, IHqlExpression * transform)
{
    if (!recordTypesMatch(ds, transform))
    {
        reportError(ERR_TRANSFORM_TYPE_MISMATCH,errpos,"Type returned from transform must match the source dataset type");
        return false;
    }
    return true;
}

void HqlGram::ensureTransformTypeMatch(attribute & tattr, IHqlExpression * ds)
{
    if (!checkTransformTypeMatch(tattr, ds, tattr.queryExpr()))
    {
        tattr.release();
        tattr.setExpr(createClearTransform(ds->queryRecord(), tattr));
    }
}

void HqlGram::ensureDatasetTypeMatch(attribute & tattr, IHqlExpression * ds)
{
    if (!recordTypesMatch(ds, tattr.queryExpr()))
    {
        reportError(ERR_TRANSFORM_TYPE_MISMATCH,tattr,"Output dataset must match the source dataset type");
        tattr.release();
        tattr.setExpr(createDataset(no_null, LINK(ds->queryRecord())));
    }
}

void HqlGram::ensureDataset(attribute & attr)
{
    if (attr.queryExpr()->isDatarow())
    {
        IHqlExpression * ds = createDatasetFromRow(attr.getExpr());
        attr.setExpr(ds);
    }
    checkDataset(attr);
}
void HqlGram::expandPayload(HqlExprArray & fields, IHqlExpression * payload, IHqlSimpleScope * scope, ITypeInfo * & lastFieldType, const attribute & errpos)
{
    ForEachChild(i2, payload)
    {
        IHqlExpression * cur = payload->queryChild(i2);
        switch (cur->getOperator())
        {
        case no_record:
            expandPayload(fields, cur, scope, lastFieldType, errpos);
            break;
        case no_ifblock:
            lastFieldType = NULL;
            fields.append(*LINK(cur));
            break;
        case no_field:
            {
                OwnedHqlExpr match = scope->lookupSymbol(cur->queryName());
                if (match)
                {
                    //Ignore any fields that are completely duplicated in the payload to allow
                    //INDEX(ds, { ds.x, ds.y }, {ds} ) to mean add everything else as payload.
                    IHqlExpression * matchValue = match->queryChild(0);
                    IHqlExpression * curValue = cur->queryChild(0);
                    if (matchValue)
                        matchValue = matchValue->queryNormalizedSelector();
                    if (curValue)
                        curValue = curValue->queryNormalizedSelector();
                    if (matchValue != curValue)
                        reportError(ERR_REC_DUPFIELD, errpos, "Field %s is already defined in the key portion", cur->queryName()->str());
                }
                else
                {
                    lastFieldType = cur->queryType();
                    fields.append(*LINK(cur));
                }
                break;
            }
        }
    }
}

void HqlGram::modifyIndexPayloadRecord(SharedHqlExpr & record, SharedHqlExpr & payload, SharedHqlExpr & extra, const attribute & errpos)
{
    IHqlSimpleScope * scope = record->querySimpleScope();

    HqlExprArray fields;
    ForEachChild(i3, record)
    {
        IHqlExpression * cur = record->queryChild(i3);
        if (cur->isAttribute())
            fields.append(*LINK(cur));
    }
    ForEachChild(i1, record)
    {
        IHqlExpression * cur = record->queryChild(i1);
        if (!cur->isAttribute())
            fields.append(*LINK(cur));
    }

    unsigned payloadCount = 0;
    ITypeInfo * lastFieldType = NULL;
    if (payload)
    {
        unsigned oldFields = fields.ordinality();
        expandPayload(fields, payload,  scope, lastFieldType, errpos);
        payloadCount = fields.ordinality() - oldFields;
    }
    //This needs to be here until filepositions are no longer special cased.
    if (!lastFieldType || !lastFieldType->isInteger())
    {
        IHqlSimpleScope * payloadScope = payload ? payload->querySimpleScope() : NULL;
        _ATOM implicitFieldName;
        for (unsigned suffix =1;;suffix++)
        {
            StringBuffer name;
            name.append("__internal_fpos");
            if (suffix > 1)
                name.append(suffix);
            name.append("__");
            implicitFieldName = createIdentifierAtom(name);
            OwnedHqlExpr resolved = scope->lookupSymbol(implicitFieldName);
            if (!resolved && payloadScope)
                resolved.setown(payloadScope->lookupSymbol(implicitFieldName));
            if (!resolved)
                break;
        }

        fields.append(*createField(implicitFieldName, makeIntType(8, false), createConstant(I64C(0)), createAttribute(_implicitFpos_Atom)));
        payloadCount++;
    }

    extra.setown(createComma(extra.getClear(), createAttribute(_payload_Atom, createConstant((__int64)payloadCount))));
    record.setown(createRecord(fields));
}

void HqlGram::extractRecordFromExtra(SharedHqlExpr & record, SharedHqlExpr & extra)
{
    while (record->getOperator() == no_comma)
    {
        extra.setown(createComma(LINK(record->queryChild(1)), extra.getClear()));
        record.set(record->queryChild(0));
    }
}


void HqlGram::transferOptions(attribute & filenameAttr, attribute & optionsAttr)
{
    if (filenameAttr.queryExpr()->getOperator() == no_comma)
    {
        OwnedHqlExpr filename = filenameAttr.getExpr();
        do
        {
            optionsAttr.setExpr(createComma(LINK(filename->queryChild(1)), optionsAttr.getExpr()));
            filename.set(filename->queryChild(0));
        } while (filename->getOperator() == no_comma);
        filenameAttr.setExpr(filename.getClear());
    }
}


IHqlExpression * HqlGram::extractTransformFromExtra(SharedHqlExpr & extra)
{
    IHqlExpression * ret = NULL;
    if (extra)
    {
        HqlExprArray args;
        extra->unwindList(args, no_comma);
        if (args.item(0).isTransform())
        {
            ret = &args.item(0);
            args.remove(0, true);
            extra.setown(createComma(args));
        }
    }
    return ret;
}
            

void HqlGram::applyPayloadAttribute(const attribute & errpos, IHqlExpression * record, SharedHqlExpr & extra)
{
    IHqlExpression * payload = queryPropertyInList(payloadAtom, extra);
    if (payload)
    {
        HqlExprArray fields;
        unwindChildren(fields, record);
        IHqlExpression * search = payload->queryChild(0);
        if (search->getOperator() == no_select)
            search = search->queryChild(1);
        unsigned match = fields.find(*search);
        if (match != NotFound)
        {
            HqlExprArray args;
            extra->unwindList(args, no_comma);
            args.zap(*payload);
            args.append(*createAttribute(_payload_Atom, createConstant((__int64)fields.ordinality()-match)));
            extra.setown(createComma(args));
        }
        else
            reportError(ERR_TYPEMISMATCH_RECORD, errpos, "The argument to the payload isn't found in the index record");
    }
}


void HqlGram::checkBoolean(attribute &atr)
{
    if (!atr.queryExpr()->isBoolean())
    {
        reportError(ERR_EXPECTED_BOOLEANEXP, atr, "Expected boolean expression");
        // error recovery
        atr.release().setExpr(createConstant(true));
    }
}

void HqlGram::checkBooleanOrNumeric(attribute &atr)
{
    ITypeInfo * type = atr.queryExprType();
    if (!type || !(type->getTypeCode() == type_boolean || isNumericType(type)))
    {
        reportError(ERR_EXPECTED_BOOLEANEXP, atr,"Expected boolean or integer expression");
        // error recovery
        atr.getExpr()->Release();
        atr.setExpr(createConstant(true));
    }
}

void HqlGram::checkDataset(attribute &atr)
{
    if (!atr.queryExpr()->isDataset())
    {
        reportError(ERR_EXPECTED_DATASET, atr, "Expected dataset expression");
        atr.release().setExpr(createNullDataset());
    }
}

void HqlGram::checkDatarow(attribute &atr)
{
    if (!atr.queryExpr()->isDatarow())
    {
        reportError(ERR_EXPECTED_ROW, atr, "Expected datarow expression");
        atr.release().setExpr(createRow(no_null, LINK(queryNullRecord())));
    }
}

void HqlGram::checkList(attribute &atr)
{
    if (!atr.queryExpr()->isList())
    {
        reportError(ERR_EXPECTED_LIST, atr, "Expected a list");
        atr.release().setExpr(createValue(no_list, makeSetType(NULL)));
    }
}

void HqlGram::checkScalar(attribute &atr)
{
    if (!atr.queryExprType()->isScalar())
    {
        reportError(ERR_EXPECTED_SCALAR, atr, "Expected a single valued expression");
        atr.release().setExpr(getSizetConstant(0));
    }
}

void HqlGram::checkDedup(IHqlExpression *ds, IHqlExpression *flags, attribute &atr)
{
}

void HqlGram::checkDistributer(attribute & err, HqlExprArray & args)
{
    IHqlExpression * input = &args.item(0);
    IHqlExpression * inputPayload = queryProperty(_payload_Atom, args);
    ForEachItemIn(idx, args)
    {
        IHqlExpression & cur = args.item(idx);
        if (cur.getOperator() == no_distributer)
        {
            
            IHqlExpression * index = cur.queryChild(0);
            unsigned numKeyedFields = firstPayloadField(index);
            unsigned inputKeyedFields = firstPayloadField(input->queryRecord(), inputPayload ? (unsigned)getIntValue(inputPayload->queryChild(0)) : 1);
            if (numKeyedFields != inputKeyedFields)
                reportError(ERR_DISTRIBUTED_MISSING, err, "Index and DISTRIBUTE(index) have different numbers of keyed fields");
            checkRecordTypes(args.item(0).queryRecord(), cur.queryChild(0)->queryRecord(), err, numKeyedFields);
        }
    }
}

bool HqlGram::convertAllToAttribute(attribute &atr)
{
    if (atr.getOperator() != no_all)
        return false;
    
    atr.release().setExpr(createAttribute(allAtom));
    return true;
}


void HqlGram::checkValidRecordMode(IHqlExpression * dataset, attribute & atr, attribute & modeattr)
{
    IHqlExpression * mode = dataset->queryChild(2);
    switch (mode->getOperator())
    {
    case no_csv:
        checkValidCsvRecord(atr, dataset->queryRecord());
        break;
    case no_xml:
        if (!isValidXmlRecord(dataset->queryRecord()))
            reportError(ERR_INVALID_XML_RECORD, atr, "XML cannot be used on this record structure");
        break;
    }
}


void HqlGram::checkValidCsvRecord(const attribute & errpos, IHqlExpression * record)
{
    if (record)
    {
        IHqlExpression * badField = queryInvalidCsvRecordField(record);
        if (badField)
            reportError(ERR_INVALID_CSV_RECORD, errpos, "CSV cannot be used on this record structure (field %s)", badField->queryName()->str());
    }
}

void HqlGram::checkValidPipeRecord(const attribute & errpos, IHqlExpression * record, IHqlExpression * attrs, IHqlExpression * expr)
{
    if (queryPropertyInList(csvAtom, attrs) || (expr && expr->hasProperty(csvAtom)))
        checkValidCsvRecord(errpos, record);
}

int HqlGram::checkRecordTypes(IHqlExpression *left, IHqlExpression *right, attribute &atr, unsigned maxFields)
{
    if (recordTypesMatch(left, right)) 
        return 0;

    IHqlExpression * lrecord = left->queryRecord();
    IHqlExpression * rrecord = right->queryRecord();
    
    unsigned lnumChildren = lrecord->numChildren();
    unsigned rnumChildren = rrecord->numChildren();
    if (lnumChildren > maxFields) lnumChildren = maxFields;
    if (rnumChildren > maxFields) rnumChildren = maxFields;

    if(lnumChildren != rnumChildren) 
    {
        if (getFieldCount(lrecord) != getFieldCount(rrecord))
            reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same number of fields: %d vs %d", lnumChildren, rnumChildren);
        else
            reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same attributes");
        return -1;
    }
    
    for (unsigned idx = 0; idx < lnumChildren; idx++)
    {
        IHqlExpression *lfield = lrecord->queryChild(idx);
        IHqlExpression *rfield = rrecord->queryChild(idx);
        if (lfield->isAttribute() || rfield->isAttribute())
        {
            if (lfield != rfield)
                reportError(ERR_TYPEMISMATCH_DATASET, atr, "Record attributes differ: %d vs %d", lnumChildren, rnumChildren);
        }
        
        assertex(lfield);
        assertex(rfield);
        
        ITypeInfo * lchildrectype = lfield->queryRecordType();
        ITypeInfo * rchildrectype = rfield->queryRecordType();
        
        if(lchildrectype == rchildrectype) // both can be NULL.
        {
            // both are not not record types
            ITypeInfo * lType = lfield->queryType();
            ITypeInfo * rType = rfield->queryType();
            if(!isSameBasicType(lType, rType))
            {
                StringBuffer ltype, rtype;
                getFriendlyTypeStr(lfield, ltype);
                getFriendlyTypeStr(rfield, rtype);

                IHqlAlienTypeInfo * lAlien = queryAlienType(lType);
                IHqlAlienTypeInfo * rAlien = queryAlienType(rType);
                if (lAlien && rAlien && 
                    queryExpression(lType)->queryFunctionDefinition() == queryExpression(rType)->queryFunctionDefinition())
                {
                    reportError(ERR_TYPEMISMATCH_DATASET, atr, "Fields %s and %s use incompatible instances of the same user type %s",lfield->queryName()->str(), rfield->queryName()->str(), ltype.str());
                }
                else
                {
                    reportError(ERR_TYPEMISMATCH_DATASET, atr, "Type mismatch for corresponding fields %s (%s) vs %s (%s)",lfield->queryName()->str(), ltype.str(), rfield->queryName()->str(), rtype.str());
                }
            }
        }
        else if(lchildrectype == NULL || rchildrectype == NULL) 
        {
            reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same types for field %d: one is Record, the other is not", idx+1);
            return -1;
        }
        
        // recursive call to check sub fields.
        if(lchildrectype && rchildrectype && checkRecordTypes(lfield, rfield, atr) != 0) 
            return -1;
    }

    return 0;
}


bool HqlGram::checkRecordCreateTransform(HqlExprArray & assigns, IHqlExpression *leftExpr, IHqlExpression *leftSelect, IHqlExpression *rightExpr, IHqlExpression *rightSelect, attribute &atr)
{
    if (leftExpr->getOperator() != rightExpr->getOperator())
    {
        if (leftExpr->isAttribute() || rightExpr->isAttribute())
            reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same attributes");
        else
            reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same structure");
        return false;
    }

    switch (rightExpr->getOperator())
    {
    case no_ifblock:
        return checkRecordCreateTransform(assigns, leftExpr->queryChild(1), leftSelect, rightExpr->queryChild(1), rightSelect, atr);
    case no_record:
    {
        unsigned lnumChildren = leftExpr->numChildren();
        unsigned rnumChildren = rightExpr->numChildren();
        if (lnumChildren != rnumChildren) 
        {
            reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same number of fields: %d vs %d", lnumChildren, rnumChildren);
            return false;
        }
        for (unsigned i= 0; i < lnumChildren; i++)
            if (!checkRecordCreateTransform(assigns, leftExpr->queryChild(i), leftSelect, rightExpr->queryChild(i), rightSelect, atr))
                return false;
        return true;
    }
    case no_attr:
    case no_attr_expr:
    case no_attr_link:
        return true;
    case no_field:
        {
            OwnedHqlExpr leftSelected = createSelectExpr(LINK(leftSelect), LINK(leftExpr));
            OwnedHqlExpr rightSelected = createSelectExpr(LINK(rightSelect), LINK(rightExpr));
            if (isSameBasicType(leftExpr->queryType(), rightExpr->queryType()))
            {
                assigns.append(*createAssign(LINK(leftSelected), LINK(rightSelected)));
                return true;
            }

            _ATOM leftName = leftExpr->queryName();
            _ATOM rightName = rightExpr->queryName();
            if (leftName != rightName)
            {
                reportError(ERR_TYPEMISMATCH_DATASET, atr, "Name mismatch for corresponding fields %s vs %s",leftName->str(), rightName->str());
                return false;
            }

            IHqlExpression * leftRecord = leftExpr->queryRecord();
            IHqlExpression * rightRecord = rightExpr->queryRecord();
            if (!leftRecord || !rightRecord)
            {
                if (!leftRecord && !rightRecord)
                {
                    StringBuffer ltype, rtype;
                    getFriendlyTypeStr(leftExpr, ltype);
                    getFriendlyTypeStr(rightExpr, rtype);
                    reportError(ERR_TYPEMISMATCH_DATASET, atr, "Type mismatch for corresponding fields %s (%s) vs %s (%s)",leftName->str(), ltype.str(), rightName->str(), rtype.str());
                }
                else
                    reportError(ERR_TYPEMISMATCH_DATASET, atr, "Datasets must have the same types for field %s vs %s: one is Record, the other is not", leftName->str(), rightName->str());
                return false;
            }

            if (rightExpr->isDatarow())
                return checkRecordCreateTransform(assigns, leftRecord, leftSelected, rightRecord, rightSelected, atr);

            assigns.append(*createAssign(LINK(leftSelected), checkEnsureRecordsMatch(leftSelected, rightSelected, atr, false)));
            return true;
        }
    }
    UNIMPLEMENTED;
    return false;
}


IHqlExpression * HqlGram::checkEnsureRecordsMatch(IHqlExpression * left, IHqlExpression * right, attribute & errpos, bool rightIsRow)
{
    checkRecordTypes(left, right, errpos);

    //Need to add a project to make the field names correct, otherwise problems occur if one the left side is optimized away,
    //because that causes the record type and fields to change.
    if (recordTypesMatch(left, right)) 
        return LINK(right);
    
    HqlExprArray assigns;
    OwnedHqlExpr seq = createSelectorSequence();
    OwnedHqlExpr rightSelect = createSelector(no_left, right, seq);
    OwnedHqlExpr leftSelect = getSelf(left);
    if (!checkRecordCreateTransform(assigns, left->queryRecord(), leftSelect, right->queryRecord(), rightSelect, errpos))
        return LINK(right);

    IHqlExpression * transform = createValue(no_transform, makeTransformType(LINK(left->queryRecordType())), assigns);
    HqlExprArray args;
    args.append(*LINK(right));
    args.append(*transform);
    args.append(*LINK(seq));
    //args.append(*createUniqueId());
    if (rightIsRow)
        return createRow(no_projectrow, args);
    else
        return createDataset(no_hqlproject, args);
}

void HqlGram::checkMergeSortOrder(attribute &atr, IHqlExpression *ds1, IHqlExpression *ds2, IHqlExpression * sortorder)
{
    if (!recordTypesMatch(ds1, ds2)) 
        reportError(ERR_TYPE_INCOMPATIBLE, atr, "Datasets in list must have identical records");
    return;
    checkRecordTypes(ds1, ds2, atr);
    // MORE - should check that sort orders match
    // but tricky because they don't have to apply to the same records...
}

IHqlExpression * HqlGram::createScopedSequenceExpr()
{
    unsigned numScopes = defineScopes.ordinality();
    //Not sure this test is correct for forward scopes...
    if (numScopes == minimumScopeIndex)
        return createSequenceExpr();
        
    assertex(numScopes >= 2);
    ActiveScopeInfo & targetScope = defineScopes.item(numScopes-2);
    if (!targetScope.isParametered)
        return createSequenceExpr();

    HqlExprArray & parameters = targetScope.activeParameters;
    StringBuffer paramName;
    paramName.append("_implicit_hidden_").append(parameters.ordinality());
    OwnedHqlExpr value = createSequenceExpr();
    HqlExprArray attrs;
    attrs.append(*createAttribute(_hidden_Atom));
    IHqlExpression * param = createParameter(createIdentifierAtom(paramName.str()), parameters.ordinality(), value->getType(), attrs);
    parameters.append(*param);
    targetScope.activeDefaults.append(*LINK(value));
    return LINK(param);
}

static bool isZeroSize(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_record:
        {
            ForEachChild(i, expr)
                if (!isZeroSize(expr->queryChild(i)))
                    return false;
            return true;
        }
    case no_ifblock:
        {
            OwnedHqlExpr folded = foldExprIfConstant(expr->queryChild(0));
            IValue * value = folded->queryValue();
            if (value && !value->getBoolValue())
                return true;
            //Not really sure what to do...
            return isZeroSize(expr->queryChild(1));
        }
    case no_field:
        {
            ITypeInfo * type = expr->queryType();
            switch (type->getTypeCode())
            {
            case type_record:
            case type_row:
                return isZeroSize(expr->queryRecord());
            case type_alien:
                //more
                return false;
            case type_bitfield:
                return false;
            default:
                return type->getSize() == 0;
            }
        }
    }
    return true;
}


void HqlGram::checkRecordIsValid(attribute &atr, IHqlExpression *record)
{
    if (isZeroSize(record))
        reportError(ERR_ZEROSIZE_RECORD, atr, "Record must not be zero length");
}


void HqlGram::checkMergeInputSorted(attribute &atr, bool isLocal)
{
    IHqlExpression * expr = atr.queryExpr();
    if (appearsToBeSorted(expr->queryType(), isLocal, true))
        return;
    if (!isLocal && appearsToBeSorted(expr->queryType(), true, true))
    {
        reportWarning(WRN_MERGE_NOT_SORTED, atr.pos, "INPUT to MERGE appears to be sorted locally but not globally");
        return;
    }
        
    if (isGrouped(expr))
    {
        switch (expr->getOperator())
        {
        case no_sort:
            reportError(WRN_MERGE_NOT_SORTED, atr, "SORT on MERGE input should be applied to an ungrouped dataset");
            return;
        case no_sorted:
            reportError(WRN_MERGE_NOT_SORTED, atr, "SORTED on MERGE input should be applied to an ungrouped dataset");
            return;
        }
    }
    
    if (isGrouped(expr) && appearsToBeSorted(expr->queryType(), false, false))
        reportWarning(WRN_MERGE_NOT_SORTED, atr.pos, "Input to MERGE is only sorted with the group");
    else
        reportWarning(WRN_MERGE_NOT_SORTED, atr.pos, "Input to MERGE doesn't appear to be sorted");
}


void HqlGram::checkGrouped(attribute & atr)
{
    if (!isGrouped(atr.queryExpr()))
        reportError(ERR_ROLLUP_NOT_GROUPED, atr, "Input to activity must be grouped");
}

void HqlGram::checkRegrouping(attribute & atr, HqlExprArray & args)
{
    IHqlExpression * left = &args.item(0);
    ForEachItemIn(i, args)
    {
        args.replace(*checkEnsureRecordsMatch(left, &args.item(i), atr, false), i);
        IHqlExpression * cur = &args.item(i);
        if (!isGrouped(cur))
            reportError(ERR_ROLLUP_NOT_GROUPED, atr, "Input %d to REGROUP must be grouped", i);
    }
}

void HqlGram::checkRecordsMatch(attribute & atr, HqlExprArray & args)
{
    IHqlExpression * left = &args.item(0);
    ForEachItemIn(i, args)
    {
        IHqlExpression & cur = args.item(i);
        if (!cur.isAttribute() && !recordTypesMatch(&cur, left))
            reportError(ERR_TYPE_INCOMPATIBLE, atr, "Datasets in list must have identical records");
    }
}

void HqlGram::checkNotAlreadyDefined(_ATOM name, IHqlScope * scope, const attribute & idattr)
{
    OwnedHqlExpr expr = scope->lookupSymbol(name, LSFsharedOK|LSFignoreBase, lookupCtx);
    if (expr)
    {
        if (legacyEclSemantics && isImport(expr))
            reportWarning(ERR_ID_REDEFINE, idattr.pos, "Identifier '%s' hides previous import", name->str());
        else
            reportError(ERR_ID_REDEFINE, idattr, "Identifier '%s' is already defined", name->str());
    }
}


void HqlGram::checkNotAlreadyDefined(_ATOM name, const attribute & idattr)
{
    ActiveScopeInfo & activeScope = defineScopes.tos();
    if (activeScope.localScope)
        checkNotAlreadyDefined(name, activeScope.localScope, idattr);
    checkNotAlreadyDefined(name, activeScope.privateScope, idattr);
    unsigned numScopes = defineScopes.ordinality();
    if ((numScopes > 1) && defineScopes.item(numScopes-2).queryParameter(name))
        reportError(ERR_ID_REDEFINE, idattr, "Identifier '%s' is already defined as a parameter", name->str());
}


IHqlScope * HqlGram::queryPrimaryScope(bool isPrivate)
{
    ActiveScopeInfo & cur = defineScopes.item(0);
    if (isPrivate)
        return cur.privateScope;
    return cur.localScope;
}

IHqlExpression * HqlGram::addSideEffects(IHqlExpression * expr)
{
    unsigned first = defineScopes.tos().firstSideEffect;
    if (parseResults.ordinality() <= first)
        return LINK(expr);
        
#ifdef USE_WHEN_FOR_SIDEEFFECTS
    if (expr->isDataset())
    {
//      ensureActions(parseResults, first, parseResults.ordinality());
        IHqlExpression * actions = createActionList(parseResults, first, parseResults.ordinality());
        parseResults.trunc(first);
        return createDataset(no_executewhen, LINK(expr), actions);
    }   
#endif

#if 0
    IHqlExpression * actions = createActionList(parseResults, first, parseResults.ordinality());
    parseResults.trunc(first);
    return createCompound(actions, LINK(expr));
#endif

    //Slightly weird - but side-effects need to be nested so they are associated with the correct attribute/RETURN
    //So scope contains the index of the number of side effects active when it is created, and these are preserved
    IHqlExpression * compound = NULL;
    while (parseResults.ordinality() > first)
    {
        OwnedHqlExpr next = &parseResults.popGet();
        if (!next->isAction())
        {
            //Retain any side-effects that were attached to the item being ignored.
            while (next->getOperator() == no_compound)
            {
                parseResults.append(*LINK(next->queryChild(0)));
                next.set(next->queryChild(1));
            }

            ECLlocation location(next);
            reportWarning(ERR_RESULT_IGNORED, location, "Expression ignored");
        }
        else
            compound = createCompound(next.getClear(), compound);
    }
    return createCompound(compound, LINK(expr));
}

void HqlGram::createAppendFiles(attribute & targetAttr, attribute & leftAttr, attribute & rightAttr, _ATOM kind)
{
    OwnedHqlExpr left = leftAttr.getExpr();
    OwnedHqlExpr right = rightAttr.getExpr();
    if (left->isDatarow()) 
        left.setown(createDatasetFromRow(LINK(left)));
    right.setown(checkEnsureRecordsMatch(left, right, rightAttr, right->isDatarow()));
    if (right->isDatarow())
        right.setown(createDatasetFromRow(LINK(right)));
    IHqlExpression * attr = kind ? createAttribute(kind) : NULL;
    targetAttr.setExpr(createDataset(no_addfiles, LINK(left), createComma(LINK(right), attr)));
    targetAttr.setPosition(leftAttr);
}


IHqlExpression * HqlGram::processIfProduction(attribute & condAttr, attribute & trueAttr, attribute * falseAttr)
{
    OwnedITypeInfo type;
    if (falseAttr)
        type.setown(checkPromoteIfType(trueAttr, *falseAttr));  // convert to (ds,ds)(dr,dr),(list,list),(scalar, scalar)

    OwnedHqlExpr cond = condAttr.getExpr();
    OwnedHqlExpr left = trueAttr.getExpr();
    OwnedHqlExpr right = falseAttr ? falseAttr->getExpr() : NULL;
    if (!right)
    {
        if (left->isDatarow())
        {
            OwnedHqlExpr transform = createClearTransform(queryOriginalRecord(left), trueAttr);
            right.setown(createRow(no_createrow, LINK(transform)));
        }
        else
            right.setown(createNullExpr(left));
    }

    //MORE: Not sure about this!
    if (parsingTemplateAttribute && cond->isConstant())
    {
        OwnedHqlExpr folded = quickFoldExpression(cond);
        if (folded->queryValue())
            return folded->queryValue()->getBoolValue() ? left.getClear() : right.getClear();
    }

    if (left->queryRecord() && falseAttr)
        right.setown(checkEnsureRecordsMatch(left, right, *falseAttr, false));

    if (isGrouped(left) != isGrouped(right))
        reportError(ERR_GROUPING_MISMATCH, trueAttr, "Branches of the condition have different grouping");

    return ::createIf(cond.getClear(), left.getClear(), right.getClear());
}



bool HqlGram::isVirtualFunction(DefineIdSt * defineid, const attribute & errpos)
{
    if (defineid->scope & (EXPORT_FLAG | SHARED_FLAG))
    {
        IHqlScope * scope = defineScopes.tos().localScope;
        if (defineid->scope & VIRTUAL_FLAG)
        {
            if (scope && defineScopes.ordinality() > minimumScopeIndex)
                return true;
            reportError(ERR_BAD_VIRTUAL, errpos, "VIRTUAL can only be used inside a local module definition");
            return false;
        }
        if (scope && queryExpression(scope)->hasProperty(virtualAtom))
            return true;
    }

    if (defineid->scope & VIRTUAL_FLAG)
        reportError(ERR_BAD_VIRTUAL, errpos, "EXPORT or SHARED required on a virtual definition");
    return false;
}

//Allow the types to be grouped by different expressions, and sorted by different fields.
static bool isEquivalentType(ITypeInfo * l, ITypeInfo * r)
{
    loop
    {
        if (isSameUnqualifiedType(l, r))
            return true;
        if (l->getTypeCode() != r->getTypeCode())
            return false;
        switch (l->getTypeCode())
        {
        case type_table:
        case type_groupedtable:
        case type_row:
            {
                l = l->queryChildType();
                r = r->queryChildType();
                if (!l || !r)
                    return false;
                break;
            }
        default:
            return false;
        }
    }
}


bool HqlGram::areSymbolsCompatible(IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, IHqlExpression * prevValue)
{
    bool ok = false;
    if (isParametered)
    {
        if (prevValue->isFunction())
        {
            ITypeInfo * exprReturnType = stripFunctionType(expr->queryType());
            ITypeInfo * prevReturnType = stripFunctionType(prevValue->queryType());
            IHqlExpression * formals = queryFunctionParameters(prevValue);
            if (formals->numChildren() == parameters.ordinality() && 
                isEquivalentType(exprReturnType, prevReturnType))
            {
                ok = true;
                ForEachItemIn(iParam, parameters)
                {
                    IHqlExpression * curParam = &parameters.item(iParam);
                    IHqlExpression * curBaseParam = formals->queryChild(iParam);
                    if ((curParam->queryName() != curBaseParam->queryName()) ||
                        !isEquivalentType(curParam->queryType(), curBaseParam->queryType()))
                        ok = false;
                }
            }
        }
    }
    else
    {
        if (!prevValue->isFunction())
            ok = isEquivalentType(expr->queryType(), prevValue->queryType());
    }
    return ok;
}


void HqlGram::checkDerivedCompatible(_ATOM name, IHqlExpression * scope, IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, attribute const & errpos)
{
    ForEachChild(i, scope)
    {
        IHqlScope * base = scope->queryChild(i)->queryScope();
        if (base)
        {
            OwnedHqlExpr match = base->lookupSymbol(name, LSFsharedOK|LSFignoreBase, lookupCtx);
            if (match)
            {
                if (!canBeVirtual(match))
                    reportError(ERR_MISMATCH_PROTO, errpos, "Definition %s, cannot override this kind of definition", name->str());
                else
                {
                    if (!areSymbolsCompatible(expr, isParametered, parameters, match))
                        reportError(ERR_MISMATCH_PROTO, errpos, "Prototypes for %s in base and derived modules must match", name->str());
                }
            }
        }
    }
}

bool HqlGram::okToAddSideEffects(IHqlExpression * expr)
{
    switch (expr->getOperator())
    {
    case no_transform:
    case no_record:
    case no_macro:
    case no_remotescope:
    case no_mergedscope:
    case no_privatescope:
    case no_type:
    case no_typedef:
    case no_forwardscope:
    case no_enum:
        return false;
    default:
        {
            ITypeInfo * type = expr->queryType();
            if (!type)
                return false;

            type_t etc = type->getTypeCode();
            if ((etc == type_pattern) || (etc == type_rule) || (etc == type_token) || (etc == type_feature) || (etc == type_event))
                return false;
            break;
        }
    }
    return true;
}

IHqlExpression * HqlGram::associateSideEffects(IHqlExpression * expr, const ECLlocation & errpos)
{
    if (sideEffectsPending())
    {
        if (legacyEclSemantics)
        {
            if (okToAddSideEffects(expr))
                return addSideEffects(expr);
            reportError(ERR_RESULT_IGNORED, errpos, "Cannot associate a side effect with this type of definition - action must precede an expression");
        }
        else
        {
            reportError(ERR_RESULT_IGNORED, errpos, "WHEN must be used to associated an action with a definition");
        }
        clearSideEffects();
    }
    return LINK(expr);
}


void HqlGram::doDefineSymbol(DefineIdSt * defineid, IHqlExpression * _expr, IHqlExpression * failure, const attribute & idattr, int assignPos, int semiColonPos, bool isParametered)
{
    OwnedHqlExpr expr = _expr;
    // env symbol
    _ATOM name = defineid->id;
    checkNotAlreadyDefined(name, idattr);

    ActiveScopeInfo & activeScope = defineScopes.tos();
    if (activeScope.templateAttrContext)
        expr.setown(createTemplateFunctionContext(expr.getClear(), closeScope(activeScope.templateAttrContext.getClear())));

    if (!activeScope.localScope)
    {
        expr.setown(associateSideEffects(expr, idattr.pos));

        //Ignore SHARED and EXPORT flags 
        if (defineid->scope & (EXPORT_FLAG | SHARED_FLAG))
            reportWarning(WRN_EXPORT_IGNORED, idattr.pos, "EXPORT/SHARED qualifiers are ignored in this context");

        defineid->scope = 0;
        defineSymbolInScope(activeScope.privateScope, defineid, expr.getClear(), failure, idattr, assignPos, semiColonPos, isParametered, activeScope.activeParameters, activeScope.createDefaults());
    }
    else
    {
        // define the symbol
        if (defineid->scope & (EXPORT_FLAG | SHARED_FLAG))
        {
            if (expectedAttribute && (expectedAttribute != name) && (activeScope.localScope == parseScope))
            {
                OwnedHqlExpr resolved = parseScope->lookupSymbol(expectedAttribute, LSFsharedOK, lookupCtx);
                if (resolved)
                {
                    reportError(ERR_UNEXPECTED_PUBLIC_ID, idattr.pos, "Definition of '%s' has a trailing public definition '%s'", expectedAttribute->str(), name->str());
                }
                else
                {
                    //Make this warning come out now - otherwise a subsequent error about an undefined symbol makes less sense.
                    RestoreValueBlock<bool> block(associateWarnings, false);
                    reportWarning(ERR_UNEXPECTED_PUBLIC_ID, idattr.pos, "Name of exported symbol '%s' does not match the expected name '%s'", name->str(), expectedAttribute->str());
                }
                defineid->scope = 0;
            }
        }

        if (defineid->scope & (EXPORT_FLAG | SHARED_FLAG))
        {
            if (isQuery && !insideNestedScope())
            {
                //If this is a global query, and not inside a nested attribute, then keep any actions on the global list of results
                if (!legacyEclSemantics)
                {
                    //Should we give a warning here?? export/shared would not be legal if this was within the repository
                }
            }
            else
            {
                //Otherwise, actions are associated with the symbol just being exported.
                expr.setown(associateSideEffects(expr, idattr.pos));
            }

            IHqlExpression * scopeExpr = queryExpression(activeScope.localScope);
            if (scopeExpr->getOperator() == no_virtualscope)
                checkDerivedCompatible(name, scopeExpr, expr, isParametered, activeScope.activeParameters, idattr);

            //static int i = 0;
            //PrintLog("Kill private scope: %d at %s:%d because of %s", ++i, filename->str(), idattr.lineno, current_id->str());
            activeScope.newPrivateScope();
            defineSymbolInScope(activeScope.localScope, defineid, expr.getClear(), failure, idattr, assignPos, semiColonPos, isParametered, activeScope.activeParameters, activeScope.createDefaults());
            lastpos = semiColonPos+1;
        }
        else
        {
            defineSymbolInScope(activeScope.privateScope, defineid, expr.getClear(), failure, idattr, assignPos, semiColonPos, isParametered, activeScope.activeParameters, activeScope.createDefaults());
        }
    }

    ::Release(failure);
    // clean up
    activeScope.resetParameters();
    
    current_id = NULL; 
    current_type = NULL;
    delete defineid;
}

IHqlExpression * HqlGram::attachPendingWarnings(IHqlExpression * ownedExpr)
{
    //Now attach some warnings...
    ForEachItemInRev(i, pendingWarnings)
        ownedExpr = createWarningAnnotation(ownedExpr, &OLINK(pendingWarnings.item(i)));
    pendingWarnings.kill();
    return ownedExpr;
}

void HqlGram::doAttachPendingWarnings(attribute & exprAttr)
{
    IHqlExpression * expr = exprAttr.getExpr();
    if (expr)
        expr = attachPendingWarnings(expr);
    exprAttr.setExpr(expr);
}

IHqlExpression * HqlGram::attachMetaAttributes(IHqlExpression * ownedExpr, HqlExprArray & meta)
{
    if (meta.ordinality())
        ownedExpr = createMetaAnnotation(ownedExpr, meta);
    return ownedExpr;
}

void HqlGram::defineSymbolInScope(IHqlScope * scope, DefineIdSt * defineid, IHqlExpression * expr, IHqlExpression * failure, const attribute & idattr, int assignPos, int semiColonPos, bool isParametered, HqlExprArray & parameters, IHqlExpression * defaults)
{
    IHqlScope * exprScope = expr->queryScope();
    IHqlExpression * scopeExpr = queryExpression(scope);
    _ATOM moduleName = NULL;
    if (!inType)
        moduleName = createIdentifierAtom(scope->queryFullName());

    unsigned symbolFlags = 0;
    if (scopeExpr && scopeExpr->getOperator() == no_virtualscope)
        symbolFlags |= ob_member;

    HqlExprCopyArray activeParameters;
    gatherActiveParameters(activeParameters);

    HqlExprArray meta;
    expr = attachWorkflowOwn(meta, expr, failure, &activeParameters);
    if (isParametered)
    {
        IHqlExpression * formals = createValue(no_sortlist, makeSortListType(NULL), parameters);
        expr = createFunctionDefinition(defineid->id, expr, formals, defaults, NULL);
    }

    expr = attachPendingWarnings(expr);
    expr = attachMetaAttributes(expr, meta);

    IPropertyTree * doc = defineid->queryDoc();
    if (doc)
        expr = createJavadocAnnotation(expr, LINK(doc));

    Owned<IFileContents> contents = createFileContentsSubset(lexObject->query_FileContents(), lastpos, semiColonPos+1-lastpos);
    scope->defineSymbol(defineid->id, moduleName, expr, (defineid->scope & EXPORT_FLAG) != 0, (defineid->scope & SHARED_FLAG) != 0, symbolFlags, contents, idattr.pos.lineno, idattr.pos.column, 0, assignPos+2-lastpos, semiColonPos+1-lastpos);
}


void HqlGram::defineSymbolProduction(attribute & nameattr, attribute & paramattr, attribute & assignattr, attribute * valueattr, attribute * failattr, attribute & semiattr)
{
    OwnedHqlExpr expr;
    IHqlExpression * failure = NULL;
    DefineIdSt* defineid = nameattr.getDefineId();
    _ATOM name = defineid->id;
    ITypeInfo *type = defineid->getType();
    assertex(name);

    ActiveScopeInfo & activeScope = defineScopes.tos();
    if (valueattr && failattr)
    {
        if (isExceptionalCase(nameattr,*valueattr,*failattr))
        {
            activeScope.resetParameters();
            delete defineid;
            ::Release(type);
            return;
        }

        failure = failattr->getExpr();
        if (failure && (valueattr->queryExpr()->getOperator() == no_typedef))
            reportError(ERR_WORKFLOW_ILLEGAL, *valueattr, "Workflow actions are illegal on a typedef");

        checkWorkflowScheduling(failure, *failattr);

        checkFormals(defineid->id,activeScope.activeParameters,activeScope.activeDefaults,*valueattr);
        expr.setown(valueattr->getExpr());
    }
    else
    {
        expr.setown(createPureVirtual(type));

        if (!(defineid->scope & (EXPORT_FLAG|SHARED_FLAG)))
            reportError(ERR_SHOULD_BE_EXPORTED, nameattr, "Pure definitions should be exported or shared");
    }

    ITypeInfo *etype = expr->queryType();
    if (isSaved(failure) && !type)
    {
        if ((etype->getSize() == 0) && (etype->isScalar()))
            reportError(ERR_ZEROLENSTORED, nameattr, "Saved definition has zero length - missing type?");
        else if ((etype->getTypeCode() == type_set) && etype->queryChildType() == NULL) 
            reportError(ERR_ZEROLENSTORED, nameattr, "Type must be specified for this stored list");
    }

    // type specific handling
    IHqlExpression * base = queryNonDelayedBaseAttribute(expr); 
    node_operator op = base->getOperator();
    switch(op)
    {
    case no_service:  // service
        if (type)
        {
            reportError(ERR_SVC_NOYPENEEDED, nameattr, "Service can not have a type");
            type->Release();
            type = NULL;
        }
        if (activeScope.isParametered)
        {
            reportError(ERR_SVC_NOPARAMALLOWED, nameattr, "Service can not have any parameters");
            activeScope.resetParameters();
        }
        break;
    
    case no_macro:
        if (!activeScope.isParametered)
        {
            //reportError(ERR_MACRO_NOPARAMDEFINED, "Macro needs formal parameters; use () if no parameter needed", nameattr);
            activeScope.isParametered = true;
        }
        break;

    case no_externalcall: 
        // I'm not convinced this works at all - code appears to
        // translate a external dataset returning a function into a dataset with a functional mode.
        if (etype && etype->getTypeCode()==type_record)
        {
            IHqlExpression *recordDef = queryExpression(etype);
            expr.setown(createDatasetF(no_table, createConstant(*name), LINK(recordDef), LINK(expr), NULL));
        }
        break;

    case no_virtualscope:
        {
            IHqlExpression * libraryInterface = queryPropertyChild(base, libraryAtom, 0);
            if (libraryInterface)
            {
                //check the parameters for this symbol match the parameters on the library attribute
                checkLibraryParametersMatch(nameattr, activeScope.isParametered, activeScope.activeParameters, libraryInterface);
            }
            break;
        }

    }

    IHqlScope * localScope = activeScope.localScope;
    IHqlExpression * localScopeExpr = queryExpression(localScope);
    if (localScope && localScopeExpr->getOperator() == no_virtualscope)
    {
        OwnedHqlExpr anyMatch = localScope->lookupSymbol(name, LSFsharedOK, lookupCtx);
        OwnedHqlExpr localMatch  = localScope->lookupSymbol(name, LSFsharedOK|LSFignoreBase, lookupCtx);

        if (!(defineid->scope & (EXPORT_FLAG|SHARED_FLAG)))
        {
            if (anyMatch && !localMatch)
            {
                //only report error in base class clash, others will be reported later.
                if (!localMatch)
                    reportError(ERR_SHOULD_BE_EXPORTED, nameattr, "Private symbol %s clashes with public symbol in base module", name->str());
            }

            if (localScopeExpr->hasProperty(interfaceAtom))
            {
//              defineid->scope |= EXPORT_FLAG;
                reportError(ERR_SHOULD_BE_EXPORTED, nameattr, "Symbol %s in INTERFACE should be EXPORTed or SHARED", name->str());
            }
        }
        else
        {
            if (anyMatch && !localScopeExpr->hasProperty(virtualAtom))
            {
                //Not quite right - it is a problem if the place it is defined in isn't virtual
                reportError(ERR_CANNOT_REDEFINE, nameattr, "Cannot redefine definition %s from a non-virtual MODULE", name->str());
            }
            else if (anyMatch && !localMatch)
            {
                ITypeInfo * matchType = stripFunctionType(anyMatch->queryType());
                //check the parameters and return type (if specified) are compatible, promote expression return type to same
                if (type)
                {
                    if (!isSameUnqualifiedType(type, matchType))
                    {
                        //allow dataset with no record to match, as long as base type is the same
                        if (queryRecord(type) != queryNullRecord() || (type->getTypeCode() != matchType->getTypeCode()))
                            reportError(ERR_SAME_TYPE_REQUIRED, nameattr, "Explicit type for %s doesn't match definition in base module", name->str());
                        else
                        {
                            type->Release();
                            type = LINK(matchType);
                        }
                    }
                }
                else
                {
                    if (matchType->getTypeCode() == type_scope)
                    {
                        if (!matchType->assignableFrom(etype))
                        {
                            canNotAssignTypeError(type, etype, paramattr);
                            expr.setown(createNullExpr(matchType));
                        }
                    }
                    else
                        type = LINK(matchType);
                }
            }
        }
    }
    
    // type cast if necessary
    if (type && etype)
    {
        if (op == no_transform)
        {
            if (!recordTypesMatch(type, etype))
                canNotAssignTypeError(type,etype,paramattr);
        }
        else
        {
            if (type != etype)
            {
                if (!type->assignableFrom(etype))
                {
                    if (queryRecord(type) != queryNullRecord())
                    {
                        canNotAssignTypeError(type,etype,paramattr);
                        switch (type->getTypeCode())
                        {
                        case type_record:
                            expr.set(queryNullRecord());
                            break;
                        default:
                            expr.setown(createNullExpr(type));
                            break;
                        }
                    }
                }
                else
                {
                    switch (etype->getTypeCode())
                    {
                    case type_table:
                    case type_groupedtable:
                    case type_record:
                    case type_row:
                    case type_transform:
                        break;
                    default:
                        expr.setown(forceEnsureExprType(expr, type));
                        break;
                    }
                }
            }
        }
    }

    ::Release(type);

    // env symbol
    doDefineSymbol(defineid, expr.getClear(), failure, nameattr, assignattr.pos.position, semiattr.pos.position, activeScope.isParametered);
}


void HqlGram::definePatternSymbolProduction(attribute & nameattr, const attribute & assignAttr, attribute & valueAttr, attribute & workflowAttr, const attribute & semiattr)
{
    DefineIdSt* defineid = nameattr.getDefineId();

    checkPatternFailure(workflowAttr);
    IHqlExpression * failure = workflowAttr.getExpr();

    checkFormals(defineid->id,defineScopes.tos().activeParameters,defineScopes.tos().activeDefaults,valueAttr);

    _ATOM name = defineid->id;
    Owned<ITypeInfo> idType = defineid->getType();
    IHqlExpression *expr = valueAttr.getExpr();
    ITypeInfo *etype = expr->queryType();
    if (idType->getTypeCode() != etype->getTypeCode())
    {
        //If a token is being defined from a pattern add an implicit token creator.
        if ((idType->getTypeCode() == type_token) &&
            (etype->getTypeCode() == type_pattern))
            expr = createValue(no_pat_imptoken, makeTokenType(), expr);
    }
    if ((idType->getTypeCode() == type_rule) && (etype->getTypeCode() == type_rule))
    {
        ITypeInfo * iRecord = idType->queryChildType();
        ITypeInfo * eRecord = etype->queryChildType();
        if (iRecord != eRecord)
        {
            if (!iRecord || !eRecord || !recordTypesMatch(iRecord, eRecord))
                reportError(ERR_PATTERN_TYPE_MATCH, nameattr, "Declared rule type must match type of productions");
        }
    }

    IHqlExpression *features = getFeatureParams();
    if (features)
        expr = createValue(no_pat_featureparam, expr->getType(), expr, features);
    assertex(name);

    {
        _ATOM moduleName = globalScope->queryName();
        HqlExprArray args;
        args.append(*expr);
        args.append(*createAttribute(name));
        if (moduleName)
            args.append(*createAttribute(moduleName));
        if (queryParametered())
            args.append(*createAttribute(_function_Atom));
        expr = createValue(no_pat_instance, expr->getType(), args);
    }
    doDefineSymbol(defineid, expr, failure, nameattr, assignAttr.pos.position, semiattr.pos.position, queryParametered());
}

//-- SAS style conditional assignments
void HqlGram::expandScopeEntries(HqlExprArrayArray & branches, IHqlExpression * scope)
{
    HqlExprArrayItem & next = *new HqlExprArrayItem;
    if (scope)
    {
        scope->queryScope()->getSymbols(next.array);
        next.array.sort(compareSymbolsByName);
    }
    branches.append(next);
}


bool HqlGram::checkCompatibleSymbol(const attribute & errpos, IHqlExpression * prevValue, IHqlExpression * newValue)
{
//  if (!areSymbolsCompatible(IHqlExpression * expr, bool isParametered, HqlExprArray & parameters, IHqlExpression * prevValue)
    return true;
}

    
IHqlExpression * HqlGram::extractBranchMatch(const attribute & errpos, IHqlExpression & curSym, HqlExprArray & values)
{
    _ATOM name = curSym.queryName();
    ForEachItemIn(i, values)
    {
        IHqlExpression & cur = values.item(i);
        if (cur.queryName() == name)
        {
            if (!checkCompatibleSymbol(errpos, &curSym, &cur))
                return NULL;

            OwnedHqlExpr ret = LINK(&cur);
            //check the types of the attributes/functions match.  May need to remap parameters of functions.
            values.remove(i);
            return ret.getClear();
        }
    }

    //No Match found, check if is was previously defined in the current scope (?what about nesting?)
    OwnedHqlExpr match = lookupSymbol(name, errpos);
    if (!match)
    {
        //MORE If this was used as a temporary variable just within this branch, then that's ok.
        // The following test is no good though, we need to check if reused only in this branch.
//      if (curSym.isShared())
//          return NULL;
        reportWarning(WRN_COND_ASSIGN_NO_PREV, errpos.pos, "Conditional assignment to %s isn't defined in all branches, and has no previous definition", name->str());
        return NULL;
    }

    if (!checkCompatibleSymbol(errpos, &curSym, match))
        return NULL;

    return LINK(match);
}

        
ITypeInfo * HqlGram::extractBranchMatches(const attribute & errpos, IHqlExpression & curSym, HqlExprArrayArray & branches, HqlExprArray & extracted)
{
    //This is a n^2 algorithm, but I really don't expect very large numbers of conditional assignments.  Hopefully that will remain true.
    ForEachItemIn(i, branches)
    {
        HqlExprArrayItem & curArray = branches.item(i);
        IHqlExpression * match = extractBranchMatch(errpos, curSym, curArray.array);
        if (!match)
            return NULL;
        extracted.append(*match);
    }
    return promoteToSameType(extracted, errpos, NULL, true);
}


void HqlGram::processIfScope(const attribute & errpos, IHqlExpression * cond, IHqlExpression * trueScope, IHqlExpression * falseScope)
{
    HqlExprArrayArray branches;
    expandScopeEntries(branches, trueScope);

    HqlExprArray falseBranches;
    if (falseScope)
        falseScope->unwindList(falseBranches, no_comma);

    OwnedHqlExpr elseScope;
    if (falseBranches.ordinality())
    {
        if (falseBranches.tos().getOperator() != no_mapto)
            elseScope.setown(&falseBranches.popGet());
    }
    ForEachItemIn(iFalse, falseBranches)
        expandScopeEntries(branches, falseBranches.item(iFalse).queryChild(1));
    expandScopeEntries(branches, elseScope);

    ActiveScopeInfo & activeScope = defineScopes.tos();
    IHqlScope * defineScope = activeScope.privateScope;
    ForEachItemIn(i, branches)
    {
        HqlExprArrayItem & curArray = branches.item(i);
        ForEachItemInRev(j, curArray.array)
        {
            HqlExprArray matches;
            IHqlExpression & curSym = curArray.array.item(j);
            _ATOM name = curSym.queryName();
            OwnedITypeInfo condType = extractBranchMatches(errpos, curSym, branches, matches);
            if (condType)
            {
                OwnedHqlExpr value;
                unsigned numMatches = matches.ordinality();
                if (numMatches > 2)
                {
                    //Multi way elseif -> create a map expression
                    HqlExprArray args;
                    for (unsigned i=0; i < numMatches-1; i++)
                    {
                        IHqlExpression * test = (i == 0) ? cond : falseBranches.item(i-1).queryChild(0);
                        args.append(*createValue(no_mapto, LINK(condType), LINK(test), LINK(matches.item(i).queryBody())));
                    }
                    args.append(*LINK(matches.item(numMatches-1).queryBody()));
                    value.setown(createTypedValue(no_map, condType, args));
                }
                else
                {
                    HqlExprArray args;
                    args.append(*LINK(cond));
                    args.append(*LINK(matches.item(0).queryBody()));
                    args.append(*LINK(matches.item(1).queryBody()));
                    value.setown(createTypedValue(no_if, condType, args));
                }
                OwnedHqlExpr newSym = createSymbolFromValue(&curSym, value);
                if (activeScope.localScope)
                {
                    OwnedHqlExpr match = activeScope.localScope->lookupSymbol(name, LSFsharedOK|LSFignoreBase, lookupCtx);
                    if (match)
                        reportError(ERR_ID_REDEFINE, errpos, "Identifier '%s' is already defined as a public symbol in this context", name->str());
                }
                defineScope->defineSymbol(newSym.getClear());
            }
        }
    }
}


// virtual scope processing code
    
void HqlGram::cloneInheritedAttributes(IHqlScope * scope, const attribute & errpos)
{
    IHqlExpression * scopeExpr = queryExpression(scope);
    AtomArray derived;
    IHqlExpression * virtualAttr = scopeExpr->queryProperty(virtualAtom);
    ForEachChild(i, scopeExpr)
    {
        LinkedHqlExpr cur = scopeExpr->queryChild(i);
        IHqlScope * base = cur->queryScope();
        if (cur->getOperator() == no_param)
        {
            cur.setown(base->lookupSymbol(_parameterScopeType_Atom, LSFpublic, lookupCtx));
            base = cur->queryScope();
        }
        if (base)
        {
            IHqlExpression * baseVirtualAttr = cur->queryProperty(virtualAtom);
            IHqlScope * concreteBase = base->queryConcreteScope();
            bool baseIsLibrary = cur->getOperator() == no_libraryscopeinstance;

            HqlExprArray syms;
            base->getSymbols(syms);
            syms.sort(compareSymbolsByName);
            ForEachItemIn(iSym, syms)
            {
                IHqlExpression & baseSym = syms.item(iSym);
                _ATOM name = baseSym.queryName();
                OwnedHqlExpr match  = scope->lookupSymbol(name, LSFsharedOK|LSFignoreBase, lookupCtx);
                LinkedHqlExpr mapped = &baseSym;
                if (baseIsLibrary)
                    mapped.setown(concreteBase->lookupSymbol(name, LSFsharedOK, lookupCtx));        // creates a no_libraryselect
                else if (baseVirtualAttr)
                    mapped.setown(quickFullReplaceExpression(&baseSym, baseVirtualAttr, virtualAttr));
                if (match)
                {
                    if (derived.contains(*name))
                    {
                        //Ignore differences in the named symbol.  Should think about setting start/end to 0.  What would it break?
                        if (mapped->queryBody() != match->queryBody())
                            reportError(ERR_AMBIGUOUS_DEF, errpos, "Definition %s must be specified, it has different definitions in base modules", name->str());
                    }
                }
                else
                {
                    scope->defineSymbol(mapped.getClear());
                    derived.append(*name);
                }
            }
        }
    }

    if (virtualAttr)
        scopeExpr->addOperand(errpos.pos.createLocationAttr());
}

void HqlGram::checkExportedModule(const attribute & errpos, IHqlExpression * scopeExpr)
{
    checkNonGlobalModule(errpos, scopeExpr);
    IHqlExpression * interfaceExpr = scopeExpr;
    HqlExprArray symbols;
    interfaceExpr->queryScope()->getSymbols(symbols);

    IHqlScope * scope = scopeExpr->queryScope();
    ForEachItemIn(i, symbols)
    {
        IHqlExpression & cur = symbols.item(i);
        if (isExported(&cur))
        {
            _ATOM name = cur.queryName();
            OwnedHqlExpr value = scope->lookupSymbol(name, LSFpublic, lookupCtx);

            if (value)
            {
                if (value->isFunction())
                    reportError(ERR_BAD_LIBRARY_SYMBOL, errpos, "Library modules cannot export functional definition %s", name->str());
                else if (value->isDataset() || value->isDatarow() || value->queryType()->isScalar())
                {
                }
                else if (value->isList())
                    reportError(ERR_BAD_LIBRARY_SYMBOL, errpos, "Library modules cannot export list definition %s (use a dataset instead)", name->str());
                else
                {
                    //Should we report an error, or should we just ignore it?  Probably doesn't cause any problems as long as caught
                    //on invalid use.
                    //reportError(ERR_BAD_LIBRARY_SYMBOL, errpos, "Library modules cannot export functional definition %s", name->str());
                }
            }
        }
    }
}


void HqlGram::checkLibraryParametersMatch(const attribute & errpos, bool isParametered, const HqlExprArray & activeParameters, IHqlExpression * definition)
{
    if (definition->getOperator() == no_funcdef)
    {
        if (isParametered)
        {
            IHqlExpression * formals = definition->queryChild(1);
            if (formals->numChildren() == activeParameters.ordinality())
            {
                ForEachItemIn(i, activeParameters)
                {
                    IHqlExpression & cur = activeParameters.item(i);
                    IHqlExpression * expected = formals->queryChild(i);
                    if ((cur.queryName() != expected->queryName()) || (cur.queryType() != expected->queryType()))
                        reportError(ERR_PROTOTYPE_MISMATCH, errpos, "Parameter %s does not match the appropriate parameter as the LIBRARY interface it implements", cur.queryName()->str());
                }
            }
            else
                reportError(ERR_PROTOTYPE_MISMATCH, errpos, "Symbol does not have the same number of parameters as the LIBRARY interface it implements");
        }
// Not quite sure if the following is actually valid - example of aliasing the library, but mluber76.xhql uses it.
//      else
//          reportError(ERR_PROTOTYPE_MISMATCH, errpos, "Symbol is not functional, whilst LIBRARY(interface) is");
    }
    else
    {
        if (isParametered)
        {
            //Could report this as an error, but I think I'd prefer it to be accepted.
            //reportError(ERR_PROTOTYPE_MISMATCH, errpos, "Symbol is functional, whilst LIBRARY(interface) is not");
        }
    }
}




void HqlGram::checkNonGlobalModule(const attribute & errpos, IHqlExpression * scopeExpr)
{
    if ((scopeExpr->getOperator() == no_remotescope) || (scopeExpr->getOperator() == no_mergedscope))
        reportError(ERR_NO_GLOBAL_MODULE, errpos, "Global module cannot be used in this context");
}


IHqlExpression * HqlGram::createLibraryInstance(const attribute & errpos, IHqlExpression * name, IHqlExpression * func, HqlExprArray & actuals)
{
    if (!checkParameters(func, actuals, errpos))
        return createNullScope();

    //Create a library scope, but parameterised with the real serialized parameters, rather than the logical ecl parameters
    HqlExprArray oldSymbols, newSymbols, args;
    IHqlExpression * body = func->queryChild(0);

    //MORE: Check that none of the parameters to body are dependent on the parameters
    ForEachChild(iBody, body)
    {
        IHqlExpression * child = body->queryChild(iBody);
        if (!child->isAttribute() && !child->isFullyBound())
        {
            reportError(ERR_PROTOTYPE_MISMATCH, errpos, "LIBRARY interface definition cannot be dependent on the parameters");
            return createNullScope();
        }
    }

    if (body->hasProperty(libraryAtom))
        reportWarning(WRN_NOT_INTERFACE, errpos.pos, "LIBRARY() seems to reference an implementation rather than the interface definition");
    IHqlExpression * internalAttr = queryPropertyInList(internalAtom,name);
    if (internalAttr)
    {
        IHqlExpression * internalFunc = internalAttr->queryChild(0);
        IHqlExpression * internalModule = internalFunc->queryChild(0);
        IHqlExpression * libraryAttr = internalModule->queryProperty(libraryAtom);
        if (!libraryAttr || func != libraryAttr->queryChild(0))
            reportError(ERR_PROTOTYPE_MISMATCH, errpos, "Module referenced in INTERNAL() doesn't implement the required library interface");
    }

    bool needToMapOutputs = false;
    body->queryScope()->getSymbols(oldSymbols);
    ForEachItemIn(i, oldSymbols)
    {
        IHqlExpression & cur = oldSymbols.item(i);
        if (isExported(&cur) && !cur.isFunction())
        {
            OwnedHqlExpr newValue;
            if (cur.isDataset() || cur.isDatarow())
            {
                newValue.setown(createPureVirtual(cur.queryType()));
            }
            else
            {
                //non dataset returns are nasty!  Need to be converted to selects from datasets...
                OwnedHqlExpr field = createField(unknownAtom, cur.getType(), NULL, NULL);
                OwnedHqlExpr newRecord = createRecord(field);
                OwnedHqlExpr ds = createDataset(no_null, LINK(newRecord));
                newValue.setown(createPureVirtual(ds->queryType()));
                needToMapOutputs = true;
            }
            newSymbols.append(*cur.cloneAllAnnotations(newValue));
        }
    }
    unwindChildren(args, body);
    name->unwindList(args, no_comma);
    args.append(*createExprAttribute(implementsAtom, LINK(func)));
    IHqlScope * libraryScope = createLibraryScope();
    OwnedHqlExpr newBody = populateScopeAndClose(libraryScope, args, newSymbols);

    LibraryInputMapper inputMapper(func);
    OwnedHqlExpr newParameters = createValueSafe(no_sortlist, makeSortListType(NULL), inputMapper.queryRealParameters());
    OwnedHqlExpr newFunction = createFunctionDefinition(func->queryName(), newBody.getClear(), newParameters.getClear(), NULL, NULL);

    HqlExprArray realActuals;
    inputMapper.mapLogicalToReal(realActuals, actuals);
    OwnedHqlExpr bound = bindParameters(errpos, newFunction, realActuals);
    if (!needToMapOutputs)
        return bound.getClear();

    //Really nasty... if any of the outputs are scalar, then we need to create a wrapping module which maps 
    //the scalar values to selects from the datasets we've created in the underlying library module
    //mapModule := module(body^) 
    //  x := library(a,b,c,d).x
    //  s := library(a,b,c,d).s[1].f

    IHqlScope * boundScope = bound->queryScope();
    HqlExprArray mappingArgs, mappingSymbols;
    ForEachItemIn(iMap, oldSymbols)
    {
        IHqlExpression & cur = oldSymbols.item(iMap);
        if (isExported(&cur) && !cur.isFunction())
        {
            OwnedHqlExpr boundSymbol = boundScope->lookupSymbol(cur.queryName(),LSFpublic,lookupCtx);
            assertex(boundSymbol);
            LinkedHqlExpr newValue = boundSymbol->queryBody();
            if (!cur.isDataset() && !cur.isDatarow())
            {
                //This was a scalar, so need to select the value from it.
                assertex(newValue->isDataset());
                IHqlExpression * field = newValue->queryRecord()->queryChild(0);
                OwnedHqlExpr select = createRow(no_selectnth, newValue.getClear(), createConstantOne());
                newValue.setown(createSelectExpr(select.getClear(), LINK(field)));      // no newAtom because not normalised yet
            }
            mappingSymbols.append(*cur.cloneAllAnnotations(newValue));
        }
    }
    ForEachChild(i2, body)
    {
        IHqlExpression * cur = body->queryChild(i2);
        //Base ourselves on any base classes that the body contains
        if (cur->isScope())
            mappingArgs.append(*LINK(cur));
    }
    IHqlScope * mappingScope = createVirtualScope();
    return populateScopeAndClose(mappingScope, mappingArgs, mappingSymbols);
}

//==========================================================================================================

IHqlExpression * HqlGram::createEvaluateOutputModule(const attribute & errpos, IHqlExpression * scopeExpr, IHqlExpression * ifaceExpr, node_operator outputOp)
{
    if (!ifaceExpr->queryType()->assignableFrom(scopeExpr->queryType()))
        reportError(ERR_NOT_BASE_MODULE, errpos, "Module doesn't implement the interface supplied");
    return ::createEvaluateOutputModule(lookupCtx, scopeExpr, ifaceExpr, lookupCtx.queryExpandCallsWhenBound(), outputOp);
}

IHqlExpression * HqlGram::createStoredModule(const attribute & errpos, IHqlExpression * scopeExpr)
{
    if (!scopeExpr->queryProperty(virtualAtom))
        reportError(ERR_NOT_INTERFACE, errpos, "Argument must be an interface or virtual module");
    return ::createStoredModule(scopeExpr);
}

//==========================================================================================================

static IHqlExpression * createSingleValueTransform(IHqlExpression * record, IHqlExpression * value)
{
    IHqlExpression * field = record->queryChild(0);
    OwnedHqlExpr self = getSelf(record);
    OwnedHqlExpr lhs = createSelectExpr(LINK(self), LINK(field));
    OwnedHqlExpr assign = createAssign(lhs.getClear(), LINK(value));
    return createValue(no_transform, makeTransformType(record->getType()), assign.getClear());
}

IHqlExpression * HqlGram::createIffDataset(IHqlExpression * record, IHqlExpression * value)
{
    IHqlExpression * field = record->queryChild(0);
    if (value->getOperator() == no_select)
    {
        IHqlExpression * lhs = value->queryChild(0);
        IHqlExpression * rhs = value->queryChild(1);
        if ((lhs->getOperator() == no_selectnth) && matchesConstantValue(lhs->queryChild(1), 1))
        {
            IHqlExpression * ds = lhs->queryChild(0);
            if (rhs == field)
                return LINK(ds);

            OwnedHqlExpr seq = createActiveSelectorSequence(ds, NULL);
            OwnedHqlExpr left = createSelector(no_left, ds, seq);
            OwnedHqlExpr selectedValue = createSelectExpr(LINK(left), LINK(rhs));
            OwnedHqlExpr transform = createSingleValueTransform(record, selectedValue);
            return createDatasetF(no_hqlproject, LINK(ds), LINK(transform), LINK(seq), NULL);
        }

    }
    OwnedHqlExpr transform = createSingleValueTransform(record, value);
    return createDataset(no_inlinetable, createValue(no_transformlist, makeNullType(), transform.getClear()), LINK(record));
}


IHqlExpression * HqlGram::createIff(attribute & condAttr, attribute & leftAttr, attribute & rightAttr)
{
    //MORE: PromoteIfType should ideally add a maxlength to the type if possible.
    ITypeInfo * type = checkPromoteIfType(leftAttr, rightAttr);
    OwnedHqlExpr left = leftAttr.getExpr();
    OwnedHqlExpr right = rightAttr.getExpr();
    OwnedHqlExpr field = createField(valueAtom, type, NULL, NULL);
    OwnedHqlExpr record = createRecord(field);
    OwnedHqlExpr lhs = createIffDataset(record, left);
    OwnedHqlExpr rhs = createIffDataset(record, right);
    OwnedHqlExpr ifDs = createDatasetF(no_if, condAttr.getExpr(), lhs.getClear(), rhs.getClear(), NULL);
    OwnedHqlExpr row1 = createRow(no_selectnth, ifDs.getClear(), getSizetConstant(1));
    return createSelectExpr(LINK(row1), LINK(field));
}



IHqlExpression * HqlGram::createListIndex(attribute & list, attribute & which, IHqlExpression * attr)
{
    OwnedHqlExpr expr = list.getExpr();
    ITypeInfo * childType = expr->queryType()->queryChildType();
    if (expr->getOperator() == no_all)
    {
        reportError(ERR_LIST_INDEXINGALL, which, "Indexing ALL is undefined");
        if (!childType)
            childType = defaultIntegralType;
    }
    else if (!childType)
    {
        reportError(ERR_ELEMENT_NO_TYPE, list, "List element has unknown type");
        childType = defaultIntegralType;
    }
    if (which.queryExpr()->getOperator() == no_comma)
        reportError(ERR_NO_MULTI_ARRAY, list, "Multi dimension array index is not supported");

    if (childType && isDatasetType(childType))
        return createDataset(no_rowsetindex, expr.getClear(), createComma(which.getExpr(), attr));
    else
        return createList(no_index, LINK(childType), createComma(expr.getClear(), which.getExpr(), attr));
}

void HqlGram::checkCompatibleTransforms(HqlExprArray & values, IHqlExpression * record, attribute & errpos)
{
    ForEachItemIn(i, values)
    {
        IHqlExpression * curRecord = values.item(i).queryRecord();
        if (!recordTypesMatch(curRecord, record))
        {
            reportError(ERR_TYPE_INCOMPATIBLE, errpos, "All TRANSFORMS must return the same record type");
            break;
        }
    }
}

void HqlGram::canNotAssignTypeError(ITypeInfo* expected, ITypeInfo* given, const attribute& errpos)
{
    StringBuffer msg("Incompatible types: can not assign ");
    getFriendlyTypeStr(given, msg).append(" to ");
    getFriendlyTypeStr(expected, msg);
    reportError(ERR_TYPE_INCOMPATIBLE, errpos, "%s", msg.str());
}

void HqlGram::canNotAssignTypeWarn(ITypeInfo* expected, ITypeInfo* given, const attribute& errpos)
{
    StringBuffer msg("Incompatible types: should cast ");
    getFriendlyTypeStr(given, msg).append(" to a ");
    getFriendlyTypeStr(expected, msg);
    reportWarning(ERR_TYPE_INCOMPATIBLE, errpos.pos, "%s", msg.str());
}

_ATOM HqlGram::getNameFromExpr(attribute& attr)
{
    OwnedHqlExpr expr = attr.getExpr();
    loop
    {
        switch (expr->getOperator())
        {
        case no_select:
            expr.set(expr->queryChild(1));
            break;
        case no_indirect:
            expr.set(expr->queryChild(0));
            break;
        default:
            {
                _ATOM name = expr->queryName();
                if (name && (name != unnamedAtom))
                    return name;
                reportError(ERR_EXPECTED_IDENTIFIER, attr, "Expected an identifier");
                return unknownAtom;
            }
        }
    }
}

_ATOM HqlGram::createFieldNameFromExpr(IHqlExpression * expr)
{
//  while (expr->getOperator() == no_indirect)
//      expr = expr->queryChild(0);

    _ATOM name = expr->queryName();
    if (!name)
    {
        switch (expr->getOperator())
        {
        case no_select:
            return createFieldNameFromExpr(expr->queryChild(1));
        case no_indirect:
            return createFieldNameFromExpr(expr->queryChild(0));
        case no_countgroup:
            name = createUnnamedFieldName("_unnamed_cnt_");
            break;
        case no_existsgroup:
            name = createUnnamedFieldName("_unnamed_exists_");
            break;
        case no_vargroup:
        case no_avegroup:
        case no_maxgroup:
        case no_mingroup:
        case no_sumgroup:
            {
                StringBuffer temp;
                temp.append("_unnamed_").append(getOpString(expr->getOperator())).append("_");
                temp.toLowerCase();
                temp.append(createFieldNameFromExpr(expr->queryChild(0)));
                name = createUnnamedFieldName(temp.str());
            }
            break;
        case no_covargroup:
        case no_corrgroup:
            {
                StringBuffer temp;
                temp.append("_unnamed_").append(getOpString(expr->getOperator())).append("_");
                temp.toLowerCase();
                temp.append(createFieldNameFromExpr(expr->queryChild(0)));
                temp.append("_");
                temp.append(createFieldNameFromExpr(expr->queryChild(1)));
                name = createUnnamedFieldName(temp.str());
            }
            break;
        }
    }
    return name;
}

inline bool isDollarModule(IHqlExpression * expr)
{
    return expr->isAttribute() && (expr->queryName() == selfAtom);
}

IHqlExpression * HqlGram::resolveImportModule(const attribute & errpos, IHqlExpression * expr)
{
    _ATOM name = expr->queryName();
    if (isDollarModule(expr))
        return LINK(queryExpression(globalScope));
    if (name != _dot_Atom)
    {
        if (!lookupCtx.queryRepository())
        {
            //This never happens in practice since a null repository is generally passed.
            reportError(ERR_MODULE_UNKNOWN, "Import not supported with no repository specified",  
                        lexObject->getActualLineNo(), 
                        lexObject->getActualColumn(), 
                        lexObject->get_yyPosition());
            return NULL;
        }

        OwnedHqlExpr importMatch = lookupCtx.queryRepository()->queryRootScope()->lookupSymbol(name, LSFimport, lookupCtx);
        if (!importMatch)
            importMatch.setown(parseScope->lookupSymbol(name, LSFsharedOK, lookupCtx));

        if (!importMatch || !importMatch->queryScope())
        {
            if (lookupCtx.queryParseContext().ignoreUnknownImport)
                return NULL;

            StringBuffer msg;
            if (!importMatch)
                msg.appendf("Import names unknown module \"%s\"", name->getAtomNamePtr()); 
            else
                msg.appendf("Import item  \"%s\" is not a module", name->getAtomNamePtr()); 
            reportError(ERR_MODULE_UNKNOWN, msg.toCharArray(),  
                        lexObject->getActualLineNo(), 
                        lexObject->getActualColumn(), 
                        lexObject->get_yyPosition());
            return NULL;
        }

        return importMatch.getClear();
    }

    OwnedHqlExpr parent = resolveImportModule(errpos, expr->queryChild(0));
    if (!parent)
        return NULL;
    _ATOM childName = expr->queryChild(1)->queryName();
    OwnedHqlExpr resolved = parent->queryScope()->lookupSymbol(childName, LSFpublic, lookupCtx);
    if (!resolved)
    {
        reportError(ERR_OBJ_NOSUCHFIELD, errpos, "Object '%s' does not have a field named '%s'", parent->queryName()->str(), childName->str());
        return NULL;
    }
    IHqlScope * ret = resolved->queryScope();
    if (!ret)
    {
        reportError(ERR_OBJ_NOSUCHFIELD, errpos, "'%s' is not a module", childName->str());
        return NULL;
    }
    return resolved.getClear();
}

void HqlGram::processImportAll(attribute & modulesAttr)
{
    HqlExprArray modules;
    modulesAttr.unwindCommaList(modules);
    ForEachItemIn(i, modules)
    {
        OwnedHqlExpr module = resolveImportModule(modulesAttr, &modules.item(i));
        if (module)
        {
            IHqlScope * moduleScope = module->queryScope();
            if (!defaultScopes.contains(*moduleScope))
                defaultScopes.append(*LINK(moduleScope));
            if (!isDollarModule(&modules.item(i)))
                defineImport(modulesAttr, module, module->queryName());
        }
    }
}

void HqlGram::processImport(attribute & modulesAttr, _ATOM aliasName)
{
    HqlExprArray modules;
    modulesAttr.unwindCommaList(modules);

    if (aliasName && (modules.ordinality() > 1))
    {
        reportError(ERR_BAD_IMPORT, modulesAttr, "Expected a single module with an alias");
        return;
    }

    ForEachItemIn(i, modules)
    {
        IHqlExpression & cur = modules.item(i);
        //IMPORT $; is a no-op.  Don't add the real name of the $ module into scope
        if (!aliasName && isDollarModule(&cur))
            continue;

        OwnedHqlExpr module = resolveImportModule(modulesAttr, &cur);
        if (module)
        {
            _ATOM newName = aliasName ? aliasName : module->queryName();
            defineImport(modulesAttr, module, newName);
            IHqlScope * moduleScope = module->queryScope();
            if (moduleScope->isImplicit())
                defaultScopes.append(*LINK(moduleScope));
        }
    }
}



void HqlGram::processImport(attribute & membersAttr, attribute & modulesAttr, _ATOM aliasName)
{
    HqlExprArray modules;
    HqlExprArray members;
    modulesAttr.unwindCommaList(modules);
    membersAttr.unwindCommaList(members);
    ForEachItemIn(i1, members)
    {
        if (members.item(i1).queryName() == _dot_Atom)
        {
            reportError(ERR_BAD_IMPORT, membersAttr, "Only top level members can be imported");
            return;
        }
    }

    if (modules.ordinality() > 1)
    {
        reportError(ERR_BAD_IMPORT, modulesAttr, "Expected a single module reference");
        return;
    }
    if (aliasName && (members.ordinality() > 1))
    {
        reportError(ERR_BAD_IMPORT, modulesAttr, "Expected a single definition with an alias");
        return;
    }
    OwnedHqlExpr module = resolveImportModule(modulesAttr, &modules.item(0));
    if (!module)
        return;
    IHqlScope * moduleScope = module->queryScope();
    ForEachItemIn(i, members)
    {
        _ATOM name = members.item(i).queryName();
        Owned<IHqlExpression> resolved = moduleScope->lookupSymbol(name, LSFpublic, lookupCtx);
        if (resolved)
        {
            _ATOM newName = aliasName ? aliasName : name;
            defineImport(membersAttr, resolved, newName);
        }
        else
            reportError(ERR_OBJ_NOSUCHFIELD, modulesAttr, "Module '%s' does not contain a definition named '%s'", module->queryName()->str(), name->str());
    }
}


void HqlGram::defineImport(const attribute & errpos, IHqlExpression * imported, _ATOM newName)
{
    if (!imported || !newName)
        return;

    Owned<IHqlExpression> previous = parseScope->lookupSymbol(newName,LSFsharedOK,lookupCtx);
    if (previous)
    {
        if (previous->queryBody() == imported->queryBody())
            return;

        reportWarning(ERR_ID_REDEFINE, errpos.pos, "import hides previously defined identifier");
    }

    parseScope->defineSymbol(newName, NULL, LINK(imported), false, false, ob_import);
    lookupCtx.noteImport(imported, newName, errpos.pos.position);
}


IHqlExpression * HqlGram::expandedSortListByReference(attribute * module, attribute & list)
{
    OwnedHqlExpr ds;
    if (module)
    {
        ds.setown(module->getExpr());
    }
    else
    {
        ds.set(queryTopScope());
        if (!ds || !ds->queryRecord())
        {
            reportError(ERR_OBJ_NOACTIVEDATASET, list, "No active dataset to resolve field list");
            return getSizetConstant(0);
        }
    }

    OwnedHqlExpr sortlist = list.getExpr();
    switch (sortlist->getOperator())
    {
    case no_param:
        return LINK(sortlist);
    case no_sortlist:
        break;
    default:
        throwUnexpected();
    }
    OwnedHqlExpr ret;
    ForEachChild(i, sortlist)
    {
        ret.setown(createComma(ret.getClear(), createIndirectSelect(LINK(ds), LINK(sortlist->queryChild(i)), list)));
    }
    return ret.getClear();
}

static void getTokenText(StringBuffer & msg, int token)
{
    switch (token)
    {
    case ABS: msg.append("ABS"); break;
    case ACOS: msg.append("ACOS"); break;
    case AFTER: msg.append("AFTER"); break;
    case AGGREGATE: msg.append("AGGREGATE"); break;
    case ALIAS: msg.append("__ALIAS__"); break;
    case ALL: msg.append("ALL"); break;
    case ALLNODES: msg.append("ALLNODES"); break;
    case AND: msg.append("AND"); break;
    case ANDAND: msg.append("&&"); break;
    case ANY: msg.append("ANY"); break;
    case APPLY: msg.append("APPLY"); break;
    case _ARRAY_: msg.append("_ARRAY_"); break;
    case AS: msg.append("AS"); break;
    case ASCII: msg.append("ASCII"); break;
    case ASIN: msg.append("ASIN"); break;
    case TOK_ASSERT: msg.append("ASSERT"); break;
    case ASSTRING: msg.append("ASSTRING"); break;
    case ATAN: msg.append("ATAN"); break;
    case ATAN2: msg.append("ATAN2"); break;
    case ATMOST: msg.append("ATMOST"); break;
    case AVE: msg.append("AVE"); break;
    case BACKUP: msg.append("BACKUP"); break;
    case BEFORE: msg.append("BEFORE"); break;
    case BEST: msg.append("BEST"); break;
    case BETWEEN: msg.append("BETWEEN"); break;
    case BIG: msg.append("BIG_ENDIAN"); break;
    case TOK_BITMAP: msg.append("BITMAP"); break;
    case BLOB: msg.append("BLOB"); break;
    case BNOT: msg.append("BNOT"); break;
    case BUILD: msg.append("BUILD"); break;
    case CARDINALITY: msg.append("CARDINALITY"); break;
    case CASE: msg.append("CASE"); break;
    case TOK_CATCH: msg.append("CATCH"); break;
    case CHECKPOINT: msg.append("CHECKPOINT"); break;
    case CHOOSE: msg.append("CHOOSE"); break;
    case CHOOSEN: msg.append("CHOOSEN"); break;
    case CHOOSENALL: msg.append("CHOOSEN:ALL"); break;
    case CHOOSESETS: msg.append("CHOOSESETS"); break;
    case CLUSTER: msg.append("CLUSTER"); break;
    case CLUSTERSIZE: msg.append("CLUSTERSIZE"); break;
    case COGROUP: msg.append("COGROUP"); break;
    case COMBINE: msg.append("COMBINE"); break;
    case __COMMON__: msg.append("__COMMON__"); break;
    case __COMPOUND__: msg.append(""); break;
    case COMPRESSED: msg.append("COMPRESSED"); break;
    case __COMPRESSED__: msg.append("__COMPRESSED__"); break;
    case TOK_CONST: msg.append("CONST"); break;
    case CORRELATION: msg.append("CORRELATION"); break;
    case COS: msg.append("COS"); break;
    case COSH: msg.append("COSH"); break;
    case COUNT: msg.append("COUNT"); break;
    case COUNTER: msg.append("COUNTER"); break;
    case COVARIANCE: msg.append("COVARIANCE"); break;
    case CPPBODY: msg.append("BEGINC++"); break;
    case CRC: msg.append("HASHCRC"); break;
    case CRON: msg.append("CRON"); break;
    case CSV: msg.append("CSV"); break;
    case DATASET: msg.append("DATASET"); break;
    case __DEBUG__: msg.append("__DEBUG__"); break;
    case DEDUP: msg.append("DEDUP"); break;
    case DEFINE: msg.append("DEFINE"); break;
    case DENORMALIZE: msg.append("DENORMALIZE"); break;
    case DEPRECATED: msg.append("DEPRECATED"); break;
    case DESC: msg.append("DESC"); break;
    case DISTRIBUTE: msg.append("DISTRIBUTE"); break;
    case DISTRIBUTED: msg.append("DISTRIBUTED"); break;
    case DISTRIBUTION: msg.append("DISTRIBUTION"); break;
    case DIV: msg.append("DIV"); break;
    case DOTDOT: msg.append(".."); break;
    case DYNAMIC: msg.append("DYNAMIC"); break;
    case EBCDIC: msg.append("EBCDIC"); break;
    case ECLCRC: msg.append("ECLCRC"); break;
    case ELSE: msg.append("ELSE"); break;
    case ELSEIF: msg.append("ELSEIF"); break;
    case EMBEDDED: msg.append("EMBEDDED"); break;
    case _EMPTY_: msg.append("_EMPTY_"); break;
    case ENCODING: msg.append("ENCODING"); break;
    case ENCRYPT: msg.append("ENCRYPT"); break;
    case ENCRYPTED: msg.append("ENCRYPTED"); break;
    case END: msg.append("END"); break;
    case ENDCPP: msg.append("ENDCPP"); break;
    case ENTH: msg.append("ENTH"); break;
    case ENUM: msg.append("ENUM"); break;
    case TOK_ERROR: msg.append("ERROR"); break;
    case EVALUATE: msg.append("EVALUATE"); break;
    case EVENT: msg.append("EVENT"); break;
    case EVENTEXTRA: msg.append("EVENTEXTRA"); break;
    case EVENTNAME: msg.append("EVENTNAME"); break;
    case EXCEPT: msg.append("EXCEPT"); break;
    case EXCLUSIVE: msg.append("EXCLUSIVE"); break;
    case EXISTS: msg.append("EXISTS"); break;
    case EXP: msg.append("expression"); break;
    case EXPIRE: msg.append("EXPIRE"); break;
    case EXPORT: msg.append("EXPORT"); break;
    case EXTEND: msg.append("EXTEND"); break;
    case FAIL: msg.append("FAIL"); break;
    case FAILCODE: msg.append("FAILCODE"); break;
    case FAILMESSAGE: msg.append("FAILMESSAGE"); break;
    case FAILURE: msg.append("FAILURE"); break;
    case FEATURE: msg.append("FEATURE"); break;
    case FETCH: msg.append("FETCH"); break;
    case FEW: msg.append("FEW"); break;
    case TOK_FALSE: msg.append("FALSE"); break;
    case FIELD_REF: msg.append("<?>"); break;
    case FIELDS_REF: msg.append("<\?\?>"); break;
    case FILEPOSITION: msg.append("FILEPOSITION"); break;
    case FILTERED: msg.append("FILTERED"); break;
    case FIRST: msg.append("FIRST"); break;
    case TOK_FIXED: msg.append("FIXED"); break;
    case FLAT: msg.append("FLAT"); break;
    case FORMAT_ATTR: msg.append("FORMAT"); break;
    case FORWARD: msg.append("FORWARD"); break;
    case FROM: msg.append("FROM"); break;
    case FROMUNICODE: msg.append("FROMUNICODE"); break;
    case FROMXML: msg.append("FROMXML"); break;
    case FULL: msg.append("FULL"); break;
    case FUNCTION: msg.append("FULL"); break;
    case GETENV: msg.append("GETENV"); break;
    case GLOBAL: msg.append("GLOBAL"); break;
    case GRAPH: msg.append("GRAPH"); break;
    case GROUP: msg.append("GROUP"); break;
    case GROUPBY: msg.append("GROUPBY"); break;
    case GROUPED: msg.append("GROUPED"); break;
    case __GROUPED__: msg.append("__GROUPED__"); break;
    case GUARD: msg.append("GUARD"); break;
    case HASH: msg.append("HASH"); break;
    case HASH32: msg.append("HASH32"); break;
    case HASH64: msg.append("HASH64"); break;
    case HASHMD5: msg.append("HASHMD5"); break;
    case HAVING: msg.append("HAVING"); break;
    case HEADING: msg.append("HEADING"); break;
    case HINT: msg.append("HINT"); break;
    case HOLE: msg.append("HOLE"); break;
    case IF: msg.append("IF"); break;
    case IFF: msg.append("IFF"); break;
    case IFBLOCK: msg.append("IFBLOCK"); break;
    case TOK_IGNORE: msg.append("IGNORE"); break;
    case IMPLEMENTS: msg.append("IMPLEMENTS"); break;
    case IMPORT: msg.append("IMPORT"); break;
    case INDEPENDENT: msg.append("INDEPENDENT"); break;
    case INDEX: msg.append("INDEX"); break;
    case INLINE: msg.append("INLINE"); break;
    case TOK_IN: msg.append("IN"); break;
    case INNER: msg.append("INNER"); break;
    case INTERFACE: msg.append("INTERFACE"); break;
    case INTERNAL: msg.append("INTERNAL"); break;
    case INTFORMAT: msg.append("INTFORMAT"); break;
    case ISNULL: msg.append("ISNULL"); break;
    case ISVALID: msg.append("ISVALID"); break;
    case ITERATE: msg.append("ITERATE"); break;
    case JOIN: msg.append("JOIN"); break;
    case JOINED: msg.append("JOINED"); break;
    case KEEP: msg.append("KEEP"); break;
    case KEYDIFF: msg.append("KEYDIFF"); break;
    case KEYED: msg.append("KEYED"); break;
    case KEYPATCH: msg.append("KEYPATCH"); break;
    case KEYUNICODE: msg.append("KEYUNICODE"); break;
    case LABELED: msg.append("LABELED"); break;
    case LAST: msg.append("LAST"); break;
    case LEFT: msg.append("LEFT"); break;
    case LENGTH: msg.append("LENGTH"); break;
    case LIBRARY: msg.append("LIBRARY"); break;
    case LIMIT: msg.append("LIMIT"); break;
    case _LINKCOUNTED_: msg.append("_LINKCOUNTED_"); break;
    case LITERAL: msg.append("LITERAL"); break;
    case LITTLE: msg.append("LITTLE_ENDIAN"); break;
    case LN: msg.append("LN"); break;
    case LOADXML: msg.append("LOADXML"); break;
    case LOCAL: msg.append("LOCAL"); break;
    case LOCALE: msg.append("LOCALE"); break;
    case LOCALFILEPOSITION: msg.append("LOCALFILEPOSITION"); break;
    case TOK_LOG: msg.append("LOG"); break;
    case LOGICALFILENAME: msg.append("LOGICALFILENAME"); break;
    case LOOKUP: msg.append("LOOKUP"); break;
    case LOOP: msg.append("LOOP"); break;
    case LZW: msg.append("LZW"); break;
    case MANY: msg.append("MANY"); break;
    case MAP: msg.append("MAP"); break;
    case MATCHED: msg.append("MATCHED"); break;
    case MATCHLENGTH: msg.append("MATCHLENGTH"); break;
    case MATCHPOSITION: msg.append("MATCHPOSITION"); break;
    case MATCHROW: msg.append("MATCHROW"); break;
    case MATCHTEXT: msg.append("MATCHTEXT"); break;
    case MATCHUNICODE: msg.append("MATCHUNICODE"); break;
    case MATCHUTF8: msg.append("MATCHUTF8"); break;
    case MAX: msg.append("MAX"); break;
    case MAXCOUNT: msg.append("MAXCOUNT"); break;
    case MAXLENGTH: msg.append("MAXLENGTH"); break;
    case MAXSIZE: msg.append("MAXSIZE"); break;
    case MERGE: msg.append("MERGE"); break;
    case MERGE_ATTR: msg.append("MERGE"); break;
    case MERGEJOIN: msg.append("MERGEJOIN"); break;
    case MIN: msg.append("MIN"); break;
    case MODULE: msg.append("MODULE"); break;
    case MOFN: msg.append("MOFN"); break;
    case NAMED: msg.append("NAMED"); break;
    case NAMEOF: msg.append("__NAMEOF__"); break;
    case NAMESPACE: msg.append("NAMESPACE"); break;
    case NOBOUNDCHECK: msg.append("NOBOUNDCHECK"); break;
    case NOCASE: msg.append("NOCASE"); break;
    case NOFOLD: msg.append("NOFOLD"); break;
    case NOHOIST: msg.append("NOHOIST"); break;
    case NOLOCAL: msg.append("NOLOCAL"); break;
    case NONEMPTY: msg.append("NONEMPTY"); break;
    case NOOVERWRITE: msg.append("NOOVERWRITE"); break;
    case NORMALIZE: msg.append("NORMALIZE"); break;
    case NOROOT: msg.append("NOROOT"); break;
    case NOSCAN: msg.append("NOSCAN"); break;
    case NOSORT: msg.append("NOSORT"); break;
    case __NOSTREAMING__: msg.append(""); break;        // internal
    case NOT: msg.append("NOT"); break;
    case NOTHOR: msg.append("NOTHOR"); break;
    case NOTIFY: msg.append("NOTIFY"); break;
    case NOTRIM: msg.append("NOTRIM"); break;
    case OF: msg.append("OF"); break;
    case OMITTED: msg.append("OMITTED"); break;
    case ONCE: msg.append("ONCE"); break;
    case ONFAIL: msg.append("ONFAIL"); break;
    case ONLY: msg.append("ONLY"); break;
    case ONWARNING: msg.append("ONWARNING"); break;
    case OPT: msg.append("OPT"); break;
    case OR : msg.append("OR "); break;
    case ORDER: msg.append("ORDER"); break;
    case OUTER: msg.append("OUTER"); break;
    case OUTPUT: msg.append("OUTPUT"); break;
    case TOK_OUT: msg.append("OUT"); break;
    case OVERWRITE: msg.append("OVERWRITE"); break;
    case __OWNED__: msg.append("__OWNED__"); break;
    case PACKED: msg.append("PACKED"); break;
    case PARALLEL: msg.append("PARALLEL"); break;
    case PARSE: msg.append("PARSE"); break;
    case PARTITION: msg.append("PARTITION"); break;
    case PARTITION_ATTR: msg.append("PARTITION"); break;
    case TOK_PATTERN: msg.append("PATTERN"); break;
    case PAYLOAD: msg.append("PAYLOAD"); break;
    case PENALTY: msg.append("PENALTY"); break;
    case PERSIST: msg.append("PERSIST"); break;
    case PHYSICALFILENAME: msg.append("PHYSICALFILENAME"); break;
    case PIPE: msg.append("PIPE"); break;
    case POWER: msg.append("POWER"); break;
    case PREFETCH: msg.append("PREFETCH"); break;
    case PRELOAD: msg.append("PRELOAD"); break;
    case PRIORITY: msg.append("PRIORITY"); break;
    case PRIVATE: msg.append("PRIVATE"); break;
    case PROCESS: msg.append("PROCESS"); break;
    case PROJECT: msg.append("PROJECT"); break;
    case PULL: msg.append("PULL"); break;
    case PULLED: msg.append("PULLED"); break;
    case QUOTE: msg.append("QUOTE"); break;
    case RANDOM: msg.append("RANDOM"); break;
    case RANGE: msg.append("RANGE"); break;
    case RANK: msg.append("RANK"); break;
    case RANKED: msg.append("RANKED"); break;
    case REALFORMAT: msg.append("REALFORMAT"); break;
    case RECORD: msg.append("RECORD"); break;
    case RECORDOF: msg.append("RECORDOF"); break;
    case RECOVERY: msg.append("RECOVERY"); break;
    case REGEXFIND: msg.append("REGEXFIND"); break;
    case REGEXREPLACE: msg.append("REGEXREPLACE"); break;
    case REGROUP: msg.append("REGROUP"); break;
    case REJECTED: msg.append("REJECTED"); break;
    case RELATIONSHIP: msg.append("RELATIONSHIP"); break;
    case REMOTE: msg.append("REMOTE"); break;
    case REPEAT: msg.append("REPEAT"); break;
    case RESPONSE: msg.append("RESPONSE"); break;
    case RETRY: msg.append("RETRY"); break;
    case RETURN: msg.append("RETURN"); break;
    case RIGHT: msg.append("RIGHT"); break;
    case RIGHT_NN: msg.append("RIGHT2"); break;
    case ROLLUP: msg.append("ROLLUP"); break;
    case ROUND: msg.append("ROUND"); break;
    case ROUNDUP: msg.append("ROUNDUP"); break;
    case ROW: msg.append("ROW"); break;
    case ROWS: msg.append("ROWS"); break;
    case ROWSET: msg.append("ROWSET"); break;
    case ROWDIFF: msg.append("ROWDIFF"); break;
    case RULE: msg.append("RULE"); break;
    case SAMPLE: msg.append("SAMPLE"); break;
    case SCAN: msg.append("SCAN"); break;
    case SECTION: msg.append("SECTION"); break;
    case SELF: msg.append("SELF"); break;
    case SEPARATOR: msg.append("SEPARATOR"); break;
    case __SEQUENCE__: msg.append("__SEQUENCE__"); break;
    case SEQUENTIAL: msg.append("SEQUENTIAL"); break;
    case SERVICE: msg.append("SERVICE"); break;
    case SET: msg.append("SET"); break;
    case SHARED: msg.append("SHARED"); break;
    case SIN: msg.append("SIN"); break;
    case SINGLE: msg.append("SINGLE"); break;
    case SINH: msg.append("SINH"); break;
    case SIZEOF: msg.append("SIZEOF"); break;
    case SKEW: msg.append("SKEW"); break;
    case SKIP: msg.append("SKIP"); break;
    case SOAPACTION: msg.append("SOAPACTION"); break;
    case __STAND_ALONE__: msg.append("__STAND_ALONE__"); break;
    case HTTPHEADER: msg.append("HTTPHEADER"); break;
    case PROXYADDRESS: msg.append("PROXYADDRESS"); break;
    case HTTPCALL: msg.append("HTTPCALL"); break;
    case SOAPCALL: msg.append("SOAPCALL"); break;
    case SORT: msg.append("SORT"); break;
    case SORTED: msg.append("SORTED"); break;
    case SQL: msg.append("SQL"); break;
    case SQRT: msg.append("SQRT"); break;
    case STABLE: msg.append("STABLE"); break;
    case STEPPED: msg.append("STEPPED"); break;
    case STORED: msg.append("STORED"); break;
    case STREAMED: msg.append("STREAMED"); break;
    case SUCCESS: msg.append("SUCCESS"); break;
    case SUM: msg.append("SUM"); break;
    case SWAPPED: msg.append("SWAPPED"); break;
    case TABLE: msg.append("TABLE"); break;
    case TAN: msg.append("TAN"); break;
    case TANH: msg.append("TANH"); break;
    case TERMINATOR: msg.append("TERMINATOR"); break;
    case THEN: msg.append("THEN"); break;
    case THISNODE: msg.append("THISNODE"); break;
    case THOR: msg.append("THOR"); break;
    case THRESHOLD: msg.append("THRESHOLD"); break;
    case TIMEOUT: msg.append("TIMEOUT"); break;
    case TIMELIMIT: msg.append("TIMELIMIT"); break;
    case TOKEN: msg.append("TOKEN"); break;
    case TOPN: msg.append("TOPN"); break;
    case TOUNICODE: msg.append("TOUNICODE"); break;
    case TOXML: msg.append("TOXML"); break;
    case TRANSFER: msg.append("TRANSFER"); break;
    case TRANSFORM: msg.append("TRANSFORM"); break;
    case TRIM: msg.append("TRIM"); break;
    case TRUNCATE: msg.append("TRUNCATE"); break;
    case TOK_TRUE: msg.append("TRUE"); break;
    case TYPE: msg.append("TYPE"); break;
    case TYPEOF: msg.append("TYPEOF"); break;
    case UNGROUP: msg.append("UNGROUP"); break;
    case UNICODEORDER: msg.append("UNICODEORDER"); break;
    case UNORDERED: msg.append("UNORDERED"); break;
    case UNSIGNED: msg.append("UNSIGNED"); break;
    case UNSORTED: msg.append("UNSORTED"); break;
    case UNSTABLE: msg.append("UNSTABLE"); break;
    case UPDATE: msg.append("UPDATE"); break;
    case USE: msg.append("USE"); break;
    case VALIDATE: msg.append("VALIDATE"); break;
    case VARIANCE: msg.append("VARIANCE"); break;
    case VIRTUAL: msg.append("VIRTUAL"); break;
    case WAIT: msg.append("WAIT"); break;
    case TOK_WARNING: msg.append("WARNING"); break;
    case WHEN: msg.append("WHEN"); break;
    case WHICH: msg.append("WHICH"); break;
    case WIDTH: msg.append("WIDTH"); break;
    case WILD: msg.append("WILD"); break;
    case WITHIN: msg.append("WITHIN"); break;
    case WHOLE: msg.append("WHOLE"); break;
    case WORKUNIT: msg.append("WORKUNIT"); break;
    case XML_TOKEN: msg.append("XML"); break;
    case XMLDECODE: msg.append("XMLDECODE"); break;
    case XMLDEFAULT: msg.append("XMLDEFAULT"); break;
    case XMLENCODE: msg.append("XMLENCODE"); break;
    case XMLPROJECT: msg.append("XMLPROJECT"); break;
    case XMLTEXT: msg.append("XMLTEXT"); break;
    case XMLUNICODE: msg.append("XMLUNICODE"); break;
    case XPATH: msg.append("XPATH"); break;

    case HASH_CONSTANT: msg.append("#CONSTANT"); break;
    case HASH_ONWARNING: msg.append("#ONWARNING"); break;
    case HASH_OPTION: msg.append("#OPTION"); break;
    case HASH_STORED: msg.append("#STORED"); break;
    case HASH_LINK: msg.append("#LINK"); break;
    case HASH_WORKUNIT: msg.append("#WORKUNIT"); break;
    case SIMPLE_TYPE: msg.append("<typename>"); break;

    case EQ: msg.append("="); break;
    case NE: msg.append("<>"); break;
    case LE: msg.append("<="); break;
    case LT: msg.append("<"); break;
    case GE: msg.append(">="); break;
    case GT: msg.append(">"); break;
    case ASSIGN: msg.append(":="); break;
    case GOESTO: msg.append("=>"); break;
    case SHIFTL: msg.append("<<"); break;
    case SHIFTR: msg.append(">>"); break;

    case DATASET_ID: msg.append("dataset"); break;
    case DATAROW_ID: msg.append("datarow"); break;
    case RECORD_ID: msg.append("record-name"); break;
    case RECORD_FUNCTION: msg.append("record-name"); break;
    case VALUE_ID: msg.append("identifier"); break;
    case VALUE_ID_REF: msg.append("field reference"); break;
    case UNKNOWN_ID: msg.append("identifier"); break;
    case SCOPE_ID: msg.append("module-name"); break;
    case ACTION_ID: msg.append("identifier"); break;
    case TRANSFORM_ID: msg.append("transform-name"); break;
    case ALIEN_ID: msg.append("type name"); break;
    case TYPE_ID: msg.append("type name"); break;
    case SET_TYPE_ID: msg.append("type name"); break;
    case PATTERN_TYPE_ID: msg.append("type name"); break;
    case PATTERN_ID: msg.append("pattern-name"); break;
    case FEATURE_ID: msg.append("feature-name"); break;
    case EVENT_ID: msg.append("identifier"); break;
    case ENUM_ID: msg.append("identifier"); break;
    case LIST_DATASET_ID: msg.append("identifier"); break;
    case SORTLIST_ID: msg.append("field list"); break;

    case ACTION_FUNCTION: msg.append("action"); break;
    case PATTERN_FUNCTION: msg.append("pattern"); break;
    case EVENT_FUNCTION: msg.append("event"); break;
    case SCOPE_FUNCTION: msg.append("module-name"); break;
    case TRANSFORM_FUNCTION: msg.append("transform-name"); break;
    case DATAROW_FUNCTION: msg.append("datarow"); break;
    case LIST_DATASET_FUNCTION: msg.append("identifier"); break;

    case VALUE_FUNCTION: 
    case DATASET_FUNCTION: 
        msg.append("function-name"); 
        break;

    case REAL_CONST:
    case INTEGER_CONST: msg.append("number"); break;
    case STRING_CONST: msg.append("string"); break;
    case UNICODE_CONST: msg.append("unicode-string"); break;
    case DATA_CONST: msg.append("constant"); break;
    case TYPE_LPAREN: msg.append("(>"); break;
    case TYPE_RPAREN: msg.append("<)"); break;
    case HASHBREAK: msg.append("#BREAK"); break;
    case SKIPPED: msg.append("SKIPPED"); break;
    case HASHEND: msg.append("#END"); break;
    case HASHELIF: msg.append("#ELIF"); break;      //? not in the lexer...

    case MACRO: msg.append("MACRO"); break;
    case COMPLEX_MACRO: msg.append("complex-macro"); break;
    case VALUE_MACRO:   msg.append("macro-name"); break;
    case DEFINITIONS_MACRO: msg.append("macro-name"); break;
    case ENDMACRO: msg.append("ENDMACRO"); break;
    case INTERNAL_READ_NEXT_TOKEN: break;

    default:
        if (token < 128)
            msg.appendf("'%c'", (char)token);
        else
        {
            /* if fail, use "hqltest -internal" to find out why. */
            msg.appendf("???");
            //PrintLog("Internal error: Error handler unknown token %d", expected[i]);
            assertex(!"Token not mapped to text");
        }
    }
}

inline bool containsToken(int first, const int * expected)
{
    for (const int *finger = expected;*finger; finger++)
    {
        if (*finger == first)
            return true;
    }
    return false;
}

inline void removeToken(int token, int * expected)
{
    for (int *finger = expected;*finger; finger++)
    {
        if (*finger == token)
        {
            *finger = ' ';
            break;
        }
    }
}

void HqlGram::simplify(int *expected, int first, ...)
{
    if (!containsToken(first, expected))
        return;

    va_list args;
    va_start(args, first);
    for (;;)
    {
        int parm = va_arg(args, int);
        if (!parm)
            break;
        removeToken(parm, expected);
    }
    va_end(args);
}

void HqlGram::simplifyExpected(int *expected)
{
    //simplify checks if the first item in the list is expected next, and if so it removes all of the others as expected tokens.
    simplify(expected, DISTRIBUTE, DISTRIBUTE, ASCII, CHOOSEN, CHOOSESETS, DEDUP, DISTRIBUTED, EBCDIC, ENTH, SAMPLE, SORT, SORTED, TABLE, DATASET, FETCH,
                       GROUP, GROUPED, KEYED, UNGROUP, JOIN, PULL, ROLLUP, ITERATE, PROJECT, NORMALIZE, PIPE, DENORMALIZE, CASE, MAP, 
                       HTTPCALL, SOAPCALL, LIMIT, PARSE, FAIL, MERGE, PRELOAD, ROW, TOPN, ALIAS, LOCAL, NOFOLD, NOHOIST, NOTHOR, IF, GLOBAL, __COMMON__, __COMPOUND__, TOK_ASSERT, _EMPTY_,
                       COMBINE, ROWS, REGROUP, XMLPROJECT, SKIP, LOOP, CLUSTER, NOLOCAL, REMOTE, PROCESS, ALLNODES, THISNODE, GRAPH, MERGEJOIN, STEPPED, NONEMPTY, HAVING,
                       TOK_CATCH, '@', SECTION, WHEN, IFF, COGROUP, HINT, INDEX, PARTITION, AGGREGATE, 0);
    simplify(expected, EXP, ABS, SIN, COS, TAN, SINH, COSH, TANH, ACOS, ASIN, ATAN, ATAN2, 
                       COUNT, CHOOSE, MAP, CASE, IF, HASH, HASH32, HASH64, HASHMD5, CRC, LN, TOK_LOG, POWER, RANDOM, ROUND, ROUNDUP, SQRT, 
                       TRUNCATE, LENGTH, TRIM, INTFORMAT, REALFORMAT, ASSTRING, TRANSFER, MAX, MIN, EVALUATE, SUM,
                       AVE, VARIANCE, COVARIANCE, CORRELATION, WHICH, REJECTED, SIZEOF, RANK, RANKED, COUNTER, '+', '-', '(', '~', TYPE_LPAREN, ROWDIFF, WORKUNIT,
                       FAILCODE, FAILMESSAGE, FROMUNICODE, __GROUPED__, ISNULL, ISVALID, XMLDECODE, XMLENCODE, XMLTEXT, XMLUNICODE,
                       MATCHED, MATCHLENGTH, MATCHPOSITION, MATCHTEXT, MATCHUNICODE, MATCHUTF8, NOFOLD, NOHOIST, NOTHOR, OPT, REGEXFIND, REGEXREPLACE, RELATIONSHIP, SEQUENTIAL, SKIP, TOUNICODE, UNICODEORDER, UNSORTED,
                       KEYUNICODE, TOK_TRUE, TOK_FALSE, NOT, EXISTS, WITHIN, LEFT, RIGHT, SELF, '[', HTTPCALL, SOAPCALL, ALL, TOK_ERROR, TOK_CATCH, __COMMON__, __COMPOUND__, RECOVERY, CLUSTERSIZE, CHOOSENALL, BNOT, STEPPED, ECLCRC, NAMEOF,
                       TOXML, '@', SECTION, EVENTEXTRA, EVENTNAME, __SEQUENCE__, IFF, OMITTED, GETENV, __DEBUG__, __STAND_ALONE__, 0);
    simplify(expected, DATA_CONST, REAL_CONST, STRING_CONST, INTEGER_CONST, UNICODE_CONST, 0);
    simplify(expected, VALUE_MACRO, DEFINITIONS_MACRO, 0);
    simplify(expected, VALUE_ID, DATASET_ID, RECORD_ID, ACTION_ID, UNKNOWN_ID, SCOPE_ID, VALUE_FUNCTION, DATAROW_FUNCTION, DATASET_FUNCTION, LIST_DATASET_FUNCTION, LIST_DATASET_ID, ALIEN_ID, TYPE_ID, SET_TYPE_ID, TRANSFORM_ID, TRANSFORM_FUNCTION, RECORD_FUNCTION, FEATURE_ID, EVENT_ID, EVENT_FUNCTION, SCOPE_FUNCTION, ENUM_ID, PATTERN_TYPE_ID, 0); 
    simplify(expected, LIBRARY, LIBRARY, SCOPE_FUNCTION, STORED, PROJECT, INTERFACE, MODULE, 0);
    simplify(expected, MATCHROW, MATCHROW, LEFT, RIGHT, IF, IFF, ROW, HTTPCALL, SOAPCALL, PROJECT, GLOBAL, NOFOLD, NOHOIST, ALLNODES, THISNODE, SKIP, DATAROW_FUNCTION, TRANSFER, RIGHT_NN, FROMXML, 0);
    simplify(expected, TRANSFORM_ID, TRANSFORM_FUNCTION, TRANSFORM, '@', 0);
    simplify(expected, RECORD, RECORDOF, RECORD_ID, RECORD_FUNCTION, SCOPE_ID, VALUE_MACRO, '{', '@', 0);
    simplify(expected, IFBLOCK, ANY, PACKED, BIG, LITTLE, 0);
    simplify(expected, SCOPE_ID, '$', 0);
    simplify(expected, END, '}', 0);
}

void HqlGram::syntaxError(const char *s, int token, int *expected)
{ 
    if (errorDisabled || !s || !errorHandler)
        return;

    int lineno = lexObject->getActualLineNo();
    int column = lexObject->getActualColumn();
    int pos = lexObject->get_yyPosition();
    const char * yytext = lexObject->get_yyText();
    if (yytext)
        column -= strlen(yytext);

    if (expected)
        simplifyExpected(expected);

    StringBuffer msg;
    if (expected == NULL) // expected is NULL when fatal internal error occurs.
    {
        msg.append(s);
    } 
    else if (token==UNKNOWN_ID)
    {
        msg.append("Unknown identifier");
        if (yytext && *yytext)
            msg.append(" \"").append(yytext).append('\"');

        reportError(ERR_UNKNOWN_IDENTIFIER,msg.toCharArray(), lineno, column, pos);
        return;
    }
    else if ((token == '.') && (expected[0] == ASSIGN) && !expected[1])
    {
        reportError(ERR_UNKNOWN_IDENTIFIER,"Unknown identifier before \".\" (expected :=)", lineno, column, pos);
        return;
    }
    else switch(token)
    {
    case DATAROW_ID:
    case DATASET_ID:
    case SCOPE_ID:
    case VALUE_ID:
    case VALUE_ID_REF:
    case ACTION_ID:
    case UNKNOWN_ID:
    case ENUM_ID:
    case RECORD_ID:
    case ALIEN_ID:
    case TYPE_ID:
    case SET_TYPE_ID:
    case PATTERN_TYPE_ID:
    case TRANSFORM_ID:
    case PATTERN_ID:
    case FEATURE_ID:
    case EVENT_ID:
    case LIST_DATASET_ID:
    case DATAROW_FUNCTION:
    case DATASET_FUNCTION:
    case VALUE_FUNCTION:
    case ACTION_FUNCTION:
    case PATTERN_FUNCTION:
    case RECORD_FUNCTION:
    case EVENT_FUNCTION:
    case SCOPE_FUNCTION:
    case TRANSFORM_FUNCTION:
    case LIST_DATASET_FUNCTION:
    {
        for (int j = 0; expected[j] ; j++)
        {
            if (expected[j]==UNKNOWN_ID)
            {
                msg.append("Identifier '");
                if (yytext && *yytext)
                    msg.append(yytext);
                msg.append("' is already defined");
                reportError(ERR_ID_REDEFINE,msg.toCharArray(), lineno, column, pos);
                return;
            }
        }
        // fall into...
    }
    
    default:
        msg.append(s);
        if (yytext && *yytext)
        { 
            if (yytext[0]=='\n')
                msg.append(" near the end of the line");
            else
                msg.append(" near \"").append(yytext).append('\"');
        }

        bool first = true;
        for (int i = 0; expected[i] ; i++)
        {
            if (expected[i] == ' ')
                continue;
            if (first)
                msg.append(" : expected ");
            else
                msg.append(", ");

            first = false;
            getTokenText(msg, expected[i]);
        }
    }

    reportError(ERR_EXPECTED, msg.toCharArray(), lineno, column, pos);
}


void HqlGram::abortParsing()
{
    // disable more error report
    disableError();
    aborting = true;
}

IHqlExpression * HqlGram::createCheckMatchAttr(attribute & attr, type_t tc)
{
    OwnedITypeInfo type;
    switch (tc)
    {
    case type_unicode:
        type.setown(makeUnicodeType(UNKNOWN_LENGTH, NULL));
        break;
    case type_string:
        type.setown(makeStringType(UNKNOWN_LENGTH, NULL));
        break;
    case type_utf8:
        type.setown(makeUtf8Type(UNKNOWN_LENGTH, NULL));
        break;
    default:
        throwUnexpectedType(type);
    }

    OwnedHqlExpr arg = attr.getExpr();
    if (arg->getOperator() == no_matchattr)
    {
        IHqlExpression * path= arg->isDatarow() ? arg->queryChild(1) : arg->queryChild(0);
        return createValue(no_matchattr, LINK(type), LINK(path)); //, createUniqueId());
    }

    reportError(ERR_EXPECTED_ATTRIBUTE, attr, "Expected an attribute");
    return createNullExpr(type);
}


void HqlGram::checkWorkflowScheduling(IHqlExpression * expr, attribute& errpos)
{
    if (!expr)
        return;

    HqlExprArray args;
    expr->unwindList(args, no_comma);

    bool hasScheduling = false;
    ForEachItemIn(idx, args)
    {
        IHqlExpression * cur = &args.item(idx);

        switch (cur->getOperator())
        {
        case no_when:
        case no_priority:
            hasScheduling = true;
            break;
        }
    }
    if(hasScheduling)
        reportError(ERR_SCHEDULING_DEFINITION, errpos, "Scheduling is not allowed on definitions");
}

void HqlGram::checkWorkflowMultiples(IHqlExpression * previousWorkflow, IHqlExpression * newWorkflow, attribute & errpos)
{
    if(!previousWorkflow || !newWorkflow) return;
    node_operator newOp = newWorkflow->getOperator();
    HqlExprArray workflows;
    previousWorkflow->unwindList(workflows, no_comma);
    _ATOM newName = newWorkflow->queryName();
    ForEachItemIn(idx, workflows)
    {
        IHqlExpression & cur = workflows.item(idx);
        node_operator oldOp = cur.getOperator();
        switch(newOp)
        {
        case no_persist:
        case no_stored:
        case no_checkpoint:
        case no_once:
            if((oldOp==no_persist)||(oldOp==no_stored)||(oldOp==no_once)||(oldOp==no_checkpoint))
                reportError(ERR_MULTIPLE_WORKFLOW, errpos, "Multiple scoping controls are not allowed on an action or expression");
            break;
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            if (cur.isAttribute() && cur.queryName() == newName)
            {
                if (newName != onWarningAtom)
                {
                    StringBuffer buff;
                    buff.append("Multiple ").append(newName).append(" controls are not allowed on an expression");
                    reportError(ERR_MULTIPLE_WORKFLOW, errpos, "%s", buff.str());
                }
            }
            break;
        default:
            if(oldOp == newOp)
            {
                StringBuffer buff;
                buff.append("Multiple ").append(getOpString(newOp)).append(" controls are not allowed on an expression");
                reportError(ERR_MULTIPLE_WORKFLOW, errpos, "%s", buff.str());
            }
        }
    }
}

bool HqlGram::isSaved(IHqlExpression * failure)
{
    if (!failure) return false;
    HqlExprArray args;
    failure->unwindList(args, no_comma);
    ForEachItemIn(idx, args)
    {
        switch (args.item(idx).getOperator())
        {
        case no_persist:
        case no_stored:
        case no_checkpoint:
        case no_global:
        case no_once:
            return true;
        }
    }
    return false;
}

void HqlGram::checkPatternFailure(attribute & attr)
{
    IHqlExpression * failure = attr.queryExpr();
    if (!failure)
        return;
    HqlExprArray args;
    failure->unwindList(args, no_comma);
    ForEachItemIn(idx, args)
    {
        IHqlExpression & cur = args.item(idx);
        if (!cur.isAttribute() || (cur.queryName() != defineAtom))
            reportError(ERR_INVALIDPATTERNCLAUSE, attr, "Invalid clause for a pattern definition");
    }
    if (failure->getOperator() == no_comma)
        reportError(ERR_INVALIDPATTERNCLAUSE, attr, "Only one define allowed on each pattern definition");
}

bool HqlGram::isExceptionalCase(attribute& defineid, attribute& object, attribute& failure)
{
    bool isBad = false;

    IHqlExpression *expr = object.queryExpr();
    if (expr == NULL)
        isBad = true;
    else if (expr->getOperator()==no_loadxml)
    {
        reportError(ERR_LOADXML_NOANATTRIBUTE, defineid, "LOADXML is not an definition: it can be used only alone for test purpose");
        isBad = true;
    }

    if (isBad)
    {
        defineid.release();
        object.release();
        failure.release();
    }

    return isBad;
}

static void expandDefJoinAttrs(IHqlExpression * newRec, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            if (!newRec->hasProperty(cur->queryName()))
                newRec->addOperand(LINK(cur));
            break;
        }
    }
}

static void checkExtraCommonFields(IHqlSimpleScope * scope, IHqlExpression * record, bool * hasExtra, HqlExprCopyArray & common)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr match = scope->lookupSymbol(cur->queryName());
                if (!match)
                    *hasExtra = true;
                else
                    common.append(*match);
                break;
            }
        case no_record:
            checkExtraCommonFields(scope, cur, hasExtra, common);
            break;
        case no_ifblock:
            checkExtraCommonFields(scope, cur->queryChild(1), hasExtra, common);
            break;
        }
    }
}

static void expandDefJoinFields(IHqlExpression * newRec, IHqlSimpleScope * scope, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            {
                OwnedHqlExpr match = scope->lookupSymbol(cur->queryName());
                if (!match)
                    newRec->addOperand(LINK(cur));
                break;
            }
        case no_record:
            expandDefJoinFields(newRec, scope, cur);
            break;
        case no_ifblock:
            expandDefJoinFields(newRec, scope, cur->queryChild(1));
            break;
        }
    }
}


void HqlGram::addConditionalAssign(const attribute & errpos, IHqlExpression * self, IHqlExpression * leftSelect, IHqlExpression * rightSelect, IHqlExpression * field)
{
    IHqlSimpleScope * rightScope = rightSelect->queryRecord()->querySimpleScope();
    OwnedHqlExpr tgt = createSelectExpr(LINK(self), LINK(field));
    OwnedHqlExpr leftSrc = createSelectExpr(LINK(leftSelect), LINK(field));
    OwnedHqlExpr rightSrc = createSelectExpr(LINK(rightSelect), rightScope->lookupSymbol(field->queryName()));
    if (field->isDatarow())
    {
        addConditionalRowAssign(errpos, tgt, leftSrc, rightSrc, field->queryRecord());
    }
    else
    {
        OwnedHqlExpr null = createNullExpr(leftSrc);
        OwnedHqlExpr src = createIf(createBoolExpr(no_ne, LINK(leftSrc), LINK(null)), LINK(leftSrc), LINK(rightSrc));
        addAssignment(errpos, tgt, src);
    }
}

void HqlGram::addConditionalRowAssign(const attribute & errpos, IHqlExpression * self, IHqlExpression * leftSelect, IHqlExpression * rightSelect, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_field:
            addConditionalAssign(errpos, self, leftSelect, rightSelect, cur);
            break;
        case no_record:
            addConditionalRowAssign(errpos, self, leftSelect, rightSelect, cur);
            break;
        case no_ifblock:
            addConditionalRowAssign(errpos, self, leftSelect, rightSelect, cur->queryChild(1));
            break;
        }
    }
}

IHqlExpression* HqlGram::createDefJoinTransform(IHqlExpression* left,IHqlExpression* right,attribute& errpos, IHqlExpression * seq, IHqlExpression * flags)
{
    IHqlExpression * leftRecord = left->queryRecord();
    IHqlExpression * rightRecord = right->queryRecord();

    OwnedHqlExpr res_rec;
    bool hasExtraFields = false;
    HqlExprCopyArray commonFields;
    checkExtraCommonFields(leftRecord->querySimpleScope(), rightRecord, &hasExtraFields, commonFields);
    if (hasExtraFields)
    {
        //
        // create result record
        CHqlRecord * newRec = (CHqlRecord *)createRecord();

        //MORE: maxlength should be handled better
        expandDefJoinAttrs(newRec, leftRecord);
        expandDefJoinAttrs(newRec, rightRecord);
        //Clone left - including ifblocks etc.
        ForEachChild(i, leftRecord)
        {
            IHqlExpression *field = leftRecord->queryChild(i);
            if (!field->isAttribute())
                newRec->addOperand(LINK(field));
        }
        expandDefJoinFields(newRec, newRec->querySimpleScope(), rightRecord);

        res_rec.setown(newRec->closeExpr());
    }
    else
        res_rec.set(leftRecord);

    // create transform
    beginTransform(res_rec->queryRecordType());

    OwnedHqlExpr self = createSelector(no_self, res_rec, NULL);
    OwnedHqlExpr leftSelect = createSelector(no_left, left, seq);
    OwnedHqlExpr rightSelect = createSelector(no_right, right, seq);

    if (queryPropertyInList(fullouterAtom, flags) || queryPropertyInList(fullonlyAtom, flags))
    {
        ForEachItemIn(i, commonFields)
        {
            IHqlExpression & cur = commonFields.item(i);
            addConditionalAssign(errpos, self, leftSelect, rightSelect, &cur);
        }
    }

    if (queryPropertyInList(rightouterAtom, flags) || queryPropertyInList(rightonlyAtom, flags))
    {
        addAssignall(LINK(self), LINK(rightSelect), errpos);
        addAssignall(LINK(self), LINK(leftSelect), errpos);
    }
    else
    {
        addAssignall(LINK(self), LINK(leftSelect), errpos);
        addAssignall(LINK(self), LINK(rightSelect), errpos);
    }

    // close transform
    checkAllAssigned(res_rec, errpos);
    return endTransform(errpos);
}

IHqlExpression * HqlGram::createProjectRow(attribute & rowAttr, attribute & transformAttr, attribute & seqAttr)
{
    OwnedHqlExpr row = rowAttr.getExpr();
    OwnedHqlExpr transform = transformAttr.getExpr();
    OwnedHqlExpr seq = seqAttr.getExpr();

    return createRow(no_projectrow, row.getClear(), createComma(transform.getClear(), seq.getClear()));
}

void HqlGram::beginTransform(ITypeInfo * type)
{
    if (curTransform)
    {
        TransformSaveInfo * saved = new TransformSaveInfo;
        saved->curTransform.setown(curTransform);
        saved->transformScope.setown(transformScope);
        saved->selfUsedOnRhs = selfUsedOnRhs;
        transformSaveStack.append(*saved);
    }
    curTransform = createOpenValue(no_transform, makeTransformType(LINK(type)));
    transformScope = createPrivateScope();
    enterScope(transformScope, false);
#ifdef FAST_FIND_FIELD
    lockTransformMutex();
#endif
}

IHqlExpression * HqlGram::endTransform(const attribute &errpos)
{
    if (!curTransform)
        return NULL;

    ::Release(transformScope);
    transformScope = NULL;
    leaveScope(errpos);
#ifdef FAST_FIND_FIELD
    unlockTransformMutex();
#endif
    IHqlExpression *ret = curTransform->closeExpr();
    curTransform = NULL;
    selfUsedOnRhs = false;
    if (transformSaveStack.ordinality())
    {
        Owned<TransformSaveInfo> saved = &transformSaveStack.popGet();
        transformScope = saved->transformScope.getClear();
        curTransform = saved->curTransform.getClear();
        selfUsedOnRhs = saved->selfUsedOnRhs;
    }
    return ret;
}


void HqlGram::beginRecord()
{
    IHqlExpression *r = createRecord();
    pushSelfScope(r);
    pushRecord(r); 
    LinkedHqlExpr locale = queryDefaultLocale();
    if (!locale)
        locale.setown(createConstant(""));
    pushLocale(locale.getClear());
}

void HqlGram::addRecordOption(attribute & attr)
{
    OwnedHqlExpr expr = attr.getExpr();
    if (expr->queryName() == localeAtom)
    {
        popLocale();
        pushLocale(LINK(expr->queryChild(0)));
    }
    else
        activeRecords.tos().addOperand(LINK(expr));
}


void HqlGram::checkSizeof(IHqlExpression* expr, attribute& errpos)
{
    if (!expr || !expr->queryType())
        return;

    type_t tc = expr->queryType()->getTypeCode();
    switch(tc)
    {
    case type_record: 
    case type_row: 
        {
            //MORE: I'm not at all convinced about this code for type_row.
            bool isDataset = expr->isDataset();
            ForEachChild(idx,expr)
            {
                IHqlExpression *kid = expr->queryChild(idx);

                if (kid->getOperator() == no_ifblock && isDataset)
                    reportError(ERR_SIZEOF_WRONGPARAM,errpos,"Can not determine the size of record with IFBLOCK");
                else if (kid->getOperator() == no_field)
                {
                    if (kid->isDataset())
                        checkSizeof(kid,errpos); // recursion
                    else if (kid->queryRecord())
                        checkSizeof(kid->queryRecord(),errpos); // recursion
                    else
                        checkSizeof(kid->queryType(),errpos,isDataset);
                }
                else if (kid->getOperator() == no_record)
                    checkSizeof(kid, errpos);
            }
            break;
        }
    
    default: 
        checkSizeof(expr->queryType(),errpos);
        break;
    }
}

void HqlGram::checkSizeof(ITypeInfo* type, attribute& errpos, bool isDataset)
{
    if (type)
    {
        type_t tc = type->getTypeCode();
        switch(tc)
        {
        case type_bitfield: 
            if (!isDataset)
                reportError(ERR_SIZEOF_WRONGPARAM,errpos,"Can not determine the size of BITFIELD");
            break;          
        case type_set:
            reportError(ERR_SIZEOF_WRONGPARAM,errpos,"Can not determine the size of SET");
            break;
        case type_qstring: 
            if (type->getSize() == UNKNOWN_LENGTH)
                reportError(ERR_SIZEOF_WRONGPARAM,errpos,"SIZEOF: QSTRING has unknown size");
            break;
        case type_varstring: 
            if (type->getSize() == UNKNOWN_LENGTH)
                reportError(ERR_SIZEOF_WRONGPARAM,errpos,"SIZEOF: VARSTRING has unknown size");
            break;
        case type_string:
            if (type->getSize() == UNKNOWN_LENGTH)
                reportError(ERR_SIZEOF_WRONGPARAM,errpos,"SIZEOF: STRING has unknown size");
            break;
        case type_alien:
            reportError(ERR_SIZEOF_WRONGPARAM,errpos,"SIZEOF: Can not determine the size of alien type ");
            break;
        default: 
            break;
        }
    }
}

void HqlGram::checkFieldMap(IHqlExpression* map, attribute& errpos)
{
    HqlExprArray maps;
    map->unwindList(maps, no_comma);    
    unsigned mapCount = maps.length();

    // check duplication
    for (unsigned i=0; i<mapCount; i+=2)
    {
        for (unsigned j=i+2;j<mapCount; j+=2)
            if (maps.item(i).queryName()==maps.item(j).queryName())
                reportError(ERR_DSPARM_MAPDUPLICATE, errpos, "Field '%s' is mapped more than once", maps.item(i).queryName()->str());
    }

    // Note: a->b, b->c is allowed. Even the following are allowed:
    //   a->b, b->c: switch two fields
    //   a->c, b->c: merge two fields
}

void HqlGram::setTemplateAttribute()
{
#ifndef NEW_VIRTUAL_DATASETS
    ActiveScopeInfo & activeScope = defineScopes.tos();
    if (!activeScope.templateAttrContext) 
        activeScope.templateAttrContext.setown(new CHqlContextScope());
#endif
}


IHqlExpression * reparseTemplateFunction(IHqlExpression * funcdef, IHqlScope *scope, HqlLookupContext & ctx, bool hasFieldMap)
{
    // get func body text
    IHqlExpression * symbol = queryAnnotation(funcdef, annotate_symbol);
    assertex(symbol);
    IHqlNamedAnnotation * nameExtra = static_cast<IHqlNamedAnnotation *>(symbol->queryAnnotation());
    assertex(nameExtra);
    Owned<IFileContents> contents = nameExtra->getBodyContents();
    StringBuffer text;
    text.append("=>").append(contents->length(), contents->getText());

    //Could use a merge string implementation of IFileContents instead of expanding...
    Owned<IFileContents> parseContents = createFileContentsFromText(text.str(), contents->querySourcePath());
    HqlGram parser(scope, scope, parseContents, ctx, NULL, hasFieldMap, true);
    unsigned startLine = funcdef->getStartLine();

    //MORE: I need a better calculation of the column/line that the body begins at 
    //e.g. if multiple lines of parameters etc.
    //I may need to add annotations to the funcdef to save the body column and the body line
    parser.getLexer()->set_yyLineNo(startLine);
    parser.getLexer()->set_yyColumn(1);         // need to take off 2 for => that has been added.

    //MORE: May also need to setup current_type
    return parser.yyParse(true, false);
}
                        
//===============================================================================================

PseudoPatternScope::PseudoPatternScope(IHqlExpression * _patternList) : CHqlScope(no_privatescope, NULL, NULL)
{
    patternList = _patternList;     // Do not link!
}

IHqlExpression * PseudoPatternScope::lookupSymbol(_ATOM name, unsigned lookupFlags, HqlLookupContext & ctx)
{
    const char * text = name->str();
    if (*text == '$')
    {
        unsigned index = atoi(text+1);
        IHqlExpression * match = patternList->queryChild(index-1);
        if (match)
        {
            Owned<ITypeInfo> retType;
            ITypeInfo * type = match->queryType()->queryChildType();
            IHqlExpression * indexExpr = createConstant((__int64)index-1);
            if (type)
                return createRow(no_matchattr, LINK(queryOriginalRecord(type)), indexExpr);

            return createValue(no_matchattr, makeStringType(UNKNOWN_LENGTH, NULL), indexExpr);
        }
    }
    return NULL;
}


//---------------------------------------------------------------------------------------------------------------------

extern HQL_API IHqlExpression * parseQuery(IHqlScope *scope, IFileContents * contents, HqlLookupContext & ctx, IXmlScope *xmlScope, bool loadImplicit)
{
    assertex(scope);
    try
    {
        ctx.noteBeginQuery(scope, contents);

        HqlGram parser(scope, scope, contents, ctx, xmlScope, false, loadImplicit);
        parser.setQuery(true);
        parser.getLexer()->set_yyLineNo(1);
        parser.getLexer()->set_yyColumn(1);
        OwnedHqlExpr ret = parser.yyParse(false, true);
        ctx.noteEndQuery();
        return parser.clearFieldMap(ret.getClear());
    }
    catch (IException *E)
    {
        if (ctx.errs)
        {
            ISourcePath * sourcePath = contents->querySourcePath();
            if (E->errorCode()==0)
            {
                StringBuffer s;
                ctx.errs->reportError(ERR_INTERNALEXCEPTION, E->errorMessage(s).toCharArray(), sourcePath->str(), 0, 0, 1);
            }
            else
            {
                StringBuffer s("Internal error: ");
                E->errorMessage(s);
                ctx.errs->reportError(ERR_INTERNALEXCEPTION, s.toCharArray(), sourcePath->str(), 0, 0, 1);
            }
        }
        E->Release();
    }
    return NULL;
}


extern HQL_API IHqlExpression * parseQuery(const char * text, IErrorReceiver * errs)
{
    Owned<IHqlScope> scope = createScope();
    HqlDummyLookupContext ctx(errs);
    Owned<IFileContents> contents = createFileContentsFromText(text, NULL);
    return parseQuery(scope, contents, ctx, NULL, true);
}


bool parseForwardModuleMember(HqlGramCtx & _parent, IHqlScope *scope, IHqlExpression * forwardSymbol, HqlLookupContext & ctx)
{
    //The attribute will be added to the current scope as a side-effect of parsing the attribute.
    IFileContents * contents = forwardSymbol->queryDefinitionText();
    HqlGram parser(_parent, scope, contents, NULL); 
    parser.setExpectedAttribute(forwardSymbol->queryName());
    parser.setAssociateWarnings(true);
    parser.getLexer()->set_yyLineNo(forwardSymbol->getStartLine());
    parser.getLexer()->set_yyColumn(forwardSymbol->getStartColumn());
    unsigned prevErrors = ctx.errs->errCount();
    ::Release(parser.yyParse(false, false));
    return (prevErrors == ctx.errs->errCount());
}


void parseAttribute(IHqlScope * scope, IFileContents * contents, HqlLookupContext & ctx, _ATOM name)
{
    HqlLookupContext attrCtx(ctx);
    attrCtx.noteBeginAttribute(scope, contents, name);

    //The attribute will be added to the current scope as a side-effect of parsing the attribute.
    const char * moduleName = scope->queryFullName();
    Owned<IHqlScope> globalScope = getResolveDottedScope(moduleName, LSFpublic, ctx);
    HqlGram parser(globalScope, scope, contents, attrCtx, NULL, false, true);
    parser.setExpectedAttribute(name);
    parser.setAssociateWarnings(true);
    parser.getLexer()->set_yyLineNo(1);
    parser.getLexer()->set_yyColumn(1);
    ::Release(parser.yyParse(false, false));
    attrCtx.noteEndAttribute();
}

void testHqlInternals()
{
    //
    // test getOpString()
    int error = 0,i;
    node_operator op;

    printf("Testing getOpString()...\n");
    for (op = no_none; op < no_last_op; op = (node_operator)(op+1))
    {
        try {
            getOpString(op);
        } catch (...) {
            error++;
            printf("   Error: getOpString(%d) is not defined\n",op);
        }
    }

    //
    // test HqlGram::yyError
    printf("Testing HqlGram::yyError() ...\n");
    for (i= 258; i<YY_LAST_TOKEN;i++)
    {
        try {
            StringBuffer temp;
            getTokenText(temp, i);
        } catch (...) {
            error++;
            printf("   Error: getTokenText() does not handle expected: %d\n",i);
        }
    }

    // 
    // report test result
    if (error)
        printf("%d error%s found!\n", error, error<=1?"":"s");
    else
        printf("No errors\n");
}

IHqlExpression *HqlGram::yyParse(bool _parsingTemplateAttribute, bool catchAbort)
{
    parsingTemplateAttribute = _parsingTemplateAttribute;
    try 
    {
        return doParse();
    }
    catch (IException * e)
    {
        if (e->errorCode() != ERR_ABORT_PARSING)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            reportError(e->errorCode(), msg.str(), 0, 0, 0);
        }
        else if (!catchAbort)
            throw;


        e->Release();
        return NULL;
    }
    catch(int)
    {
        return NULL;
    }
    catch (RELEASE_CATCH_ALL)
    {
        PrintLog("Unexpected exception caught");
        return NULL;
    }
}

extern int eclyyparse (HqlGram* parser);

IHqlExpression *HqlGram::doParse()
{
    if (expectedAttribute)
    {
        if (queryExpression(containerScope)->getOperator() == no_forwardscope)
            enterScope(containerScope, true);
        if (legacyEclSemantics)
            enterScope(globalScope, true);

        //If expecting a particular attribute, add symbols to a private scope, and then copy result across
        enterScope(parseScope, true);
    }
    else
    {
        //Either a query that is not part of the source tree, or an entire module (plugins/legacy support), parse direct into the scope
        enterScope(containerScope, true);
    }
    minimumScopeIndex = defineScopes.ordinality();

    unsigned prevErrors = errorHandler ? errorHandler->errCount() : 0;
    if (eclyyparse(this) != 0)
        return NULL;
    unsigned nowErrors = errorHandler ? errorHandler->errCount() : 0;
    if (prevErrors != nowErrors)
        return NULL;

    lookupCtx.noteFinishedParse(defineScopes.tos().privateScope);
    lookupCtx.noteFinishedParse(parseScope);
    if (parsingTemplateAttribute)
    {
        if (parseResults.ordinality() == 0)
            return NULL;

        if (parseResults.ordinality() != 1)
            reportError(ERR_EXPORT_OR_SHARE, "Expected a single result", 1, 1, 1);
        return &parseResults.popGet();
    }

    if (expectedAttribute)
    {
        //If we are expecting an attribute, then we either have a list of side-effects which are returned as a compound object
        //or we have a field defined in the parseScope which needs cloning into the container scope
        OwnedHqlExpr resolved = parseScope->lookupSymbol(expectedAttribute, LSFsharedOK, lookupCtx);

        //Cover an ugly case where a module with the same name as the attribute being defined has been imported.
        //This should really have a isImport flag like export/shared to avoid false positives.
        if (resolved && parseResults.ordinality())
        {
            if (resolved->queryScope() && isImport(resolved))
                resolved.clear();
        }

        if (resolved)
        {
            if (!parseResults.empty())
            {
                ECLlocation location(&parseResults.item(0));
                reportError(ERR_RESULT_IGNORED, location, "Definition contains actions after the EXPORT has been defined");
            }

            containerScope->defineSymbol(resolved.getClear());
            return NULL;
        }
    }

    if (parseResults.empty())
    {
        //if (expectedAttribute) .....
        //An error about a missing definition will be reported by the caller so don't duplicate
        return NULL;
    }

    //Check the results are ok - i.e. if a module, then only a single attribute
    unsigned lastPos = parseResults.ordinality() - 1;
    bool hadValue = false;
    ForEachItemIn(i, parseResults)
    {
        IHqlExpression & cur = parseResults.item(i);
        ECLlocation location(&cur);
        if (cur.getOperator() == no_setmeta)
            continue;

        if (i == lastPos)
        {
            if (cur.isScope() && hadValue)
                reportError(ERR_EXPORT_OR_SHARE, location, "A scope result cannot be combined with any other results");
        }
        else
        {
            hadValue = true;

            //All but the last result should be actions.
            if (!cur.isAction())
            {
                node_operator actionOp = no_none;
                if (cur.isDataset())
                    actionOp = no_output;
                else if (cur.isDatarow())
                    actionOp = no_output;
                else if (cur.isList() || cur.queryType()->isScalar())
                    actionOp = no_outputscalar;

                if (actionOp == no_none)
                {
                    reportError(ERR_EXPORT_OR_SHARE, location, "Object cannot be used as an action");
                }
                else
                    parseResults.replace(*createValue(actionOp, makeVoidType(), LINK(&cur)), i);
            }
        }
    }

    OwnedHqlExpr actions = createCompound(parseResults);
    actions.setown(attachPendingWarnings(actions.getClear()));

    if (!expectedAttribute)
        return actions.getClear();

    _ATOM moduleName = createIdentifierAtom(globalScope->queryFullName());
    Owned<IFileContents> contents = LINK(lexObject->query_FileContents());
    unsigned lengthText = 0;
    containerScope->defineSymbol(expectedAttribute, moduleName, actions.getClear(), true, false, 0, contents, 1, 1, 0, 0, lengthText);
    return NULL;
}

