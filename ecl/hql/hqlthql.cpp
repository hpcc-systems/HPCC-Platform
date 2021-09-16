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
#include "hqlthql.hpp"
#include <limits.h>
#include "jmisc.hpp"
#include "jlog.hpp"

#include "hqltrans.ipp"
#include "hqlutil.hpp"
#include "hqlmeta.hpp"
#include "workunit.hpp"
#include "hqlerrors.hpp"

//The following constants can be uncommented to increase the level of detail which is added to the processed graphs
//E.g. generated when -fl used in hqltest

//#define SHOWADDRSYM
//#define SHOWBRACKETS
//#define SHOWADDR
//#define SHOWTYPES
//#define SHOWFLAGS
//#define SHOWCONTEXTDETAIL
//#define SHOWFLAGSVALUE
//#define SHOW_TABLES
//#define SHOW_TABLES_EXISTANCE
//#define SHOWCRC
//#define SHOW_ANNOTATIONS
//#define SHOW_NORMALIZED
//#define SHOW_EXPAND_LEFTRIGHT
//#define SHOW_DSRECORD
//#define SHOW_DISTRIBUTION
//#define SHOW_ORDER
//#define SHOW_GROUPING
//#define SHOW_GROUPING_DETAIL
//#define SHOW_MODULE_STATUS
//#define SHOW_FUNC_DEFINTION
//#define SHOW_SYMBOL_LOCATION

#define MAX_GRAPHTEXT_LEN   80      // Truncate anything more than 80 characters

bool endsWithDotDotDot(const StringBuffer & s)
{
    if (s.length() < 3)
        return false;
    return (memcmp(s.str() + s.length() -3, "...", 3) == 0);
}

typedef CopyReferenceArrayOf<HqlExprArray> HqlExprArrayArray;

class HqltHql
{
public:

    HqltHql(bool recurse, bool _xgmmlGraphText);
    ~HqltHql();

    void toECL(IHqlExpression *expr, StringBuffer &s, bool paren, bool inType, unsigned recordIndex=0, bool isNamedSymbol=false);
    StringBuffer & gatherDefinitions(StringBuffer & out);
    StringBuffer & gatherServices(StringBuffer & out) { return out.append(m_services); }
    void setExpandProcessed(bool value)     { expandProcessed = value; }
    void setExpandNamed(bool value)         { expandNamed = value; }
    void setIgnoreModuleNames(bool value)   { ignoreModuleNames = value; }
    void setIgnoreVirtualAttrs(bool value)  { ignoreVirtualAttrs = value; }
    void setLowerCaseIds(bool value)        { lowerCaseIds = value; }
    void setMinimalSelectors(bool value)    { minimalSelectors = value; }
    void setMaxRecurseDepth(int depth)      { maxDatasetDepth = depth; }
    void setTryToRegenerate(bool value)         { tryToRegenerate = value; }
    StringBuffer &doAlias(IHqlExpression * expr, StringBuffer &name, bool inType);

private:
    void childrenToECL(IHqlExpression *expr, StringBuffer &s, bool inType, bool needComma, unsigned first);
    void defaultToECL(IHqlExpression *expr, StringBuffer &s, bool inType);
    void defaultChildrenToECL(IHqlExpression *expr, StringBuffer &s, bool inType);

    void appendSortOrder(StringBuffer & s, const char * prefix, IHqlExpression * order);
    void createPseudoSymbol(StringBuffer & s, const char * prefix, IHqlExpression * expr);
    void expandTransformValues(StringBuffer & s, IHqlExpression * expr, bool & first);
    StringBuffer &getTypeString(ITypeInfo * i, StringBuffer &s);
    StringBuffer &getFieldTypeString(IHqlExpression * e, StringBuffer &s);
    const char *getEclOpString(node_operator op);
    void defineCallTarget(IHqlExpression * expr, StringBuffer & name);
    bool isFunctionDefined(IHqlExpression * expr);
    IHqlExpression * querySymbolDefined(IHqlExpression * expr);
    bool isSymbolDefined(IHqlExpression * expr) { return querySymbolDefined(expr) != NULL; }
    bool isAlienTypeDefined(ITypeInfo * type);
    IHqlExpression * queryAlienDefinition(ITypeInfo * type);
    void addExport(StringBuffer &s);
    void clearVisited();
    StringBuffer &callEclFunction(StringBuffer &s, IHqlExpression * expr, bool inType);
    bool isSelect(IHqlExpression * expr);
    StringBuffer &lookupSymbolName(IHqlExpression * expr, StringBuffer &s);
    StringBuffer &makeUniqueName(IHqlExpression * expr, StringBuffer &s);
    void addVisited(IHqlExpression * expr);
    HqlExprArray * findVisitedArray();
    void clearAllVisitedArrays();
    bool isServiceDefined(IHqlExpression * expr);
    bool isExportDefined(IHqlExpression * expr);
    void doFunctionDefinition(StringBuffer & newdef, IHqlExpression * funcdef, const char * name, bool inType);
    void sortlistToEcl(IHqlExpression *expr, StringBuffer &s, bool addCurleys, bool inType);

    bool matchesActiveDataset(IHqlExpression * expr);
    void pushScope(IHqlExpression * expr);
    void popScope();
    void mapDatasetRecord(IHqlExpression * expr);
    void popMapping();
    bool queryAddDotDotDot(StringBuffer & s, unsigned startLength);
    IHqlExpression * queryMapped(IHqlExpression * expr);
    IHqlExpression * queryChild(IHqlExpression * e, unsigned i);

    StringBuffer & appendId(StringBuffer & s, IIdAtom * name);
    StringBuffer & queryNewline(StringBuffer &s);

    bool isExported(IHqlExpression * expr)
    {
        return ::isExported(expr) && !tryToRegenerate;
    }

    bool isShared(IHqlExpression * expr)
    {
        return ::isShared(expr) && !tryToRegenerate;
    }

    bool isPublicSymbol(IHqlExpression * expr)
    {
        return ::isPublicSymbol(expr) && !tryToRegenerate;
    }

    bool isInternalAttribute(IHqlExpression * e)
    {
        if (e->isAttribute())
        {
            IAtom * name= e->queryName();
            if ((name == sequenceAtom) || isInternalAttributeName(name))
                return true;
            if ((name == updateAtom) && e->hasAttribute(alwaysAtom))
                return true;
            if (tryToRegenerate)
            {
                if (name == jobTempAtom || name == loopFirstAtom)
                    return true;
            }
        }
        return false;
    }

private:
    bool          m_recurse;
    bool          ignoreModuleNames;
    bool          insideNewTransform;
    int           maxDatasetDepth;
    int           curDatasetDepth;
    StringBuffer  m_definitions;
    StringBuffer  m_services;
    IArray         m_visitedAlienTypes;
    bool          m_isTop;
    int           m_export_level;
    unsigned      indent;
    bool          xgmmlGraphText;
    bool          expandNamed;
    bool          expandProcessed;
    bool          ignoreVirtualAttrs;
    bool          lowerCaseIds;
    bool          minimalSelectors;
    bool          tryToRegenerate;
    StringBufferArray m_exports;
    StringBufferArray m_service_names;
    StringBufferArray m_export_names;
    HqlExprArrayArray m_visited_array;
    PointerArray        scope;
    HqlExprArray    mapped;
    IPointerArray   mapSaved;
    unsigned        clashCounter;
};

HqltHql::HqltHql(bool recurse, bool _xgmmlGraphText) :
    m_recurse(recurse),
    maxDatasetDepth(_xgmmlGraphText ? 1 : INT_MAX),
    m_isTop(true),
    m_export_level(0)
{
    lockTransformMutex();
    indent = 1;
    xgmmlGraphText = _xgmmlGraphText;
    curDatasetDepth = 0;
    ignoreModuleNames = false;
    expandNamed = true;
    expandProcessed = false;
    insideNewTransform = false;
    ignoreVirtualAttrs = false;
    minimalSelectors = false;
    lowerCaseIds = false;
    tryToRegenerate = false;
    clashCounter = 0;
}

HqltHql::~HqltHql()
{
    clearAllVisitedArrays();
    unlockTransformMutex();
}

StringBuffer & HqltHql::appendId(StringBuffer & s, IIdAtom * id)
{
    if (lowerCaseIds)
        return s.append(str(lower(id)));
    else
        return s.append(str(id));
}


StringBuffer &HqltHql::makeUniqueName(IHqlExpression * expr, StringBuffer &s)
{
    IIdAtom * moduleName = expr->queryFullContainerId();
    if (moduleName && !ignoreModuleNames)
    {
        if (isPublicSymbol(expr))
        {
            if (xgmmlGraphText)
                appendId(s, moduleName).append(".");
            else
            {
                const char * moduleNameText = str(lower(moduleName));
                for (;;)
                {
                    const char * dot = strchr(moduleNameText, '.');
                    if (!dot)
                    {
                        s.append(moduleNameText);
                        break;
                    }
                    s.append(dot-moduleNameText, moduleNameText).append("__");
                    moduleNameText = dot+1;
                }
                s.append("__");
            }
        }
    }

    appendId(s, expr->queryId());
#ifdef SHOWADDRSYM
    if (expandProcessed)
        s.appendf("[%p:%p]",expr, expr->queryBody());
#endif
    return s;
}

bool HqltHql::isSelect(IHqlExpression * expr)
{
    return expr->getOperator() == no_select;
}


void HqltHql::mapDatasetRecord(IHqlExpression * expr)
{ 
    IHqlExpression * record = expr->queryRecord()->queryBody();
    mapped.append(*LINK(expr)); 
    IInterface * mapping = LINK(record->queryTransformExtra());
    mapSaved.append(mapping);
    if (!mapping && expr->queryName())
        record->setTransformExtra(expr);
}

void HqltHql::popMapping()
{ 
    OwnedHqlExpr expr = &mapped.popGet();
    IHqlExpression * record = expr->queryRecord()->queryBody();
    if (record->queryTransformExtra() == expr)
        record->setTransformExtra(mapSaved.tos());
    mapSaved.pop();
}

IHqlExpression * HqltHql::queryMapped(IHqlExpression * expr)
{
    for (;;)
    {
        IHqlExpression * extra = (IHqlExpression *)expr->queryTransformExtra();
        if (!extra || extra->isAttribute() || extra == expr)
            return expr;
        if (expr->queryName() == lower(unnamedId))
            return expr;
        expr = extra;
    }
}



void HqltHql::addExport(StringBuffer &s)
{
    // Need a map of StringBuffers indexed by m_export_level
    for(int idx = m_exports.ordinality(); idx <= m_export_level; idx++)
    {
        m_exports.append(*new StringBufferItem);
    }
    m_exports.item(m_export_level).append(s);
}

StringBuffer & HqltHql::gatherDefinitions(StringBuffer & out)
{
    out.append(m_definitions);
    for(int idx = (m_exports.length() - 1); idx >=0; idx--)
        out.append(m_exports.item(idx));
    return out;
}

void HqltHql::addVisited(IHqlExpression * expr)
{
    findVisitedArray()->append(*LINK(expr));
}

HqlExprArray * HqltHql::findVisitedArray()
{
    for(int idx = m_visited_array.ordinality(); idx <= m_export_level; idx++)
        m_visited_array.append(*(new HqlExprArray));
    return &(m_visited_array.item(m_export_level));
}

void HqltHql::clearVisited()
{
    HqlExprArray  * visited = findVisitedArray();
    ForEachItemInRev(idx, *visited)
    {
        IHqlExpression * expr = &(visited->item(idx));
        if (!isPublicSymbol(expr))
        {
            visited->remove(idx);
        }
    }
}

void HqltHql::clearAllVisitedArrays()
{
    // Empty the array of visited IHqlExpression Arrays
    for(unsigned jdx = 0; jdx < m_visited_array.length(); jdx++)
    {
        delete &(m_visited_array.item(jdx));
    }
    m_visited_array.kill();
}

bool HqltHql::isServiceDefined(IHqlExpression * expr)
{
    StringBuffer name;
    makeUniqueName(expr, name);

    for(unsigned jdx = 0; jdx < m_service_names.length(); jdx++)
    {
        if(!stricmp(m_service_names.item(jdx).str(), name.str()))
            return true;
    }
    return false;
}

bool HqltHql::isExportDefined(IHqlExpression * expr)
{
    StringBuffer name;
    makeUniqueName(expr, name);

    for(unsigned jdx = 0; jdx < m_export_names.length(); jdx++)
    {
        if(!stricmp(m_export_names.item(jdx).str(), name.str()))
            return true;
    }
    return false;
}

bool HqltHql::isFunctionDefined(IHqlExpression * expr)
{
    // THIS FUNCTION IS EXPORTED !!! CHECK THE OTHER EXPORTS TOO
    if(isExportDefined(expr))
        return true;

    return findVisitedArray()->contains(*expr);
}

StringBuffer &HqltHql::lookupSymbolName(IHqlExpression * expr, StringBuffer &s)
{
    IHqlExpression * extra = (IHqlExpression *)expr->queryTransformExtra();
    if (extra)
    {
        if ((extra != expr) && !extra->isAttribute())
            return lookupSymbolName(extra, s);

        return appendId(s, extra->queryId());
    }

    makeUniqueName(expr, s);
    return s;
}

IHqlExpression * HqltHql::querySymbolDefined(IHqlExpression * expr)
{
    StringBuffer xxx;
    lookupSymbolName(expr, xxx);

    HqlExprArray * visited = findVisitedArray();
    for (unsigned i = 0; i < visited->ordinality(); i++)
    {
        IHqlExpression * item = &(visited->item(i));
        IHqlExpression * extra = static_cast<IHqlExpression *>(item->queryTransformExtra());

        if(extra->queryName() && xxx.length())
        {
            if(!stricmp(str(extra->queryName()), xxx.str()))
                return item;
        }
    }
    return NULL;
}

IHqlExpression * HqltHql::queryAlienDefinition(ITypeInfo * type)
{
    IHqlExpression * expr = queryExpression(queryUnqualifiedType(type));
    return expr->queryFunctionDefinition();         // Not really queryFunctionDefinition - original definition of the alien type
}


bool HqltHql::isAlienTypeDefined(ITypeInfo * type)
{
    IHqlExpression * expr = queryAlienDefinition(type);
    if (m_visitedAlienTypes.find(*expr) != NotFound)
        return true;
    m_visitedAlienTypes.append(*LINK(expr));
    return false;
}


IHqlExpression * HqltHql::queryChild(IHqlExpression * expr, unsigned i)
{
    IHqlExpression * child = expr->queryChild(i);
    if (!expandProcessed && isInternalAttribute(child))
        return NULL;
    return child;
}


StringBuffer &HqltHql::callEclFunction(StringBuffer &s, IHqlExpression * expr, bool inType)
{
    assertex(expr->isNamedSymbol());
    IHqlExpression * funcdef = expr->queryFunctionDefinition();
    switch (funcdef->getOperator())
    {
    case no_funcdef:
    case no_internalselect:
    case no_delayedselect:
        break;
    default:
        throw makeStringExceptionV(ERR_INTERNALEXCEPTION, "Internal: Unexpected function definition %s", getOpString(funcdef->getOperator()));
    }

    IHqlExpression * formals = queryFunctionParameters(funcdef);

    s.append('(');
    unsigned numParameters = formals->numChildren();
    bool first = true;
    for (unsigned idx = 0; idx < numParameters; idx++)
    {
        if (expandProcessed || !formals->queryChild(idx)->hasAttribute(_hidden_Atom))
        {
            if (first)
                first = false;
            else
                s.append(", ");

            if(funcdef->isNamedSymbol())
            {
                IHqlExpression * param = expr->queryAnnotationParameter(idx);
                if(param)
                    toECL(param, s, false, inType);
            }
        }
    }
    s.append(')');

    return s;
}


void HqltHql::popScope()
{
    scope.pop();
}

void HqltHql::pushScope(IHqlExpression * expr)
{
    if (expr)
    {
        expr = expr->queryNormalizedSelector();
        if (expr->getOperator() == no_selectnth)
            expr = expr->queryChild(0)->queryNormalizedSelector();
    }
    scope.append(expr);
}

bool HqltHql::matchesActiveDataset(IHqlExpression * expr)
{
    if (!expr || (scope.ordinality() == 0))
        return false;
    return expr->queryNormalizedSelector() == scope.tos();
}

bool isEclAlias(IHqlExpression * expr)
{
    IHqlExpression * symbol = queryNamedSymbol(expr);
    if (!symbol)
        return false;
    //don't add an alias around the definition of an external call.   Maybe the tree shouldn't include the named symbol in the first place
    if (expr->getOperator() != no_externalcall)
        return true;
    IHqlExpression * funcdef = expr->queryFunctionDefinition();
    if (!funcdef)           // attr := call();
        return true;
    return (funcdef->queryChild(0)->getOperator() != no_external);
}


static bool needParen(int precedence, IHqlExpression * child)
{
    if (!child)
        return false;
    if (isEclAlias(child))
        return false;
    return (child->getPrecedence() < precedence) && (child->numChildren() > 1);
}

static bool needParen(IHqlExpression * expr, IHqlExpression * child)
{
    return needParen(expr->getPrecedence(), child);
}



StringBuffer & HqltHql::queryNewline(StringBuffer &s)
{
    const unsigned maxLineLength = 1000;
    unsigned len = s.length();
    if (len < maxLineLength)
        return s;
    const char * text = s.str();
    for (unsigned i = 0; i < maxLineLength; i++)
        if (text[--len] == '\n')
            return s;
    return s.newline();
}




void HqltHql::childrenToECL(IHqlExpression *expr, StringBuffer &s, bool inType, bool needComma, unsigned first)
{
    unsigned kids = expr->numChildren();
    for (unsigned idx = first; idx < kids; idx++)
    {
        IHqlExpression * child = queryChild(expr, idx);
        if (child && (expandProcessed || !isInternalAttribute(expr)))
        {
            if (needComma) queryNewline(s.append(", "));
            needComma = true;
            toECL(child, s, false, inType);
            if (expandProcessed && child->isAction())
                s.newline();
        }
    }
}

void splitPayload(SharedHqlExpr & keyed, SharedHqlExpr & payload, IHqlExpression * expr, unsigned payloadFields)
{
    unsigned numFields = 0;
    ForEachChild(i, expr)
        if (!expr->queryChild(i)->isAttribute())
            numFields++;

    HqlExprArray keyedArgs, payloadArgs;
    unsigned cnt = 0;
    ForEachChild(j, expr)
    {
        IHqlExpression * field = expr->queryChild(j);
        if (!field->isAttribute())
        {
            if (cnt++ < numFields - payloadFields)
                keyedArgs.append(*LINK(field));
            else
                payloadArgs.append(*LINK(field));
        }
    }
    keyed.setown(expr->clone(keyedArgs));
    payload.setown(expr->clone(payloadArgs));
}

bool HqltHql::queryAddDotDotDot(StringBuffer & s, unsigned startLength)
{
    if (xgmmlGraphText && (s.length() - startLength) > MAX_GRAPHTEXT_LEN)
    {
        s.append("...");
        return true;
    }
    return false;
}

void HqltHql::expandTransformValues(StringBuffer & s, IHqlExpression * expr, bool & first)
{
    switch (expr->getOperator())
    {
    case no_assign:
        if (first)
            first = false;
        else
            s.append(",");
        toECL(expr->queryChild(1), s, false, false, 0, false);
        break;
    case no_transform:
    case no_assignall:
        ForEachChild(i, expr)
            expandTransformValues(s, expr->queryChild(i), first);
        break;
    }
}


void HqltHql::appendSortOrder(StringBuffer & s, const char * prefix, IHqlExpression * order)
{
    if (order)
    {
        s.append(prefix).append("[");
        toECL(order, s, false, false, 0, false);
        s.append("]");
    }
}

void HqltHql::createPseudoSymbol(StringBuffer & s, const char * prefix, IHqlExpression * expr)
{
    StringBuffer name;
    bool alreadyDefined = findVisitedArray()->contains(*expr);
    if (!alreadyDefined)
        addVisited(expr);

    name.append(prefix).append(findVisitedArray()->find(*expr));
    if (!alreadyDefined)
    {
        expr->setTransformExtra(expr);

        bool wasInsideNewTransform = insideNewTransform;
        insideNewTransform = false;
        
        StringBuffer temp;
        scope.append(NULL);
        temp.append(name);
#ifdef SHOW_NORMALIZED
        if (expandProcessed && expr->isDataset())
            temp.appendf(" [N%p]", expr->queryNormalizedSelector());
#endif
        temp.append(" := ");

        toECL(expr, temp, false, false, 0, true);
        temp.append(";").newline();
        addExport(temp);

        scope.pop();
        insideNewTransform = wasInsideNewTransform;

    }   

    s.append(name);
}


void HqltHql::toECL(IHqlExpression *expr, StringBuffer &s, bool paren, bool inType, unsigned recordIndex, bool isNamedSymbol)
{
    if (expandProcessed)
    {
#ifdef SHOWBRACKETS
    paren = true;
#endif
#ifdef SHOWCRC
        s.appendf("[%p]", getExpressionCRC(expr));
#endif
#ifdef SHOWADDR
        s.appendf("[%p]", expr);
#endif
#ifdef SHOWTYPES
        if (expr->queryType())
            expr->queryType()->getECLType(s.append("[T")).append("]");
#endif
#ifdef SHOWFLAGS
        s.append("[");
#ifdef SHOWFLAGSVALUE
        s.appendf("%p/", expr->getInfoFlags());
#endif
        if (!expr->isFullyBound())
            s.append("*U");
        if (expr->isPure())
            s.append('P');
        if (expr->isConstant())
            s.append('C');
        if (containsActiveDataset(expr))
            s.append('D');
        if (containsAnyDataset(expr))
            s.append('A');
        if (containsInternalSelect(expr))
            s.append('V');
        if (expr->getInfoFlags() & HEFaction)
            s.append('N');
        if (containsWorkflow(expr))
            s.append('W');
        if (containsSelf(expr))
            s.append('S');
        if (isContextDependent(expr))
        {
            s.append('X');
#ifdef SHOWCONTEXTDETAIL
            s.append('[');
            unsigned flags = expr->getInfoFlags();
            if (flags & HEFnoduplicate) s.append('V');
            if (flags & HEFgraphDependent) s.append('G');
            if (flags & HEFcontainsSkip) s.append('S');
            if (flags & HEFcontainsCounter) s.append('C');
            if (flags & HEFtransformDependent) s.append('D');
            if (flags & HEFtranslated) s.append('R');
            if (flags & HEFonFailDependent) s.append('F');
            if (flags & HEFcontextDependentException) s.append('E');
            if (flags & HEFthrowscalar) s.append('T');
            if (flags & HEFthrowds) s.append('M');
            s.append(']');
#endif
        }
        if (containsImplicitNormalize(expr))
            s.append('.');
        if (containsAssertKeyed(expr))
            s.append('K');
        if (containsAliasLocally(expr))
            s.append('L');
        if (containsCall(expr, false))
            s.append('c');
        if (isNamedSymbol)
        {
            if (expr->isDataset())
            {
                IHqlExpression * group = queryGrouping(expr);
                if (group)
                    getExprECL(group, s.append("G(")).append(")");
                IHqlExpression * distrib = queryDistribution(expr);
                if (distrib) getExprECL(distrib, s.append("D(")).append(")");
                appendSortOrder(s, "GO", queryGlobalSortOrder(expr));
                appendSortOrder(s, "LO", queryLocalUngroupedSortOrder(expr));
                appendSortOrder(s, "RO", queryGroupSortOrder(expr));
            }
        }
        s.append("]");
#endif
#if defined(SHOW_TABLES) || defined(SHOW_TABLES_EXISTANCE)
        if (expr->isAction() || expr->isDataset() || expr->getOperator() == no_assign)
        {
            if (expr->getOperator() != no_rows)
            {
                HqlExprCopyArray inScope;
                expr->gatherTablesUsed(inScope);
    #ifdef SHOW_TABLES_EXISTANCE
                if (inScope.ordinality())
                    s.append("[[!]]");
    #endif
    #ifdef SHOW_TABLES
                if (inScope.ordinality())
                {
                    s.append("[[");
                    ForEachItemIn(i, inScope)
                    {
                        if (i) s.append(",");
                        toECL(&inScope.item(i), s, false, false, 0, false);
                    }
                    s.append("]]");
                }
    #endif
            }
        }
        else if (!expr->isIndependentOfScope())
        {
            s.append("[[!]]");
        }
#endif
#ifdef SHOW_DSRECORD
        if (expr->isDataset() || expr->isTransform())
        {
            s.append("R[");
            toECL(expr->queryRecord(), s, false, false, 0, false);
            s.append("]R");
        }
#endif
#ifdef SHOW_DISTRIBUTION
        if (expr->isDataset())
        {
            IHqlExpression * distribution = queryDistribution(expr);
            if (distribution)
            {
                s.append("dist[");
                toECL(distribution, s, false, false, 0, false);
                s.append("]");
            }
        }
#endif
#ifdef SHOW_ORDER
        if (expr->isDataset())
        {
            ITypeInfo * type = expr->queryType();
            appendSortOrder(s, "gorder", queryGlobalSortOrder(type));
            appendSortOrder(s, "lorder", queryLocalUngroupedSortOrder(type));
            appendSortOrder(s, "grorder", queryGroupSortOrder(type));
        }
#endif
#ifdef SHOW_GROUPING
        if (expr->isDataset())
        {
            IHqlExpression * grouping = queryGrouping(expr);
            if (grouping)
            {
                s.append("gr[");
#ifdef SHOW_GROUPING_DETAIL
                toECL(grouping, s, false, false, 0, false);
#endif
                s.append("]");
            }
        }
#endif
    }

//  bool isTop = m_isTop;
    if(m_isTop)
        m_isTop = false;

    node_operator no = expr->getOperator();

    int precedence;
    bool lparen, rparen;
    IHqlExpression * child0 = expr->queryChild(0);
    IHqlExpression * child1 = expr->queryChild(1);

    unsigned savedDatasetDepth = curDatasetDepth;
    if(paren)
        s.append('(');

    for (;;)
    {
        annotate_kind kind = expr->getAnnotationKind();
        if ((kind == annotate_none) || (kind == annotate_symbol))
            break;
#ifdef SHOW_ANNOTATIONS
        if (expandProcessed)
        {
            switch (kind)
            {
            case annotate_meta:
                s.append("#M[");
                for (unsigned i= 0; expr->queryAnnotationParameter(i); i++)
                {
                    if (i) s.append(",");
                    toECL(expr->queryAnnotationParameter(i), s, false, false);
                }
                s.append("]#");
                break;
            case annotate_warning:
                s.append("#W#");
                break;
            case annotate_javadoc:
                s.append("#J#");
                break;
            case annotate_location:
                s.append("#L#");
                break;
            }
        }
#endif
        expr = expr->queryBody(true);
    }

    if (isEclAlias(expr))
    {
        StringBuffer name;
        if(/*isTop || */ m_recurse)
        {
            doAlias(expr, name, inType);
            s.append(name.str());
        }
        else
            lookupSymbolName(expr, s);

        if(!inType && expr->queryFunctionDefinition() && !expandProcessed)
        {
            callEclFunction(s, expr, inType);
        }
    }
    else if (!expandNamed && !expr->isAttribute() && expr->queryId() && lower(expr->queryId()) != lower(unnamedId) )
    {
        appendId(s, expr->queryId());
    }
    else
    {
        if (expr->isDataset() || expr->isDictionary())
        {
            if (!isNamedSymbol && (expandProcessed || tryToRegenerate) && no != no_field && no != no_rows && !isTargetSelector(expr))
            {
                if (!expr->queryTransformExtra())
                {
                    bool wasInsideNewTransform = insideNewTransform;
                    insideNewTransform = false;
                    
                    StringBuffer temp;
                    scope.append(NULL);
                    __uint64 uid = querySeqId(expr);
                    if (uid == 0)
                        uid = (__uint64)(memsize_t)expr;
                    if (expr->isDataset())
                        temp.appendf("dataset%012" I64F "x ", uid);
                    else
                        temp.appendf("dictionary%012" I64F "x ", uid);
#ifdef SHOW_NORMALIZED
                    if (expandProcessed)
                        temp.appendf("[N%p] ", expr->queryNormalizedSelector());
#endif
                    temp.append(":= ");

                    toECL(expr, temp, false, false, 0, true);
                    temp.append(";").newline();
                    addExport(temp);

                    scope.pop();
                    insideNewTransform = wasInsideNewTransform;
                    expr->setTransformExtra(expr);
                }   
                if (expr->isDataset())
                    s.appendf("dataset%012" I64F "x", (__uint64)(memsize_t)expr);
                else
                    s.appendf("dictionary%012" I64F "x", (__uint64)(memsize_t)expr);
                if (paren)
                    s.append(')');
                return;
            }

            switch (no)
            {
            case no_addfiles:   // looks silly otherwise...
            case no_select:
            case no_field:
            case no_compound_selectnew:
                break;
            case no_compound_diskread:
            case no_compound_indexread:
            case no_compound_disknormalize:
            case no_compound_diskaggregate:
            case no_compound_diskcount:
            case no_compound_diskgroupaggregate:
            case no_compound_indexnormalize:
            case no_compound_indexaggregate:
            case no_compound_indexcount:
            case no_compound_indexgroupaggregate:
            case no_compound_childread:
            case no_compound_childnormalize:
            case no_compound_childaggregate:
            case no_compound_childcount:
            case no_compound_childgroupaggregate:
            case no_compound_inline:
                curDatasetDepth = -1000;
                break;
            default:
                if (curDatasetDepth >= maxDatasetDepth)
                {
                    s.append("...");
                    if (paren)
                        s.append(')');
                    return;
                }
                curDatasetDepth++;
                break;
            }
        }
        else if (expandProcessed && no == no_alias)
        {
            if (!isNamedSymbol)
            {
                if (!expr->queryTransformExtra())
                {
                    bool wasInsideNewTransform = insideNewTransform;
                    insideNewTransform = false;

                    StringBuffer temp;
                    scope.append(NULL);
                    temp.appendf("alias%012" I64F "x ", (__uint64)(memsize_t)expr);
                    temp.append(":= ");

                    toECL(expr, temp, false, false, 0, true);
                    temp.append(";").newline();
                    addExport(temp);

                    scope.pop();
                    insideNewTransform = wasInsideNewTransform;
                    expr->setTransformExtra(expr);
                }
                s.appendf("alias%012" I64F "x", (__uint64)(memsize_t)expr);
                if (paren)
                    s.append(')');
                return;
            }
        }

        if (expandProcessed && !isNamedSymbol && no == no_record)
        {
            if (!expr->queryTransformExtra())
            {
                StringBuffer temp;
                scope.append(NULL);
                temp.appendf("record%012" I64F "x := ", (__uint64)(memsize_t)expr);

                toECL(expr, temp, false, false, 0, true);
                temp.append(";").newline();
                addExport(temp);

                scope.pop();
                expr->setTransformExtra(expr);
            }   
            s.appendf("record%012" I64F "x", (__uint64)(memsize_t)expr);
            return;
        }

        unsigned startLength = s.length();
        switch(no)
        {   
        case no_none:
            s.append("<NONE>");
            break;
        case no_pat_instance:
            if (!isNamedSymbol && expandProcessed)
            {
                //Stops generated code exploding - not strictly correct...
                if (!expr->queryTransformExtra())
                {
                    bool wasInsideNewTransform = insideNewTransform;
                    insideNewTransform = false;
                    
                    StringBuffer temp;
                    scope.append(NULL);
                    temp.appendf("pattern%012" I64F "x := ", (__uint64)(memsize_t)expr);

                    toECL(expr, temp, false, false, 0, true);
                    temp.append(";").newline();
                    addExport(temp);

                    scope.pop();
                    insideNewTransform = wasInsideNewTransform;
                    expr->setTransformExtra(expr);
                }
                s.appendf("pattern%012" I64F "x", (__uint64)(memsize_t)expr);
                return;
            }
            else if (expandProcessed)
            {
                defaultToECL(expr, s, inType);
            }
            else
                toECL(expr->queryChild(0), s, false, inType);
            break;

        case no_cast:
        {
            s.append('(');
            getTypeString(expr->queryType(), s);
            s.append(") ");

            toECL(child0, s, needParen(expr->getPrecedence(), child0), inType);
            break;
        }
        case no_implicitcast:
            if (expandProcessed)
            {
                s.append("((");
                getTypeString(expr->queryType(), s);
                s.append(")) ");
            }
            toECL(child0, s, child0->getPrecedence() < 0, inType);
            break;
        case no_param:
        {
            if (expandProcessed)
                s.append("no_param(");
            if(inType) // Hack !
            {
                getTypeString(expr->queryType(), s);
                s.append(' ');
            }
            lookupSymbolName(expr, s);
            if (expandProcessed)
                s.append(")");
            break;
        }
        case no_substring:
            toECL(child0, s, child0->getPrecedence() < 0, inType);
            if(child1)
            {
                s.append("[");
                toECL(child1, s, false, inType);
                s.append("]");
            }
            break;

        case no_rangefrom:
            toECL(child0, s, false, inType);
            s.append("..");
            break;
        case no_rangecommon:
            toECL(child0, s, false, inType);
            s.append("..*");
            break;
        case no_range:
            toECL(child0, s, false, inType);
            s.append("..");
            toECL(child1, s, false, inType);
            break;
        case no_rangeto:
            s.append("..");
            toECL(child0, s, false, inType);
            break;
        case NO_AGGREGATEGROUP:
        {
            s.append(getEclOpString(no));
            s.append("(group");
            childrenToECL(expr, s, inType, true, 0);
            s.append(')');
            break;
        }
        case no_sortlist:
        {
            sortlistToEcl(expr, s, false, inType);
            break;
        }
        case no_rowvalue:
        {
            s.append("{");
            childrenToECL(expr, s, inType, false, 0);
            s.append("}");
            break;
        }
        case no_record:
        {
            bool fieldsInline = (recordIndex == 0) && (expr->numChildren() <= 3) && !isNamedSymbol;
            if (fieldsInline)
                s.append('{');
            else
            {
                s.append("RECORD");
                indent++;
            }

            //Slightly weird.  no_record inside a record imply inheritance, and have a slightly different syntax.
            //Should probably be expanded when the graph is normalized...
            ForEachChild(i2, expr)
            {
                IHqlExpression *child = queryChild(expr, i2);
                if (child && child->getOperator() == no_record)
                {
                    s.append("(");
                    if (isEclAlias(child) || !m_recurse)
                        toECL(child, s, false, inType, i2+1);
                    else
                        createPseudoSymbol(s, "record__", child);
                    s.append(")");
                }
            }
                
            ForEachChild(i1, expr)
            {
                IHqlExpression *child = queryChild(expr, i1);
                if (child && child->isAttribute())
                {
                    s.append(",");
                    toECL(child, s, false, inType, 0);
                }
            }
            if (!fieldsInline)
                s.newline();

            //First output the attributes...
            //MORE: Add attributes to the record definition
            bool first = true;
            bool firstPayload = true;
            ForEachChild(idx, expr)
            {
                IHqlExpression *child = queryChild(expr, idx);
                if (child && !child->isAttribute() && child->getOperator() != no_record)
                {
                    if (queryAddDotDotDot(s, startLength))
                        break;
                    if (fieldsInline)
                    {
                        if (!first)
                        {
                            if (firstPayload && child->hasAttribute(_payload_Atom))
                            {
                                s.append(" =>");
                                firstPayload = false;
                            }
                            else
                                s.append(",");
                        }
                        s.append(" ");
                    }
                    else
                    {
                        if (!first)
                        {
                            if (firstPayload && child->hasAttribute(_payload_Atom))
                            {
                                s.remove(s.length()-2,2).append("\n").pad(indent).append("=>\n");
                                firstPayload = false;
                            }
                        }
                        s.pad(indent);
                    }
                    toECL(child, s, false, inType, idx+1);
                    if (!fieldsInline)
                        s.append(";\n");
                    first = false;
                }
            }
            if (fieldsInline)
                s.append(" }");
            else
                s.pad(--indent).append("END");
            break;
        }
        case no_ifblock:
        {
            s.append("IFBLOCK");
            indent++;
            toECL(expr->queryChild(0), s, true, inType);
            s.append("\n");
            IHqlExpression * record = expr->queryChild(1);
            ForEachChild(idx2, record)
            {
                s.pad(indent);
                toECL(record->queryChild(idx2), s, false, inType, indent*100*recordIndex+idx2+1);
                s.append(";\n");
            }
            s.pad(--indent).append("END");
            break;
        }
        case no_assignall:
        {
            IHqlExpression * original = expr->queryAttribute(_original_Atom);
            if (original)
            {
                toECL(original->queryChild(0), s, false, inType);
                s.append(" := ");
                IHqlExpression * rhs = original->queryChild(1);
                if (rhs->getOperator() == no_null)
                    s.append("[]");
                else
                    toECL(rhs, s, false, inType);
            }
            else if (child0)
            {
                s.append("SELF := ");
                IHqlExpression * rhs = child0->queryChild(1);
                if (rhs->getOperator() == no_select)
                {
                    IHqlExpression * lhs = child0->queryChild(0);
                    while (lhs->getOperator() == no_select)
                    {
                        lhs = lhs->queryChild(0);
                        rhs = rhs->queryChild(0);
                    }
                    toECL(rhs, s, false, inType);
                }
                else
                    s.append("[]");
            }

            if (expandProcessed)
            {
                s.append("/* ");
                unsigned max = expr->numChildren();
                for (unsigned idx = 0; idx < max; idx++)
                {
                    if (idx) s.append("; ");
                    toECL(expr->queryChild(idx), s, false, inType);
                }
                s.append(" */");
            }
            break;
        }
        case no_index:
        case no_selectnth:
        case no_selectmap:
        case no_pat_index:
        {
            toECL(child0, s, child0->getPrecedence() < 0, inType);
            s.append('[');
            toECL(child1, s, child1->getPrecedence() < 0, inType);
            s.append(']');
            break;
        }
        case no_bor:
        case no_band:
        case no_bxor:
        case no_div:
        case no_mul:
        case no_modulus:
        case no_concat:
        case no_add:
        case no_addsets:
        case no_assign:
        case no_addfiles:
        case no_sub:
        case no_eq:
        case no_ne:
        case no_lt:
        case no_le:
        case no_gt:
        case no_ge:
        case no_and:
        case no_or:
        case no_pat_or:
        case no_xor:
        case no_mapto:
        case no_order:
        case no_notin:
        case no_in:
        case no_indict:
        case no_colon:
        case no_pat_select:
        case no_lshift:
        case no_rshift:
        {
            // Standard binary infix operators
            precedence = expr->getPrecedence();
            lparen = needParen(precedence, child0);
            if (child0)
                toECL(child0, s, lparen, inType);
            else
                s.append("?NULL?");
            const char * opText = getEclOpString(no);
            if ((no == no_div) && expr->queryType()->isInteger())
                opText = "DIV";
            if (no == no_addfiles)
            {
                if (isOrdered(expr))
                    opText = "&";
                if (expr->hasAttribute(pullAtom))
                    opText = "&&";
            }
            unsigned num = expr->numChildren();
            for (unsigned i=1; i < num; i++)
            {
                IHqlExpression * child = queryChild(expr, i);
                if (child)
                {
                    if ((no == no_addfiles) && child->isAttribute())
                    {
                        if (child->queryName() == orderedAtom)
                            continue;
                    }
                    if (xgmmlGraphText && endsWithDotDotDot(s))
                        break;
                    s.append(' ').append(opText).append(' ');
                    if (queryAddDotDotDot(s, startLength))
                        break;
                    toECL(child, s, needParen(precedence, child), inType);
                }
            }
            break;
        }
        case no_subsort:
            {
                s.append(getEclOpString(expr->getOperator()));
                s.append('(');
                if (!xgmmlGraphText)
                {
                    toECL(child0, s, false, inType);
                    queryNewline(s.append(", "));
                }
                pushScope(child0);
                //MORE: Sortlists should always be generated using the {} syntax - then this could be simplified
                //NOTE: child(1) and child(2) are output in a different order from their representation
                s.append("{");
                toECL(expr->queryChild(2), s, false, inType);
                s.append("}");
                queryNewline(s.append(", "));
                s.append("{");
                toECL(expr->queryChild(1), s, false, inType);
                s.append("}");
                childrenToECL(expr, s, inType, true, 3);
                popScope();
                s.append(')');
                break;
            }

        case no_select:
        {
            if (!expandProcessed && 
                    (matchesActiveDataset(child0) || 
                     (child0->getOperator() == no_self && insideNewTransform) ||
                     (minimalSelectors && !isAlwaysActiveRow(child0)) ||
                     (minimalSelectors && child0->getOperator() == no_activetable))
                     )
                toECL(child1, s, false, inType);
            else
            {
                if (!expandProcessed)
                {
                    OwnedHqlExpr aggregate = convertToSimpleAggregate(expr);
                    if (aggregate)
                    {   
                        toECL(aggregate, s, false, inType);
                        break;
                    }
                }

                {
                    lparen = needParen(expr, child0);
                    if (expandProcessed && !expr->hasAttribute(newAtom) && child0->queryName())
                        s.append("<").append(child0->queryName()).append(">");
                    if (xgmmlGraphText && expr->hasAttribute(newAtom))
                        s.append("<...>");
                    else
                        toECL(child0, s, lparen, inType);
                }
                s.append(getEclOpString(no)); // <- note: no padding
                rparen = needParen(expr, child1);
                toECL(child1, s, rparen, inType);
            }
            if (expandProcessed)
            {
                unsigned max = expr->numChildren();
                if (max != 2)
                {
                    s.append("(<");
                    for (unsigned i=2; i < max; i++)
                    {
                        if (i != 2) s.append(",");
                        toECL(expr->queryChild(i), s, false, inType);
                    }
                    s.append(">)");
                }
            }
            break;
        }
        case no_notnot:
        case no_not:
        case no_negate:
        case no_within:
            // Standard unary operators
            precedence = expr->getPrecedence();
            rparen = child0->getPrecedence() < precedence;
            s.append(getEclOpString(no));
            toECL(child0, s, rparen, inType);
            break;
        case no_between:
        case no_notbetween:
            // Standard ternary operators
            toECL(child0, s, child0->getPrecedence() < 0, inType);
            s.append(getEclOpString(no));
            toECL(child1, s, child0->getPrecedence() < 0, inType);
            s.append(',');
            toECL(expr->queryChild(2), s, expr->queryChild(2)->getPrecedence() < 0, inType);
            break;
#ifndef SHOW_EXPAND_LEFTRIGHT
        case no_left:
        case no_right:
        case no_top:
#endif
        case no_self:
        case no_rows:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else
                s.append(getEclOpString(no));
#ifdef SHOWADDR
            if (expandProcessed)
                s.appendf("{@%p:%p}", expr->queryChild(0), expr->queryChild(1));
#endif
            break;
        case no_getgraphloopresultset:
            if (xgmmlGraphText)
                s.append("ROWSET");
            else
                defaultToECL(expr, s, inType);
            break;
        case no_sql:
        case no_flat:
        case no_all:
        case no_activetable:
        case no_counter:
            s.append(getEclOpString(no));
            if (expandProcessed && expr->querySequenceExtra())
            {
                s.append("{").append(expr->querySequenceExtra()).append("}");
            }
            break;
        case no_selfref:
            if (expandProcessed)
                s.append("SELFref");
            else
                s.append(getEclOpString(no));
            break;
        case no_thor:
        {
            unsigned kids = expr->numChildren();
            if(kids)
            {
                if (expandProcessed)
                    s.append(getEclOpString(no)).append("(");
                bool first=true;
                for (unsigned idx = 0; idx < kids; idx++)
                {
                    IHqlExpression *child = expr->queryChild(idx);
                    if (expandProcessed || !child->isAttribute())
                    {
                        if (!first)
                            s.append(", ");
                        first=false;
                        toECL(child, s, false, inType);
                    }
                }
                if (expandProcessed)
                    s.append(")");
            }
            else
            {
                s.append(getEclOpString(no));
            }
            break;
        }
        case no_attr:
        case no_attr_expr:
        case no_attr_link:
            {
                if (isInternalAttribute(expr) && !expandProcessed)
                    break;
                IAtom * name = expr->queryName();
                s.append(expr->queryName());
                if (name == _workflowPersist_Atom || name == _original_Atom)
                {
                    s.append("(...)");
                    break;
                }

                if (child0)
                {
                    s.append('(');
                    childrenToECL(expr, s, false, false, 0);
                    s.append(')');
                }
                else if (expr->querySequenceExtra())
                {
                    //Not sure any of these should be included
                    s.append("(").append(expr->querySequenceExtra()).append(")");
                }
                break;
            }
        case no_constant:
            {
                expr->queryValue()->generateECL(s);
                if (xgmmlGraphText && (s.length() - startLength) > MAX_GRAPHTEXT_LEN)
                {
                    bool addQuote = s.charAt(s.length()-1) == '\'';
                    s.setLength(startLength+MAX_GRAPHTEXT_LEN);
                    s.append("...");
                    if (addQuote)
                        s.append("'");
                }
                break;
            }
        case no_list:
        case no_datasetlist:
        case no_recordlist:
        case no_transformlist:
        {
            s.append('[');
            for (unsigned idx = 0; idx < expr->numChildren(); idx++)
            {
                IHqlExpression *child = expr->queryChild(idx);
                if (idx)
                    s.append(", ");
                queryNewline(s);
                toECL(child, s, child->getPrecedence() < 0, inType);
            }
            s.append(']');
            break;
        }
        case no_call:
            if (expandProcessed)
            {
                s.append("CALL(");
                toECL(expr->queryBody()->queryFunctionDefinition(), s, false, inType);
                defaultChildrenToECL(expr, s, inType);
                s.append(")");
                break;
            }
            //fall through
        case no_externalcall:
        {
            // OK we got us an external call. Let's try and define it.
            StringBuffer name;
            if (m_recurse)
                defineCallTarget(expr, name);
            else if (xgmmlGraphText)
                lookupSymbolName(expr, name);
            else
            {
                lookupSymbolName(expr, name);
                name.append('.');
                lookupSymbolName(expr, name);
            }
            IHqlExpression *funcdef = (no==no_externalcall) ?  expr->queryBody()->queryExternalDefinition() : expr->queryBody()->queryFunctionDefinition();
            IHqlExpression *formals = funcdef ? funcdef->queryChild(1) : nullptr;
            s.append(name);
            s.append('(');
            unsigned idx = 0;
            bool first = true;
            while(IHqlExpression *kid = expr->queryChild(idx))
            {
                bool isHidden = !expandProcessed && kid->isAttribute();
                if (formals && !expandProcessed)
                {
                    IHqlExpression *formal = formals->queryChild(idx);
                    isHidden = formal && formal->hasAttribute(_hidden_Atom);
                }
                if (!isHidden && (expandProcessed || !isInternalAttribute(kid)))
                {
                    if (first)
                        first = false;
                    else
                        s.append(", ");
                    toECL(kid, s, kid->getPrecedence() < 0, inType);
                }
                idx++;
            }
            s.append(')');
            break;
        }
        case no_external:
        {
            unsigned kids = expr->numChildren();
            bool first = true;
            for (unsigned idx = 0; idx < kids; idx++)
            {
                IHqlExpression *kid = expr->queryChild(idx);
                if (kid->queryName() != pluginAtom)
                {
                    if(first)
                        first = false;
                    else
                        s.append(", ");

                    s.append(kid->queryName());

                    if (kid->isAttribute())
                    {
                        IHqlExpression * value = kid->queryChild(0);
                        if (value)
                        {
                            IValue * val = value->queryValue();
                            if (val)
                            {
                                s.append("='");
                                val->getStringValue(s);
                                s.append('\'');
                            }
                            else
                            {
                                s.append("=");
                                toECL(value, s, false, inType);
                            }
                        }
                    }
                }
            }
            break;
        }
        case no_pat_const:
        case no_pat_imptoken:
            toECL(expr->queryChild(0), s, false, inType);
            break;
        case no_pat_pattern:
            s.append("PATTERN(");
            toECL(expr->queryChild(0), s, false, inType);
            s.append(")");
            break;
        case no_pat_checkin:
            toECL(expr->queryChild(0), s, false, inType);
            s.append(" IN ");
            toECL(expr->queryChild(1), s, false, inType);
            break;
        case no_pat_repeat:
            {
                unsigned low = getRepeatMin(expr);
                unsigned high = getRepeatMax(expr);
                if (isStandardRepeat(expr))
                {
                    toECL(expr->queryChild(0), s, false, inType);
                    if ((low == 0) && (high == 1))
                        s.append("?");
                    else if ((low == 0) && (high == (unsigned)-1))
                        s.append("*");
                    else if ((low == 1) && (high == (unsigned)-1))
                        s.append("+");
                }
                else
                {
                    s.append("REPEAT(");
                    toECL(expr->queryChild(0), s, false, inType);
                    s.append(",").append(low);
                    if (low != high)
                    {
                        s.append(",");
                        if (high == (unsigned)-1)
                            s.append("ANY");
                        else
                            s.append(high);
                    }
                    if (expr->hasAttribute(minimalAtom))
                        s.append(",MINIMAL");
                    s.append(")");
                }
                break;
            }
        case no_pat_follow:
            {
                precedence = expr->getPrecedence();
                ForEachChild(idx, expr)
                {
                    if (idx) s.append(" ");
                    IHqlExpression * child = expr->queryChild(idx);
                    toECL(child, s, needParen(precedence, child), inType);
                }
                break;
            }
        case no_field:
        {
            IHqlExpression * kid = expr->queryChild(0);
            if (kid && kid->isAttribute())
                kid = NULL;
            if (recordIndex || inType)
            {
                if (expr->queryName())
                {
                    bool addTypeAndName = true;
                    if (kid)
                    {
                        IHqlExpression * field = NULL;
                        if (kid->getOperator() == no_field)
                            field = kid;
                        else if (kid->getOperator() == no_select)
                            field = kid->queryChild(1);
                        if (field)
                            if ((expr->queryType() == field->queryType()) && (expr->queryName() == field->queryName()))
                                addTypeAndName = false;
                    }

                    if (addTypeAndName)
                    {
                        getFieldTypeString(expr, s);
                        s.append(' ');
                        lookupSymbolName(expr, s);

                        //Add the attributes....
                        bool seenAttr = false;
                        ForEachChild(kids, expr)
                        {
                            IHqlExpression *attr = expr->queryChild(kids);
                            if (attr->isAttribute())
                            {
                                IAtom * name = attr->queryName();
                                if (name != countAtom && name != sizeofAtom && 
                                    !(isInternalAttribute(attr) && !expandProcessed))
                                {
                                    if ((name != virtualAtom) || !ignoreVirtualAttrs)
                                    {
                                        if (!seenAttr)
                                        {
                                            s.append('{');
                                            seenAttr = true;
                                        }
                                        else 
                                            s.append(", ");
                                        toECL(attr, s, false, inType);
                                    }
                                }
                            }
                        }
                        if (seenAttr)
                            s.append('}');

                        if (kid)
                        {
                            s.append(" := ");
                            toECL(kid, s, false, inType);
                        }
                    }
                    else
                        toECL(kid, s, false, inType);
                }
                else if (kid)
                {
                    assertex(expr->queryType() == kid->queryType());
                    toECL(kid, s, false, inType);
                }
                else
                {
                    getTypeString(expr->queryType(), s);

                    s.append(" _unnamed_").append(recordIndex-1);
                }
            }
            else
            {
                if (expr->queryName())
                    lookupSymbolName(expr, s);
                else if (kid)
                    toECL(kid, s, false, inType);
                else
                    s.append("????");
                break;
            }
            break;
        }
        case no_filter:
        {
            StringBuffer tmp;
            unsigned kids = expr->numChildren();
            if (xgmmlGraphText)
                s.append("FILTER");
            else
                toECL(child0, s, child0->getPrecedence() < 0, inType);
            
            pushScope(child0);
            for (unsigned idx = 1; idx < kids; idx++)
            {
                if (idx > 1)
                    queryNewline(tmp.append(", "));

                IHqlExpression *child = expr->queryChild(idx);
                toECL(child, tmp, false, inType);
            }
            popScope();
            
            if(tmp.length())
                queryNewline(s.append('(').append(tmp).append(')'));
            break;
        }
        case no_httpcall:
        case no_soapcall:
        case no_soapcall_ds:
        case no_newsoapcall_ds:
        {
            IHqlExpression * resultFormat = queryNewColumnProvider(expr);
            s.append(getEclOpString(no));
            s.append('(');
            bool needComma = false;
            StringBuffer tmp;
            unsigned kids = expr->numChildren();
            if (!xgmmlGraphText)
            {
                toECL(child0, s, child0->getPrecedence() < 0, inType);
                needComma = true;
            }
            
            pushScope(child0);
            for (unsigned idx = 1; idx < kids; idx++)
            {
                if (needComma)
                    s.append(", ");
                IHqlExpression *child = expr->queryChild(idx);
                if (expr->isDataset() && (child == resultFormat))
                {
                    s.append("DATASET(");
                    toECL(child, s, false, inType);
                    s.append(")");
                }
                else
                    toECL(child, s, false, inType);
                needComma = true;
            }
            popScope();
            s.append(")");
            break;
        }
        case no_inlinetable:
        {
            s.append("DATASET([");
            ForEachChild(i, child0)
            {
                IHqlExpression * cur = child0->queryChild(i);
                if (i)
                    s.append(",");
                s.append("{");
                bool first = true;
                expandTransformValues(s, cur, first);
                s.append("}");
                queryNewline(s);
                if (xgmmlGraphText)
                {
                    if (child0->numChildren() != 1)
                        s.append(",...");
                    break;
                }
            }
            s.append("]");
            childrenToECL(expr, s, inType, true, 1);
            s.append(')');
            break;
        }
        case no_temptable:
        {
            s.append("DATASET(");
            ITypeInfo * child0Type = child0->queryType();
            if (child0Type && child0Type->getTypeCode() == type_set)
            {
                toECL(child0, s, child0->getPrecedence() < 0, inType);
            }
            else
            {
                HqlExprArray records;
                child0->unwindList(records, no_recordlist);
                s.append('[');
                for (unsigned idx = 0; idx < records.length(); idx++)
                {
                    IHqlExpression * kid = &records.item(idx);
                    if(kid && !kid->isAttribute())
                    {
                        if (kid->getOperator() == no_record)
                        {
                            s.append('{');
                            bool first = true;
                            for (unsigned i = 0; i < kid->numChildren(); i++)
                            {
                                if (kid->queryChild(i)->isAttribute())
                                    continue;
                                if (!first)
                                    s.append(", ");
                                toECL(kid->queryChild(i)->queryChild(0), s, false, inType);
                                first = false;
                            }
                            s.append('}');
                        }
                        else
                            toECL(kid, s, false, inType);
                    }
                    if (xgmmlGraphText)
                    {
                        if (records.length() != 1)
                            s.append(",...");
                        break;
                    }
                    if (idx < records.length()-1)
                        s.append(',');
                }
                s.append(']');
            }
            if (child1)
            {
                toECL(child1, s.append(", "), child1->getPrecedence() < 0, inType);
            }
            if (expandProcessed)
                childrenToECL(expr, s, inType, true, 2);
            s.append(')');
            break;
        }
        case no_temprow:
        {
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else
            {
                s.append("ROW(");
                toECL(child0, s, child0->getPrecedence() < 0, inType);
                toECL(child1, s.append(", "), child1->getPrecedence() < 0, inType);
                s.append(")");
            }
            break;
        }
        case no_workunit_dataset:
        {
            s.append("DATASET(WORKUNIT(");
            toECL(child1, s, false, inType);
            if (!isInternalAttribute(expr->queryChild(2)) || expandProcessed)
            {
                s.append(", ");
                toECL(expr->queryChild(2), s, false, inType);
            }
            if (expandProcessed)
                childrenToECL(expr, s, false, true, 3);
            s.append("), ");
            toECL(child0, s, false, inType);
            s.append(")");
            break;
        }
        case no_funcdef:
        {
            if (expandProcessed)
            {
                s.append("funcdef");
                defaultChildrenToECL(expr->queryChild(1), s, inType);
                s.append(" := {");
                defaultToECL(expr->queryChild(0), s, inType);
                s.append("}");
            }
            else
            {
                unsigned kids = expr->numChildren();
                for (unsigned idx = 0; idx < kids; idx++)
                {
                    if (idx)
                    {
                        s.append(", ");
                    }
                    IHqlExpression *child = expr->queryChild(idx);
                    toECL(child, s, child->getPrecedence() < 0, inType);
                }
            }
            break;
        }
        case no_buildindex:
        case no_output:
        {
            s.append(getEclOpString(expr->getOperator())).append('(');
            if (child1 && child1->queryName() == sequenceAtom && !expandProcessed)
                child1 = NULL;
            if (xgmmlGraphText)
            {
                if (child1)
                    s.append("..., ");
                else
                    s.append("...");
            }
            else
            {
                toECL(child0, s, false, inType);
                if (child0->getOperator() == no_selectfields)
                {
                    pushScope(child0->queryChild(0));
                    IHqlExpression * c1 = child0->queryChild(1);
                    if(c1->numChildren())
                    {
                        s.append(", ");
                        if (hasNamedSymbol(c1))
                            toECL(c1, s, c1->getPrecedence() < 0, inType);
                        else
                        {
                            s.append('{');
                            for (unsigned idx = 0; idx < c1->numChildren(); idx++)
                            {
                                if (idx)
                                    s.append(", ");
                                IHqlExpression *child = c1->queryChild(idx);
                                toECL(child, s, child->getPrecedence() < 0, inType, idx+1);
                            }
                            s.append('}');
                        }
                    }
                    else if(child1 && !isInternalAttribute(child1))
                    {
                        s.append(", ");
                    }
                    popScope();
                }
                else if(child1 && !isInternalAttribute(child1))
                {
                    s.append(", ");
                }
            }

            StringBuffer next;
            unsigned kids = expr->numChildren();
            for (unsigned idx=1; idx < kids; idx++)
            {
                IHqlExpression * cur = queryChild(expr, idx);
                if (cur)
                {
                    toECL(cur, next.clear(), cur->getPrecedence() < 0, inType);
                    if (next.length())
                        s.append(", ").append(next);
                }
            }
            s.append(')');
            break;
        }
        case no_null:
            if (expr->isDataset())
            {
                s.append("_EMPTY_(");
                toECL(child0, s, child0->getPrecedence() < 0, inType);
                s.append(')');
            }
            else
            {
                //MORE!
            }
            break;
        case no_typetransfer:
        {
            if (expr->isDatarow())
            {
                s.append(getEclOpString(no));
                s.append('(');
                toECL(child1, s, child0->getPrecedence() < 0, inType);
                s.append(", ");
                toECL(child0, s, child0->getPrecedence() < 0, inType);
                s.append(')');
            }
            else
            {
                s.append(getEclOpString(no));
                s.append('(');
                toECL(child0, s, child0->getPrecedence() < 0, inType);
                s.append(", ");
                getTypeString(expr->queryType(), s);
                s.append(')');
            }
            break;
        }
        case no_embedbody:
            {
                s.append("BEGINC++\n");
                IValue * value = child0->queryValue();
                if (value)
                    value->getUTF8Value(s);
                s.append("ENDC++\n");
                break;
            }
        case no_nofold:
        case no_nohoist:
        case no_nocombine:
        case no_selectfields:
        case no_pure:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else
                toECL(child0, s, false, inType);
            break;
        case no_clustersize:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else
                s.append(getEclOpString(no));
            break;
        case no_map:
        {
            unsigned kids = expr->numChildren();
            s.append(getEclOpString(no));
            s.append('(');
            for (unsigned idx = 0; idx < kids; idx++)
            {
                if (idx)
                {
                    s.append(",");
                    if (!xgmmlGraphText)
                        s.append("\n\t ");
                }

                IHqlExpression *child = expr->queryChild(idx);
                if (xgmmlGraphText && expr->isDataset() && idx != kids-1)
                    child = child->queryChild(0);
                toECL(child, s, child->getPrecedence() < 0, inType);
            }
            s.append(')');
            break;
        }
        case no_newtransform:
        {
            if (expandProcessed)
            {
                defaultToECL(expr, s, inType);
                break;
            }
//          assertex(expandProcessed || !insideNewTransform);
            bool wasInsideNewTransform = insideNewTransform;
            insideNewTransform = true;
            s.append("{ ");
            unsigned kids = expr->numChildren();
            for (unsigned idx = 0; idx < kids; idx++)
            {
                if (idx) s.append(", ");
                IHqlExpression *child = expr->queryChild(idx);
                if (!child->isAttribute())
                    getTypeString(child->queryChild(0)->queryType(), s);

                toECL(child, s.append(' '), child->getPrecedence() < 0, inType);
            }
            s.append(" }");
            insideNewTransform = wasInsideNewTransform;
            break;
        }
        case no_transform:
        {
            if (isNamedSymbol)
            {
                s.append(getEclOpString(no)).newline();
                unsigned kids = expr->numChildren();
                for (unsigned idx = 0; idx < kids; idx++)
                {
                    IHqlExpression *child = expr->queryChild(idx);
                    toECL(child, s.append('\t'), child->getPrecedence() < 0, inType);
                    s.append(';').newline();
                }
                s.append("\tEND");
            }
            else
            {
                s.append("TRANSFORM(");
                getTypeString(expr->queryType(), s);
                s.append(",");
                if (tryToRegenerate)
                    s.newline();
                unsigned kids = expr->numChildren();
                for (unsigned idx = 0; idx < kids; idx++)
                {
                    if (queryAddDotDotDot(s, startLength))
                        break;
                    IHqlExpression *child = expr->queryChild(idx);
                    if (tryToRegenerate)
                        s.append("\t");
                    toECL(child, s, child->getPrecedence() < 0, inType);
                    s.append(';');
                    if (tryToRegenerate)
                        s.newline();
                }
                s.append(")");
            }
            break;
        }
        case no_newaggregate:
        case no_newusertable:
        {
            s.append(getEclOpString(no));
            s.append('(');
            assertex(child0);
            bool addComma = !xgmmlGraphText;
            if (!xgmmlGraphText)
                toECL(child0, s, false, inType);

            pushScope(child0);
            unsigned start = 2;
            if (expandProcessed)
                start = 1;
            else if (isEclAlias(child1))
            {
                if (addComma)
                    s.append(", ");
                toECL(child1, s, false, inType);
                addComma = true;
                start = 3;
            }
            childrenToECL(expr, s, inType, addComma, start);
            popScope();
            s.append(')');
            break;
        }
        case no_newkeyindex:
        {
            s.append(getEclOpString(no));
            s.append('(');

            bool addComma = false;
            if (!xgmmlGraphText)
            {
                toECL(child0, s, false, inType);
                addComma = true;
            }

            pushScope(child0);
            unsigned kids = expr->numChildren();
            unsigned payloadFields = numPayloadFields(expr);
            for (unsigned idx = 1; idx < kids; idx++)
            {
                IHqlExpression * child = queryChild(expr, idx);
                if (child && ((idx != 2) || expandProcessed))
                {
                    if (addComma) s.append(", ");
                    addComma = true;
                    //Normally this is called when file position special field has been removed.
                    if ((idx == 1) && (payloadFields != 0) && !expandProcessed)
                    {
                        OwnedHqlExpr keyedRecord;
                        OwnedHqlExpr payloadRecord;
                        splitPayload(keyedRecord, payloadRecord, child, payloadFields);
                        toECL(keyedRecord, s, false, inType);
                        toECL(payloadRecord, s.append(", "), false, inType);
                    }
                    else
                        toECL(child, s, false, inType);
                }
            }
            popScope();
            s.append(')');
            break;
        }
        case no_keyindex:
        {
            unsigned payloadFields = numPayloadFields(expr);
            s.append(getEclOpString(no));
            s.append('(');
            bool addComma = false;
            if (!xgmmlGraphText)
            {
                toECL(child0, s, false, inType);
                addComma = true;
            }

            pushScope(child0);
            unsigned kids = expr->numChildren();
            for (unsigned idx = 1; idx < kids; idx++)
            {
                IHqlExpression * child = queryChild(expr, idx);
                if (child)
                {
                    if (addComma) s.append(", ");
                    addComma = true;
                    if ((idx == 1) && (payloadFields != 1) && !expandProcessed)
                    {
                        OwnedHqlExpr keyedRecord;
                        OwnedHqlExpr payloadRecord;
                        splitPayload(keyedRecord, payloadRecord, child, payloadFields);
                        toECL(keyedRecord, s, false, inType);
                        toECL(payloadRecord, s.append(", "), false, inType);
                    }
                    else
                        toECL(child, s, false, inType);
                }
            }
            popScope();
            s.append(')');
            break;
        }

        case no_preservemeta:
            if (xgmmlGraphText)
            {
                curDatasetDepth--;
                toECL(child0, s, false, inType);
                curDatasetDepth++;
            }
            else if (tryToRegenerate)
                defaultToECL(child0, s, inType);
            else
                defaultToECL(expr, s, inType);
            break;
        case no_callsideeffect:
            if (xgmmlGraphText)
                s.append("...");
            else
                defaultToECL(expr, s, inType);
            break;
        case no_hqlproject:
        case no_iterate:
        {
            s.append(getEclOpString(no));
            s.append('(');

            // Left
            if (!xgmmlGraphText)
                toECL(child0, s, false, inType);

            pushScope(NULL);
            // Transform
            if(child1)
            {
                // I need to feed Left to the transform function definition.
                mapDatasetRecord(child0);
                if (!xgmmlGraphText)
                    s.append(", ");
                toECL(child1, s, false, inType);
                popMapping();
            }
            childrenToECL(expr, s, inType, true, 2);
            popScope();
            s.append(')');
            break;
        }
        case no_fetch:
        case no_join:
        case no_selfjoin:
        {
            unsigned kids = expr->numChildren();
            s.append(getEclOpString(no));
            s.append('(');

            if (!xgmmlGraphText)
            {
                toECL(child0, s, false, inType);
                
                // Right
                s.append(", ");
                toECL(queryJoinRhs(expr), s, false, inType);
            }

            pushScope(NULL);
            // Condition
            if(kids > 2)
            {
                if (!xgmmlGraphText)
                    s.append(", ");
                toECL(expr->queryChild(2), s, false, inType);
            }

            // Transform
            if(kids > 3)
            {
                // I need to feed Left and Right to the transform function definition.
                mapDatasetRecord(child0);
                mapDatasetRecord(queryJoinRhs(expr));
                toECL(expr->queryChild(3), s.append(", "), false, inType);
                popMapping();
                popMapping();
            }

            // Join type
            if(kids > 4)
            {
                for (unsigned i=4; i < kids; i++)
                {
                    IHqlExpression * next = queryChild(expr, i);
                    if (next)
                        toECL(next, s.append(", "), false, inType);
                }
            }

            s.append(')');
            popScope();
            break;
        }
        case no_sizeof:
        {
            IHqlExpression* child = expr->queryChild(0);
            s.append("sizeof(");
            if (child->getOperator()==no_null)
                child->queryType()->getECLType(s);
            else
                toECL(child,s,false,inType);
            if (child1)
                toECL(child1, s.append(", "), false, inType);
            s.append(")");
            break;
        }   
        case no_evaluate:
            {
                s.append(getEclOpString(no));
                s.append('(');
                toECL(child0, s, false, inType);
                s.append(", ");
                pushScope(child0);
                toECL(child1, s, false, inType);
                popScope();
                s.append(')');
                break;
            }
        case no_setmeta:
            {
                IAtom * kind = expr->queryChild(0)->queryName();
                if (kind == debugAtom)
                    s.append("#OPTION");
                else if (kind == constAtom)
                    s.append("#CONSTANT");
                else if (kind == storedAtom)
                    s.append("#STORED");
                else if (kind == workunitAtom)
                    s.append("#WORKUNIT");
                else if (kind == webserviceAtom)
                    s.append("#WEBSERVICE");
                else
                    s.append("#META:").append(kind);

                s.append(" (");
                unsigned kids = expr->numChildren();
                for (unsigned idx = 1; idx < kids; idx++)
                {
                    if (idx != 1)
                        s.append(", ");
                    toECL(expr->queryChild(idx), s, false, inType);
                }
                s.append(")");
                break;
            }
        case NO_AGGREGATE:
            {
                // standard function-style operators
                unsigned kids = expr->numChildren();
                s.append(getEclOpString(no));
                if (child0)
                {
                    s.append('(');
                    toECL(child0, s, false, inType);
                    pushScope(child0);
                    for (unsigned idx = 1; idx < kids; idx++)
                    {
                        IHqlExpression * cur = expr->queryChild(idx);
                        if (expandProcessed || !cur->isAttribute())
                        {
                            if (idx != 0)
                                s.append(", ");
                            toECL(cur, s, false, inType);
                        }
                    }
                    popScope();
                    s.append(')');
                }
                break;
            }
        case no_pat_featureactual:
            {
                toECL(child0, s, false, inType);
                s.append("{");
                toECL(child1, s, false, inType);
                s.append("}");
                break;
            }
        case no_comma:
            toECL(child0, s, false, inType);
            s.append(",");
            toECL(child1, s, false, inType);
            break;
        case no_compound:
            if (!expandProcessed)
            {
                toECL(child0, s, false, inType);
                s.append(",");
                if (!xgmmlGraphText)
                    s.newline();
                toECL(child1, s, false, inType);
            }
            else
                defaultToECL(expr, s, inType);
            break;
        case no_choosen:
            {
                // standard function-style operators
                unsigned kids = expr->numChildren();
                s.append(getEclOpString(no));
                s.append('(');
                unsigned noCommaArg = 1;
                if (!xgmmlGraphText || !child0->queryDataset())
                {
                    toECL(child0, s, false, inType);
                    noCommaArg = 0;
                }
                pushScope(child0);
                for (unsigned idx = 1; idx < kids; idx++)
                {
                    if (idx != noCommaArg)
                        s.append(", ");
                    IHqlExpression * cur = expr->queryChild(idx);
                    if ((idx == 1) && isChooseNAllLimit(cur))
                        s.append("ALL");
                    else
                        toECL(cur, s, false, inType);
                }
                popScope();
                s.append(')');
                break;
            }
        case no_matchattr:
            {
                unsigned index;
                if (expr->isDatarow())
                    index = (unsigned)getIntValue(child1);
                else
                    index = (unsigned)getIntValue(child0);
                s.append('$').append(index+1);
                break;
            }
        case no_setresult:
#if 0
            if (xgmmlGraphText && scope.ordinality() == 0 && child0->getOperator() == no_select)
            {
                pushScope(child0->queryChild(0));
                defaultToECL(expr, s, inType);
                popScope();
            }
            else
#endif
                defaultToECL(expr, s, inType);
            break;
        case no_getresult:
            {
                IHqlExpression * name = queryAttributeChild(expr, namedAtom, 0);
                switch (getIntValue(queryAttributeChild(expr, sequenceAtom, 0)))
                {
                case ResultSequencePersist: 
                    s.append("PERSIST(");
                    break;
                case ResultSequenceStored: 
                    s.append("STORED(");
                    break;
                case ResultSequenceInternal:
                    s.append("INTERNAL(");
                    break;
                default:
                    s.append("RESULT(");
                    if (!name)
                        s.append(getIntValue(queryAttributeChild(expr, sequenceAtom, 0)+1));
                    break;
                }
                if (name)
                    name->toString(s);
                s.append(")");
                break;
            }
        case no_metaactivity:
            if (expr->hasAttribute(pullAtom))
                s.append("PULL");
            else if (expr->hasAttribute(traceAtom))
                s.append("TRACE");
            else
                s.append("no_metaactivity:unknown");
            defaultChildrenToECL(expr, s, inType);
            break;
        case no_getgraphresult:
        case no_setgraphresult:
            if (expandProcessed || tryToRegenerate)
                defaultToECL(expr, s, inType);
            else
            {
                s.append(getOpString(no));
                toECL(expr->queryChild(2), s, true, inType);
            }
            break;
        case no_alias:
        case no_activerow:
        case no_outofline:
        case no_inline:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else
                toECL(child0, s, false, inType);
            break;
        case no_loopcounter:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else
                s.append(getOpString(no));
            break;
        case no_omitted:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            break;
        case no_compound_selectnew:
            if (expandProcessed)
                defaultToECL(expr, s, inType);
            else if (child0->getOperator() == no_select)
                toECL(child0->queryChild(1), s, false, inType);
            else
                toECL(child0, s, false, inType);
            break;
        case no_random:
            defaultToECL(expr, s, inType);
            //s.append("()"); not needed since it always has a hidden parameter which causes () to be generated.
            break;
        case no_subgraph:
            {
                if (tryToRegenerate)
                {
                    childrenToECL(expr, s, false, false, 0);
                }
                else
                {
                    s.append(getEclOpString(expr->getOperator())).append("(");
                    ForEachChild(i, expr)
                    {
                        s.newline().append("\t");
                        toECL(expr->queryChild(i), s, false, inType);
                    }
                    s.append(")");
                }
                break;
            }
        case no_sequence:
            if (expr->queryName())
                s.append(expr->queryName());
            else
                s.append("SEQ");
            s.append("(").append(expr->querySequenceExtra()).append(")");
            break;
        case no_virtualscope:
        case no_concretescope:
        case no_forwardscope:
            {
                IHqlScope * scope = expr->queryScope();
#ifdef SHOW_MODULE_STATUS
                if (expandProcessed)
                {
                    IHqlScope * concrete = scope->queryConcreteScope();
                    if (scope == concrete)
                        s.append("[concrete]");
                    else if (!concrete)
                        s.append("[abstract]");
                    else
                        s.append("[virtual with concrete]");
                }
#endif
                defaultToECL(expr, s, inType);
                s.newline();
                HqlExprArray syms;
                scope->getSymbols(syms);
                syms.sort(compareSymbolsByName);
                ForEachItemIn(i, syms)
                {
                    toECL(&syms.item(i), s.append("\t"), false, false);
                    s.append(";").newline();
                }
                s.append("END;").newline();
                break;
            }
        case no_type:
            {
                IHqlScope * scope = expr->queryScope();
                s.append("TYPE").newline();
                HqlExprArray syms;
                scope->getSymbols(syms);
                syms.sort(compareSymbolsByName);
                ForEachItemIn(i, syms)
                {
                    toECL(&syms.item(i), s, false, true);
                }
                s.append("END");
                break;
            }
        case no_libraryscopeinstance:
            if (xgmmlGraphText)
            {
                IHqlExpression * module = expr->queryDefinition()->queryChild(0);
                s.append("LIBRARY(");
                toECL(module->queryAttribute(nameAtom)->queryChild(0), s, false, inType);
                s.append(')');
            }
            else
            {
                s.append(getEclOpString(no));
                s.append('(');
                childrenToECL(expr, s, inType, false, 0);
                s.append(')');
            }
            break;
        case no_typedef:
            {
                getTypeString(expr->queryType(), s);
                if (expr->numChildren())
                {
                    s.append("{");
                    childrenToECL(expr, s, inType, false, 0);
                    s.append("}");
                }
                break;
            }
        case no_split:
        case no_compound_diskread:
            if (xgmmlGraphText && isEclAlias(child0))
            {
                curDatasetDepth--;
                toECL(child0, s, false, inType);
                curDatasetDepth++;
            }
            else if (tryToRegenerate)
                toECL(child0, s, false, inType);
            else
                defaultToECL(expr, s, inType);
            break;
        case no_assert_ds:
            if (xgmmlGraphText)
            {
                pushScope(child0);
                childrenToECL(expr, s, inType, false, 1);
                popScope();
            }
            else
                defaultToECL(expr, s, inType);
            break;
        case no_compound_indexread:
        case no_compound_disknormalize:
        case no_compound_diskaggregate:
        case no_compound_diskcount:
        case no_compound_diskgroupaggregate:
        case no_compound_indexnormalize:
        case no_compound_indexaggregate:
        case no_compound_indexcount:
        case no_compound_indexgroupaggregate:
        case no_compound_childread:
        case no_compound_childnormalize:
        case no_compound_childaggregate:
        case no_compound_childcount:
        case no_compound_childgroupaggregate:
        case no_compound_inline:
            if (tryToRegenerate)
                toECL(child0, s, false, inType);
            else
                defaultToECL(expr, s, inType);
            break;
        //case no_table:
        //case no_count:
        //case no_if:
        default:
            defaultToECL(expr, s, inType);
            break;
        }
    }

    if (paren)
        s.append(')');
    curDatasetDepth = savedDatasetDepth;
}

void HqltHql::defaultToECL(IHqlExpression *expr, StringBuffer &s, bool inType)
{
    // standard function-style operators
    s.append(getEclOpString(expr->getOperator()));
    defaultChildrenToECL(expr, s, inType);
}

void HqltHql::defaultChildrenToECL(IHqlExpression *expr, StringBuffer &s, bool inType)
{
    // standard function-style operators
    IHqlExpression * child0 = expr->queryChild(0);
    if (child0)
    {
        s.append('(');
        bool needComma = false;
        if (!xgmmlGraphText || (!child0->queryDataset() && !isInternalAttribute(child0)))
        {
            toECL(child0, s, false, inType);
            needComma = true;
        }
        if (child0->queryDataset())
            pushScope(child0);
        childrenToECL(expr, s, inType, needComma, 1);
        if (child0->queryDataset())
            popScope();
        s.append(')');
    }
}

void HqltHql::sortlistToEcl(IHqlExpression *expr, StringBuffer &s, bool addCurleys, bool inType)
{
    unsigned startLength = s.length();
    if (addCurleys)
        s.append("{ ");
    bool needComma = false;
    ForEachChild(idx, expr)
    {
        IHqlExpression * child = queryChild(expr, idx);
        if (child && (expandProcessed || !isInternalAttribute(expr)))
        {
            if (needComma) queryNewline(s.append(", "));
            if (queryAddDotDotDot(s, startLength))
                break;
            needComma = true;
            toECL(child, s, false, inType);
        }
    }
    if (addCurleys)
        s.append(" }");
}

StringBuffer &HqltHql::getFieldTypeString(IHqlExpression * e, StringBuffer &s)
{
    ITypeInfo * type = e->queryType();
    ITypeInfo * cur = type;
    for(;;)
    {
        typemod_t mod = cur->queryModifier();
        if (mod == typemod_none)
            break;
        if (mod == typemod_attr)
        {
            IHqlExpression * attr = (IHqlExpression *)cur->queryModifierExtra();
            if (!isInternalAttribute(attr))
                s.append(attr->queryName()).append(" ");
        }

        cur = cur->queryTypeBase();
    }

    switch (type->getTypeCode())
    {
    case type_groupedtable:
        s.append("GROUPED ");
        type = type->queryChildType();
        //fall through
    case type_table:
    case type_dictionary:
        {
            s.append(type->getTypeCode()==type_dictionary ? "DICTIONARY(" : "DATASET(");
            getTypeString(type->queryChildType()->queryChildType(), s);
            ForEachChild(i, e)
            {
                IHqlExpression * cur = e->queryChild(i);
                if (cur->isAttribute())
                {
                    IAtom * name = cur->queryName();
                    if (name == countAtom || name == sizeofAtom)
                    {
                        toECL(cur, s.append(", "), false, false);
                    }
                }
            }
            return s.append(")");
        }
    case type_row:
        return getTypeString(type->queryChildType(), s);
    default:
        return getTypeString(type, s);
    }
}


StringBuffer &HqltHql::getTypeString(ITypeInfo * i, StringBuffer &s)
{
    type_t t = i->getTypeCode();
    switch (t)
    {
    case type_transform:
        i = i->queryChildType();
        //fallthrough
    case type_record:
    case type_row:
    {   
        ITypeInfo * original = queryModifier(i, typemod_original);
        IHqlExpression * expr;
        StringBuffer name;
        if (original)
        {
            expr = (IHqlExpression *)original->queryModifierExtra();
            toECL(expr, name, false, false);
        }
        else
        {
            i->getECLType(name);
            expr = queryMapped(queryExpression(queryRecordType(i)));
        }

        StringBuffer tmp;
        if(expr->queryName()==lower(unnamedId))
        {
            HqlExprArray * visited = findVisitedArray();
            for (unsigned idx = 0; idx < visited->ordinality(); idx++)
            {
                IHqlExpression * e = &(visited->item(idx));
                IHqlExpression * extra = static_cast<IHqlExpression *>(e->queryTransformExtra());
                if(e->queryChild(1) && extra)
                {
                    if(expr->queryType() == e->queryChild(1)->queryType())
                    {
                        appendId(tmp, e->queryId());
                        break;
                    }
                }
            }
            if (!tmp.length())
            {
                //unusual - an inline record definition e.g. { string x } myTransform(...) := transform
                toECL(expr, tmp, false, false);
            }
        }
        else if (isPublicSymbol(expr))
        {
            doAlias(expr, name, false);
        }
        else if (!expr->queryTransformExtra())
        {
            if (isSymbolDefined(expr))
            {
                clashCounter++;
                name.append("___").append(clashCounter);
            }
            OwnedHqlExpr extra = createId(createIdAtom(name.str()));
            expr->setTransformExtra(extra); // LINKs !
            addVisited(expr);

            tmp.append(name);
            tmp.append(" := ");
            toECL(expr->queryBody(), tmp, false, false, 0, true);
            tmp.append(";").newline().newline();
            addExport(tmp);
            tmp.clear();
        }
        else
        {
            IHqlExpression * prev = (IHqlExpression *)expr->queryTransformExtra();
            IIdAtom * prevId = prev->queryId();
            appendId(name.clear(), prevId);
        }

        if(tmp.length())
        {
            s.append(tmp);  
        }
        else
        {
            s.append(name);
        }

        break;
    }
    case type_alien:
    {
        IHqlExpression * expr = queryExpression(i);
        if (expr)
        {
            toECL(expr, s, false, false);
            return s;
        }
        break;
    }
//  case type_row:
    case type_table:
//  case type_groupedtable:
        break;
    case type_filepos:
        getTypeString(i->queryChildType(), s);
        break;
    default:
        ITypeInfo * original = queryModifier(i, typemod_original);
        IHqlExpression * originalExpr = NULL;
        if (original)
            originalExpr = (IHqlExpression *)original->queryModifierExtra();

        if (originalExpr && (originalExpr->getOperator() == no_typedef))
            toECL(originalExpr, s, false, false);
        else if (t == type_set)
        {
            s.append("set of ");
            i = i->queryChildType();
            if (i)
                getTypeString(i, s);
            else
                s.append("any");
        }
        else
            i->getECLType(s);
        break;
    }
    
    return s;
}

const char * HqltHql::getEclOpString(node_operator op)
{
    switch(op)
    {
    case no_ave:
    case no_avegroup:
        return "AVE";
    case no_concat:
        return "+";
    case no_ne:
        return "!=";
    case no_not:
        return "NOT ";
    case no_or:
        return "OR";
    case no_and:
        return "AND";
    case no_filepos:
        return "FILEPOSITION";
    case no_file_logicalname:
        return "LOGICALFILENAME";
    case no_compound_diskread:
        return "DISKREAD";
    case no_compound_indexread:
        return "INDEXREAD";
    case no_keyedlimit:
        if (expandProcessed)
            return "KEYEDLIMIT";
        return ::getOpString(op);
    default:
        return ::getOpString(op);
    }
}

static bool addExplicitType(IHqlExpression * expr)
{
    ITypeInfo * type = stripFunctionType(expr->queryType());
    if (!type)
        return false;
    switch (type->getTypeCode())
    {
    case type_transform:
        return true;
    case type_dictionary:
    case type_row:
    case type_table:
    case type_groupedtable:
    case type_alien:
        return false;
    }
    return true;
}

void HqltHql::doFunctionDefinition(StringBuffer & newdef, IHqlExpression * funcdef, const char * name, bool inType)
{
    assertex(funcdef->getOperator() == no_funcdef);
    if (addExplicitType(funcdef))
    {
        ITypeInfo * returnType = funcdef->queryType()->queryChildType();
        StringBuffer tmp;
        getTypeString(returnType, tmp);

        if(tmp.length())
            newdef.append(tmp).append(' ');
    }
    newdef.append(name).append('(');

    IHqlExpression * formals = funcdef->queryChild(1);
    IHqlExpression * defaults = funcdef->queryChild(2);
    assertex( !defaults || formals->numChildren() ==defaults->numChildren());
    bool first = true;
    ForEachChild(idx, formals)
    {
        IHqlExpression * curParam = formals->queryChild(idx);
        if (expandProcessed || !curParam->hasAttribute(_hidden_Atom))
        {
            if (first)
                first = false;
            else
                newdef.append(", ");
            toECL(curParam, newdef, false, hasNamedSymbol(curParam) ? inType : true);
            if (defaults)
            {
                IHqlExpression * defexpr = defaults->queryChild(idx);
                if (defexpr->getOperator() != no_omitted)
                {
                    newdef.append("=");
                    toECL(defexpr, newdef, false, false);
                }
            }

        }
    }
    newdef.append(')');

    toECL(funcdef->queryChild(0), newdef.append(" := "), false, false, 0, true);
}

StringBuffer &HqltHql::doAlias(IHqlExpression * expr, StringBuffer &name, bool inType)
{
    bool wasInsideNewTransform = insideNewTransform;
    insideNewTransform = false;
    // Start fresh
    name.clear();
    
    StringBuffer tmp;
    StringBuffer exports;

    scope.append(NULL);
    bool undefined = isPublicSymbol(expr) ? !expr->queryTransformExtra() : (findVisitedArray()->find(*expr) == NotFound);
    if (undefined || inType)
    {
        IHqlExpression * body = expr->queryBody(true);
        StringBuffer newdef;
        bool nameClash = false;
        IHqlExpression * funcdef = expr->queryFunctionDefinition();

        if (!funcdef && expr->getOperator() == no_funcdef)
        {
            //True when doAlias() is called on members of a alien type or scope
            //This is a complete mess!
            funcdef = expr;
        }

        if(isExported(expr) || isShared(expr))
        {   
            if (isExported(expr))
                exports.append("EXPORT ");
            else
                exports.append("SHARED ");
            m_export_level++;

            nameClash = (!expr->queryTransformExtra() && isExportDefined(expr) && !funcdef);
        }
        else
            nameClash = (!expr->queryTransformExtra() && isSymbolDefined(expr) && !funcdef);

        if (nameClash)
        {
            StringBuffer temp;
            makeUniqueName(expr, temp);
            clashCounter++;
            temp.append("___").append(clashCounter);
            OwnedHqlExpr extra = createId(createIdAtom(temp.str()));
            expr->setTransformExtra(extra); // LINKs !
        }

#ifdef SHOW_SYMBOL_LOCATION
        if (expandProcessed)
            newdef.append(expr->queryFullContainerId()).append("(").append(expr->getStartLine()).append(",").append(expr->getStartColumn()).append("):");
#endif
        newdef.append(exports);
        lookupSymbolName(expr, name);

        bool wasFunctionDefined = funcdef && isFunctionDefined(funcdef) && !inType;
        //Add the export name before the body is processed to prevent clashing names (e.g., badrecord2.xhql)
        if (exports.length())
            m_export_names.append(*new StringBufferItem(name));
        
        if(funcdef && !expandProcessed)
        {
            if(!wasFunctionDefined || inType)
            {
                if(!expr->queryTransformExtra())
                {
                    OwnedHqlExpr extra = createId(createIdAtom(name.str()));
                    expr->setTransformExtra(extra);
                    addVisited(expr);
                    addVisited(funcdef);
                    funcdef->setTransformExtra(extra);
                }       

                doFunctionDefinition(newdef, funcdef, name.str(), inType);
            }
            else
            {
                // ???? Huh?
                newdef.clear();
            }
        }
        else if(!inType)
        {
            OwnedHqlExpr extra = createId(createIdAtom(name.str()));
            expr->setTransformExtra(extra); // LINKs !
            addVisited(expr);
        
            // Special cases 
            node_operator op = body->getOperator();
            switch(op)
            {
            case no_record:
            {
                newdef.append(name.str()).append(" := ");
                toECL(body, newdef, false, false, 0, true);
                break;
            }
            default:
                if (addExplicitType(expr))
                    getTypeString(expr->queryType(), tmp.clear());

                if(tmp.length())
                    newdef.append(tmp).append(' ');

                newdef.append(name.str());

                bool isNamed = true;
                if (expr->isDataset())
                {
                    if (expandProcessed && op != no_field && op != no_rows && !isTargetSelector(expr))
                        isNamed = false;
                }

                toECL (body, newdef.append(" := "), false, inType, 0, isNamed); 
                break;
            }

        }
        else
        {
            getTypeString(expr->queryType(), tmp.clear());
            if(tmp.length())
                newdef.append(tmp).append(' ');

            newdef.append(name.str());
            toECL (body, newdef.append(" := "), false, false, 0, true); 
        }

#ifdef SHOW_FUNC_DEFINTION
        if (expandProcessed && expr->getOperator() == no_funcdef)
        {
            newdef.append("{");
            defaultToECL(expr->queryChild(0),newdef,inType);
            newdef.append("}");
        }
#endif

        if(newdef.length())
        {
            newdef.append(';').newline();
            if(inType)
            {
                name.clear().append(' ').append(newdef);
            }
            else
            {
                addExport(tmp.clear().append(newdef).newline());
                if(exports.length())
                {
                    if(m_exports.isItem(m_export_level))
                    {
                        m_definitions.append(m_exports.item(m_export_level));
                        m_exports.item(m_export_level).clear();
                        clearVisited();
                    }   
                }
            }
        }

        // If we got here then we must have finished with the current export
        if(exports.length())
        {
            m_export_level--;
        }
    }
    else
    {
        lookupSymbolName(expr, name);
    }

    scope.pop();
    insideNewTransform = wasInsideNewTransform;
    return name;
}

void HqltHql::defineCallTarget(IHqlExpression * call, StringBuffer & name)
{
    // OK we got us an external call. Let's try and define it.
    //This is a mess!  (To put it politely)
    IHqlExpression * funcdef;
    if (call->getOperator() == no_externalcall)
    {
        funcdef = call->queryExternalDefinition();
    }
    else
    {
        funcdef = call->queryBody()->queryFunctionDefinition();
    }

    IHqlExpression * prevName = static_cast<IHqlExpression *>(call->queryTransformExtra());
    if (funcdef && !prevName && !isServiceDefined(call))
    {
        makeUniqueName(call, name);

        StringBuffer newdef;
        m_service_names.append(*new StringBufferItem(name));

        OwnedHqlExpr extra = createId(createIdAtom(name.str()));
        call->setTransformExtra(extra);
        addVisited(call);

        if(hasNamedSymbol(call))
        {
            if(isExported(call))
            {
                newdef.append("EXPORT ");           
            }
            else if(isShared(call))
            {
                newdef.append("SHARED ");
            }
        }
        else if (!tryToRegenerate)
            newdef.append("EXPORT ");           //not really right - the information is lost about whether the service definition is exported or not.

        newdef.append(name).append(" := ").newline();

        if (call->getOperator() == no_externalcall)
        {
            newdef.append("SERVICE").newline();
            getTypeString(call->queryType(), newdef);
            newdef.append(" ");
            lookupSymbolName(call, newdef);
        }

        if (funcdef->isFunctionDefinition())
        {
            newdef.append("(");
            toECL(funcdef->queryChild(1), newdef, false, true);
            newdef.append(") : ");

            IHqlExpression *child = funcdef->queryChild(0);
            toECL ( child, newdef, child->getPrecedence() < 0, false);
        }
        else
            toECL ( funcdef, newdef, false, false);

        newdef.append(';').newline();

        if (call->getOperator() == no_externalcall)
        {
            newdef.append("END;").newline().newline();
        }

        m_services.append(newdef);
    }
    else
    {
        if (prevName)
            appendId(name, prevName->queryId());
        else
            lookupSymbolName(call, name);
    }



    if (call->getOperator() == no_externalcall)
    {
        name.append('.');
        lookupSymbolName(call, name);
    }
}

static StringBuffer &toECL(StringBuffer &s, HqltHql & hqlthql, HqlExprArray & queries, bool recurse, bool isNamedSymbol = false)
{
    int startpos = s.length();
    try
    {
        ForEachItemIn(idx, queries)
        {
            hqlthql.toECL((IHqlExpression *) &queries.item(idx), s, false, false, 0, isNamedSymbol);

            if(s.length() && s.charAt(s.length() - 1) != ';')
            {
                s.append(';').newline();
            }
        }

        if (recurse)
        {
            StringBuffer definitions;
            hqlthql.gatherServices(definitions);
            hqlthql.gatherDefinitions(definitions);
            s.insert(startpos, definitions.str());
        }
    }
#ifdef _DEBUG
    catch(int*****************){}
#else
    catch(...)
    {
        IERRLOG("toECL() threw an exception");
        s.setLength(startpos);
    }
#endif
    return s;
}


static StringBuffer &toECLDefinition(StringBuffer &s, HqltHql & hqlthql, HqlExprArray & queries, bool recurse)
{
    int startpos = s.length();
    try
    {
        ForEachItemIn(idx, queries)
        {
            StringBuffer name;
            hqlthql.doAlias((IHqlExpression *) &queries.item(idx), name, false);
        }

        if (recurse)
        {
            StringBuffer definitions;
            hqlthql.gatherServices(definitions);
            hqlthql.gatherDefinitions(definitions);
            s.insert(startpos, definitions.str());
        }
    }
#ifdef _DEBUG
    catch(int*****************){}
#else
    catch(...)
    {
        IERRLOG("toECLDefinition() threw an exception");
        s.setLength(startpos);
    }
#endif
    return s;
}

StringBuffer &toECL(IHqlExpression * expr, StringBuffer &s, bool recurse, bool xgmmlGraphText)
{
    HqltHql hqlthql(recurse, xgmmlGraphText);

    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    return toECL(s, hqlthql, queries, recurse);
}


StringBuffer &regenerateECL(IHqlExpression * expr, StringBuffer &s)
{
    HqltHql hqlthql(true, false);
    hqlthql.setIgnoreModuleNames(true);
    hqlthql.setTryToRegenerate(true);

    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    return toECL(s, hqlthql, queries, true);
}


StringBuffer &regenerateDefinition(IHqlExpression * expr, StringBuffer &s)
{
    if (!isEclAlias(expr))
        throw makeStringExceptionV(ERR_INTERNALEXCEPTION, "Internal: regenerateDefinition requires ECL alias expression");
    HqltHql hqlthql(true, false);
    hqlthql.setIgnoreModuleNames(true);

    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    return toECLDefinition(s, hqlthql, queries, true);
}


StringBuffer &toUserECL(StringBuffer &s, IHqlExpression * expr, bool recurse)
{
    HqltHql hqlthql(recurse, false);
    hqlthql.setIgnoreModuleNames(true);

    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    return toECL(s, hqlthql, queries, recurse);
}

void splitECL(IHqlExpression * expr, StringBuffer &s, StringBuffer &d)
{
#ifndef _DEBUG
    int startpos = s.length();
#endif
    try
    {
        HqltHql hqlthql(true, false);
        hqlthql.toECL(expr, s, false, false);

        hqlthql.gatherServices(d);
        hqlthql.gatherDefinitions(d);
    }
#ifdef _DEBUG
    catch(int*****************){}
#else
    catch(...)
    {
        IERRLOG("toECL() threw an exception");
        s.setLength(startpos);
    }
#endif
}

StringBuffer &expandDotLiteral(StringBuffer &s, const char *f)
{
    unsigned lines = 0;
    unsigned chars = 0;
    char c;
    while ((c = *f++))
    {
        switch (c)
        {
        case '\t':
            s.append(" ");
            break;
        case '\r':
            break;
        case '\n':
            s.append("\\l");  // Means left justify.
            if (lines++ > 10)
                return s;
            break;
        case '}':
        case '{':
        case '<':
        case '>':  // Special chars in dot graphs

        case '\\':
        case '\"':
        case '\'':
            s.append('\\');
            // fallthrough
        default:
            if (chars++ > 1000)
                return s;
            s.append(c);
            break;
        }
    }
    return s;
}


StringBuffer & doGetExprECL(IHqlExpression * expr, StringBuffer & out, bool minimalSelectors, bool xgmmlGraphText, unsigned _maxDepth)
{
    const bool recurse = false;
    HqltHql hqlthql(recurse, xgmmlGraphText);
    hqlthql.setExpandNamed(false);
    hqlthql.setIgnoreModuleNames(true);
    hqlthql.setMinimalSelectors(minimalSelectors);
    if (_maxDepth)
        hqlthql.setMaxRecurseDepth(_maxDepth);

    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    toECL(out, hqlthql, queries, recurse);
    const char * text = out.str();
    unsigned len = out.length();
    while (len)
    {
        char c = text[len-1];
        if ((c != ';') && (c != '\n') &&(c != '\r'))
            break;
        len--;
    }
    out.setLength(len);
    return out;
}

StringBuffer & getExprECL(IHqlExpression * expr, StringBuffer & out, bool minimalSelectors, bool xgmmlGraphText)
{
    return doGetExprECL(expr, out, minimalSelectors, xgmmlGraphText, 0);
}


StringBuffer &getRecordECL(IHqlExpression * expr, StringBuffer & out)
{
    HqltHql hqlthql(true, false);
    hqlthql.setIgnoreVirtualAttrs(true);
    hqlthql.setLowerCaseIds(true);


    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    return toECL(out, hqlthql, queries, true, false);
}

StringBuffer & processedTreeToECL(IHqlExpression * expr, StringBuffer &s)
{
    HqltHql hqlthql(true, false);
    hqlthql.setExpandProcessed(true);
    hqlthql.setTryToRegenerate(true);  // Removes EXPORTs and makes it much easier to spot differences in graphs

    HqlExprArray queries;
    unwindCommaCompound(queries, expr);
    return toECL(s, hqlthql, queries, true);
}


StringBuffer & getExprIdentifier(StringBuffer & out, IHqlExpression * expr)
{
    if (expr->queryName())
        return out.append(expr->queryName());
    return doGetExprECL(expr, out, false, false, 1);
}



void dbglogUnboundedText(size32_t len, const char * ecl)
{
    const size32_t chunkSize = 32768;
    while (len > chunkSize)
    {
        const char * next = strchr(ecl+chunkSize, '\n');
        if (!next || !next[1])
            break;
        unsigned size = next-ecl;
        if (ecl[size-1] == '\r')
            size--;
        DBGLOG("%.*s", size, ecl);
        len -= (next+1-ecl);
        ecl = next+1;
    }
    DBGLOG("%s", ecl);
}

void dbglogExpr(IHqlExpression * expr)
{
    if (expr)
    {
        StringBuffer s;
        processedTreeToECL(expr, s);
        dbglogUnboundedText(s.length(), s.str());
    }
}
