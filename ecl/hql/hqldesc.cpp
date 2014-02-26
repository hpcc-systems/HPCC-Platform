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
#include "platform.h"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqldesc.hpp"

void getFullName(StringBuffer & name, IHqlExpression * expr)
{
    IHqlScope * scope = expr->queryScope();
    if (scope)
    {
        name.append(scope->queryFullName());
    }
    else
    {
        const char * module = expr->queryFullContainerId()->str();
        if (module && *module)
            name.append(module).append(".");
        name.append(expr->queryName());
    }
}

void setFullNameProp(IPropertyTree * tree, const char * prop, const char * module, const char * def)
{
    if (module && *module)
    {
        StringBuffer s;
        s.append(module).append('.').append(def);
        tree->setProp(prop, s.str());
    }
    else
        tree->setProp(prop, def);
}

void setFullNameProp(IPropertyTree * tree, const char * prop, IHqlExpression * expr)
{
    IHqlScope * scope = expr->queryScope();
    if (scope)
        tree->setProp(prop, scope->queryFullName());
    else
        setFullNameProp(tree, prop, expr->queryFullContainerId()->lower()->str(), expr->queryName()->str());
}

static int compareSymbolsByPosition(IInterface * * pleft, IInterface * * pright)
{
    IHqlExpression * left = static_cast<IHqlExpression *>(*pleft);
    IHqlExpression * right = static_cast<IHqlExpression *>(*pright);

    int startLeft = left->getStartLine();
    int startRight = right->getStartLine();
    if (startLeft != startRight)
        return startLeft < startRight ? -1 : +1;
    return stricmp(left->queryName()->str(), right->queryName()->str());
}

static void setNonZeroPropInt(IPropertyTree * tree, const char * path, int value)
{
    if (value)
        tree->setPropInt(path, value);
}

static void expandRecordSymbolsMeta(IPropertyTree * metaTree, IHqlExpression * record)
{
    ForEachChild(i, record)
    {
        IHqlExpression * cur = record->queryChild(i);
        switch (cur->getOperator())
        {
        case no_record:
            //MORE: If this is a public symbol it should be expanded, otherwise it will be elsewhere.
            expandRecordSymbolsMeta(metaTree, cur);
            break;
        case no_field:
            {
                IPropertyTree * field = metaTree->addPropTree("Field", createPTree("Field"));
                field->setProp("@name", cur->queryName()->str());
                StringBuffer ecltype;
                cur->queryType()->getECLType(ecltype);
                field->setProp("@type", ecltype);
                break;
            }
        case no_ifblock:
            {
                IPropertyTree * block = metaTree->addPropTree("IfBlock", createPTree("IfBlock"));
                expandRecordSymbolsMeta(block, cur->queryChild(1));
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            {
                IPropertyTree * attr = metaTree->addPropTree("Attr", createPTree("Attr"));
                attr->setProp("@name", cur->queryName()->str());
                break;
            }
        }
    }
}


void expandSymbolMeta(IPropertyTree * metaTree, IHqlExpression * expr)
{
    IPropertyTree * def = NULL;
    if (isImport(expr))
    {
        def = metaTree->addPropTree("Import", createPTree("Import"));
        IHqlExpression * original = expr->queryBody(true);
        setFullNameProp(def, "@ref", original);
    }
    else
    {
        def = metaTree->addPropTree("Definition", createPTree("Definition"));
    }

    if (def)
    {
        IHqlNamedAnnotation * symbol = queryNameAnnotation(expr);
        def->setProp("@name", expr->queryName()->str());
        def->setPropInt("@line", expr->getStartLine());
        if (expr->isExported())
            def->setPropBool("@exported", true);
        else if (isPublicSymbol(expr))
            def->setPropBool("@shared", true);

        if (symbol)
        {
            setNonZeroPropInt(def, "@start", symbol->getStartPos());
            setNonZeroPropInt(def, "@body", symbol->getBodyPos());
            setNonZeroPropInt(def, "@end", symbol->getEndPos());
        }

        if (expr->isScope() && !isImport(expr))
        {
            expandScopeSymbolsMeta(def, expr->queryScope());
        }
        else if (expr->isRecord())
        {
            def->setProp("@type", "record");
            expandRecordSymbolsMeta(def, expr);
        }
    }
}


void expandScopeSymbolsMeta(IPropertyTree * meta, IHqlScope * scope)
{
    if (!scope)
        return;

    HqlExprArray symbols;
    scope->getSymbols(symbols);
    symbols.sort(compareSymbolsByPosition);

    ForEachItemIn(i, symbols)
        expandSymbolMeta(meta, &symbols.item(i));
}
