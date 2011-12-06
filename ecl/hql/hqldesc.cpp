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
        const char * module = expr->queryFullModuleName()->str();
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
        setFullNameProp(tree, prop, expr->queryFullModuleName()->str(), expr->queryName()->str());
}

int compareSymbolsByPosition(IInterface * * pleft, IInterface * * pright)
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
