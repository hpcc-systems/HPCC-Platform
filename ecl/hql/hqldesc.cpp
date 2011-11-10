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

void expandSymbolMeta(IPropertyTree * metaTree, IHqlExpression * expr)
{
    IPropertyTree * def = NULL;
    if (isImport(expr))
    {
        def = metaTree->addPropTree("Import", createPTree("Import"));
    }
    else
    {
        def = metaTree->addPropTree("Definition", createPTree("Definition"));
    }

    if (def)
    {
        def->setProp("@name", expr->queryName()->str());
        def->setPropInt("@line", expr->getStartLine());
        if (expr->isExported())
            def->setPropBool("@exported", true);
        else if (isPublicSymbol(expr))
            def->setPropBool("@shared", true);

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
