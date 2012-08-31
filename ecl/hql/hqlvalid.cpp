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
#include "jliball.hpp"
#include "hql.hpp"
#include "eclrtl.hpp"

#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jstream.ipp"
#include "hqlerrors.hpp"
#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqlutil.hpp"
#include "hqlvalid.hpp"
#include "hqlerror.hpp"

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
}


//----------------------------------------------------------------------------

IHqlExpression * createLocationAttr(ISourcePath * sourcePath, int lineno, int column, int position)
{
    HqlExprArray args;
    args.append(*getSizetConstant(lineno));
    args.append(*getSizetConstant(column));
    args.append(*getSizetConstant(position));
    if (sourcePath)
        args.append(*createUnknown(no_attr, makeNullType(), filenameAtom, LINK(sourcePath)));
    return createAttribute(_location_Atom, args);
}


IHqlExpression * ECLlocation::createLocationAttr() const
{
    return ::createLocationAttr(sourcePath, lineno, column, position);
}


bool ECLlocation::extractLocationAttr(const IHqlExpression * location)
{
    if (!location)
    {
        clear();
        return false;
    }
    if (location->isAttribute())
    {
        IHqlExpression * sourceExpr = location->queryChild(3);
        if (sourceExpr)
            sourcePath = static_cast<ISourcePath *>(sourceExpr->queryUnknownExtra());
        else
            sourcePath = NULL;
        lineno = (int)getIntValue(location->queryChild(0));
        column = (int)getIntValue(location->queryChild(1));
        position = (int)getIntValue(location->queryChild(2));
        return true;
    }
    IHqlExpression * annotation = queryLocation(const_cast<IHqlExpression *>(location));
    if (annotation)
    {
        sourcePath = annotation->querySourcePath();
        lineno = annotation->getStartLine();
        column = annotation->getStartColumn();
        position = 0;
        return true;
    }
    return false;
}

StringBuffer & ECLlocation::getText(StringBuffer & text) const
{
    if (!sourcePath && !lineno)
        return text;

    text.append(sourcePath->str());
    if (lineno)
    {
        text.append("(").append(lineno);
        if (column)
            text.append(",").append(column);
        text.append(")");
    }
    return text;
}


//----------------------------------------------------------------------------


IHqlExpression * checkCreateConcreteModule(IErrorReceiver * errors, IHqlExpression * expr, const ECLlocation & errpos)
{
    IHqlScope * scope = expr->queryScope();
    if (scope && scope->queryConcreteScope())
    {
        IHqlScope * concrete = scope->queryConcreteScope();
        return LINK(queryExpression(concrete));
    }

    if (!areAllBasesFullyBound(expr))
        return LINK(expr);

    StringBuffer fieldText;
    if (scope)
    {
        HqlExprArray symbols;
        scope->getSymbols(symbols);
        symbols.sort(compareSymbolsByName);
        ForEachItemIn(i, symbols)
        {
            IHqlExpression & cur = symbols.item(i);
            if (isPureVirtual(&cur))
            {
                if (fieldText.length())
                    fieldText.append(",");
                fieldText.append(cur.queryName());
            }
        }
    }

    if (fieldText.length())
        reportError(errors, ERR_ABSTRACT_MODULE, errpos, "Cannot use an abstract MODULE in this context (%s undefined)", fieldText.str());
    else if (expr->hasProperty(interfaceAtom))
        reportError(errors, ERR_ABSTRACT_MODULE, errpos, "Cannot use an abstract MODULE in this context (INTERFACE must be instantiated)");
    else
        reportError(errors, ERR_ABSTRACT_MODULE, errpos, "Cannot use an abstract MODULE in this context");
    return LINK(expr);
}

IHqlExpression * checkCreateConcreteModule(IErrorReceiver * errors, IHqlExpression * expr, const IHqlExpression * locationExpr)
{
    ECLlocation location(locationExpr);
    return checkCreateConcreteModule(errors, expr, location);
}
