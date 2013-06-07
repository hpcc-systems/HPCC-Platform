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
#include "hqlexpr.hpp"
#include "hqlgram.hpp"
#include "hqlerrors.hpp"
#include "hqlexpr.ipp"
#include "hqltrans.ipp"

/**
 *  This file contains functions to handle virtual dataset parameters.
 */

/* In expr: linked. from, to: not linked */
IHqlExpression* bindField(IHqlExpression* expr, IHqlExpression* from, IHqlExpression* to)
{
    return quickFullReplaceExpression(expr, from, to);
}

IHqlExpression* HqlGram::bindDatasetParameter(IHqlExpression* expr, IHqlExpression* formal, IHqlExpression* actual, const attribute& errpos)
{
    if (isAbstractDataset(actual))
        return bindAbstractDataset(expr, formal, actual, errpos);
    return bindConcreteDataset(expr, formal, actual, errpos);
}


IHqlExpression* HqlGram::processAbstractDataset(IHqlExpression* _expr, IHqlExpression* formal, IHqlExpression* actual, IHqlExpression * mapping, const attribute& errpos, bool errorIfNotFound, bool & hadError)
{
    LinkedHqlExpr transformed = _expr;
    IHqlExpression* formalRecord = formal->queryRecord();
    IHqlExpression* actualRecord = actual->queryRecord();
    assertex(formalRecord && actualRecord); 

    hadError = false;
    IHqlSimpleScope *actualScope = actualRecord->querySimpleScope();
    unsigned numChildren = formalRecord->numChildren();
    for (unsigned idx = 0; idx < numChildren; idx++)
    {
        IHqlExpression* kid = formalRecord->queryChild(idx);
        if ((kid->getOperator() == no_ifblock) || kid->isAttribute())
            continue;

        IIdAtom * name = kid->queryId();
        IIdAtom * mapto = fieldMapTo(mapping, name);

        OwnedHqlExpr match = actualScope->lookupSymbol(mapto);
        if (match)
        {
            if (!kid->queryType()->assignableFrom(match->queryType()))
            {
                StringBuffer fromType, toType;
                getFriendlyTypeStr(kid,fromType);
                getFriendlyTypeStr(match,toType);
                reportError(ERR_DSPARAM_TYPEMISMATCH, errpos, "Can not mapping type %s(field '%s') to %s(field '%s')",
                    fromType.str(), kid->queryName()->str(), toType.str(), match->queryName()->str());
                hadError = true;
            }
            //MORE: This should really be mapped in a single go
            if (transformed)
                transformed.setown(bindField(transformed, kid, match));
        }
        else if (errorIfNotFound)
        {
            reportError(ERR_DSPARM_MISSINGFIELD,errpos,"Dataset %s has no field named '%s'", actual->queryName()->str(), mapto->str());
            hadError = true;
        }
    }       

    return transformed.getClear();
}


IHqlExpression* HqlGram::bindAbstractDataset(IHqlExpression* _expr, IHqlExpression* formal, IHqlExpression* actual, const attribute& errpos)
{
    OwnedHqlExpr expr = _expr;

    IHqlExpression* mapping = queryFieldMap(actual);
    bool hadError = false;
    return processAbstractDataset(expr, formal, actual, mapping, errpos, false, hadError);
}

IHqlExpression* HqlGram::bindConcreteDataset(IHqlExpression* _expr, IHqlExpression* formal, IHqlExpression* actual, const attribute& errpos)
{
    OwnedHqlExpr expr = _expr;
    IHqlExpression * mapping = NULL;
    if (actual->getOperator() == no_fieldmap)
    {
        mapping = actual->queryChild(1);
        actual = actual->queryChild(0);
    } 

    bool hadError = false;
    return processAbstractDataset(expr, formal, actual, mapping, errpos, true, hadError);
}


//================================================================================================

bool HqlGram::checkTemplateFunctionParameters(IHqlExpression* func, HqlExprArray& actuals, const attribute& errpos)
{
    bool anyErrors = false;
    IHqlExpression * formals = func->queryChild(1);
    ForEachItemIn(idx, actuals)
    {
        IHqlExpression* formal = formals->queryChild(idx);
        if (isAbstractDataset(formal))
        {
            IHqlExpression * actual = &actuals.item(idx);
            IHqlExpression* mapping = queryFieldMap(actual);
            bool hadError = false;
            OwnedHqlExpr ignore = processAbstractDataset(NULL, formal, actual, mapping, errpos, true, hadError);
            if (hadError)
                anyErrors = true;
        }
    }

    return !anyErrors;
}

/* mapping is a no_sortlist of comma pairs */
IIdAtom * HqlGram::fieldMapTo(IHqlExpression* mapping, IIdAtom * id)
{
    if (!mapping)
        return id;

    IAtom * name = id->lower();
    ForEachChild(i, mapping)
    {
        IHqlExpression * map = mapping->queryChild(i);
        if (map->queryChild(0)->queryName() == name)
            return map->queryChild(1)->queryId();
    }

    return id;
}

/* mapping is a no_sortlist of comma pairs */
IIdAtom * HqlGram::fieldMapFrom(IHqlExpression* mapping, IIdAtom * id)
{
    if (!mapping)
        return id;

    IAtom * name = id->lower();
    ForEachChild(i, mapping)
    {
        IHqlExpression * map = mapping->queryChild(i);
        if (map->queryChild(1)->queryName() == name)
            return map->queryChild(0)->queryId();
    }

    return id;
}

// either a abstract dataset 
bool HqlGram::requireLateBind(IHqlExpression* funcdef, Array& actuals)
{
    ForEachItemIn(idx, actuals)
    {
        IHqlExpression *actual = (IHqlExpression *) &actuals.item(idx);
        if (isAbstractDataset(actual))
            return true;
    }
    
    return false;
}

//================================================================================================

IHqlExpression* HqlGram::queryFieldMap(IHqlExpression* expr)
{
    if (!fieldMapUsed)
        return NULL;
    loop
    {
        if (expr == NULL)
            return NULL;
        //Walks down the dataset list, trying to find a field map on any dataset..
        if (expr->getOperator() == no_fieldmap)
            return expr->queryChild(1);
        expr = expr->queryChild(0);
    }
}

static IHqlExpression* doClearFieldMap(IHqlExpression* expr);

IHqlExpression* HqlGram::clearFieldMap(IHqlExpression* expr)
{
    if (!fieldMapUsed || !expr)
        return expr;
    TransformMutexBlock lock;
    IHqlExpression* ret = doClearFieldMap(expr);
    expr->Release();
    return ret;
}

/* Precondition: expr != NULL */
static IHqlExpression* doClearFieldMap(IHqlExpression* expr)
{
    IHqlExpression * v = (IHqlExpression *)expr->queryTransformExtra();
    if (v)
        return LINK(v);

    IHqlExpression * cur = expr;
    if (cur->getOperator() == no_fieldmap)
        cur = cur->queryChild(0);

    HqlExprArray newkids;
    bool diff = false;
    unsigned max = cur->numChildren();
    for (unsigned idx = 0; idx < max; idx++)
    {
        IHqlExpression *oldkid = cur->queryChild(idx);
        IHqlExpression *newkid = doClearFieldMap(oldkid);
        if (oldkid != newkid)
            diff = true;
        newkids.append(*newkid);
    }

    IHqlExpression * ret;
    if (diff)
        ret = cur->clone(newkids);
    else
        ret = LINK(cur);

    if (cur != expr)
        cur->setTransformExtra(ret);
    expr->setTransformExtra(ret);
    return ret;
}

/* In, out linked */
IHqlExpression* HqlGram::bindFieldMap(IHqlExpression* expr, IHqlExpression* map)
{
    if (map)
    {
        IHqlExpression* oldmap = queryFieldMap(expr);
        if (oldmap)
        {
            HqlExprArray newmaps;
            ForEachChild(i, oldmap)
            {
                IHqlExpression * old = oldmap->queryChild(i);
                IHqlExpression * from = old->queryChild(0);
                IHqlExpression * to = old->queryChild(1);
                IIdAtom * name = from->queryId();
                IIdAtom * mappedName = fieldMapFrom(map, name);

                if (name == mappedName)
                    newmaps.append(*LINK(old));
                else
                    newmaps.append(*createComma(createId(mappedName), LINK(to)));
            }
            map->Release();
            IHqlExpression * newmap = createSortList(newmaps);

            expr = clearFieldMap(expr);
            expr = createFieldMap(expr,newmap);
        }
        else
            expr = createFieldMap(expr,map);
        fieldMapUsed = true;
    }
    return expr;
}

//================================================================================================

IHqlExpression* HqlGram::bindTemplateFunctionParameters(IHqlExpression* funcdef, HqlExprArray& actuals, const attribute& errpos)
{
    IHqlExpression* func = funcdef->queryChild(0);
    IHqlExpression* formals = funcdef->queryChild(1);
    IHqlExpression* defaults = funcdef->queryChild(2);
    assert(func->getOperator() == no_template_context);
    
    if (!checkTemplateFunctionParameters(funcdef, actuals, errpos))
        return createNullExpr(func);

    // Get the scope which preserves the symbols which we resolved when it was originally parsed
    IHqlScope* scope = func->queryScope();
    
    // Add parameters to that scope
    Owned<IHqlScope> newScope = new CHqlContextScope(scope);
    ForEachItemIn(idx, actuals)
    {
        IHqlExpression* formal = formals->queryChild(idx);
        IHqlExpression* actual = &actuals.item(idx);
        if (isOmitted(actual))
        {
            actual = defaults->queryChild(idx);
            //MORE: rebind the parameter
        }
        IIdAtom * parentModuleName = NULL;
        newScope->defineSymbol(formal->queryId(),parentModuleName,actual,true,false,0);
    }

    IHqlExpression* expr = reparseTemplateFunction(funcdef, newScope, lookupCtx, fieldMapUsed);
    if (!expr)
        expr = createNullExpr(func);

    return expr;
}
