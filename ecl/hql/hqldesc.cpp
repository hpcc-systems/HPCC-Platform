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
#include "platform.h"

#include "hql.hpp"
#include "hqlexpr.hpp"
#include "hqldesc.hpp"
#include "hqlatoms.hpp"

enum InheritType : unsigned short
{
    inherited,
    override,
    local
};

const char * getInheritTypeText(InheritType ihType)
{
    switch(ihType)
    {
        case inherited : return "inherited";
        case override : return "override";
        case local : return "local";
    }
    return "unknown";
}

void getFullName(StringBuffer & name, IHqlExpression * expr)
{
    IHqlScope * scope = expr->queryScope();
    if (scope)
    {
        name.append(scope->queryFullName());
    }
    else
    {
        const char * module = str(expr->queryFullContainerId());
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
    if (scope && !containsCall(expr, false))
        tree->setProp(prop, scope->queryFullName());
    else
        setFullNameProp(tree, prop, str(lower(expr->queryFullContainerId())), str(expr->queryName()));
}

static int compareSymbolsByPosition(IInterface * const * pleft, IInterface * const * pright)
{
    IHqlExpression * left = static_cast<IHqlExpression *>(*pleft);
    IHqlExpression * right = static_cast<IHqlExpression *>(*pright);

    int startLeft = left->getStartLine();
    int startRight = right->getStartLine();
    if (startLeft != startRight)
        return startLeft < startRight ? -1 : +1;
    return stricmp(str(left->queryName()), str(right->queryName()));
}

static void expandRecordSymbolsMeta(IPropertyTree *, IHqlExpression *);

void expandType(IPropertyTree * def, ITypeInfo * type)
{
    type_t tc = type->getTypeCode();
    switch (tc)
    {
        case type_record:
        {
            def->setProp("@type", "record");
            ITypeInfo * original = queryModifier(type, typemod_original);
            if (original)
            {
                IHqlExpression * expr = (IHqlExpression *)original->queryModifierExtra();
                setFullNameProp(def, "@fullname", expr);
                def->setProp("@name", str(expr->queryId()));

            }
            else
            {
                def->setPropBool("@unnamed", true);
                IHqlExpression * record = queryExpression(type);
                expandRecordSymbolsMeta(def, record);
            }
            break;
        }
        case type_scope:
        {
            IHqlExpression * original = queryExpression(type);
            if (original->hasAttribute(interfaceAtom))
            {
                def->setProp("@type", "interface");
            }
            else
            {
                def->setProp("@type", "module");
            }
            setFullNameProp(def, "@fullname", original);
            def->setProp("@name", str(original->queryId()));
            break;
        }
        case type_table:
        case type_groupedtable:
        case type_dictionary:
        {
            def->setProp("@type", type->queryTypeName());
            IPropertyTree * childtype = def->addPropTree("Type");
            expandType(childtype, type->queryChildType()->queryChildType());
            break;
        }
        case type_function:
        {
            IHqlExpression * params = (IHqlExpression * )((IFunctionTypeExtra *)type->queryModifierExtra())->queryParameters();
            IPropertyTree * ptree = def->addPropTree("Params");
            ForEachChild(i, params)
            {
                IPropertyTree * ptype = ptree->addPropTree("Type");
                expandType(ptype, params->queryChild(i)->queryType());
            }
        }
            //fallthrough
        case type_set:
        case type_row:
        case type_pattern:
        case type_rule:
        case type_token:
        case type_transform:
        case type_pointer:
        case type_array:
        {
            def->setProp("@type", type->queryTypeName());
            if (type->queryChildType())
            {
                IPropertyTree * childtype = def->addPropTree("Type");
                expandType(childtype, type->queryChildType());
            }
            break;
        }
        case type_none:
        case type_ifblock:
        case type_alias:
        case type_blob:
            throwUnexpected();
            break;
        case type_class:
            def->setProp("@type", "class");
            def->setProp("@class", type->queryTypeName());
            break;
        default:
        {
            StringBuffer s;
            type->getECLType(s);
            def->setProp("@type", s.str());
            break;
        }
    }
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
                IPropertyTree * field = metaTree->addPropTree("Field");
                field->setProp("@name", str(cur->queryId()));
                IPropertyTree * typeTree = field->addPropTree("Type");
                expandType(typeTree, cur->queryType());
                break;
            }
        case no_ifblock:
            {
                IPropertyTree * block = metaTree->addPropTree("IfBlock");
                expandRecordSymbolsMeta(block, cur->queryChild(1));
                break;
            }
        case no_attr:
        case no_attr_link:
        case no_attr_expr:
            {
                IPropertyTree * attr = metaTree->addPropTree("Attr");
                attr->setProp("@name", str(cur->queryName()));
                break;
            }
        }
    }
}

void expandScopeMeta(IPropertyTree * meta, IHqlExpression * expr)
{
    if (expr->hasAttribute(virtualAtom))
        meta->setPropBool("@virtual", true);

    if (expr->hasAttribute(interfaceAtom))
    {
        meta->setProp("@type", "interface");
    }
    else
    {
        meta->setProp("@type", "module");
    }

    IPropertyTree* scopes = meta->addPropTree("Parents");

    // Walk Attributes to determine inherited scopes
    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        IHqlScope * curBase = cur->queryScope();
        if (curBase)
        {
            IPropertyTree* inherited = scopes->addPropTree("Parent");
            inherited->setProp("@name", str(cur->queryId()));
            setFullNameProp(inherited, "@ref", cur);
        }
    }

    expandScopeSymbolsMeta(meta, expr->queryScope());
}

void expandParamMeta(IPropertyTree * meta, IHqlExpression * cur)
{
    IPropertyTree * param = meta->addPropTree("Param");
    param->setProp("@name", str(cur->queryId()));
    IPropertyTree * typeTree = param->addPropTree("Type");
    expandType(typeTree, cur->queryType());
}

void expandFunctionMeta(IPropertyTree * meta, IHqlExpression * expr)
{
    IHqlExpression * child = queryFunctionParameters(expr);
    IPropertyTree * params = meta->addPropTree("Params");
    ForEachChild(i, child)
    {
        expandParamMeta(params, child->queryChild(i));
    }

    if (expr->isScope())
    {
        child = expr->queryChild(0);
        if (child->isScope() && !isImport(child))
        {
            expandScopeMeta(meta, child);
        }
        return;
    }
    else if (expr->isTransform())
    {
        meta->setProp("@type", "transform");
        IPropertyTree * returnTree = meta->addPropTree("Type");
        expandType(returnTree, expr->queryType()->queryChildType()->queryChildType());
        return;
    }
    else if (isEmbedFunction(expr))
    {
        meta->setProp("@type", "embed");
    }
    else if (expr->isMacro())
    {
        meta->setProp("@type", "macro");
    }
    else if (expr->isType())
    {
        meta->setProp("@type", "type");
    }
    else
    {
        meta->setProp("@type", "function");
    }

    IPropertyTree * returnTree = meta->addPropTree("Type");
    expandType(returnTree, expr->queryType()->queryChildType());
}

void expandSymbolMeta(IPropertyTree * metaTree, IHqlExpression * expr, InheritType ihType)
{
    IPropertyTree * def = NULL;
    if (isImport(expr))
    {
        def = metaTree->addPropTree("Import");
        IHqlExpression * original = expr->queryBody(true);
        setFullNameProp(def, "@ref", original);
        IHqlScope * scope = expr->queryScope();
        if(scope)
        {
            IHqlRemoteScope * remoteScope = queryRemoteScope(scope);
            if (remoteScope)
            {
                def->setPropBool("@remotescope", true);
            }
        }
    }
    else
    {
        def = metaTree->addPropTree("Definition");
    }

    if (def)
    {
        Owned<IPropertyTree> javadoc = getJavadocAnnotation(expr);
        if (javadoc)
            def->addPropTree("Documentation", javadoc.getClear());
        IHqlNamedAnnotation * symbol = queryNameAnnotation(expr);
        def->setProp("@name", str(expr->queryId()));
        def->setPropInt("@line", expr->getStartLine());

        if (expr->isExported())
            def->setPropBool("@exported", true);
        else if (isPublicSymbol(expr))
            def->setPropBool("@shared", true);

        def->setProp("@inherittype", getInheritTypeText(ihType));

        if (symbol)
        {
            def->setPropInt("@start", symbol->getStartPos());
            def->setPropInt("@body", symbol->getBodyPos());
            def->setPropInt("@end", symbol->getEndPos());
            setFullNameProp(def, "@fullname", expr);
        }

        if(expr->isFunction())
        {
            expandFunctionMeta(def, expr);
        }
        else if (expr->isScope() && !isImport(expr))
        {
            expandScopeMeta(def, expr);
        }
        else if (expr->isRecord())
        {
            def->setProp("@type", "record");
            expandRecordSymbolsMeta(def, expr);
        }
        else if (expr->isType())
        {
            def->setProp("@type", "type");
        }
        else if (isImport(expr))
        {

        }
        else
        {
            def->setProp("@type", "attribute");
            IPropertyTree * returnTree = def->addPropTree("Type");
            expandType(returnTree, expr->queryType());
        }
    }
}


void expandScopeSymbolsMeta(IPropertyTree * meta, IHqlScope * scope)
{
    if (!scope)
        return;

    //The following symbols will not have parsed all their members, and can cause recursive dependency errors trying.
    switch (queryExpression(scope)->getOperator())
    {
    case no_forwardscope:
        meta->setPropBool("@forward", true);
        return;
    case no_remotescope:
        //Strange e.g. shared me := myModule;
        meta->setPropBool("@global", true);
        return;
    }

    HqlExprArray symbols;
    scope->getSymbols(symbols);
    symbols.sort(compareSymbolsByPosition);

    IHqlExpression * expr = queryExpression(scope);

    IArrayOf<IHqlScope> bases;

    ForEachChild(i, expr)
    {
        IHqlExpression * cur = expr->queryChild(i);
        IHqlScope * curBase = cur->queryScope();
        if (curBase)
        {
            bases.append(*LINK(curBase));
        }
    }

    ForEachItemIn(i, symbols)
    {
        IHqlExpression * curSym = &symbols.item(i);
        HqlDummyLookupContext lookupCtx(NULL);
        OwnedHqlExpr lookupSym = scope->lookupSymbol(curSym->queryId(), LSFsharedOK|LSFignoreBase, lookupCtx);
        InheritType ihType = local;
        ForEachItemIn(iScope, bases)
        {
            IHqlScope * base = &bases.item(iScope);
            OwnedHqlExpr baseSym = base->lookupSymbol(lookupSym->queryId(), LSFsharedOK|LSFfromderived, lookupCtx);
            if (baseSym)
            {
                ihType = override;
                if (baseSym->queryBody() == lookupSym->queryBody())
                {
                    ihType = inherited;
                    break;
                }
            }
        }
        expandSymbolMeta(meta, curSym, ihType);
    }
}
