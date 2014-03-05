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

#include "hqlrepository.hpp"
#include "hqlerrors.hpp"
#include "hqlplugins.hpp"
#include "jdebug.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"
#include "hqlexpr.ipp"
#include "hqlerror.hpp"

//-------------------------------------------------------------------------------------------------------------------

static void getRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope)
{
    HqlExprArray rootSymbols;
    scope->getSymbols(rootSymbols);
    ForEachItemIn(i, rootSymbols)
    {
        IHqlExpression & cur = rootSymbols.item(i);
        IHqlScope * scope = cur.queryScope();
        if (scope)
            rootScopes.append(*LINK(scope));
    }
}

void getRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope, HqlLookupContext & ctx)
{
    scope->ensureSymbolsDefined(ctx);
    getRootScopes(rootScopes, scope);
}

void getRootScopes(HqlScopeArray & rootScopes, IEclRepository * repository, HqlLookupContext & ctx)
{
    getRootScopes(rootScopes, repository->queryRootScope(), ctx);
}

void getImplicitScopes(HqlScopeArray& implicitScopes, IEclRepository * repository, IHqlScope * scope, HqlLookupContext & ctx)
{
    //Any implicit scope requires explicit module imports
    if (scope->isImplicit())
        return;

    //See note before CMergedScope for notes about implicit scopes.
    HqlScopeArray rootScopes;
    getRootScopes(rootScopes, repository->queryRootScope(), ctx);
    ForEachItemIn(i, rootScopes)
    {
        IHqlScope & scope = rootScopes.item(i);
        if (scope.isImplicit())
            implicitScopes.append(OLINK(scope));
    }
}


extern HQL_API void importRootModulesToScope(IHqlScope * scope, HqlLookupContext & ctx)
{
    IEclRepository * eclRepository = ctx.queryRepository();
    HqlScopeArray rootScopes;
    getRootScopes(rootScopes, eclRepository, ctx);
    ForEachItemIn(i, rootScopes)
    {
        IHqlScope & cur = rootScopes.item(i);
        IIdAtom * curName = cur.queryId();
        OwnedHqlExpr resolved = eclRepository->queryRootScope()->lookupSymbol(curName, LSFpublic, ctx);
        if (resolved)
            scope->defineSymbol(curName, NULL, resolved.getClear(), false, true, ob_import);
    }
}

//-------------------------------------------------------------------------------------------------------------------

void lookupAllRootDefinitions(IHqlScope * scope, HqlLookupContext & ctx)
{
    HqlExprArray rootSymbols;
    scope->getSymbols(rootSymbols);
    ForEachItemIn(i, rootSymbols)
    {
        ::Release(scope->lookupSymbol(rootSymbols.item(i).queryId(), LSFsharedOK, ctx));
    }
}

void lookupAllRootDefinitions(IEclRepository * repository)
{
    HqlParseContext parseCtx(repository, NULL);
    ThrowingErrorReceiver errs;
    HqlLookupContext ctx(parseCtx, &errs);
    lookupAllRootDefinitions(repository->queryRootScope(), ctx);
}

//-------------------------------------------------------------------------------------------------------------------

IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx)
{
    Owned<IHqlScope> parentScope;
    const char * item = attrname;
    loop
    {
        const char * dot = strchr(item, '.');
        IIdAtom * moduleName;
        if (dot)
        {
            moduleName = createIdAtom(item, dot-item);
            item = dot + 1;
        }
        else
        {
            moduleName = createIdAtom(item);
        }

        OwnedHqlExpr resolved;
        if (parentScope)
        {
            resolved.setown(parentScope->lookupSymbol(moduleName, lookupFlags, ctx));
        }
        else
        {
            resolved.setown(ctx.queryRepository()->queryRootScope()->lookupSymbol(moduleName, lookupFlags, ctx));
        }

        if (!resolved || !dot)
            return resolved.getClear();
        IHqlScope * scope = resolved->queryScope();

        if (!scope)
            return NULL;

        parentScope.set(scope);
    }
}


IHqlScope * getResolveDottedScope(const char * modname, unsigned lookupFlags, HqlLookupContext & ctx)
{
    if (!modname || !*modname)
        return LINK(ctx.queryRepository()->queryRootScope());
    OwnedHqlExpr matched = getResolveAttributeFullPath(modname, lookupFlags, ctx);
    if (matched)
        return LINK(matched->queryScope());
    return NULL;
}


//-------------------------------------------------------------------------------------------------------------------

class HQL_API CompoundEclRepository : public CInterface, implements IEclRepository
{
public:
    CompoundEclRepository() { rootScope.setown(new CHqlMergedScope(NULL, NULL)); }

    IMPLEMENT_IINTERFACE;

    void addRepository(IEclRepository & _repository);

    virtual IHqlScope * queryRootScope() { return rootScope; }

protected:
    IArrayOf<IEclRepository> repositories;
    Owned<CHqlMergedScope> rootScope;
};

void CompoundEclRepository::addRepository(IEclRepository & _repository)
{
    repositories.append(OLINK(_repository));
    rootScope->addScope(_repository.queryRootScope());
}

//-------------------------------------------------------------------------------------------------------------------

extern HQL_API IEclRepository * createCompoundRepositoryF(IEclRepository * repository, ...)
{
    Owned<CompoundEclRepository> compound = new CompoundEclRepository;
    compound->addRepository(*repository);
    va_list args;
    va_start(args, repository);
    for (;;)
    {
        IEclRepository * next = va_arg(args, IEclRepository*);
        if (!next)
            break;
        compound->addRepository(*next);
    }
    va_end(args);
    return compound.getClear();
}


extern HQL_API IEclRepository * createCompoundRepository(EclRepositoryArray & repositories)
{
    Owned<CompoundEclRepository> compound = new CompoundEclRepository;
    ForEachItemIn(i, repositories)
        compound->addRepository(repositories.item(i));
    return compound.getClear();
}


//-------------------------------------------------------------------------------------------------------------------

class HQL_API NestedEclRepository : public CInterface, implements IEclRepository
{
public:
    NestedEclRepository(IIdAtom * name, IEclRepository * _repository) : repository(_repository)
    {
        rootScope.setown(createScope());
        IHqlExpression * scope = repository->queryRootScope()->queryExpression();

        rootScope->defineSymbol(name, NULL, LINK(scope), true, false, 0);
    }

    IMPLEMENT_IINTERFACE;

    virtual IHqlScope * queryRootScope() { return rootScope; }

protected:
    Linked<IEclRepository> repository;
    Owned<IHqlScope> rootScope;
};

//-------------------------------------------------------------------------------------------------------------------

extern HQL_API IEclRepository * createNestedRepository(IIdAtom * name, IEclRepository * repository)
{
    if (!repository)
        return NULL;
    return new NestedEclRepository(name, repository);
}

//-------------------------------------------------------------------------------------------------------------------

static IIdAtom * queryModuleIdFromFullName(const char * name)
{
    if (!name)
        return NULL;
    const char * dot = strrchr(name, '.');
    if (dot)
        return createIdAtom(dot+1);
    return createIdAtom(name);
}

class HQL_API CNewEclRepository : public CInterface, implements IEclRepositoryCallback
{
public:
    CNewEclRepository(IEclSourceCollection * _collection, const char * rootScopeFullName) : collection(_collection)
    {
        rootScope.setown(createRemoteScope(queryModuleIdFromFullName(rootScopeFullName), rootScopeFullName, this, NULL, NULL, true, NULL));
    }
    IMPLEMENT_IINTERFACE

    virtual IHqlScope * queryRootScope() { return rootScope->queryScope(); }
    virtual bool loadModule(IHqlRemoteScope *scope, IErrorReceiver *errs, bool forceAll);
    virtual IHqlExpression * loadSymbol(IHqlRemoteScope *scope, IIdAtom * searchName);

protected:
    IHqlExpression * createSymbol(IHqlRemoteScope * rScope, IEclSource * source);

protected:
    Linked<IEclSourceCollection> collection;
    Owned<IHqlRemoteScope> rootScope;
    CriticalSection cs;
};



bool CNewEclRepository::loadModule(IHqlRemoteScope * rScope, IErrorReceiver *errs, bool forceAll)
{
    IEclSource * parent = rScope->queryEclSource();
    CHqlRemoteScope * targetScope = static_cast<CHqlRemoteScope *>(rScope);
    Owned<IEclSourceIterator> iter = collection->getContained(parent);
    if (iter)
    {
        ForEach(*iter)
        {
            IEclSource * next = &iter->query();
            Owned<IHqlExpression> element = createSymbol(rScope, next);
            targetScope->defineSymbol(LINK(element));
        }
    }
    return true;
}

IHqlExpression * CNewEclRepository::loadSymbol(IHqlRemoteScope * rScope, IIdAtom * searchName)
{
    IEclSource * parent = rScope->queryEclSource();
    Owned<IEclSource> source = collection->getSource(parent, searchName);
    return createSymbol(rScope, source);
}


static void getFullName(StringBuffer & fullName, IHqlScope * scope, IIdAtom * attrName)
{
    fullName.append(scope->queryFullName());
    if (fullName.length())
        fullName.append(".");
    fullName.append(attrName->str());
}


IHqlExpression * CNewEclRepository::createSymbol(IHqlRemoteScope * rScope, IEclSource * source)
{
    IIdAtom * eclId = source->queryEclId();
    IHqlScope * scope = rScope->queryScope();
    StringBuffer fullName;
    getFullName(fullName, scope, eclId);

    EclSourceType sourceType = source->queryType();
    IFileContents * contents = source->queryFileContents();
    OwnedHqlExpr body;
    unsigned symbolFlags = 0;
    switch (sourceType)
    {
    case ESTdefinition:
        {
            Owned<IProperties> props = source->getProperties();
            if (props)
            {
                unsigned flags = props->getPropInt(flagsAtom->str(), 0);
                if (flags & ob_sandbox)
                    symbolFlags |= ob_sandbox;
            }

            /*
            int flags = t->getPropInt("@flags", 0);
            if(access >= cs_read)
                flags|=ob_showtext;
                */
            break;
        }
    case ESTplugin:
    case ESTmodule:
    case ESTlibrary:
        {
            //Slightly ugly create a "delayed" nested scope instead.  But with a NULL owner - so will never be called back
            //Probably should be a difference class instance
            Owned<IProperties> props = source->getProperties();
            Owned<IHqlRemoteScope> childScope = createRemoteScope(eclId, fullName.str(), NULL, props, contents, true, source);
            body.set(queryExpression(childScope->queryScope()));
            break;
        }
    case ESTcontainer:
        {
            Owned<IProperties> props = source->getProperties();
            Owned<IHqlRemoteScope> childScope = createRemoteScope(eclId, fullName.str(), this, props, NULL, true, source);
            body.set(queryExpression(childScope->queryScope()));
            break;
        }
    default:
        throwUnexpected();
    }
    return ::createSymbol(eclId, scope->queryId(), body.getClear(), NULL, true, true, symbolFlags, contents, 0, 0, 0, 0, 0);
}


extern HQL_API IEclRepository * createRepository(IEclSourceCollection * source, const char * rootScopeFullName)
{
    return new CNewEclRepository(source, rootScopeFullName);
}

extern HQL_API IEclRepository * createRepository(EclSourceCollectionArray & sources)
{
    if (sources.ordinality() == 0)
        return NULL;
    if (sources.ordinality() == 1)
        return createRepository(&sources.item(0));

    EclRepositoryArray repositories;
    ForEachItemIn(i, sources)
        repositories.append(*createRepository(&sources.item(i)));
    return createCompoundRepository(repositories);
}

//-------------------------------------------------------------------------------------------------------------------

#include "hqlcollect.hpp"

extern HQL_API IEclRepository * createNewSourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace)
{
    Owned<IEclSourceCollection> source = createFileSystemEclCollection(errs, path, flags, trace);
    return createRepository(source);
}

extern HQL_API IEclRepository * createSingleDefinitionEclRepository(const char * moduleName, const char * attrName, IFileContents * contents)
{
    Owned<IEclSourceCollection> source = createSingleDefinitionEclCollection(moduleName, attrName, contents);
    return createRepository(source);
}
