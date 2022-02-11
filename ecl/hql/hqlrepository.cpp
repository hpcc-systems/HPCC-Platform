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

#include "hqlrepository.hpp"
#include "hqlerrors.hpp"
#include "hqlplugins.hpp"
#include "jdebug.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"
#include "hqlexpr.ipp"
#include "hqlerror.hpp"
#include "hqlutil.hpp"

static const char * queryExtractFilename(const char * urn)
{
    if (hasPrefix(urn, "file:", true))
        return urn + 5;
    switch (*urn)
    {
    case '~':
    case '.':
    case '/':
        return urn;
    }
#ifdef _WIN32
    //Check for drive:....
    if (*urn && (urn[1] == ':'))
        return urn;
#endif
    return nullptr;
}

bool startsWithGitProtocol(const char * urn)
{
    if (startsWith(urn, "git+") || startsWith(urn, "git:"))
        return true;
    return false;
}

bool looksLikeGitPackage(const char * urn)
{
    if (startsWithGitProtocol(urn))
        return true;
    if (strstr(urn, ".git#"))
        return true;
    return false;
}

static bool splitRepoVersion(StringBuffer & repoUrn, StringBuffer & repo, StringBuffer & version, const char * urn, const char * defaultGitPrefix)
{
    const char * cur = urn;
    //Allow either protocol://<server>/<user>/<repo>[#version] or <user>/<repo>[#version]
    if (startsWithGitProtocol(urn))
    {
        //skip to the end of the protocol
        const char * colon = strchr(urn, ':');
        if (!colon)
            return false;
        if (colon[1] != '/' || colon[2] != '/')
            return false;
        const char * slash = strchr(colon+3, '/');
        if (!slash)
            return false;
        //cur now points at the user - same as the other syntax
        cur = slash + 1;
    }
    else if (isalnum(*urn))
    {
        //Use defaultGitPrefix so gitlab can also be used by default. HPCC-26423
        repoUrn.append(defaultGitPrefix);
        addPathSepChar(repoUrn);
    }
    else
        return false;

    const char * hash = strchr(cur, '#');
    if (hash)
    {
        repoUrn.append(hash-urn, urn);
        repo.append(hash-cur, cur);
        version.set(hash + 1);
    }
    else
    {
        repoUrn.append(urn);
        repo.set(cur);
    }

    if (endsWith(repo, ".git"))
        repo.setLength(repo.length()-4);

    return true;
}


//A (very) temporary solution - to prevent other dependencies from node projects from causing problems
//the correct fix HPCC-27173, to delay processing the package until actually used.
bool canReadPackageFrom(const char * urn)
{
    if (queryExtractFilename(urn))
        return true;
    if (looksLikeGitPackage(urn))
        return true;
    if (!isalnum(*urn))
        return false;
    if (endsWith(urn, ".tgz"))
        return false;
    return true;
}

//-------------------------------------------------------------------------------------------------------------------

static void extractRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope, HqlLookupContext & ctx)
{
    HqlExprArray rootSymbols;
    scope->getSymbols(rootSymbols);
    rootSymbols.sort(compareSymbolsByName);
    ForEachItemIn(i, rootSymbols)
    {
        IHqlExpression & cur = rootSymbols.item(i);
        //Is this symbol a plugin, or a remote scope?  But we do not want to parse the code if it is neither of
        //these because that may cause spurious syntax errors from attributes in the root directory.
        //OwnedHqlExpr resolved = scope->lookupSymbol(cur.queryId(), LSFpublic, ctx);
        IHqlScope * scope = cur.queryScope();
        if (scope)
            rootScopes.append(*LINK(scope));
    }
}

void getRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope, HqlLookupContext & ctx)
{
    scope->ensureSymbolsDefined(ctx);
    extractRootScopes(rootScopes, scope, ctx);
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
        {
            implicitScopes.append(OLINK(scope));
        }
    }
}


extern HQL_API void importRootModulesToScope(IHqlScope * scope, HqlLookupContext & ctx)
{
    IEclRepository * eclRepository = ctx.queryPackage();
    if (eclRepository)
    {
        HqlExprArray rootSymbols;
        eclRepository->queryRootScope()->getSymbols(rootSymbols);
        rootSymbols.sort(compareSymbolsByName);

        ForEachItemIn(i, rootSymbols)
        {
            IHqlExpression & cur = rootSymbols.item(i);
            IIdAtom * curName = cur.queryId();
            OwnedHqlExpr resolved = eclRepository->queryRootScope()->lookupSymbol(curName, LSFpublic, ctx);
            if (resolved)
                scope->defineSymbol(curName, NULL, resolved.getClear(), false, true, ob_import);
        }
    }
}

//-------------------------------------------------------------------------------------------------------------------

IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx, IEclPackage * optPackage)
{
    IEclPackage * package = optPackage ? optPackage : ctx.queryPackage();
    Linked<IHqlScope> parentScope = package->queryRootScope();
    const char * item = attrname;
    for (;;)
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

        //Check for empty module name
        if (!moduleName)
            return NULL;

        OwnedHqlExpr resolved = parentScope->lookupSymbol(moduleName, lookupFlags, ctx);
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
        return LINK(ctx.queryPackage()->queryRootScope());
    OwnedHqlExpr matched = getResolveAttributeFullPath(modname, lookupFlags, ctx, nullptr);
    if (matched)
        return LINK(matched->queryScope());
    return NULL;
}


//-------------------------------------------------------------------------------------------------------------------

class HQL_API CompoundEclRepository : implements CInterfaceOf<IEclPackage>
{
public:
    CompoundEclRepository(const char * _packageName)
    : packageName(_packageName)
    {
        rootScope.setown(new CHqlMergedScope(nullptr, nullptr, nullptr, this));
    }

    void addRepository(IEclRepository & _repository);

    virtual IHqlScope * queryRootScope() { return rootScope; }
    virtual IEclSource * getSource(const char * eclFullname) override;
    virtual bool includeInArchive() const override { return true; }
    virtual const char * queryPackageName() { return packageName; }

protected:
    IArrayOf<IEclRepository> repositories;
    Owned<CHqlMergedScope> rootScope;
    StringAttr packageName;
};

void CompoundEclRepository::addRepository(IEclRepository & _repository)
{
    repositories.append(OLINK(_repository));
    rootScope->addScope(_repository.queryRootScope());
}

IEclSource * CompoundEclRepository::getSource(const char * eclFullname)
{
    ForEachItemIn(i, repositories)
    {
        IEclSource * match = repositories.item(i).getSource(eclFullname);
        if (match)
            return match;
    }
    return nullptr;
}


//-------------------------------------------------------------------------------------------------------------------

extern HQL_API IEclPackage * createCompoundRepositoryF(const char * packageName, IEclRepository * repository, ...)
{
    Owned<CompoundEclRepository> compound = new CompoundEclRepository(packageName);
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


extern HQL_API IEclPackage * createCompoundRepository(const char * packageName, EclRepositoryArray & repositories)
{
    Owned<CompoundEclRepository> compound = new CompoundEclRepository(packageName);
    ForEachItemIn(i, repositories)
        compound->addRepository(repositories.item(i));
    return compound.getClear();
}


//-------------------------------------------------------------------------------------------------------------------

/*
 * This class is used to represent a source file that is not really part of the repository, contained in an implicit module of its own
 */
class HQL_API NestedEclRepository : implements IEclRepository, public CInterface
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
    virtual IEclSource * getSource(const char * path) override { return nullptr; }
    virtual bool includeInArchive() const override { return repository->includeInArchive(); }

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

class HQL_API CNewEclRepository : implements IEclRepositoryCallback, public CInterface
{
public:
    CNewEclRepository(EclRepositoryManager * _container, IEclSourceCollection * _collection, const char * rootScopeFullName, bool _addToArchive) :
        container(_container), collection(_collection), addToArchive(_addToArchive)
    {
        rootScope.setown(createRemoteScope(queryModuleIdFromFullName(rootScopeFullName), rootScopeFullName, this, NULL, NULL, true, NULL));
    }
    IMPLEMENT_IINTERFACE

    virtual IHqlScope * queryRootScope() override { return rootScope->queryScope(); }
    virtual IEclSource * getSource(const char * eclFullname) override;
    virtual bool includeInArchive() const override { return addToArchive; }

//interface IEclRepositoryCallback
    virtual bool loadModule(IHqlRemoteScope *scope, IErrorReceiver *errs, bool forceAll) override;
    virtual IHqlExpression * loadSymbol(IHqlRemoteScope *scope, IIdAtom * searchName) override;
    virtual IEclSource * getSource(IEclSource * parent, IIdAtom * searchName) override;

protected:
    IHqlExpression * createSymbol(IHqlRemoteScope * rScope, IEclSource * source);

protected:
    EclRepositoryManager * container;
    Linked<IEclSourceCollection> collection;
    Owned<IHqlRemoteScope> rootScope;
    CriticalSection cs;
    bool addToArchive;
};



IEclSource * CNewEclRepository::getSource(const char * eclFullname)
{
    Owned<IEclSource> parent = nullptr;
    for (;;)
    {
        IIdAtom * id;
        const char * dot = strchr(eclFullname, '.');
        if (dot)
            id = createIdAtom(eclFullname, dot-eclFullname);
        else
            id = createIdAtom(eclFullname);
        Owned<IEclSource> child = collection->getSource(parent, id);
        if (!child)
            return nullptr;
        if (!dot)
            return child.getClear();

        //Process the next part of the path following the dot
        eclFullname = dot + 1;
        parent.swap(child);
    }
}


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

IEclSource * CNewEclRepository::getSource(IEclSource * parent, IIdAtom * searchName)
{
    return collection->getSource(parent, searchName);
}


IHqlExpression * CNewEclRepository::loadSymbol(IHqlRemoteScope * rScope, IIdAtom * searchName)
{
    IEclSource * parent = rScope->queryEclSource();
    Owned<IEclSource> source = getSource(parent, searchName);
    if (!source)
        return nullptr;
    return createSymbol(rScope, source);
}


static void getFullName(StringBuffer & fullName, IHqlScope * scope, IIdAtom * attrName)
{
    fullName.append(scope->queryFullName());
    if (fullName.length())
        fullName.append(".");
    fullName.append(str(attrName));
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
                unsigned flags = props->getPropInt(str(flagsAtom), 0);
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
            //Slightly ugly create a "delayed" nested scope instead.
            Owned<IProperties> props = source->getProperties();
            Owned<IHqlRemoteScope> childScope = createRemoteScope(eclId, fullName.str(), this, props, contents, true, source);
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
    case ESTdependency:
        {
            const char * defaultUrl = source->queryPath();
            Owned<IProperties> props = source->getProperties();
            bool requireSHA = false;    // Should possibly be true (or true if it came from a package-lock.json file)
            IEclRepository * repo = container->queryDependentRepository(eclId, defaultUrl, requireSHA);
            IHqlScope * childScope = repo->queryRootScope();
            body.set(queryExpression(childScope));
            break;
        }
    default:
        throwUnexpected();
    }
    return ::createSymbol(eclId, scope->queryId(), body.getClear(), NULL, true, true, symbolFlags, contents, 0, 0, 0, 0, 0);
}

//-------------------------------------------------------------------------------------------------------------------

#include "hqlcollect.hpp"

void EclRepositoryManager::inherit(const EclRepositoryManager & other)
{
    options = other.options;
    ForEachItemIn(i1, other.repos)
    {
        EclRepositoryMapping & cur = other.repos.item(i1);
        repos.append(OLINK(cur));
    }
    ForEachItemIn(i2, other.sharedSources)
        sharedSources.append(OLINK(other.sharedSources.item(i2)));

    ForEachItemIn(i3, other.allSources)
        allSources.append(OLINK(other.allSources.item(i3)));
}

void EclRepositoryManager::addNestedRepository(IIdAtom * scopeId, IEclSourceCollection * source, bool includeInArchive)
{
    Owned<IEclRepository> directoryRepository = createRepository(source, str(scopeId), true);
    Owned<IEclRepository> nested = createNestedRepository(scopeId, directoryRepository);
    allSources.append(*nested.getClear());
}

void EclRepositoryManager::addSharedSourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace, bool includeInArchive)
{
    Owned<IEclRepository> repo = createNewSourceFileEclRepository(errs, path, flags, trace, includeInArchive);
    sharedSources.append(*repo.getLink());
    allSources.append(*repo.getClear());
}

void EclRepositoryManager::addQuerySourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace)
{
    Owned<IEclRepository> repo = createNewSourceFileEclRepository(errs, path, flags, trace, true);
    allSources.append(*repo.getClear());
}

void EclRepositoryManager::addSingleDefinitionEclRepository(const char * moduleName, const char * attrName, IFileContents * contents, bool includeInArchive)
{
    Owned<IEclRepository> repo = createSingleDefinitionEclRepository(moduleName, attrName, contents, includeInArchive);
    allSources.append(*repo.getClear());
}

void EclRepositoryManager::addRepository(IEclSourceCollection * source, const char * rootScopeFullName, bool includeInArchive)
{
    Owned<IEclRepository> repo = createRepository(source, rootScopeFullName, includeInArchive);
    allSources.append(*repo.getClear());
}

void EclRepositoryManager::addMapping(const char * url, const char * path)
{
    StringBuffer repoUrn, repo, version;
    if (!splitRepoVersion(repoUrn, repo, version, url, options.defaultGitPrefix))
        throw makeStringExceptionV(99, "Unsupported repository link format '%s'", url);
    repos.append(*new EclRepositoryMapping(repo, version, path));
}

void EclRepositoryManager::processArchive(IPropertyTree * archiveTree)
{
    IArrayOf<IEclRepository> savedSources;        // also includes -D options
    savedSources.swapWith(allSources);

    Owned<IPropertyTreeIterator> subArchives = archiveTree->getElements("Archive");
    ForEach(*subArchives)
    {
        IPropertyTree & cur = subArchives->query();
        const char * defaultUrl = cur.queryProp("@package");
        const char * repoKey = defaultUrl;
        Owned<IEclSourceCollection> archiveCollection(createArchiveEclCollection(&cur));
        ForEachItemIn(iShared, sharedSources)
            allSources.append(OLINK(sharedSources.item(iShared)));
        allSources.append(*createRepository(archiveCollection, nullptr, true));
        Owned<IEclPackage> compound = createPackage(defaultUrl);
        dependencies.emplace_back(repoKey, compound);
    }

    allSources.swapWith(savedSources);
    Owned<IEclSourceCollection> archiveCollection(createArchiveEclCollection(archiveTree));
    addRepository(archiveCollection, nullptr, true);
}

IEclPackage * EclRepositoryManager::queryDependentRepository(IIdAtom * name, const char * defaultUrl, bool requireSHA)
{
    //Check to see if the reference is to a filename.  Should possibly be disabled on a switch.
    const char * filename = queryExtractFilename(defaultUrl);
    const char * repoKey = filename ? filename : defaultUrl;

    //Check to see if we have already resolved this filename, and if so return the previous entry
    //Do this before performing a fetch/update on the cached git repos
    for (auto const & cur : dependencies)
    {
        if (cur.first.compare(repoKey) == 0)
            return cur.second;
    }

    StringBuffer path;
    if (!filename)
    {
        StringBuffer repoUrn, repo, version;
        if (!splitRepoVersion(repoUrn, repo, version, defaultUrl, options.defaultGitPrefix))
            throw makeStringExceptionV(99, "Unsupported repository link format '%s'", defaultUrl);
        if (isEmptyString(version))
            throw makeStringExceptionV(99, "Expected a version number in the url '%s'", defaultUrl);

        //Check to see if the location of a repository has been overriden on the command line:
        ForEachItemIn(i, repos)
        {
            const EclRepositoryMapping & cur = repos.item(i);
            if (streq(cur.url, repo) && (isEmptyString(cur.version) || streq(cur.version, version)))
            {
                filename = cur.path;
                break;
            }
        }

        if (!filename)
        {
            //Use repositories that have been cloned to ~/.HPCCSystems/repos or similar.  No branches are checked out.
            StringBuffer repoPath;
            addPathSepChar(repoPath.append(options.eclRepoPath)).append(repo);

            bool ok = false;
            if (checkDirExists(repoPath))
            {
                if (options.updateRepos)
                {
                    unsigned retCode = runGitCommand(nullptr, "fetch origin", repoPath, true);
                    if (retCode != 0)
                        DBGLOG("Failed to download the latest version of %s", defaultUrl);
                }

                ok = true;
            }
            else
            {
                if (options.fetchRepos)
                {
                    // Ensure the ~/.HPCCSystems/repos directory exists.
                    if (!recursiveCreateDirectory(options.eclRepoPath) && !checkDirExists(options.eclRepoPath))
                        throw makeStringExceptionV(99, "Failed to create directory %s'", options.eclRepoPath.str());

                    VStringBuffer params("clone %s \"%s\" --no-checkout", repoUrn.str(), repo.str());
                    unsigned retCode = runGitCommand(nullptr, params, options.eclRepoPath, true);
                    if (retCode != 0)
                        throw makeStringExceptionV(99, "Failed to clone dependency '%s'", defaultUrl);
                    ok = true;
                }
            }

            if (!ok)
                throw makeStringExceptionV(99, "Cannot locate the source code for dependency '%s'.  --fetchrepos not enabled", defaultUrl);

            if (startsWith(version, "semver:"))
                throw makeStringExceptionV(99, "Semantic versioning not yet supported for dependency '%s'.", defaultUrl);

            // Really the version should be a SHA, but for flexibility version could be a sha, a tag or a branch (on origin).
            // Check for a remote branch first - because it appears that when git clones a repo, it creates a local branch for
            // remote head.  That never gets updated, and if it matches the branch being resolved it causes problems.

            // Check for a remote branch "origin/<version>"
            VStringBuffer params("rev-parse --short origin/%s", version.str());
            StringBuffer sha;
            unsigned retCode = runGitCommand(&sha, params, repoPath, false);
            if (retCode != 0)
            {
                //Check for a tag (or local sha)
                params.clear().appendf("rev-parse --short %s", version.str());
                unsigned retCode = runGitCommand(&sha.clear(), params, repoPath, false);
                if (retCode != 0)
                    sha.clear();
            }

            //Strip any trailing newlines and spaces.
            sha.clip();

            if (sha.isEmpty())
                throw makeStringExceptionV(99, "Branch/tag '%s' could not be found for dependency '%s'.", version.str(), defaultUrl);

            if (requireSHA)
            {
                //If version was a valid sha then the version should match it (one should match the leading characters of the other)
                if (!(hasPrefix(sha, version, false) || hasPrefix(version, sha, false)))
                    throw makeStringExceptionV(99, "Expected a SHA as the git version for dependency '%s'.", defaultUrl);
            }

            path.append(repoPath).appendf("/.git/{%s}", sha.str());
            filename = path;
        }
    }

    //Create a new repository for the directory that contains the dependent package
    allSources.kill();
    Owned<IErrorReceiver> errs = createThrowingErrorReceiver();
    ForEachItemIn(iShared, sharedSources)
        allSources.append(OLINK(sharedSources.item(iShared)));
    unsigned flags = ESFdependencies;
    Owned<IEclRepository> repo = createNewSourceFileEclRepository(errs, filename, flags, 0, true);
    allSources.append(*repo.getClear());
    Owned<IEclPackage> compound = createPackage(defaultUrl);
    dependencies.emplace_back(repoKey, compound);
    return compound;
}


IEclRepository * EclRepositoryManager::createNewSourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace, bool includeInArchive)
{
    Owned<IEclSourceCollection> source = createFileSystemEclCollection(errs, path, flags, trace);
    return createRepository(source, nullptr, includeInArchive);
}

IEclRepository * EclRepositoryManager::createSingleDefinitionEclRepository(const char * moduleName, const char * attrName, IFileContents * contents, bool includeInArchive)
{
    Owned<IEclSourceCollection> source = createSingleDefinitionEclCollection(moduleName, attrName, contents);
    return createRepository(source, nullptr, includeInArchive);
}

IEclPackage * EclRepositoryManager::createPackage(const char * packageName)
{
    Owned<IEclPackage> compound = ::createCompoundRepository(packageName, allSources);
    dependencies.emplace_back("", compound);
    allSources.kill();
    return compound.getClear();
}

IEclRepository * EclRepositoryManager::createRepository(IEclSourceCollection * source, const char * rootScopeFullName, bool includeInArchive)
{
    return new CNewEclRepository(this, source, rootScopeFullName, includeInArchive);
}

void EclRepositoryManager::kill()
{
    repos.kill();
    sharedSources.kill();
    allSources.kill();
}

unsigned EclRepositoryManager::runGitCommand(StringBuffer * output, const char *args, const char * cwd, bool needCredentials)
{
    StringBuffer tempOutput;
    if (!output)
        output= &tempOutput;

    EnvironmentVector env;
    //If fetching from git and the username is specified then use the script file to provide the username/password
    if (needCredentials && getenv("HPCC_GIT_USERNAME"))
    {
        StringBuffer scriptPath;
        getPackageFolder(scriptPath);
        addPathSepChar(scriptPath).append("bin/hpccaskpass.sh");
        env.emplace_back("GIT_ASKPASS", scriptPath);
    }

    const char * cmd = "git";
    VStringBuffer runcmd("%s %s", cmd, args);
    StringBuffer error;
    unsigned ret = runExternalCommand(cmd, *output, error, runcmd, nullptr, cwd, &env);
    if (options.optVerbose)
    {
        if (ret > 0)
            printf("%s return code was %d\n%s\n", runcmd.str(), ret, error.str());
        else
            printf("%s\n", output->str());
    }
    return ret;
}

//-------------------------------------------------------------------------------------------------------------------
