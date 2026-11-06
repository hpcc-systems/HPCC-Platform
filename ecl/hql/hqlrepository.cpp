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
#include "jsecrets.hpp"

static void extractGitDiagnostics(StringBuffer & error, StringPairVector & diagnostics)
{
    diagnostics.clear();
    StringBuffer filtered;
    const char * current = error.str();
    while (*current)
    {
        const char * end = strchr(current, '\n');
        size_t length = end ? end - current : strlen(current);
        std::string line(current, length);

        bool isTracing = false;
        // Git curl traces start with HH:MM:SS.ffffff; match only the first nine characters.
        if (line.size() >= 9 && line[2] == ':' && line[5] == ':' && line[8] == '.')
        {
            if (isdigit((unsigned char)line[0]) && isdigit((unsigned char)line[1]) &&
                isdigit((unsigned char)line[3]) && isdigit((unsigned char)line[4]) &&
                isdigit((unsigned char)line[6]) && isdigit((unsigned char)line[7]))
            {
                isTracing = true;
            }
        }
        if (isTracing)
        {
            size_t marker = line.find("<= Recv header:");
            if (marker != std::string::npos)
            {
                StringBuffer header;
                header.append(line.c_str() + marker + strlen("<= Recv header:")).trim();
                if (startsWith(header.str(), "HTTP/"))
                {
                    diagnostics.clear();
                    const char * status = strchr(header.str(), ' ');
                    if (status)
                        diagnostics.emplace_back("http-status", std::string(status + 1, strcspn(status + 1, " \r\t")));
                }
                else if (const char * colon = strchr(header.str(), ':'))
                {
                    StringBuffer name;
                    name.append(colon - header.str(), header.str()).toLowerCase();
                    if (streq(name, "retry-after") || startsWith(name.str(), "ratelimit-") || startsWith(name.str(), "x-ratelimit-") ||
                        streq(name, "x-github-request-id") || streq(name, "x-vss-e2eid") || streq(name, "x-tfs-session") || streq(name, "x-request-id"))
                    {
                        StringBuffer value;
                        value.append(colon + 1).trim();
                        if (diagnostics.size() < 64)
                            diagnostics.emplace_back(name.str(), std::string(value.str(), std::min<size_t>(value.length(), 512)));
                    }
                }
            }
        }
        else
        {
            filtered.append(length, current);
            if (end)
                filtered.append('\n');
        }
        if (!end)
            break;
        current = end + 1;
    }
    error.swapWith(filtered);
}

static StringBuffer & formatGitDiagnostics(StringBuffer & text, const StringPairVector & diagnostics)
{
    for (const auto & diagnostic : diagnostics)
    {
        if (text.length())
            text.append("; ");
        text.append(diagnostic.first.c_str()).append('=');
        for (unsigned char next : diagnostic.second)
            text.append((next < ' ' || next == 0x7f) ? ' ' : (char)next);
    }
    return text;
}

static void logGitDiagnostics(const char * repoPath, const StringPairVector & diagnostics)
{
    StringBuffer text;
    formatGitDiagnostics(text, diagnostics);
    if (text.length())
        DBGLOG("Git HTTP diagnostics for '%s': %s", repoPath, text.str());
}

static std::atomic<unsigned> fetchNesting{0};
static std::atomic<bool> abortPending{false};

enum class GitFetchAction
{
    fetch,
    skipFresh,
    skipRecentFailure
};

static GitFetchAction decideGitFetch(unsigned __int64 requestTimeStamp, __int64 fetchToleranceMicroseconds, unsigned __int64 lastFetchTimestamp, unsigned __int64 lastFailedFetchTimestamp, bool knownShaMissing)
{
    if (fetchToleranceMicroseconds < 0)
        return GitFetchAction::fetch;

    unsigned __int64 toleranceValue = (unsigned __int64)fetchToleranceMicroseconds;
    //A recent fetch is irrelevant if it did not retrieve a commit that is known to exist
    if (!knownShaMissing && lastFetchTimestamp && requestTimeStamp <= lastFetchTimestamp + toleranceValue)
        return GitFetchAction::skipFresh;
    if (lastFailedFetchTimestamp && lastFailedFetchTimestamp >= lastFetchTimestamp && requestTimeStamp <= lastFailedFetchTimestamp + toleranceValue)
        return GitFetchAction::skipRecentFailure;
    return GitFetchAction::fetch;
}

static bool isFullCommitSha(const char * version)
{
    size_t length = strlen(version);
    if ((length != 40) && (length != 64))
        return false;
    for (size_t i = 0; i < length; i++)
    {
        if (!isxdigit((unsigned char)version[i]))
            return false;
    }
    return true;
}

//Reject versions that are not valid git refs, or could inject extra arguments into a git command line
static bool isSafeGitVersion(const char * version)
{
    if (isEmptyString(version) || (*version == '-'))
        return false;
    for (const char * cur = version; *cur; cur++)
    {
        unsigned char next = (unsigned char)*cur;
        if ((next <= ' ') || (next == 0x7f) || strchr("\"'~^:?*[\\", next))
            return false;
    }
    return true;
}

//Find the sha for version in the output of git ls-remote, preferring a branch, then a peeled (annotated) tag, then a tag.
static bool extractRemoteSha(StringBuffer & sha, const char * lsRemoteOutput, const char * version)
{
    VStringBuffer branchRef("refs/heads/%s", version);
    VStringBuffer tagRef("refs/tags/%s", version);
    VStringBuffer peeledTagRef("refs/tags/%s^{}", version);
    StringBuffer branchSha, tagSha, peeledTagSha;
    const char * cur = lsRemoteOutput;
    while (*cur)
    {
        const char * eol = strchr(cur, '\n');
        size_t lineLen = eol ? (size_t)(eol - cur) : strlen(cur);
        const char * tab = (const char *)memchr(cur, '\t', lineLen);
        if (tab)
        {
            StringBuffer ref;
            ref.append(lineLen - (tab + 1 - cur), tab + 1).clip();
            StringBuffer * target = nullptr;
            if (streq(ref, branchRef))
                target = &branchSha;
            else if (streq(ref, peeledTagRef))
                target = &peeledTagSha;
            else if (streq(ref, tagRef))
                target = &tagSha;
            if (target)
                target->clear().append(tab - cur, cur);
        }
        if (!eol)
            break;
        cur = eol + 1;
    }

    const StringBuffer & match = branchSha.length() ? branchSha : peeledTagSha.length() ? peeledTagSha : tagSha;
    if (!isFullCommitSha(match.str()))
        return false;
    sha.set(match);
    return true;
}

extern HQL_API bool checkAbortGitFetch()
{
    abortPending = true;
    return fetchNesting == 0;
}

class GitFetchTracker
{
public:
    GitFetchTracker()
    {
        fetchNesting++;
    }
    ~GitFetchTracker()
    {
        if (--fetchNesting == 0)
        {
            //Check if an abort was requested while the git action was in progress.  If so terminate when it completes.
            if (abortPending)
            {
                UERRLOG("Process terminated after git operation.");
                _exit(2);
            }
        }
    }
};

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
    if (startsWith(urn, "http:") || startsWith(urn, "https:"))
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
    if (endsWith(urn, ".tgz"))
        return false;
    if (looksLikeGitPackage(urn))
        return true;
    if (!isalnum(*urn))
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
    virtual void ensureCollection() {}

protected:
    EclRepositoryManager * container;
    Linked<IEclSourceCollection> collection;
    Owned<IHqlRemoteScope> rootScope;
    CriticalSection cs{SYNC_LOCATION};
    bool addToArchive;
};


IEclSource * CNewEclRepository::getSource(const char * eclFullname)
{
    ensureCollection();
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
    ensureCollection();
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
    ensureCollection();
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
            IEclRepository * repo = container->queryDependentRepository(eclId, defaultUrl);
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


class HQL_API CGitEclRepository : public CNewEclRepository
{
public:
    CGitEclRepository(EclRepositoryManager * _container, const char * rootScopeFullName, bool _addToArchive, const char * _repoPath, const char * _urn)
     : CNewEclRepository(_container, nullptr, rootScopeFullName, _addToArchive), repoPath(_repoPath), urn(_urn)
    {
    }

    virtual void ensureCollection() override;

protected:
    CriticalSection crit{SYNC_LOCATION};
    StringAttr repoPath;
    StringAttr urn;
    std::atomic<bool> resolved{false};
};



void CGitEclRepository::ensureCollection()
{
    if (resolved)
        return;

    CriticalBlock block(crit);
    if (resolved)
        return;

    collection.setown(container->resolveGitCollection(repoPath, urn));
    resolved = true;
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
    overrideSources.append(*repo.getLink());
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

void EclRepositoryManager::gatherPackagesUsed(StringArray & used) const
{
    for (auto const & cur : dependencies)
    {
        const char * url = cur.first.c_str();
        if (!isEmptyString(url))
        {
            StringBuffer repoUrn, repo, version;
            if (splitRepoVersion(repoUrn, repo, version, url, nullptr))
            {
                StringBuffer package;
                package.append(repo).append("#").append(version);
                used.append(package);
            }
        }
    }
}

unsigned __int64 EclRepositoryManager::getStatistic(StatisticKind kind) const
{
    switch (kind)
    {
    case StTimeElapsed:
        return cycle_to_nanosec(gitDownloadCycles);
    case StTimeBlocked:
        return cycle_to_nanosec(gitDownloadBlockedCycles);
    }
    return 0;
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

IEclPackage * EclRepositoryManager::queryRepository(IIdAtom * name, const char * defaultUrl, IEclSourceCollection * overrideSource, bool includeDefinitions)
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
    const char * delayedUrn = nullptr;
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
            addPathSepChar(path.append(options.eclRepoPath)).append(repo);
            filename = path;
            delayedUrn = defaultUrl;
        }
    }

    //Create a new repository for the directory that contains the dependent package
    allSources.kill();
    Owned<IErrorReceiver> errs = createThrowingErrorReceiver();
    if (includeDefinitions)
    {
        ForEachItemIn(iDefs, overrideSources)
            allSources.append(OLINK(overrideSources.item(iDefs)));
    }
    ForEachItemIn(iShared, sharedSources)
        allSources.append(OLINK(sharedSources.item(iShared)));

    //Explicitly provided sources take precedence over the contents of the repository
    if (overrideSource)
        addRepository(overrideSource, nullptr, true);

    unsigned flags = ESFdependencies;
    Owned<IEclRepository> repo;
    if (delayedUrn)
        repo.setown(createGitRepository(errs, filename, delayedUrn, flags, 0, true));
    else
        repo.setown(createNewSourceFileEclRepository(errs, filename, flags, 0, true));

    allSources.append(*repo.getClear());
    Owned<IEclPackage> compound = createPackage(defaultUrl);
    dependencies.emplace_back(repoKey, compound);
    return compound;
}


IEclPackage * EclRepositoryManager::queryDependentRepository(IIdAtom * name, const char * defaultUrl)
{
    assertex(name);
    return queryRepository(name, defaultUrl, nullptr, false);
}

IEclPackage * EclRepositoryManager::queryRepositoryAsRoot(const char * defaultUrl, IEclSourceCollection * overrideSources)
{
    return queryRepository(nullptr, defaultUrl, overrideSources, true);
}


static bool isEmptyDirectory(const char * path)
{
    Owned<IDirectoryIterator> iter = createDirectoryIterator(path);
    return !iter->first();
}

static bool checkGitDirIsValid(const char * path)
{
    if (!checkDirExists(path))
        return false;

    //When a git clone happens the following are downloaded in .git/: HEAD, objects, refs
    //If the refs/heads directory is empty then the clone has not completed.
    StringBuffer gitPath;
    addPathSepChar(gitPath.append(path)).append(".git/refs/heads");
    if (!checkDirExists(gitPath))
        return false;

    if (isEmptyDirectory(gitPath))
        return false;

    return true;
}

IEclSourceCollection * EclRepositoryManager::resolveGitCollection(const char * repoPath, const char * defaultUrl)
{
    if (options.optVerbose)
        printf("Dynamically resolve package '%s' to '%s'\n", defaultUrl, repoPath);

    StringBuffer path;
    StringBuffer repoUrn, repo, version;
    if (!splitRepoVersion(repoUrn, repo, version, defaultUrl, options.defaultGitPrefix))
        throw makeStringExceptionV(99, "Unsupported repository link format '%s'", defaultUrl);

    if (startsWith(version, "semver:"))
        throw makeStringExceptionV(ERR_CANNOT_RESOLVE_BRANCH, "Semantic versioning not yet supported for dependency '%s'.", defaultUrl);

    if (!isSafeGitVersion(version))
        throw makeStringExceptionV(ERR_CANNOT_RESOLVE_BRANCH, "Invalid version '%s' for dependency '%s'.", version.str(), defaultUrl);

    auto isCommitCached = [&](const char * commitSha)
    {
        VStringBuffer params("cat-file -e %s^{commit}", commitSha);
        return runGitCommand(nullptr, params, repoPath, false) == 0;
    };

    CCycleTimer gitDownloadTimer;
    cycle_t blockedCycles = 0;
    StringBuffer sha;
    const char * shaSource = nullptr;
    bool shaIsLocal = false;
    bool useExistingCache = !options.cleanRepos && checkGitDirIsValid(repoPath);

    if (isFullCommitSha(version.str()))
    {
        sha.set(version);
        shaSource = "version";
    }
    else if (useExistingCache && options.updateRepos && options.resolveRefsWithLsRemote)
    {
        //ls-remote only retrieves the ref advertisement, and avoids a fetch if the commit is already cached
        VStringBuffer params("ls-remote origin \"refs/heads/%s\" \"refs/tags/%s\" \"refs/tags/%s^{}\"", version.str(), version.str(), version.str());
        StringBuffer output;
        if ((runGitCommand(&output, params, repoPath, true) == 0) && extractRemoteSha(sha, output, version))
            shaSource = "ls-remote";
    }

    if (useExistingCache && shaSource && isCommitCached(sha))
    {
        DBGLOG("Skipping git fetch for '%s': '%s' commit SHA '%s' (from %s) is already cached", repoPath, version.str(), sha.str(), shaSource);
        shaIsLocal = true;
    }

    Owned<IError> error;
    if (!shaIsLocal)
    {
        CCycleTimer lockTimer;
        Owned<IGitUpdateLock> gitUpdateLock(getGitUpdateLock(repoPath));
        blockedCycles = lockTimer.elapsedCycles();
        gitDownloadBlockedCycles += blockedCycles;

        bool alreadyExists = false;
        if (checkDirExists(repoPath))
        {
            if (options.cleanRepos)
            {
                PROGLOG("Forced removal of git repository cache at '%s' (--cleanrepos)", repoPath);
                recursiveRemoveDirectory(repoPath);
            }
            if (checkGitDirIsValid(repoPath))
            {
                alreadyExists = true;
            }
            else
            {
                if (!isEmptyDirectory(repoPath))
                {
                    if (options.cleanInvalidRepos)
                    {
                        PROGLOG("Removing incomplete cache of git repository at '%s' (--cleaninvalidrepos)", repoPath);
                        recursiveRemoveDirectory(repoPath);
                    }

                    if (!isEmptyDirectory(repoPath))
                        throw makeStringExceptionV(99, "Cache of git repository %s exists, but is not valid", repoPath);
                }
            }
        }

        //Another process may have fetched the commit while this process was waiting for the lock
        if (alreadyExists && shaSource && isCommitCached(sha))
        {
            DBGLOG("Skipping git fetch for '%s': '%s' commit SHA '%s' (from %s) was cached while waiting for lock", repoPath, version.str(), sha.str(), shaSource);
            shaIsLocal = true;
        }

        if (!shaIsLocal)
        {
            if (alreadyExists)
            {
                if (options.updateRepos)
                {
                    unsigned __int64 fetchStartTimestamp = getTimeStampNowValue();
                    GitFetchAction action = gitUpdateLock ? decideGitFetch(options.requestTimeStamp, options.fetchToleranceSeconds * 1000000, gitUpdateLock->getLastFetchTimestamp(), gitUpdateLock->getLastFailedFetchTimestamp(), shaSource != nullptr) : GitFetchAction::fetch;
                    unsigned retCode = 0;
                    if (action == GitFetchAction::fetch)
                    {
                        DBGLOG("Fetching git repository '%s' to resolve '%s'", repoPath, version.str());
                        StringPairVector diagnostics;
                        retCode = runGitCommand(nullptr, "fetch origin --prune", repoPath, true, &diagnostics);
                        logGitDiagnostics(repoPath, diagnostics);
                        if (gitUpdateLock)
                            gitUpdateLock->noteFetchResult(fetchStartTimestamp, retCode == 0);
                    }
                    else
                    {
                        unsigned __int64 storedTimestamp = action == GitFetchAction::skipFresh ? gitUpdateLock->getLastFetchTimestamp() : gitUpdateLock->getLastFailedFetchTimestamp();
                        DBGLOG("Skipping git fetch for '%s': request timestamp %llu, last %s fetch timestamp %llu", repoPath, options.requestTimeStamp,
                            action == GitFetchAction::skipFresh ? "successful" : "failed", storedTimestamp);
                        if (action == GitFetchAction::skipRecentFailure)
                            retCode = 1;
                    }
                    if (retCode != 0)
                    {
                        VStringBuffer msg("Failed to download the latest version of '%s' error code (%u)", defaultUrl, retCode);
                        error.setown(createError(CategoryError, SeverityWarning, ERR_FAIL_UPDATE_REPO, msg.str(), nullptr, 0, 0, 0));
                    }
                }
            }
            else if (options.fetchRepos)
            {
                // Ensure the ~/.HPCCSystems/repos directory exists.
                if (!recursiveCreateDirectory(options.eclRepoPath) && !checkDirExists(options.eclRepoPath))
                    throw makeStringExceptionV(99, "Failed to create directory %s'", options.eclRepoPath.str());

                DBGLOG("Cloning git repository '%s' to resolve '%s'", repoPath, version.str());
                VStringBuffer params("clone %s \"%s\" --no-checkout", repoUrn.str(), repo.str());
                unsigned __int64 fetchStartTimestamp = getTimeStampNowValue();
                StringPairVector diagnostics;
                unsigned retCode = runGitCommand(nullptr, params, options.eclRepoPath, true, &diagnostics);
                logGitDiagnostics(repoPath, diagnostics);
                if (gitUpdateLock)
                    gitUpdateLock->noteFetchResult(fetchStartTimestamp, retCode == 0);
                if (retCode != 0)
                {
                    VStringBuffer msg("Failed to clone dependency '%s' error code (%u)", defaultUrl, retCode);
                    error.setown(createError(CategoryError, SeverityError, ERR_FAIL_CLONE_REPO, msg.str(), nullptr, 0, 0, 0));
                }
            }
            else
                throw makeStringExceptionV(99, "Cannot locate the source code for dependency '%s'.  --fetchrepos not enabled", defaultUrl);
        }
        //All following operations are read-only so the lock is released
    }

    gitDownloadCycles += (gitDownloadTimer.elapsedCycles() - blockedCycles);

    if (error)
    {
        if (errorReceiver)
        {
            error.setown(errorReceiver->mapError(error)); // MORE: This mapping should really be done within reportError()
            errorReceiver->report(error);
        }
        else
            throw error.getClear();
    }

    if (!shaSource)
    {
        // version could be a branch, a tag or a short sha.  Check for a remote branch first - because when git clones a repo,
        // it creates a local branch for the remote head that never gets updated.
        VStringBuffer params("rev-parse --verify --quiet \"origin/%s^{commit}\"", version.str());
        if (runGitCommand(&sha.clear(), params, repoPath, false) != 0)
        {
            params.clear().appendf("rev-parse --verify --quiet \"%s^{commit}\"", version.str());
            if (runGitCommand(&sha.clear(), params, repoPath, false) != 0)
                sha.clear();
        }
        sha.clip();

        if (sha.isEmpty())
            throw makeStringExceptionV(ERR_CANNOT_RESOLVE_BRANCH, "Branch/tag '%s' could not be found for dependency '%s'.", version.str(), defaultUrl);
    }
    else if (!shaIsLocal && !isCommitCached(sha))
        throw makeStringExceptionV(ERR_CANNOT_RESOLVE_BRANCH, "Commit '%s' (from %s) for dependency '%s' is not available locally - resubmit to retry.", sha.str(), shaSource, defaultUrl);

    if (options.optVerbose)
        DBGLOG("Version '%s' resolved to sha '%s'", version.str(), sha.str());

    path.append(repoPath).appendf("/.git/{%s", sha.str());
    if (options.gitUser)
        path.append("#").append(options.gitUser);
    path.append("}");

    Owned<IErrorReceiver> errs = createThrowingErrorReceiver();
    unsigned flags = ESFdependencies;
    return createFileSystemEclCollection(errs, path, flags, 0);
}

IEclRepository * EclRepositoryManager::createNewSourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace, bool includeInArchive)
{
    Owned<IEclSourceCollection> source = createFileSystemEclCollection(errs, path, flags, trace);
    return createRepository(source, nullptr, includeInArchive);
}

IEclRepository * EclRepositoryManager::createGitRepository(IErrorReceiver *errs, const char * path, const char * urn, unsigned flags, unsigned trace, bool includeInArchive)
{
    return new CGitEclRepository(this, nullptr, includeInArchive, path, urn);
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
    return runGitCommand(output, args, cwd, needCredentials, nullptr);
}

unsigned EclRepositoryManager::runGitCommand(StringBuffer * output, const char *args, const char * cwd, bool needCredentials, StringPairVector * diagnostics)
{
    StringBuffer tempOutput;
    if (!output)
        output= &tempOutput;

    Owned<IFile> extractedKey;
    EnvironmentVector env;
    if (diagnostics)
    {
        diagnostics->clear();
        env.emplace_back("GIT_TRACE_CURL", "1");
        env.emplace_back("GIT_TRACE_CURL_NO_DATA", "1");
        env.emplace_back("GIT_TRACE_REDACT", "1");
    }
    bool useScript = false;
    //If fetching from git and the username is specified then use the script file to provide the username/password
    //NB: This code should be kept in sync with readLfsContents() in gitfile.cpp (and ideally combined)
    if (needCredentials)
    {
        //If the username is supplied, then get the secret and write it to a temporary location.
        if (options.gitUser.length())
        {
            env.emplace_back("HPCC_GIT_USERNAME", options.gitUser.str());

            // If gituser is specified never prompt for credentials, otherwise the server can hang.
            env.emplace_back("GIT_TERMINAL_PROMPT", "0");

            extractedKey.setown(getFileWithGitAccessToken(options.gitUser.str()));
            if (extractedKey)
            {
                env.emplace_back("HPCC_GIT_PASSPATH", extractedKey->queryFilename());
                useScript = true;
            }
        }

        if (useScript)
        {
            //If fetching from git and the username is specified then use the script file to provide the username/password
            StringBuffer scriptPath;
            getPackageFolder(scriptPath);
            addPathSepChar(scriptPath).append("bin/hpccaskpass.sh");
            env.emplace_back("GIT_ASKPASS", scriptPath);
        }
    }

    GitFetchTracker gitFetchBlock;
    const char * cmd = "git";
    //proactiveAuth (git 2.46+, ignored by older versions) avoids an initial unauthenticated request that fails with 401
    VStringBuffer runcmd("%s %s%s", cmd, useScript ? "-c http.proactiveAuth=basic " : "", args);
    StringBuffer error;
    unsigned ret = runExternalCommand(cmd, *output, error, runcmd, nullptr, cwd, &env);
    if (extractedKey)
        extractedKey->remove();

    if (diagnostics)
        extractGitDiagnostics(error, *diagnostics);

    if (ret > 0)
    {
        const char * extra = "";
        if (options.gitUser.isEmpty())
            extra = " HPCC_GIT_USERNAME was not set";
        //Ensure there are no newlines in the error message so it is reported as a single line
        error.replace('\n', '/');
        DBGLOG("%s return code was %d.%s Error: %s", runcmd.str(), ret, extra, error.str());
    }
    else if (options.optVerbose)
        printf("%s\n", output->str());

    return ret;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class GitRepositoryTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(GitRepositoryTests);
        CPPUNIT_TEST(testFetchDecision);
        CPPUNIT_TEST(testFullCommitSha);
        CPPUNIT_TEST(testSafeGitVersion);
        CPPUNIT_TEST(testExtractRemoteSha);
        CPPUNIT_TEST(testGitDiagnostics);
    CPPUNIT_TEST_SUITE_END();

public:
    void testGitDiagnostics()
    {
        StringBuffer error(
            "12:34:56.123456 http.c:800 unknown trace marker\n"
            "12:34:56.123456 http.c:800 == Info: connection details\n"
            "12:34:56.123456 http.c:800 => Send header: Authorization: secret\n"
            "12:34:56.123456 http.c:800 <= Recv header: HTTP/1.1 302 Found\r\n"
            "12:34:56.123456 http.c:800 <= Recv header: X-RateLimit-Remaining: 10\r\n"
            "12:34:56.123456 http.c:800 <= Recv header: HTTP/2 429\r\n"
            "12:34:56.123456 http.c:800 <= Recv header: Retry-After: 30\r\n"
            "12:34:56.123456 http.c:800 <= Recv header: X-RATELIMIT-Remaining: 0\r\n"
            "12:34:56.123456 http.c:800 <= Recv header: Set-Cookie: secret\r\n"
            "fatal: server returned 429\n");
        StringPairVector diagnostics{{"stale", "value"}};
        extractGitDiagnostics(error, diagnostics);
        CPPUNIT_ASSERT_EQUAL_STR("fatal: server returned 429\n", error.str());
        CPPUNIT_ASSERT_EQUAL(size_t(3), diagnostics.size());
        StringBuffer text;
        formatGitDiagnostics(text, diagnostics);
        CPPUNIT_ASSERT_EQUAL_STR("http-status=429; retry-after=30; x-ratelimit-remaining=0", text.str());

        error.set("ordinary stderr without newline");
        extractGitDiagnostics(error, diagnostics);
        CPPUNIT_ASSERT(diagnostics.empty());
        CPPUNIT_ASSERT_EQUAL_STR("ordinary stderr without newline", error.str());

        error.set("12:34:56\n"
              "12:34:56 ordinary error\n"
              "12:3x:56.123456 ordinary error\n");
        extractGitDiagnostics(error, diagnostics);
        CPPUNIT_ASSERT(diagnostics.empty());
        CPPUNIT_ASSERT_EQUAL_STR("12:34:56\n12:34:56 ordinary error\n12:3x:56.123456 ordinary error\n", error.str());

        error.set("12:34:56.123456 http.c:800 <= Recv header: HTTP/2 200\n"
                  "12:34:56.123456 http.c:800 <= Recv header: X-RateLimit-Delay: 1.5\n");
        extractGitDiagnostics(error, diagnostics);
        CPPUNIT_ASSERT(error.isEmpty());
        CPPUNIT_ASSERT_EQUAL(size_t(2), diagnostics.size());
        diagnostics.emplace_back("x-request-id", "one\rtwo\nthree");
        formatGitDiagnostics(text.clear(), diagnostics);
        CPPUNIT_ASSERT_EQUAL_STR("http-status=200; x-ratelimit-delay=1.5; x-request-id=one two three", text.str());
    }

    void testFetchDecision()
    {
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::fetch, decideGitFetch(100, 10, 0, 0, false));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::skipFresh, decideGitFetch(100, 10, 95, 0, false));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::fetch, decideGitFetch(100, 10, 89, 0, false));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::skipRecentFailure, decideGitFetch(100, 10, 80, 95, false));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::fetch, decideGitFetch(110, 10, 95, 90, false));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::fetch, decideGitFetch(100, -1, 95, 95, false));

        //A known but missing sha ignores a recent successful fetch, but not a recent failure
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::fetch, decideGitFetch(100, 10, 95, 0, true));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::skipRecentFailure, decideGitFetch(100, 10, 80, 95, true));
        CPPUNIT_ASSERT_EQUAL(GitFetchAction::fetch, decideGitFetch(100, 10, 95, 80, true));
    }

    void testFullCommitSha()
    {
        CPPUNIT_ASSERT(isFullCommitSha("0123456789abcdef0123456789ABCDEF01234567"));
        CPPUNIT_ASSERT(isFullCommitSha("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"));
        CPPUNIT_ASSERT(!isFullCommitSha("0123456789abcdef0123456789abcdef0123456"));
        CPPUNIT_ASSERT(!isFullCommitSha("0123456789abcdef0123456789abcdef0123456z"));
        CPPUNIT_ASSERT(!isFullCommitSha("refs/heads/main"));
    }

    void testSafeGitVersion()
    {
        CPPUNIT_ASSERT(isSafeGitVersion("main"));
        CPPUNIT_ASSERT(isSafeGitVersion("feature/v1.2-rc_3"));
        CPPUNIT_ASSERT(isSafeGitVersion("0123456789abcdef0123456789abcdef01234567"));
        CPPUNIT_ASSERT(!isSafeGitVersion(""));
        CPPUNIT_ASSERT(!isSafeGitVersion("-main"));
        CPPUNIT_ASSERT(!isSafeGitVersion("main\" \"--upload-pack=x"));
        CPPUNIT_ASSERT(!isSafeGitVersion("main'"));
        CPPUNIT_ASSERT(!isSafeGitVersion("main --upload-pack=x"));
        CPPUNIT_ASSERT(!isSafeGitVersion("main\ttab"));
        CPPUNIT_ASSERT(!isSafeGitVersion("v1^{}"));
        CPPUNIT_ASSERT(!isSafeGitVersion("a\\b"));
    }

    void testExtractRemoteSha()
    {
        const char * branchSha = "1111111111111111111111111111111111111111";
        const char * tagSha = "2222222222222222222222222222222222222222";
        const char * peeledSha = "3333333333333333333333333333333333333333";
        VStringBuffer output("%s\trefs/heads/feature/v1\n%s\trefs/tags/v1\n%s\trefs/tags/v1^{}\n", branchSha, tagSha, peeledSha);
        StringBuffer sha;
        CPPUNIT_ASSERT(extractRemoteSha(sha, output, "v1"));
        CPPUNIT_ASSERT_EQUAL_STR(peeledSha, sha.str());

        output.appendf("%s\trefs/heads/v1\r\n", branchSha);
        CPPUNIT_ASSERT(extractRemoteSha(sha.clear(), output, "v1"));
        CPPUNIT_ASSERT_EQUAL_STR(branchSha, sha.str());

        VStringBuffer lightweight("%s\trefs/tags/v2", tagSha);
        CPPUNIT_ASSERT(extractRemoteSha(sha.clear(), lightweight, "v2"));
        CPPUNIT_ASSERT_EQUAL_STR(tagSha, sha.str());

        CPPUNIT_ASSERT(!extractRemoteSha(sha.clear(), output, "main"));
        CPPUNIT_ASSERT(!extractRemoteSha(sha.clear(), "", "main"));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(GitRepositoryTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(GitRepositoryTests, "GitRepositoryTests");
#endif

//-------------------------------------------------------------------------------------------------------------------
