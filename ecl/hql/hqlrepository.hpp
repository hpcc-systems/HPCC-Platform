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

#ifndef _HQLREPOSITORY_HPP_
#define _HQLREPOSITORY_HPP_

#include "hql.hpp"
#include "hqlcollect.hpp"

typedef IArrayOf<IEclRepository> EclRepositoryArray;
class EclRepositoryMapping : public CInterface
{
public:
    EclRepositoryMapping(const char * _url, const char * _version, const char * _path)
    : url(_url), version(_version), path(_path)
    {
    }

    StringAttr url;
    StringAttr version;
    StringAttr path;
};

class HQL_API EclRepositoryManager
{
public:
    EclRepositoryManager() = default;
    EclRepositoryManager(const EclRepositoryManager & other) = delete;

    void addNestedRepository(IIdAtom * scopeId, IEclSourceCollection * source, bool includeInArchive);
    void addQuerySourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace);
    void addSharedSourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace, bool includeInArchive);
    void addSingleDefinitionEclRepository(const char * moduleName, const char * attrName, IFileContents * contents, bool includeInArchive);
    void addRepository(IEclSourceCollection * source, const char * rootScopeFullName, bool includeInArchive);

    void addMapping(const char * url, const char * path);

    IEclPackage * createPackage(const char * packageName);
    void inherit(const EclRepositoryManager & other);
    void kill();

    void processArchive(IPropertyTree * archiveTree);
    IEclPackage * queryDependentRepository(IIdAtom * name, const char * defaultUrl, bool requireSHA);
    void setOptions(const char * _eclRepoPath, const char * _defaultGitPrefix, bool _fetchRepos, bool _updateRepos, bool _verbose)
    {
        options.eclRepoPath.set(_eclRepoPath);
        options.defaultGitPrefix.set(_defaultGitPrefix);
        options.fetchRepos = _fetchRepos;
        options.updateRepos = _updateRepos;
        options.optVerbose = _verbose;
    }

protected:
    IEclRepository * createNewSourceFileEclRepository(IErrorReceiver *errs, const char * path, unsigned flags, unsigned trace, bool includeInArchive);
    IEclRepository * createSingleDefinitionEclRepository(const char * moduleName, const char * attrName, IFileContents * contents, bool includeInArchive);
    IEclRepository * createRepository(IEclSourceCollection * source, const char * rootScopeFullName, bool includeInArchive);

    unsigned runGitCommand(StringBuffer * output, const char *args, const char * cwd, bool needCredentials);

private:
    using DependencyInfo = std::pair<std::string, Shared<IEclPackage>>;
    CIArrayOf<EclRepositoryMapping> repos;
    std::vector<DependencyInfo> dependencies;
    IArrayOf<IEclRepository> sharedSources;     // plugins, std library, bundles
    IArrayOf<IEclRepository> allSources;        // also includes -D options

    //Include all options in a nested struct to make it easy to ensure they are cloned
    struct {
        StringAttr eclRepoPath;
        StringAttr defaultGitPrefix;
        bool fetchRepos = false;
        bool updateRepos = false;
        bool optVerbose = false;
    } options;
};


extern HQL_API IEclPackage * createCompoundRepositoryF(const char * packageName, IEclRepository * repository, ...);
extern HQL_API IEclPackage * createCompoundRepository(const char * packageName, EclRepositoryArray & repositories);
extern HQL_API IEclRepository * createNestedRepository(IIdAtom * name, IEclRepository * root);

extern HQL_API void getRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void getRootScopes(HqlScopeArray & rootScopes, IEclRepository * repository, HqlLookupContext & ctx);
extern HQL_API void getImplicitScopes(HqlScopeArray & implicitScopes, IEclRepository * repository, IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void importRootModulesToScope(IHqlScope * scope, HqlLookupContext & ctx);

extern HQL_API IHqlScope * getResolveDottedScope(const char * modname, unsigned lookupFlags, HqlLookupContext & ctx);
extern HQL_API IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx, IEclPackage * optPackage);
extern HQL_API bool looksLikeGitPackage(const char * urn);
extern HQL_API bool canReadPackageFrom(const char * urn);

#endif
