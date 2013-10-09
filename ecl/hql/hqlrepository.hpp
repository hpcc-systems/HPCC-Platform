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

#ifndef _HQLREPOSITORY_HPP_
#define _HQLREPOSITORY_HPP_

#include "hql.hpp"
#include "hqlcollect.hpp"

typedef IArrayOf<IEclRepository> EclRepositoryArray;

extern HQL_API IEclRepository * createNewSourceFileEclRepository(IErrorReceiver *err, const char * pluginPath, unsigned flags, unsigned traceMask);
extern HQL_API IEclRepository * createSingleDefinitionEclRepository(const char * moduleName, const char * attrName, IFileContents * contents);

extern HQL_API IEclRepository * createCompoundRepositoryF(IEclRepository * repository, ...);
extern HQL_API IEclRepository * createCompoundRepository(EclRepositoryArray & repositories);
extern HQL_API IEclRepository * createRepository(IEclSourceCollection * source, const char * rootScopeFullName = NULL);
extern HQL_API IEclRepository * createRepository(EclSourceCollectionArray & sources);
extern HQL_API IEclRepository * createNestedRepository(IIdAtom * name, IEclRepository * root);

extern HQL_API void getRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void getRootScopes(HqlScopeArray & rootScopes, IEclRepository * repository, HqlLookupContext & ctx);
extern HQL_API void getImplicitScopes(HqlScopeArray & implicitScopes, IEclRepository * repository, IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void importRootModulesToScope(IHqlScope * scope, HqlLookupContext & ctx);


extern HQL_API void lookupAllRootDefinitions(IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void lookupAllRootDefinitions(IEclRepository * repository);

extern HQL_API IHqlScope * getResolveDottedScope(const char * modname, unsigned lookupFlags, HqlLookupContext & ctx);
extern HQL_API IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx);

#endif
