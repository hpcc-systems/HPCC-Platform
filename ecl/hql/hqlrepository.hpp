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
extern HQL_API IEclRepository * createNestedRepository(_ATOM name, IEclRepository * root);

extern HQL_API void getRootScopes(HqlScopeArray & rootScopes, IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void getRootScopes(HqlScopeArray & rootScopes, IEclRepository * repository, HqlLookupContext & ctx);
extern HQL_API void getImplicitScopes(HqlScopeArray & implicitScopes, IEclRepository * repository, IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void importRootModulesToScope(IHqlScope * scope, HqlLookupContext & ctx);


extern HQL_API void lookupAllRootDefinitions(IHqlScope * scope, HqlLookupContext & ctx);
extern HQL_API void lookupAllRootDefinitions(IEclRepository * repository);

extern HQL_API IHqlScope * getResolveDottedScope(const char * modname, unsigned lookupFlags, HqlLookupContext & ctx);
extern HQL_API IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx);

#endif
