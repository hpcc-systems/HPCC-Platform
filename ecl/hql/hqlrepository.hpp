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

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jcomp.hpp"
#include "hql.hpp"
#include "hqlexpr.hpp"
#include "jmisc.hpp"
#include "jsocket.hpp"
#include "hqlvalid.hpp"
#include "hql.hpp"

#define SOURCEFILE_CONSTANT       0x10000000
#define SOURCEFILE_PLUGIN         0x20000000

class HQL_API FileErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    IMPLEMENT_IINTERFACE;

    int errcount;
    int warncount;
    FILE *f;

    FileErrorReceiver(FILE *_f)
    {
        errcount = 0;
        warncount = 0;
        f = _f;
    }

    virtual void reportError(int errNo, const char *msg, const char * filename, int _lineno, int _column, int _pos) 
    {
        errcount++;
        if (!filename) filename = "";
        fprintf(f, "%s(%d,%d): error C%04d: %s\n", filename, _lineno, _column, errNo, msg);
    }

    virtual void reportWarning(int warnNo, const char *msg, const char * filename, int _lineno, int _column, int _pos) 
    {
        warncount++;
        if (!filename) filename = *unknownAtom;
        fprintf(f, "%s(%d,%d): warning C%04d: %s\n", filename, _lineno, _column, warnNo, msg);
    }

    virtual void report(IECLError* e)
    {
        expandReportError(this, e);
    }

    virtual size32_t errCount() { return errcount; };
    virtual size32_t warnCount() { return warncount; };
};


interface IDirectoryIterator;
class HQL_API CEclRepository : public CInterface, implements IEclRepository
{
public:
    IMPLEMENT_IINTERFACE;

    virtual void checkCacheValid() {}
};


class HQL_API ConcreteEclRepository : public CEclRepository
{
public:
    ConcreteEclRepository();
    ~ConcreteEclRepository();

    virtual void getRootScopes(HqlScopeArray & rootScopes, HqlLookupContext & ctx);
    virtual IHqlExpression * lookupRootSymbol(IAtom * name, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual void checkCacheValid();

protected:
    IHqlRemoteScope * queryResolveScope(const char * modname);

protected:
    Owned<IHqlRemoteScope> rootScope;
    CriticalSection cs;
};


//Treat a set of repositories as a single repository.
//v1 Assumes there is no overlap in the scope names - v2 will need to rectify this
class HQL_API CompoundEclRepository : public CEclRepository
{
public:
    virtual IHqlExpression * lookupRootSymbol(IAtom * name, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual bool loadModule(IHqlRemoteScope * rScope, IErrorReceiver * errs, bool forceAll);
    virtual IHqlExpression * loadSymbol(IAtom * moduleName, IAtom * attrName);

    virtual void getRootScopes(HqlScopeArray & rootScopes, HqlLookupContext & ctx);

    virtual void checkCacheValid();

    void addRepository(IEclRepository & _repository)
    {
        repositories.append(OLINK(_repository));
    }

protected:
    IArrayOf<IEclRepository> repositories;
};

class HQL_API SourceFileEclRepository : public ConcreteEclRepository
{
public:
    SourceFileEclRepository(IErrorReceiver *errs, const char * pluginPath, const char * _constSourceSearchPath, const char * _dynamicSourceSearchPath, unsigned _traceMask);

    virtual bool loadModule(IHqlRemoteScope *scope, IErrorReceiver *errs, bool forceAll);
    virtual IHqlExpression * loadSymbol(IAtom *, IAtom *) { return 0; }
    virtual void checkCacheValid();

private:
    void loadDirectoryTree(IHqlRemoteScope * parentScope, IErrorReceiver *errs, IDirectoryIterator * dir, const char * dirPath, StringBuffer & sourcePath, unsigned moduleFlags);
    void processSourceFiles(const char * sourceSearchPath, IErrorReceiver *errs, unsigned moduleFlags);

private:
    StringAttr dynamicSourceSearchPath;
    unsigned traceMask;
};

extern HQL_API IEclRepository *createSourceFileEclRepository(IErrorReceiver *err, const char * pluginPath, const char * constSourceSearchPath, const char * dynamicSourceSearchPath, unsigned traceMask);
extern HQL_API IErrorReceiver *createFileErrorReceiver(FILE *f);
extern HQL_API IHqlScope * getResolveDottedScope(const char * modname, unsigned lookupFlags, HqlLookupContext & ctx);
extern HQL_API IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx);
extern HQL_API IHqlScope * createSyntaxCheckScope(IEclRepository & repository, const char * module, const char *attributes);
extern HQL_API IEclRepository * createCompoundRepositoryF(IEclRepository * repository, ...);
extern HQL_API void getImplicitScopes(HqlScopeArray & implicitScopes, IEclRepository * repository, IHqlScope * scope, HqlLookupContext & ctx);

extern bool isPluginDllScope(IHqlScope * scope);
extern bool isImplicitScope(IHqlScope * rScope);

#endif
