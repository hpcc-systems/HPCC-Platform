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
#include "hqlrepository.hpp"
#include "hqlerrors.hpp"
#include "hqlplugins.hpp"
#include "jdebug.hpp"
#include "jfile.hpp"
#include "eclrtl.hpp"
#include "hqlexpr.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/ecl/hql/hqlrepository.cpp $ $Id: hqlrepository.cpp 66009 2011-07-06 12:28:32Z ghalliday $");

bool isPluginDllScope(IHqlScope * scope)
{
    if (!scope)
        return false;
    unsigned flags=scope->getPropInt(flagsAtom, 0);
    return (flags & PLUGIN_DLL_MODULE) != 0;
}

inline bool isDynamicModule(IHqlScope * scope)
{
    unsigned flags=scope->getPropInt(flagsAtom, 0);
    return !(flags & SOURCEFILE_CONSTANT);
}

inline bool allowImplicitImport(IHqlScope * scope)
{
    unsigned flags=scope->getPropInt(flagsAtom, 0);
    return !(flags & SOURCEFILE_PLUGIN);
}

bool isImplicitScope(IHqlScope * scope)
{
    unsigned flags=scope->getPropInt(flagsAtom, 0);
    return (flags & (PLUGIN_IMPLICIT_MODULE)) != 0;
}


IHqlExpression * getResolveAttributeFullPath(const char * attrname, unsigned lookupFlags, HqlLookupContext & ctx)
{
    Owned<IHqlScope> parentScope;
    const char * item = attrname;
    const char * dot;
    do
    {
        dot = strchr(item, '.');
        _ATOM moduleName;
        if (dot)
        {
            moduleName = createIdentifierAtom(item, dot-item);
            item = dot + 1;
        }
        else
        {
            moduleName = createIdentifierAtom(item);
        }

        OwnedHqlExpr resolved;
        if (parentScope)
        {
            resolved.setown(parentScope->lookupSymbol(moduleName, lookupFlags, ctx));
        }
        else
        {
            resolved.setown(ctx.eclRepository->lookupRootSymbol(moduleName, lookupFlags, ctx));
        }

        if (!resolved || !dot)
            return resolved.getClear();
        IHqlScope * scope = resolved->queryScope();

        if (!scope)
            return NULL;

        parentScope.set(scope);
    } while (dot);
    return LINK(queryExpression(parentScope));
}


IHqlScope * getResolveDottedScope(const char * modname, unsigned lookupFlags, HqlLookupContext & ctx)
{
    OwnedHqlExpr matched = getResolveAttributeFullPath(modname, lookupFlags, ctx);
    if (matched)
        return LINK(matched->queryScope());
    return NULL;
}


IHqlScope * createSyntaxCheckScope(IEclRepository & repository, const char * module, const char *attributes)
{
    //MORE: This doesn't currently work on child modules, and the list of attributes is overkill - a single item would be enough.
    repository.checkCacheValid();
    //GHMORE: This whole function should really die.
    HqlLookupContext GHMOREctx(NULL, NULL,  NULL, &repository);
    Owned<IHqlScope> parent = getResolveDottedScope(module, LSFimport, GHMOREctx);
    if (!parent)
        return NULL;

    return new CHqlSyntaxCheckScope(parent, &repository, attributes, true);
}

//-------------------------------------------------------------------------------------------------------------------

ConcreteEclRepository::ConcreteEclRepository()
{
    rootScope.setown(createRemoteScope(NULL, NULL, this, NULL, NULL, false));
}

ConcreteEclRepository::~ConcreteEclRepository()
{
    rootScope->invalidateParsed();
}

void ConcreteEclRepository::checkCacheValid()
{
}

IHqlExpression * ConcreteEclRepository::lookupRootSymbol(_ATOM name, unsigned lookupFlags, HqlLookupContext & ctx)
{
    CriticalBlock block(cs);
    return rootScope->queryScope()->lookupSymbol(name, lookupFlags, ctx);
}

void ConcreteEclRepository::getRootScopes(HqlScopeArray & rootScopes, HqlLookupContext & ctx)
{
    CriticalBlock block(cs);
    rootScope->queryScope()->ensureSymbolsDefined(ctx);

    HqlExprArray rootSymbols;
    rootScope->queryScope()->getSymbols(rootSymbols);
    ForEachItemIn(i, rootSymbols)
    {
        IHqlExpression & cur = rootSymbols.item(i);
        IHqlScope * scope = cur.queryScope();
        if (scope)
            rootScopes.append(*LINK(scope));
    }
}

//-------------------------------------------------------------------------------------------------------------------

void CompoundEclRepository::getRootScopes(HqlScopeArray & rootScopes, HqlLookupContext & ctx)
{
    ForEachItemIn(i, repositories)
        repositories.item(i).getRootScopes(rootScopes, ctx);
}

IHqlExpression * CompoundEclRepository::lookupRootSymbol(_ATOM name, unsigned lookupFlags, HqlLookupContext & ctx)
{
    ForEachItemInRev(i, repositories)
    {
        IHqlExpression * match = repositories.item(i).lookupRootSymbol(name, lookupFlags, ctx);
        if (match)
            return match;
    }
    return NULL;
}


bool CompoundEclRepository::loadModule(IHqlRemoteScope * rScope, IErrorReceiver * errs, bool forceAll)
{
    throwUnexpected();
}

IHqlExpression * CompoundEclRepository::loadSymbol(IAtom * moduleName, IAtom * attrName)
{
    throwUnexpected();
}

void CompoundEclRepository::checkCacheValid()
{
    CEclRepository::checkCacheValid();
    ForEachItemIn(i, repositories)
        repositories.item(i).checkCacheValid();
}

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
    return compound.getClear();
}

//-------------------------------------------------------------------------------------------------------------------

//DLLs should call CPluginCtx to manage memory
class CPluginCtx : implements IPluginContext
{
public:
    void * ctxMalloc(size_t size)               { return rtlMalloc(size); }
    void * ctxRealloc(void * _ptr, size_t size) { return rtlRealloc(_ptr, size); }
    void   ctxFree(void * _ptr)                 { rtlFree(_ptr); }
    char * ctxStrdup(char * _ptr)               { return strdup(_ptr); }
} PluginCtx;

inline bool isNullOrBlank(const char * s) { return !s || !*s; }

SourceFileEclRepository::SourceFileEclRepository(IErrorReceiver *errs, const char * _pluginPath, const char * _constSourceSearchPath, const char * _dynamicSourceSearchPath, unsigned _traceMask)
: traceMask(_traceMask), dynamicSourceSearchPath(_dynamicSourceSearchPath)
{
    //MTIME_SECTION(timer,"SourceFileEclRepository::SourceFileEclRepository")

    processSourceFiles(_pluginPath, errs, SOURCEFILE_PLUGIN|SOURCEFILE_CONSTANT);
    processSourceFiles(_constSourceSearchPath, errs, SOURCEFILE_CONSTANT);
    processSourceFiles(dynamicSourceSearchPath, errs, 0);
}

void SourceFileEclRepository::processSourceFiles(const char * sourceSearchPath, IErrorReceiver *errs, unsigned moduleFlags)
{
    if (!sourceSearchPath)
        return;

    const char * cursor = sourceSearchPath;
    for (;*cursor;)
    {
        StringBuffer searchPattern;
        while (*cursor && *cursor != ENVSEPCHAR)
            searchPattern.append(*cursor++);
        if(*cursor)
            cursor++;

        if(!searchPattern.length())
            continue;

        StringBuffer dirPath, dirWildcard;
        if (!containsFileWildcard(searchPattern))
        {
            Owned<IFile> file = createIFile(searchPattern.str());
            if (file->isDirectory() == foundYes)
            {
                dirPath.append(searchPattern);
                if (dirPath.charAt(dirPath.length() -1) != PATHSEPCHAR)
                    dirPath.append(PATHSEPCHAR);
                dirWildcard.append("*");
            }
            else if (file->isFile() == foundYes)
            {
                splitFilename(searchPattern.str(), &dirPath, &dirPath, &dirWildcard, &dirWildcard);
            }
            else
            {
                if (errs)
                {
                    StringBuffer msg;
                    msg.appendf("Explicit source file %s not found", searchPattern.str());
                    errs->reportWarning(10, msg.str());
                }
                else
                    printf("Explicit source file %s not found\n", searchPattern.str());
            }
        }
        else
            splitFilename(searchPattern.str(), &dirPath, &dirPath, &dirWildcard, &dirWildcard);

        Owned<IDirectoryIterator> dir = createDirectoryIterator(dirPath.str(), dirWildcard.str());
        StringBuffer sourcePath;
        loadDirectoryTree(rootScope, errs, dir, dirPath.str(), sourcePath, moduleFlags);
    }
}

void SourceFileEclRepository::loadDirectoryTree(IHqlRemoteScope * parentScope, IErrorReceiver *errs, IDirectoryIterator * dir, const char * dirPath, StringBuffer & sourcePath, unsigned moduleFlags)
{
    unsigned prevSourcePathLen = sourcePath.length();
    ForEach (*dir)
    {
        IFile &file = dir->query();
        Owned<IHqlRemoteScope> scope;
        StringBuffer tail;
        dir->getName(tail);
        if (tail.length() && tail.charAt(0)=='.')
            continue;
        sourcePath.append(tail);
        unsigned flags = moduleFlags;
        _ATOM attrName = NULL;
        if (file.isFile() == foundYes)
        {
            StringBuffer module, version, fullpath;
            const char *fname = tail.str();
            const char *ext = strrchr(fname, '.');

            if (ext)
            {
                Owned<IFileContents> fileContents;
                Owned<ISourcePath> path = createSourcePath(sourcePath);
                bool isECL = false;
                fullpath.clear().append(dirPath).append(tail);
                if (stricmp(ext, ".eclmod")==0)
                {
                    fileContents.setown(createFileContents(&file, path));
                    isECL = true;
                    module.clear().append(fname, 0, ext-fname);
                }
                else if (stricmp(ext, ".ecllib")==0)
                {
                    //Legacy.Same as .eclmod but always an implicit module.  Almost certainly a bad idea.
                    fileContents.setown(createFileContents(&file, path));
                    isECL = true;
                    flags |= PLUGIN_IMPLICIT_MODULE;
                    module.clear().append(fname, 0, ext-fname);
                }
                else if (stricmp(ext, ".ecl")==0)
                {
                    assertex(parentScope);
                    //Rather than defining a module this defines a single exported symbol
                    Owned<IFileContents> contents = createFileContents(&file, path);
                    StringAttr name;
                    name.set(fname, ext-fname);
                    IHqlExpression * cur = CHqlNamedSymbol::makeSymbol(createIdentifierAtom(name), parentScope->queryScope()->queryName(), 0, true, false, ob_declaration, contents, 0, 0, 0);
                    static_cast<CHqlScope*>(parentScope->queryScope())->defineSymbol(cur);
                }
                else if (stricmp(ext, SharedObjectExtension)==0)
                {
                    if (moduleFlags & SOURCEFILE_PLUGIN)
                    {
                        try 
                        {
                            HINSTANCE h=LoadSharedObject(fullpath.str(), false, false);     // don't clash getECLPluginDefinition symbol
                            if (h) 
                            {
                                EclPluginSetCtx pSetCtx = (EclPluginSetCtx) GetSharedProcedure(h,"setPluginContext");
                                if (pSetCtx)
                                    pSetCtx(&PluginCtx);

                                EclPluginDefinition p= (EclPluginDefinition) GetSharedProcedure(h,"getECLPluginDefinition");
                                if (p) 
                                {
                                    ECLPluginDefinitionBlock pb;
                                    pb.size = sizeof(pb);
                                    if (p(&pb) && (pb.magicVersion == PLUGIN_VERSION))
                                    {
                                        isECL = true;
                                        flags |= pb.flags | PLUGIN_DLL_MODULE;
                                        module.clear().append(pb.moduleName);
                                        version.clear().append(pb.version);
                                        //Don't use the dll name, or NULL - since no idea where problems are occuring.
                                        Owned<ISourcePath> pluginPath = createSourcePath(module);
                                        fileContents.setown(createFileContentsFromText(pb.ECL, pluginPath));
                                    }
                                    else
                                    {
                                        DBGLOG("Plugin %s exports getECLPluginDefinition but fails consistency check - not loading", fullpath.str());
                                    }
                                }
                                else
                                {
                                    WARNLOG("getECLPluginDefinition not found in %s, unloading", fullpath.str());
                                    FreeSharedObject(h);
                                }
                            
                                //Don't unload the plugin dll!  
                                //Otherwise if the plugin is used in a constant folding context it will keep being loaded and unloaded.
                                //FreeSharedObject(h);
                            }
                        } 
                        catch (...) 
                        {
                        }
                    }
                }
                if (isECL)
                {
                    StringBuffer fullName;
                    fullName.append(parentScope->queryScope()->queryFullName());
                    if (fullName.length())
                        fullName.append(".");
                    fullName.append(module);

                    assertex(fileContents);
                    attrName = createIdentifierAtom(module.toCharArray());
                    scope.setown(createRemoteScope(attrName, fullName.str(), this, NULL, fileContents, true));
                    scope->setProp(flagsAtom, flags);
                    if (version.length())
                        scope->setProp(versionAtom, version.str());
                    if (traceMask & flags)
                        PrintLog("Loading plugin %s[%s] version = %s", fullName.str(), fname, version.str());

                    if (flags & PLUGIN_DLL_MODULE)
                        scope->setProp(pluginAtom, fullpath.str());
                }
            }
        }
        else if (file.isDirectory() == foundYes)
        {
            StringBuffer fullName;
            fullName.append(parentScope->queryScope()->queryFullName());
            if (fullName.length())
                fullName.append(".");
            fullName.append(tail);

            attrName = createIdentifierAtom(tail.toCharArray());
            scope.setown(createRemoteScope(attrName, fullName, this, NULL, NULL, false));
            if (moduleFlags & SOURCEFILE_CONSTANT)
                scope->setProp(flagsAtom, SOURCEFILE_CONSTANT);

            Owned<IDirectoryIterator> childIter = file.directoryFiles(NULL, false, true);

            StringBuffer dirPath;
            dirPath.append(file.queryFilename()).append(PATHSEPCHAR);
            sourcePath.append(PATHSEPCHAR);
            loadDirectoryTree(scope, errs, childIter, dirPath, sourcePath, moduleFlags);
        }

        if (scope)
        {
            Owned <IHqlRemoteScope> found = parentScope->lookupRemoteModule(scope->queryScope()->queryName());
            if (found)
            {
                StringBuffer wrn;
                wrn.appendf("Duplicate module %s found at %s", tail.str(), dirPath);
                errs->reportWarning(WRN_MODULE_DUPLICATED, wrn.str());
            }
            else
            {
                parentScope->addNestedScope(scope->queryScope(), 0);
            }
        }
        sourcePath.setLength(prevSourcePathLen);
    }
}

bool SourceFileEclRepository::loadModule(IHqlRemoteScope *rScope, IErrorReceiver *errs, bool forceAll)
{
    throwUnexpected();
}

void SourceFileEclRepository::checkCacheValid()
{
    ConcreteEclRepository::checkCacheValid();

#if 0
    //v1 - hash remove all modules in the dynamic list, and then recreate
    AtomArray rootToRemove;
    HashIterator nextmodule(rootModules);
    for(nextmodule.first();nextmodule.isValid();nextmodule.next())
    {
        IHqlRemoteScope *rModule = rootModules.mapToValue(&nextmodule.query());
        IHqlScope * module = rModule->queryScope();
        if (isDynamicModule(rModule))
            rootToRemove.append(*module->queryName());
    }
    
    ForEachItemIn(iRoot, rootToRemove)
        rootModules.remove(&rootToRemove.item(iRoot));

    ForEachItemInRev(i, allModules)
    {
        IHqlRemoteScope & cur = allModules.item(i);
        if (isDynamicModule(&cur))
        {
            cur.invalidateParsed();
            allModules.remove(i);
        }
    }

    IErrorReceiver * errs = NULL;
    processSourceFiles(dynamicSourceSearchPath, errs, 0);
#endif
}

extern HQL_API IEclRepository *createSourceFileEclRepository(IErrorReceiver *errs, const char * pluginsPath, const char * constSourceSearchPath, const char * dynamicSourceSearchPath, unsigned trace)
{
    return new SourceFileEclRepository(errs, pluginsPath, constSourceSearchPath, dynamicSourceSearchPath, trace);
}

extern HQL_API IErrorReceiver *createFileErrorReceiver(FILE *f)
{
    return new FileErrorReceiver(f);
}

extern void getImplicitScopes(HqlScopeArray& implicitScopes, IEclRepository * repository, IHqlScope * scope, HqlLookupContext & ctx) 
{
    //Anything in the constant plugin list requires explicit module imports
    //Slighlty ugly for backward compatibility with old archives.
    if (!allowImplicitImport(scope) || isImplicitScope(scope))
        return;

    HqlScopeArray rootScopes;
    repository->getRootScopes(rootScopes, ctx);
    ForEachItemIn(i, rootScopes)
    {
        IHqlScope & scope = rootScopes.item(i);
        if (isImplicitScope(&scope))
            implicitScopes.append(OLINK(scope));
    }
}

