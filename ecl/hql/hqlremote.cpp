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
#include "jlib.hpp"
#include "jthread.hpp"
#include "jsocket.hpp"
#include "hql.hpp"
#include "hqlerrors.hpp"
#include "hqlexpr.hpp"
#include "hqlexpr.ipp"
#include "jmisc.hpp"
#include "hqlremote.hpp"
#include "jdebug.hpp"
#include "jptree.hpp"
#include "jencrypt.hpp"

#include "hqlerror.hpp"
#include "hqlplugins.hpp"
#include "hqlrepository.hpp"

#define INVALIDATE_ALL_PARSED

#undef new
#include <set>
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

//==============================================================================================================

class XmlEclRepository : public ConcreteEclRepository
{
protected:
    Owned<IProperties> properties;  

public:
//interface readDataServer

    virtual bool logging() { return false; }

protected:
    void setProp(const char* name,const char* value);
    void setPropInt(const char* name,int value);
    void getProp(const char* name,StringBuffer& ret);
    int  getPropInt(const char* name,int def=0);

    bool doLoadModule(IPropertyTree* repository, IHqlRemoteScope *, IErrorReceiver *);
    IHqlExpression * doLoadSymbol(IPropertyTree* repository, IAtom *, IAtom *);
    IHqlRemoteScope * createModule(IPropertyTree* t);
    IHqlRemoteScope *resolveScope(IProperties *props, const char * modname, bool deleteIfExists, bool createIfMissing);
    IHqlExpression *toNamedSymbol(IPropertyTree* t, const char* module,int access);
};


bool XmlEclRepository::doLoadModule(IPropertyTree * repository, IHqlRemoteScope * rScope, IErrorReceiver *errs)
{
    IHqlScope * scope = rScope->queryScope();
    const char * scopeName = scope->queryName()->getAtomNamePtr();

    StringBuffer s;
    const char * modName = scope->queryFullName();
    IPropertyTree* module = repository->queryPropTree(s.append("./Module[@name=\"").append(modName).append("\"]").str());
    if(!module)
    {
        if (logging())
            DBGLOG("No data for module %s",scopeName);
        return false;
    }
    int access = module->getPropInt("@access",cs_full);

    if(module->queryProp("Text"))
    {
        const char * path = module->queryProp("@sourcePath");
        Owned<ISourcePath> sourcePath = createSourcePath(path ? path : modName);
        Owned<IFileContents> text = createFileContentsFromText(module->queryProp("Text"), sourcePath);
        rScope->setText(text);
    }
    else
    {
        StringBuffer buf("./Attribute");
        Owned<IPropertyTreeIterator> it = module->getElements(buf.str());
        if (it->first())
        {
            for(;it->isValid();it->next())
            {
                Owned<IHqlExpression> item = toNamedSymbol(&it->query(), *scope->queryName(), access);
                ((CHqlScope*)scope)->defineSymbol(LINK(item));
            }
        }
        else
        {
            if (logging())
                DBGLOG("No definitions were added for module %s", scopeName);
        }
    }
    return true;
}

IHqlExpression * XmlEclRepository::doLoadSymbol(IPropertyTree * repository, IAtom * modname, IAtom * attrname)
{
    StringBuffer s;
    IPropertyTree* module = repository->queryPropTree(s.append("./Module[@name=\"").append(*modname).append("\"]").str());
    if(!module)
    {
        if (logging())
            DBGLOG("No data for module %s",modname->getAtomNamePtr());
        return 0;
    }
    int access = module->getPropInt("@access",cs_full);

    s.clear().append("./Attribute[@name=\"").append(*attrname).append("\"]");
    Owned<IPropertyTreeIterator> it = module->getElements(s.str());
    for(it->first();it->isValid();it->next())
    {
        Owned<IHqlExpression> item = toNamedSymbol(&it->query(), *modname,access);
        CHqlNamedSymbol* cur = QUERYINTERFACE(item.get(), CHqlNamedSymbol);

        if(cur)
            return LINK(cur);
    }

    return 0;
}



void XmlEclRepository::setProp(const char* name,const char* value)
{
    if(!properties)
        properties.setown(createProperties(true));
    properties->setProp(name,value);
}

void XmlEclRepository::getProp(const char* name,StringBuffer& ret)
{
    if(properties) 
        properties->getProp(name,ret);
}


void XmlEclRepository::setPropInt(const char* name,int value)
{
    StringBuffer temp;
    temp.append(value);
    setProp(name,temp.str());
}

int XmlEclRepository::getPropInt(const char* name,int def)
{
    if(properties && properties->hasProp(name))
    {
        StringBuffer temp;
        getProp(name,temp);
        return atoi(temp.str());
    }
    return def;
}

IHqlRemoteScope * XmlEclRepository::createModule(IPropertyTree* t)
{
    const char* modname = t->queryProp("@name");
    const char* access = t->queryProp("@access");
    int iflags = t->getPropInt("@flags", 0);

    //backward compatibility of old archive files
    if (stricmp(modname, "default")==0)
        iflags &= ~(PLUGIN_IMPLICIT_MODULE);

    Owned<IProperties> props = createProperties(true);
    props->setProp(*accessAtom, access); 
    bool needToDelete = (iflags & ZOMBIE_MODULE) != 0;
    Owned<IHqlRemoteScope> scope = resolveScope(props, modname, needToDelete, !needToDelete);
    if (scope)
    {
        if (logging())
            DBGLOG("Create Scope %s", modname);

        scope->setProp(flagsAtom, iflags);
        if (iflags & PLUGIN_DLL_MODULE)
        {
            const char* plugin = t->queryProp("@fullname");
            const char* version = t->queryProp("@version");
            scope->setProp(pluginAtom, plugin);
            scope->setProp(versionAtom, version);
        }

        if (t->hasProp("Text"))
        {
            const char * path = t->queryProp("@sourcePath");
            Owned<ISourcePath> sourcePath = createSourcePath(path ? path : modname);
            Owned<IFileContents> text = createFileContentsFromText(t->queryProp("Text"), sourcePath);
            scope->setText(text);
        }
    }
    return scope.getLink();
}


IHqlRemoteScope * XmlEclRepository::resolveScope(IProperties *props, const char * modname, bool deleteIfExists, bool createIfMissing)
{
    Owned<IHqlRemoteScope> parentScope = LINK(rootScope);
    
    const char * item = modname;
    const char * dot;
    do
    {
        dot = strchr(item, '.');
        _ATOM moduleName;
        StringAttr fullName;
        if (dot)
        {
            moduleName = createIdentifierAtom(item, dot-item);
            fullName.set(modname, dot - modname);
            item = dot + 1;
        }
        else
        {
            moduleName = createIdentifierAtom(item);
            fullName.set(modname);
        }

        //nested module already exist in parent scope?
        Owned<IHqlRemoteScope> rScope = parentScope->lookupRemoteModule(moduleName);

        if (!rScope && !createIfMissing)
            return NULL;

        if (rScope && deleteIfExists && !dot)
        {
            rScope->noteTextModified();
            if (rScope->isEmpty())
                parentScope->removeNestedScope(moduleName);
            return NULL;
        }

        if (!rScope)
        {
            rScope.setown(createRemoteScope(moduleName, fullName, this, dot ? NULL : props, NULL, true));

            int flags = props->getPropInt("@flags", 0);
            parentScope->addNestedScope(rScope->queryScope(), flags);
        }
        else
            rScope->invalidateParsed();

        parentScope.set(rScope);
    } while (dot);
    if (parentScope)
        parentScope->noteTextModified();
    return parentScope.getLink();
}

IHqlExpression* XmlEclRepository::toNamedSymbol(IPropertyTree* t, const char* module, int access)
{
    const char* name = t->queryProp("@name");
    if(*name==':')
        return 0;
    int flags = t->getPropInt("@flags", 0);
    if(access >= cs_read)
        flags|=ob_showtext;
    const char* text = t->queryProp("text");
    if (!text)
        text = t->queryProp("");

    Owned<IFileContents> contents;
    if (text && *text) 
    {
        const char * path = t->queryProp("@sourcePath");
        Owned<ISourcePath> sourcePath;
        if (path)
        {
            sourcePath.setown(createSourcePath(path));
        }
        else
        {
            StringBuffer defaultName;
            defaultName.append(module).append('.').append(name);
            sourcePath.setown(createSourcePath(defaultName));
        }
        contents.setown(createFileContentsFromText(text, sourcePath));
    }

    return CHqlNamedSymbol::makeSymbol(createIdentifierAtom(name), createIdentifierAtom(module), NULL, NULL, 
                    (flags&ob_exported)!=0, (flags&ob_shared)!=0, flags|ob_declaration, contents, 0);
}


//==============================================================================================================

class RemoteXmlEclRepository : public XmlEclRepository
{
    IXmlEclRepository & repository;
    Linked<IEclUser> user;
    StringBuffer lastError;
    timestamp_t cachestamp;

public:
    RemoteXmlEclRepository(IEclUser * _user, IXmlEclRepository &repository, const char* cluster, const char * _snapshot, bool _sandbox4snapshot);
    ~RemoteXmlEclRepository();

    void shutdown();
    void getActiveUsers(char *&ret);
    void logException(IException *e);

//interface readDataServer
    virtual bool loadModule(IHqlRemoteScope *, IErrorReceiver *, bool forceAll);
    virtual IHqlExpression * loadSymbol(IAtom *, IAtom *);
    virtual void checkCacheValid();

    virtual IPropertyTree* getModules(timestamp_t from);
    virtual IPropertyTree* getAttributes(const char *module, const char *attr, int version, unsigned char infoLevel); 

    virtual bool logging() { return true; }

protected:
    void setCurrentCluster(const char* cluster);
};



//==============================================================================================================

RemoteXmlEclRepository::RemoteXmlEclRepository(IEclUser * _user, IXmlEclRepository &_repository, const char* _cluster, const char * _snapshot, bool _sandbox4snapshot) 
: repository(_repository), user(_user)
{
    if (_snapshot && *_snapshot)
    {
        setProp("snapshot", _snapshot);
        setPropInt("sandbox4snapshot", _sandbox4snapshot ? 1 : 0);
    }
    cachestamp = 0;

    setCurrentCluster(_cluster);
}

RemoteXmlEclRepository::~RemoteXmlEclRepository()
{
    DBGLOG("~RemoteXmlEclRepository");
}

void RemoteXmlEclRepository::logException(IException *e)
{
    if (e)
    {
        e->errorMessage(lastError.clear());
    }
    else
    {
        lastError.clear().append("Unknown exception");
    }
    DBGLOG("Log Exception: %s", lastError.str());
}

void RemoteXmlEclRepository::checkCacheValid()
{
    ConcreteEclRepository::checkCacheValid();

    DBGLOG("check cache");
    Owned<IPropertyTree> repository = getModules(cachestamp);
    if(!repository)
    {
        DBGLOG("getModules returned null");
        //process error
        return;
    }

    Owned<IPropertyTreeIterator> it = repository->getElements("./Module");
    bool somethingChanged = false;
    for (it->first(); it->isValid(); it->next())
    {
        IPropertyTree & cur = it->query();
        Owned<IHqlRemoteScope> rScope = createModule(&cur);
        unsigned flags = cur.getPropInt("@flags", 0);
        timestamp_t timestamp = (timestamp_t)cur.getPropInt64("@timestamp"); 
        if (timestamp > cachestamp)
            cachestamp = timestamp;
        somethingChanged = true;
    }

    if(somethingChanged)
        rootScope->invalidateParsed();
}


bool RemoteXmlEclRepository::loadModule(IHqlRemoteScope * rScope, IErrorReceiver *errs, bool forceAll)
{
    IHqlScope * scope = rScope->queryScope();
    const char * modName = scope->queryFullName();
    DBGLOG("load module %s",modName);

    Owned<IPropertyTree> repository = getAttributes(modName, NULL, 0, getPropInt("preloadText",1));
    if(!repository)
    {
        DBGLOG("getAttributes %s.* returned null", modName);
        //process error
        return false;
    }
    return doLoadModule(repository, rScope, errs);
}

IHqlExpression * RemoteXmlEclRepository::loadSymbol(IAtom * modname, IAtom * attrname)
{
    Owned<IPropertyTree> repository = getAttributes(*modname, *attrname, 0, 2);
    if(!repository)
    {
        DBGLOG("getAttributes %s.%s returned null",modname->getAtomNamePtr(), attrname->getAtomNamePtr());
        //process error
        return 0;
    }
    return doLoadSymbol(repository, modname, attrname);
}

IPropertyTree* RemoteXmlEclRepository::getModules(timestamp_t from)
{ 
    StringBuffer modNames;
    IPropertyTree* repositoryTree = 0;
    try
    {
        repository.getModules(modNames, user, from);
        repositoryTree = createPTreeFromXMLString(modNames.str());
    }
    catch(IException *e) 
    {
        logException(e);
        e->Release();
    }
    catch (...)
    { 
        logException(NULL);
    }

    return repositoryTree;
}

IPropertyTree* RemoteXmlEclRepository::getAttributes(const char *module, const char *attr, int version, unsigned char infoLevel) 
{
    IPropertyTree* repositoryTree = 0;
    StringBuffer xml;
    try
    {
        StringBuffer snapshot;
        getProp("snapshot",snapshot);
        bool sandbox4snapshot = getPropInt("sandbox4snapshot", 0) != 0;

        repository.getAttributes(xml, user, module, attr, version, infoLevel, snapshot.length() ? snapshot.str() : NULL, sandbox4snapshot);
        if (xml.length())
            repositoryTree = createPTreeFromXMLString(xml, ipt_caseInsensitive);
    }
    catch(IException *e) 
    {
        logException(e);
        if (xml.length())
            DBGLOG("Xml: %s", xml.str());
        e->Release();
    }
    catch (...)
    { 
        logException(NULL);
    }

    return repositoryTree;
}


void RemoteXmlEclRepository::setCurrentCluster(const char* _cluster)
{
    setProp("cluster",_cluster);
}

extern "C" HQL_API IEclRepository * attachLocalServer(IEclUser * user, IXmlEclRepository & repository, const char* cluster, const char * snapshot, bool sandbox4snapshot)
{
    return new RemoteXmlEclRepository(user, repository, cluster, snapshot, sandbox4snapshot);
}

class LoggingDataServer: public RemoteXmlEclRepository
{
public:
    LoggingDataServer(IEclUser * user, IXmlEclRepository & repository, const char* cluster, IWorkUnit* _workunit, const char * snapshot, bool _sandbox4snapshot):
      RemoteXmlEclRepository(user, repository, cluster, snapshot, _sandbox4snapshot),
      workunit(_workunit)
    {
          setPropInt("preloadText",0);
    }

    virtual bool loadModule(IHqlRemoteScope * scope, IErrorReceiver *errs, bool forceAll)
    {
        bool res=RemoteXmlEclRepository::loadModule(scope, errs, forceAll);
        //attributes.insert(Attr(scope->queryName()));
        return res;
    }

    virtual IHqlExpression * loadSymbol(IAtom * modname, IAtom * attrname)
    {
        IHqlExpression * symbol=RemoteXmlEclRepository::loadSymbol(modname, attrname);
        if(symbol)
        {
            if(attributes.insert(Attr(modname,attrname,0)).second)
            {
                StringBuffer name, buf;
                name.append("Dependency").append((size32_t) attributes.size());

                buf.append("<Attribute module=\"").append(*modname).append("\"");
                if(attrname)
                    buf.append(" name=\"").append(*attrname).append("\"");
                buf.append(" flags=\"").append(symbol->getObType()).append("\"/>");

                workunit->setApplicationValue("SyntaxCheck",name.str(),buf.str(),true); 
            }

        }
        return symbol;
    }

    struct Attr
    {
        Attr(IAtom* _module,IAtom* _attribute=NULL,int _version=0): module(_module), attribute(_attribute), version(_version)
        {}

        bool operator < (const Attr&a) const
        {
            int cmp=stricmp(*module,*a.module);
            if (cmp<0)
                return true;
            if (cmp>0)
                return false;

            if(attribute)
            {
                if(a.attribute)
                {
                    int cmp=stricmp(*attribute,*a.attribute);
                    if (cmp<0)
                        return true;
                    if (cmp>0)
                        return false;
                }
                else
                    return false;
            }
            else
                if(a.attribute)
                    return true;

            if(version<a.version)
                return true;
            return false;
        }

        IAtom* module;
        IAtom* attribute;
        int version;
    };

    std::set<Attr> attributes;
    Linked<IWorkUnit> workunit;
};

extern "C" HQL_API IEclRepository * attachLoggingServer(IEclUser * user, IXmlEclRepository & repository, IWorkUnit* workunit, const char * snapshot, bool sandbox4snapshot)
{
    SCMStringBuffer cluster;
    workunit->getClusterName(cluster);
    return new LoggingDataServer(user, repository, cluster.str(), workunit, snapshot, sandbox4snapshot);
}


//=======================================================================================================

class ArchiveXmlEclRepository : public XmlEclRepository
{
public:
    ArchiveXmlEclRepository(IPropertyTree * _xml, IEclRepository * _defaultDataServer);
    ~ArchiveXmlEclRepository();

protected:
    virtual IHqlExpression * lookupRootSymbol(IAtom * name, unsigned lookupFlags, HqlLookupContext & ctx);
    virtual bool loadModule(IHqlRemoteScope *scope, IErrorReceiver *errs, bool forceAll);
    virtual IHqlExpression * loadSymbol(IAtom *, IAtom *);
    virtual void checkCacheValid() {};


protected:
    void init();

protected:
    Owned<IPropertyTree> repository;
    Linked<IEclRepository> defaultDataServer;
    IArrayOf<IHqlScope> inheritedModules;
};

ArchiveXmlEclRepository::ArchiveXmlEclRepository(IPropertyTree * _xml, IEclRepository * _defaultDataServer) : defaultDataServer(_defaultDataServer)
{
    //Recreate the root scope, indicating it needs to be delay loaded - to allow attributes in the root of the repository
    rootScope.setown(createRemoteScope(NULL, NULL, this, NULL, NULL, true));
    repository.set(_xml);
    init();
}

ArchiveXmlEclRepository::~ArchiveXmlEclRepository()
{
    //Remove all the inherited modules so they don't get invalidated when the module table is destroyed.
    //I'm not convinced they should be invalidated there, but I'll leave it as it is for the moment.
    ForEachItemIn(i, inheritedModules)
    {
        IHqlScope & scope = inheritedModules.item(i);
        rootScope->removeNestedScope(scope.queryName());
    }
}

IHqlExpression * ArchiveXmlEclRepository::lookupRootSymbol(IAtom * name, unsigned lookupFlags, HqlLookupContext & ctx)
{
    IHqlExpression * symbol = XmlEclRepository::lookupRootSymbol(name, lookupFlags, ctx);
    if (symbol || !(lookupFlags & LSFimport))
        return symbol;

    //scope not found - probably caused by an import that was never used...
    Owned<IHqlRemoteScope> newScope = createRemoteScope(name, name->str(), this, NULL, NULL, true);
    rootScope->addNestedScope(newScope->queryScope(), 0);
    return LINK(queryExpression(newScope->queryScope()));
}



IHqlExpression * ArchiveXmlEclRepository::loadSymbol(IAtom * modname, IAtom * attrname)
{
    return doLoadSymbol(repository, modname, attrname);
}

bool ArchiveXmlEclRepository::loadModule(IHqlRemoteScope *rScope, IErrorReceiver *errs, bool forceAll)
{
    return doLoadModule(repository, rScope, errs);
}


void ArchiveXmlEclRepository::init()
{
    HqlLookupContext GHMOREctx(NULL, NULL,  NULL, defaultDataServer);
    Owned<IPropertyTreeIterator> modit = repository->getElements("Module");
    ForEach(*modit)
    {
        IPropertyTree & cur = modit->query();
        const char* modname = cur.queryProp("@name");

        //If inheriting plugins, take all plugins from the defaultServer - otherwise you can get dependencies on previous queries (from any modules the plugins import).
        if (defaultDataServer)
        {
            _ATOM name = createIdentifierAtom(modname);
            OwnedHqlExpr match = defaultDataServer->lookupRootSymbol(name, LSFpublic, GHMOREctx);
            IHqlScope * defaultScope = match ? match->queryScope() : NULL;
            if (defaultScope && (defaultScope->getPropInt(flagsAtom, 0) & SOURCEFILE_PLUGIN))
            {
                rootScope->addNestedScope(defaultScope, 0);
                inheritedModules.append(*LINK(defaultScope));
                continue;
            }
        }

        Owned<IHqlRemoteScope> rScope = createModule(&cur);
    }
}

extern "C" HQL_API IEclRepository * createXmlDataServer(IPropertyTree * _xml, IEclRepository * defaultDataServer)
{
    return new ArchiveXmlEclRepository(_xml, defaultDataServer);
}

//==============================================================================================================

class ReplayEclRepository : public CInterface, implements IXmlEclRepository
{
public:
    ReplayEclRepository(IPropertyTree * _xmlTree) : xmlTree(_xmlTree) 
    {
        lasttimestamp = 0;
        seq = 0;
    }
    IMPLEMENT_IINTERFACE;

    virtual int getModules(StringBuffer & xml, IEclUser * user, timestamp_t timestamp)
    {
        lasttimestamp = timestamp;
        StringBuffer xpath;
        xpath.append("Timestamp[@seq=\"").append(++seq).append("\"]/GetModules/Repository");

        IPropertyTree * request = xmlTree->queryPropTree(xpath);
        if (!request)
            return 0;
        
        toXML(request, xml);
        return 1;
    }

    virtual int getAttributes(StringBuffer & xml, IEclUser * user, const char * modname, const char * attribute, int version, unsigned char infoLevel, const char * snapshot, bool sandbox4snapshot)
    {
        StringBuffer xpath;
        xpath.append("Timestamp[@seq=\"").append(seq).append("\"]/GetAttributes[");
        if (modname)
        {
            xpath.append("@module=\"").append(modname).append("\"");
            if (attribute)
                xpath.append(",@attr=\"").append(attribute).append("\"");
        }
        xpath.append("]/Repository");

        IPropertyTree * request = xmlTree->queryPropTree(xpath);
        if (!request)
            return 0;
        
        toXML(request, xml);
        return 1;
    }

    virtual void noteQuery(const char * query) 
    {
    }

protected:
    Linked<IPropertyTree> xmlTree;
    timestamp_t lasttimestamp;
    unsigned seq;
};


extern HQL_API IXmlEclRepository * createReplayRepository(IPropertyTree * xml)
{
    return new ReplayEclRepository(xml);
}

