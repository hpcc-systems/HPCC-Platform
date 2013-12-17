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

#include "jliball.hpp"
#include "dllserver.hpp"
#include "jiter.ipp"
#include "jexcept.hpp"

#include "dllservererr.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "rmtfile.hpp"
#include "dalienv.hpp"
#include "thorplugin.hpp"

#ifdef _WIN32
#define DEFAULT_SERVER_ROOTDIR          "c:\\HPCCSystems\\hpcc-data\\temp\\dllserver"
#else
#define DEFAULT_SERVER_ROOTDIR          "/var/lib/HPCCSystems/dllserver/temp"
#endif

static Owned<IConstDomainInfo> hostDomain;

IConstDomainInfo * getDomainFromIp(const char * ip)
{
    Owned<IEnvironmentFactory> ef = getEnvironmentFactory();
    Owned<IConstEnvironment> env = ef->openEnvironment();
    if (!env)
        return NULL;
    Owned<IConstMachineInfo> curMachine = env->getMachineByAddress(ip);
    if (!curMachine)
        return NULL;
    return curMachine->getDomain();
}


IConstDomainInfo * cachedHostDomain()
{
    if (!hostDomain)
    {
        StringBuffer ipText;
        queryHostIP().getIpText(ipText);
        hostDomain.setown(getDomainFromIp(ipText.str()));
    }
    return hostDomain;
}

void getPath(StringBuffer & path, const char * name)
{
    path.append("/GeneratedDlls/GeneratedDll[@uid=\"").append(name).append("\"]");
}

//---------------------------------------------------------------------------

class TreeIteratorWrapper : public CInterface, implements IIterator
{
public:
    TreeIteratorWrapper(IPropertyTreeIterator * _iter) { iter.setown(_iter); }
    IMPLEMENT_IINTERFACE

    virtual bool first()
    {
        cur.clear();
        return iter->first();
    }
    virtual bool next()
    {
        cur.clear();
        return iter->next();
    }
    virtual bool isValid()
    {
        return (cur != NULL);
    }
    virtual IInterface & get()
    {
        return *cur.getLink();
    }
    virtual IInterface & query()
    {
        return *cur;
    }

protected:
    Owned<IInterface> cur;
    Owned<IPropertyTreeIterator> iter;
};


//---------------------------------------------------------------------------


static void deleteLocationFiles(IDllLocation & cur, bool removeDirectory)
{
    RemoteFilename filename;
    cur.getDllFilename(filename);
    Owned<IFile> file = createIFile(filename);
    file->remove();

    if (cur.getLibFilename(filename))
    {
        file.setown(createIFile(filename));
        file->remove();
    }

    if (removeDirectory)
    {
        //Finally try and remove the directory - this will fail if it isn't empty
        StringBuffer path, dir;

        cur.getDllFilename(filename);
        filename.getRemotePath(path);
        splitDirTail(path.str(), dir);
        filename.setRemotePath(dir.str());
        file.setown(createIFile(filename));
        file->remove();
    }
}

class DllLocation : public CInterface, implements IDllLocation
{
    StringAttr cacheRoot;
public:
    DllLocation(IPropertyTree * _entryRoot, IPropertyTree * _locationRoot, const char *_cacheRoot) 
        : cacheRoot(_cacheRoot)
    { 
        entryRoot.set(_entryRoot); 
        locationRoot.set(_locationRoot); 
    }
    IMPLEMENT_IINTERFACE

    virtual void getDllFilename(RemoteFilename & filename)
    {
        SocketEndpoint ep(locationRoot->queryProp("@ip"));
        filename.setPath(ep, locationRoot->queryProp("@dll"));
    }
    virtual bool getLibFilename(RemoteFilename & filename)
    {
        const char * lib = locationRoot->queryProp("@lib");
        if (!lib) return false;

#ifndef _WIN32
        int namelen = strlen(lib);
        StringBuffer libnamebuf(lib);
        if(!((namelen >= 3) && (strcmp(lib+namelen-3, ".so") == 0)))
        {
            libnamebuf.append(".so");
            lib = libnamebuf.str();
        }
#endif
        SocketEndpoint ep(locationRoot->queryProp("@ip"));
        filename.setPath(ep, lib);
        return true;
    }
    virtual void getIP(IpAddress & ip)
    {
        ip.ipset(locationRoot->queryProp("@ip"));
    }
    virtual DllLocationType queryLocation()
    {
        SocketEndpoint ep(locationRoot->queryProp("@ip"));
        if (ep.isLocal())
        {
            RemoteFilename filename;
            getDllFilename(filename);
            StringBuffer lp;
            filename.getLocalPath(lp);
            if (strncmp(lp, cacheRoot, strlen(cacheRoot))==0)
                return DllLocationDirectory;
            return DllLocationLocal;
        }
#ifdef _WIN32
        Owned<IConstDomainInfo> curDomain = getDomainFromIp(locationRoot->queryProp("@ip"));
        IConstDomainInfo * hostDomain = cachedHostDomain();
        if (curDomain && hostDomain && (curDomain == hostDomain))
            return DllLocationDomain;
#endif
        return DllLocationAnywhere;
    }
    virtual void remove(bool removeFiles, bool removeDirectory);

protected:
    Owned<IPropertyTree> locationRoot;
    Owned<IPropertyTree> entryRoot;
};

static bool propsMatch(IPropertyTree & left, IPropertyTree & right, const char * prop)
{
    const char * l = left.queryProp(prop);
    const char * r = right.queryProp(prop);

    if (!l || !r) return (l == r);
    return strcmp(l, r) == 0;
}

void DllLocation::remove(bool removeFiles, bool removeDirectory)
{
    if (removeFiles)
    {
        try
        {
            deleteLocationFiles(*this, removeDirectory);
        }
        catch (IException * e)
        {
            EXCLOG(e, "Removing dll");
            e->Release();
        }
    }

    StringBuffer path;
    getPath(path, entryRoot->queryProp("@name"));
    Owned<IRemoteConnection> conn = querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_WRITE, 5000);
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("location");
    ForEach(*iter)
    {
        IPropertyTree & cur = iter->query();
        if (propsMatch(cur, *locationRoot, "@ip") && propsMatch(cur, *locationRoot, "@dll") && propsMatch(cur, *locationRoot, "@lib"))
        {
            conn->queryRoot()->removeTree(&cur);
            break;
        }
    }
}


class LocationIterator : public TreeIteratorWrapper
{
    StringAttr cacheRoot;
public:
    LocationIterator(IPropertyTree * _dllEntry, IPropertyTreeIterator * _iter, const char *_cacheRoot) 
        : TreeIteratorWrapper(_iter), cacheRoot(_cacheRoot)
    {
        dllEntry.set(_dllEntry); 
    }

    virtual bool first()
    {
        bool ok = TreeIteratorWrapper::first();
        if (ok)
            cur.setown(new DllLocation(dllEntry, &iter->query(), cacheRoot));
        return ok;
    }
    virtual bool next()
    {
        bool ok = TreeIteratorWrapper::next();
        if (ok)
            cur.setown(new DllLocation(dllEntry, &iter->query(), cacheRoot));
        return ok;
    }

protected:
    Owned<IPropertyTree>    dllEntry;
};


//---------------------------------------------------------------------------

class DllEntry : public CInterface, implements IDllEntry
{
public:
    DllEntry(IPropertyTree *root, const char *cacheRoot, IPropertyTree *owner);
    IMPLEMENT_IINTERFACE

    virtual IIterator * createLocationIterator();
    virtual IDllLocation * getBestLocation();
    virtual IDllLocation * getBestLocationCandidate();
    virtual void getCreated(IJlibDateTime & dateTime)
    {
        dateTime.setString(root->queryProp("@created"));
    }
    virtual const char * queryKind()
    {
        return root->queryProp("@kind");
    }
    virtual const char * queryName()
    {
        return root->queryProp("@name");
    }
    virtual void remove(bool removeFiles, bool removeDirectory);

protected:
    Owned<IPropertyTree> root, owner;
    StringAttr cacheRoot;
};

DllEntry::DllEntry(IPropertyTree *_root, const char *_cacheRoot, IPropertyTree *_owner)
: cacheRoot(_cacheRoot)
{
    root.set(_root);
    owner.set(_owner);
}

IIterator * DllEntry::createLocationIterator()
{
    return new LocationIterator(root, root->getElements("location"), cacheRoot);
}

typedef IArrayOf<IDllLocation> LocationArray;

int orderLocations(IInterface * * pLeft, IInterface * * pRight)
{
    IDllLocation * left = (IDllLocation *)*pLeft;
    IDllLocation * right = (IDllLocation *)*pRight;
    return right->queryLocation() - left->queryLocation();
}

IDllLocation * DllEntry::getBestLocation()
{
    LocationArray locations;
    Owned<IIterator> iter = createLocationIterator();
    ForEach(*iter)
        locations.append((IDllLocation &)iter->get());
    locations.sort(orderLocations);

    ForEachItemIn(idx, locations)
    {
        IDllLocation & cur = locations.item(idx);
        try
        {
            RemoteFilename filename;
            cur.getDllFilename(filename);
            Owned<IFile> file = createIFile(filename);
            Owned<IFileIO> io = file->open(IFOread);
            if (io)
                return LINK(&cur);
        }
        catch (IException * e)
        {
            StringBuffer s;
            EXCLOG(e, s.appendf("GetBestLocation - could not load entry for %s", root->queryProp("@name")).str());
            e->Release();
            //MORE: Should possibly remove the entry... but we don't know why we couldn't read it - could be a domain issue.
        }
    }
    throwError1(DSVERR_CouldNotFindDll, root->queryProp("@name"));
    return NULL;
}

IDllLocation * DllEntry::getBestLocationCandidate()
{
    Owned<IIterator> iter = createLocationIterator();
    Owned<IDllLocation> best;
    DllLocationType bestLocation = DllLocationNowhere;

    ForEach(*iter)
    {
        IDllLocation & cur = (IDllLocation &)iter->query();
        DllLocationType location = cur.queryLocation();
        if (location == DllLocationDirectory)
            return LINK(&cur);
        if (location > bestLocation)
        {
            best.set(&cur);
            bestLocation = location;
        }
    }

    if (!best)
        throwError1(DSVERR_CouldNotFindDll, root->queryProp("@name"));

    return best.getClear();
}



void DllEntry::remove(bool removeFiles, bool removeDirectory)
{
    if (removeFiles)
    {
        Owned<IIterator> iter = createLocationIterator();
        // a bit nasty, but don't remove the directory for the first location, since this is where things are created in the first place.
        bool isFirst = true;                
        ForEach(*iter)
        {
            try
            {
                IDllLocation & cur = (IDllLocation &)iter->query();
                deleteLocationFiles(cur, removeDirectory && !isFirst);
            }
            catch (IException * e)
            {
                EXCLOG(e, "Removing dll");
                e->Release();
            }
            isFirst = false;
        }
    }
    if (owner)
        owner->removeTree(root);
    else
    {
        StringBuffer path;
        getPath(path, root->queryProp("@name"));
        Owned<IRemoteConnection> conn = querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_WRITE, 5000);
        conn->close(true);
    }
}



class DllIterator : public TreeIteratorWrapper
{
public:
    DllIterator(IPropertyTree * _root, IPropertyTreeIterator * _iter, const char *_cacheRoot) 
        : TreeIteratorWrapper(_iter), cacheRoot(_cacheRoot)
    { 
        root.set(_root); 
    }

    virtual bool first()
    {
        bool ok = TreeIteratorWrapper::first();
        if (ok)
            cur.setown(new DllEntry(&iter->query(), cacheRoot, NULL));
        return ok;
    }
    virtual bool next()
    {
        bool ok = TreeIteratorWrapper::next();
        if (ok)
            cur.setown(new DllEntry(&iter->query(), cacheRoot, NULL));
        return ok;
    }

protected:
    Owned<IPropertyTree> root;
    StringAttr cacheRoot;
};


//---------------------------------------------------------------------------

class DllServer : public CInterface, implements IDllServer
{
public:
    DllServer(const char * _rootDir);
    IMPLEMENT_IINTERFACE

    virtual IIterator * createDllIterator();
    virtual void ensureAvailable(const char * name, DllLocationType location);
    virtual void getDll(const char * name, MemoryBuffer & dllText);
    virtual IDllEntry * getEntry(const char * name);
    virtual void getLibrary(const char * name, MemoryBuffer & dllText);
    virtual void getLocalLibraryName(const char * name, StringBuffer & libraryName);
    virtual DllLocationType isAvailable(const char * name);
    virtual ILoadedDllEntry * loadDll(const char * name, DllLocationType location);
    virtual void removeDll(const char * name, bool removeDlls, bool removeDirectory);
    virtual void registerDll(const char * name, const char * kind, const char * dllPath);
    virtual IDllEntry * createEntry(IPropertyTree *owner, IPropertyTree *entry);

protected:
    void copyFileLocally(RemoteFilename & targetName, RemoteFilename & sourceName);
    DllEntry * doGetEntry(const char * name);
    void doRegisterDll(const char * name, const char * kind, const char * dllPath, const char * libPath);
    IDllLocation * getBestMatch(const char * name);
    IDllLocation * getBestMatchEx(const char * name);

protected:
    StringAttr rootDir;
};

DllServer::DllServer(const char * _rootDir) 
{
    rootDir.set(_rootDir); 
}

void DllServer::copyFileLocally(RemoteFilename & targetName, RemoteFilename & sourceName)
{
    StringBuffer sourceLocalPath,targetLocalPath;

    sourceName.getLocalPath(sourceLocalPath);

    targetLocalPath.append(rootDir);
    recursiveCreateDirectory(targetLocalPath.str());
    addPathSepChar(targetLocalPath);
    splitFilename(sourceLocalPath.str(),NULL,NULL,&targetLocalPath,&targetLocalPath);

    SocketEndpoint hostEndpoint;
    hostEndpoint.setLocalHost((unsigned short)0);
    targetName.setPath(hostEndpoint, targetLocalPath.str());

    OwnedIFile source = createIFile(sourceName);
    OwnedIFile target = createIFile(targetName);
    copyFile(target, source);
}

IIterator * DllServer::createDllIterator()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/GeneratedDlls", myProcessSession(), 0, 5000);
    IPropertyTree * root = conn->queryRoot();
    return conn ? (IIterator *)new DllIterator(root, root->getElements("GeneratedDll"), rootDir) : (IIterator *)new CNullIterator;
}

DllEntry * DllServer::doGetEntry(const char * name)
{
    StringBuffer path;
    getPath(path, name);
    Owned<IRemoteConnection> conn = querySDS().connect(path.str(), myProcessSession(), 0, 5000);
    if (conn)
        return new DllEntry(conn->queryRoot(), rootDir, NULL);
    return NULL;
}

IDllEntry * DllServer::createEntry(IPropertyTree *owner, IPropertyTree *entry)
{
    return new DllEntry(entry, rootDir, owner);
}

void DllServer::doRegisterDll(const char * name, const char * kind, const char * dllPath, const char * libPath)
{
    Owned<IRemoteConnection> lock = querySDS().connect("/GeneratedDlls", myProcessSession(), RTM_LOCK_WRITE, 5000);

    RemoteFilename dllRemote;
    StringBuffer ipText, dllText;
    dllRemote.setRemotePath(dllPath);
    dllRemote.queryIP().getIpText(ipText);
    dllRemote.getLocalPath(dllText);

    StringBuffer path;
    getPath(path, name);
    Owned<IRemoteConnection> conn = querySDS().connect(path.str(), myProcessSession(), RTM_LOCK_WRITE, 5000);
    if (conn)
    {
        //check the entry doesn't exist already....
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("location");
        ForEach(*iter)
        {
            IPropertyTree & cur = iter->query();
            if ((stricmp(cur.queryProp("@ip"),ipText.str()) == 0) && 
                (stricmp(cur.queryProp("@dll"),dllText.str()) == 0))
                return;
        }
    }
    else
    {
        conn.setown(querySDS().connect("/GeneratedDlls/GeneratedDll", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, 5000));
        if (!conn)
        {
            ::Release(querySDS().connect("/GeneratedDlls", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, 5000));
            conn.setown(querySDS().connect("/GeneratedDlls/GeneratedDll", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_ADD, 5000));
        }

        IPropertyTree * entry = conn->queryRoot();
        entry->setProp("@name", name);
        entry->setProp("@kind", kind);

        Owned<IJlibDateTime> now = createDateTimeNow();
        StringBuffer nowText;
        StringBufferAdaptor strval(nowText);
        now->getString(strval);
        entry->setProp("@created", nowText.str());

        conn->queryRoot()->setProp("@uid", name);
    }

    IPropertyTree * locationTree = createPTree("location");
    locationTree->setProp("@ip", ipText.str());
    locationTree->setProp("@dll", dllText.str());

    if (libPath && strlen(libPath))
    {
        RemoteFilename libRemote;
        libRemote.setRemotePath(libPath);
        if (!dllRemote.queryIP().ipequals(libRemote.queryIP()))
            throwError(DSVERR_DllLibIpMismatch);

        StringBuffer libText;
        libRemote.getLocalPath(libText);
        locationTree->setProp("@lib", libText.str());
    }

    conn->queryRoot()->addPropTree("location", locationTree);
}

void DllServer::ensureAvailable(const char * name, DllLocationType location)
{
    Owned<DllEntry> match = doGetEntry(name);
    if (!match)
        throwError1(DSVERR_CouldNotFindDll, name);

    Owned<IDllLocation> bestLocation = match->getBestLocation();
    if (bestLocation->queryLocation() < location)
    {
        StringBuffer remoteDllPath, remoteLibPath;
        RemoteFilename sourceName, dllName, libName;

        bestLocation->getDllFilename(sourceName);
        copyFileLocally(dllName, sourceName);
        dllName.getRemotePath(remoteDllPath);

        if (bestLocation->getLibFilename(sourceName))
        {
            copyFileLocally(libName, sourceName);
            libName.getRemotePath(remoteLibPath);
        }

        doRegisterDll(name, "**Error**", remoteDllPath.str(), remoteLibPath.str());
        assertex(isAvailable(name) >= DllLocationLocal);
    }
}

IDllLocation * DllServer::getBestMatch(const char * name)
{
    Owned<DllEntry> match = doGetEntry(name);
    if (!match)
        return NULL;

    return match->getBestLocation();
}


IDllLocation * DllServer::getBestMatchEx(const char * name)
{
    IDllLocation * location = getBestMatch(name);
    if (!location)
        throwError1(DSVERR_CouldNotFindDll, name);
    return location;
}


IDllEntry * DllServer::getEntry(const char * name)
{
    return doGetEntry(name);
}


void DllServer::getDll(const char * name, MemoryBuffer & dllText)
{
    RemoteFilename filename;
    Owned<IDllLocation> match = getBestMatchEx(name);
    match->getDllFilename(filename);

    Owned<IFile> file = createIFile(filename);
    OwnedIFileIO io = file->open(IFOread);
    read(io, 0, (size32_t)-1, dllText);
}

void DllServer::getLibrary(const char * name, MemoryBuffer & libText)
{
    RemoteFilename filename;
    Owned<IDllLocation> match = getBestMatchEx(name);
    if (!match->getLibFilename(filename))
        throwError1(DSVERR_NoAssociatedLib, name);

    Owned<IFile> file = createIFile(filename);
    OwnedIFileIO io = file->open(IFOread);
    read(io, 0, (size32_t)-1, libText);
}

void DllServer::getLocalLibraryName(const char * name, StringBuffer & libraryName)
{
    RemoteFilename filename;
    Owned<IDllLocation> match = getBestMatchEx(name);
    if (match->queryLocation() < DllLocationLocal)
        throwError1(DSVERR_LibNotLocal, name);

    if (!match->getLibFilename(filename))
        throwError1(DSVERR_NoAssociatedLib, name);

    filename.getLocalPath(libraryName);
}

DllLocationType DllServer::isAvailable(const char * name)
{
    try
    {
        Owned<IDllLocation> match = getBestMatch(name);
        if (match)
            return match->queryLocation();
    }
    catch (IException *E)
    {
        E->Release();
    }
    return DllLocationNowhere;
}

ILoadedDllEntry * DllServer::loadDll(const char * name, DllLocationType type)
{
#ifdef _WIN32
    if (type < DllLocationDomain)
        type = DllLocationDomain;
#else
    if (type < DllLocationLocal)
        type = DllLocationLocal;
#endif
    ensureAvailable(name, type);
    Owned<IDllLocation> location = getBestMatchEx(name);
    RemoteFilename rfile;
    location->getDllFilename(rfile);
    StringBuffer x;
    rfile.getPath(x);
    LOG(MCdebugInfo, unknownJob, "Loading dll (%s) from location %s", name, x.str());
    return createDllEntry(x.str(), false, NULL);
}

void DllServer::removeDll(const char * name, bool removeDlls, bool removeDirectory)
{
    Owned<IDllEntry> entry = getEntry(name);
    if (entry)
        entry->remove(removeDlls, removeDirectory);
}

void DllServer::registerDll(const char * name, const char * kind, const char * dllLocation)
{
    doRegisterDll(name, kind, dllLocation, NULL);
}

//---------------------------------------------------------------------------

static DllServer * dllServer;
CriticalSection dllServerCrit;

IDllServer & queryDllServer()
{
    CriticalBlock b(dllServerCrit);
    if (!dllServer)
    {
        const char* dllserver_root = getenv("DLLSERVER_ROOT");
        StringBuffer dir;
        if(dllserver_root == NULL)
        {
            if (envGetConfigurationDirectory("temp","dllserver","dllserver",dir)) // not sure if different instance might be better but never separated in past
                dllserver_root = dir.str();
            else
                dllserver_root = DEFAULT_SERVER_ROOTDIR;
        }
        initDllServer(dllserver_root);
    }

    return *dllServer;
}

void closeDllServer()
{
    hostDomain.clear();
    if (dllServer)
    {
        dllServer->Release();
        dllServer = NULL;
    }
}


void initDllServer(const char * localRoot)
{
    CriticalBlock b(dllServerCrit);
    ::Release(dllServer);
    dllServer = new DllServer(localRoot);
}


void cleanUpOldDlls()
{
    Owned<IJlibDateTime> cutoff = createDateTimeNow();
    unsigned year;
    unsigned month;
    unsigned day;
    cutoff->getGmtDate(year, month, day);
    if (false && month-- == 1)
    {
        month = 12;
        year--;
    }
    cutoff->setGmtDate(year, month, day);

    //Remove all but one copy of all workunits more than a month old
    Owned<IIterator> dllIter = queryDllServer().createDllIterator();
    ForEach(*dllIter)
    {
        IDllEntry & entry = (IDllEntry &)dllIter->query();

        if (strcmp(entry.queryKind(), "workunit")==0)
        {
            Owned<IJlibDateTime> created = createDateTime();
            entry.getCreated(*created);
            if (created->compare(*cutoff) < 0)
            {
                Owned<IIterator> locIter = entry.createLocationIterator();
                bool first = true;

                ForEach(*locIter)
                {
                    IDllLocation & location = (IDllLocation &)locIter->query();
                    if (!first)
                        location.remove(true, true);
                    first = false;
                }
            }
        }
    }
}


void testDllServer()
{
    IDllServer & server = queryDllServer();
    Owned<IDllEntry> oldentry2 = server.getEntry("WorkUnit1");
    if (oldentry2)
        oldentry2->remove(false, false);
    server.registerDll("WorkUnit1","workunit","\\\\ghalliday\\c$\\edata\\ecl\\regress\\process0.dll");
    assertex(server.isAvailable("WorkUnit1") == DllLocationLocal);
    server.ensureAvailable("WorkUnit1",DllLocationLocal);
    server.registerDll("WorkUnit1","workunit","\\\\1.1.1.1\\c$\\edata\\ecl\\regress\\process0.dll");

    const char * abcFilename = "\\\\127.0.0.1\\c$\\temp\\dlltest\abc";
    Owned<IFile> temp = createIFile(abcFilename);
    recursiveCreateDirectoryForFile(abcFilename);
    Owned<IFileIO> io = temp->open(IFOcreate);
    io->write(0, 10, "abcdefghij");
    io.clear();
    server.registerDll("WorkUnitAbc","workunit",abcFilename);
    server.removeDll("WorkUnitAbc", true, true);

    cleanUpOldDlls();
}

