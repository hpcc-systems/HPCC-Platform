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

#include "jliball.hpp"
#include "dllserver.hpp"
#include "jiter.ipp"
#include "jexcept.hpp"

#include "dllservererr.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "rmtclient.hpp"
#include "thorplugin.hpp"

#ifndef _CONTAINERIZED
#include "dalienv.hpp"
#endif

#ifdef _WIN32
#define DEFAULT_SERVER_ROOTDIR          "c:\\HPCCSystems\\hpcc-data\\temp\\dllserver"
#else
#define DEFAULT_SERVER_ROOTDIR          "/var/lib/HPCCSystems/dllserver/temp"
#endif

#define CONNECTION_TIMEOUT     30000

static void getMangledTag(StringBuffer & path, const char * name)
{
    path.append("Dll");
    unsigned len = strlen(name);
    path.ensureCapacity(len);
    for (unsigned i=0; i < len; i++)
    {
        char next = name[i];
        if (isalnum((byte)next))
            path.append(next);
        else
            path.append("_");
    }
}

static void getOldXPath(StringBuffer & path, const char * name)
{
    path.append("/GeneratedDlls/GeneratedDll[@uid=\"").append(name).append("\"]");
}

static void getNewXPath(StringBuffer & path, const char * name)
{
    path.append("/GeneratedDlls/");
    getMangledTag(path, name);
    //The following qualifier is here in case two different characters were mapped to _ - creating an ambiguous tag.
    path.append("[@name=\"").append(name).append("\"]");
}

IRemoteConnection * getEntryConnection(const char * name, unsigned mode)
{
    StringBuffer xpath;
    getNewXPath(xpath, name);
    Owned<IRemoteConnection> connection = querySDS().connect(xpath.str(), myProcessSession(), mode, CONNECTION_TIMEOUT);
    if (connection)
        return connection.getClear();

    //Retain backwards compatibility for the moment
    getOldXPath(xpath.clear(), name);
    return querySDS().connect(xpath.str(), myProcessSession(), mode, CONNECTION_TIMEOUT);
}

//---------------------------------------------------------------------------

class TreeIteratorWrapper : implements IIterator, public CInterface
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

class DllLocation : implements IDllLocation, public CInterface
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

    Owned<IRemoteConnection> conn = getEntryConnection(entryRoot->queryProp("@name"), RTM_LOCK_WRITE);
    Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("location");
    ForEach(*iter)
    {
        IPropertyTree & cur = iter->query();
        if (propsMatch(cur, *locationRoot, "@ip") && propsMatch(cur, *locationRoot, "@dll"))
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

class DllEntry : implements IDllEntry, public CInterface
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

int orderLocations(IInterface * const * pLeft, IInterface * const * pRight)
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
        Owned<IRemoteConnection> conn = getEntryConnection(root->queryProp("@name"), RTM_LOCK_WRITE);
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

class DllServerBase : public CInterfaceOf<IDllServer>
{
public:
    DllServerBase(const char * _rootDir) : rootDir(_rootDir) {}
    virtual IIterator * createDllIterator() override;
    virtual void getDll(const char * name, MemoryBuffer & dllText) override;
    virtual IDllEntry * getEntry(const char * name) override;
    virtual DllLocationType isAvailable(const char * name) override;
    virtual ILoadedDllEntry * loadDll(const char * name, DllLocationType location);
    virtual ILoadedDllEntry * loadDllResources(const char * name, DllLocationType location);
    virtual void removeDll(const char * name, bool removeDlls, bool removeDirectory);
    virtual IDllEntry * createEntry(IPropertyTree *owner, IPropertyTree *entry) override;
protected:
    DllEntry * doGetEntry(const char * name);
    IDllLocation * getBestMatch(const char * name);
    ILoadedDllEntry * doLoadDll(const char * name, DllLocationType location, bool resourcesOnly);

    StringAttr rootDir;
};

class DllServer : public DllServerBase
{
public:
    DllServer(const char * _rootDir) : DllServerBase(_rootDir) {};

    virtual void ensureAvailable(const char * name, DllLocationType location) override;
    virtual void registerDll(const char * name, const char * kind, const char * dllPath) override;

protected:
    void copyFileLocally(RemoteFilename & targetName, RemoteFilename & sourceName);
    void doRegisterDll(const char * name, const char * kind, const char * dllPath);
};

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
    source->copyTo(target, 0, NULL, true);
}

IIterator * DllServerBase::createDllIterator()
{
    Owned<IRemoteConnection> conn = querySDS().connect("/GeneratedDlls", myProcessSession(), 0, CONNECTION_TIMEOUT);
    IPropertyTree * root = conn->queryRoot();
    return conn ? (IIterator *)new DllIterator(root, root->getElements("*"), rootDir) : (IIterator *)new CNullIterator;
}

DllEntry * DllServerBase::doGetEntry(const char * name)
{
    Owned<IRemoteConnection> conn = getEntryConnection(name, 0);
    if (conn)
        return new DllEntry(conn->queryRoot(), rootDir, NULL);
    return NULL;
}

IDllEntry * DllServerBase::createEntry(IPropertyTree *owner, IPropertyTree *entry)
{
    return new DllEntry(entry, rootDir, owner);
}

void DllServer::doRegisterDll(const char * name, const char * kind, const char * dllPath)
{
    RemoteFilename dllRemote;
    StringBuffer ipText, dllText;
    dllRemote.setRemotePath(dllPath);
    dllRemote.queryIP().getIpText(ipText);
    dllRemote.getLocalPath(dllText);

    Owned<IRemoteConnection> conn = getEntryConnection(name, RTM_LOCK_WRITE);
    if (conn)
    {
        /* check the entry doesn't exist already....
         * Ideally the connection above would be a RTM_LOCK_READ and be changed to a RTM_LOCK_WRITE only when 'location' not found
         */
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
        /* in theory, two clients/threads could get here at the same time
         * in practice only one client/thread will be adding a unique named generated dll
         */
        StringBuffer xpath;
        xpath.append("/GeneratedDlls/");
        getMangledTag(xpath, name);

        conn.setown(querySDS().connect(xpath, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, CONNECTION_TIMEOUT));
        assertex(conn); // RTM_CREATE_QUERY will create GeneratedDlls parent node if it doesn't exist.

        IPropertyTree * entry = conn->queryRoot();
        entry->setProp("@name", name);
        entry->setProp("@kind", kind);

        Owned<IJlibDateTime> now = createDateTimeNow();
        StringBuffer nowText;
        StringBufferAdaptor strval(nowText);
        now->getString(strval);
        entry->setProp("@created", nowText.str());
    }

    IPropertyTree * locationTree = createPTree("location");
    locationTree->setProp("@ip", ipText.str());
    locationTree->setProp("@dll", dllText.str());

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
        doRegisterDll(name, "**Error**", remoteDllPath.str());
        assertex(isAvailable(name) >= DllLocationLocal);
    }
}

IDllLocation * DllServerBase::getBestMatch(const char * name)
{
    Owned<DllEntry> match = doGetEntry(name);
    if (!match)
        return NULL;

    return match->getBestLocation();
}

IDllEntry * DllServerBase::getEntry(const char * name)
{
    return doGetEntry(name);
}


void DllServerBase::getDll(const char * name, MemoryBuffer & dllText)
{
    RemoteFilename filename;
    Owned<IDllLocation> match = getBestMatch(name);
    match->getDllFilename(filename);

    Owned<IFile> file = createIFile(filename);
    OwnedIFileIO io = file->open(IFOread);
    read(io, 0, (size32_t)-1, dllText);
}

DllLocationType DllServerBase::isAvailable(const char * name)
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

ILoadedDllEntry * DllServerBase::loadDll(const char * name, DllLocationType type)
{
    return doLoadDll(name, type, false);
}

ILoadedDllEntry * DllServerBase::loadDllResources(const char * name, DllLocationType type)
{
    return doLoadDll(name, type, true);
}

ILoadedDllEntry * DllServerBase::doLoadDll(const char * name, DllLocationType type, bool resourcesOnly)
{
    if (type < DllLocationLocal)
        type = DllLocationLocal;
    ensureAvailable(name, type);
    Owned<IDllLocation> location = getBestMatch(name);
    RemoteFilename rfile;
    location->getDllFilename(rfile);
    StringBuffer x;
    rfile.getPath(x);
    LOG(MCdebugInfo, unknownJob, "Loading dll (%s) from location %s", name, x.str());
    return createDllEntry(x.str(), false, NULL, resourcesOnly);
}

void DllServerBase::removeDll(const char * name, bool removeDlls, bool removeDirectory)
{
    Owned<IDllEntry> entry = getEntry(name);
    if (entry)
        entry->remove(removeDlls, removeDirectory);
}

void DllServer::registerDll(const char * name, const char * kind, const char * dllLocation)
{
    doRegisterDll(name, kind, dllLocation);
}

//---------------------------------------------------------------------------

class SharedVolumeDllServer : public DllServerBase
{
public:
    SharedVolumeDllServer(const char * _mountPath) : DllServerBase(_mountPath) {};

    virtual void ensureAvailable(const char * name, DllLocationType location) override;
    virtual void registerDll(const char * name, const char * kind, const char * dllPath) override;
};

void SharedVolumeDllServer::ensureAvailable(const char * name, DllLocationType location)
{
    Owned<DllEntry> match = doGetEntry(name);
    if (!match)
        throwError1(DSVERR_CouldNotFindDll, name);
    // MORE - if we still want to have roxie copy to directory, may need code here
#ifdef _DEBUG
    Owned<IDllLocation> bestLocation = match->getBestLocation();
    assertex (bestLocation->queryLocation() >= location);
#endif
}

void SharedVolumeDllServer::registerDll(const char * name, const char * kind, const char * dllPath)
{
    RemoteFilename dllRemote;
    dllRemote.setRemotePath(dllPath);
    assertex(dllRemote.isLocal());
    StringBuffer sourceLocalPath;

    dllRemote.getLocalPath(sourceLocalPath);

    StringBuffer mountPath(rootDir);
    addPathSepChar(mountPath);
    splitFilename(sourceLocalPath.str(),NULL,NULL,&mountPath,&mountPath);

    // Copy file to mountpath, if not already there
    OwnedIFile source = createIFile(dllRemote);
    OwnedIFile target = createIFile(mountPath);
    if (!target->exists())
    {
        source->copyTo(target, 0, NULL, true);
    }

    Owned<IRemoteConnection> conn = getEntryConnection(name, RTM_LOCK_WRITE);
    if (conn)
    {
        /* check the entry doesn't exist already....
         * Ideally the connection above would be a RTM_LOCK_READ and be changed to a RTM_LOCK_WRITE only when 'location' not found
         */
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("location");
        ForEach(*iter)
        {
            IPropertyTree & cur = iter->query();
            if ((stricmp(cur.queryProp("@ip"),"localhost") == 0) &&
                (stricmp(cur.queryProp("@dll"),mountPath.str()) == 0))
                return;
        }
    }
    else
    {
        /* in theory, two clients/threads could get here at the same time
         * in practice only one client/thread will be adding a unique named generated dll
         */
        StringBuffer xpath;
        xpath.append("/GeneratedDlls/");
        getMangledTag(xpath, name);

        conn.setown(querySDS().connect(xpath, myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, CONNECTION_TIMEOUT));
        assertex(conn); // RTM_CREATE_QUERY will create GeneratedDlls parent node if it doesn't exist.

        IPropertyTree * entry = conn->queryRoot();
        entry->setProp("@name", name);
        entry->setProp("@kind", kind);

        Owned<IJlibDateTime> now = createDateTimeNow();
        StringBuffer nowText;
        StringBufferAdaptor strval(nowText);
        now->getString(strval);
        entry->setProp("@created", nowText.str());
    }

    IPropertyTree * locationTree = createPTree("location");
    locationTree->setProp("@ip", "localhost");
    locationTree->setProp("@dll", mountPath.str());

    conn->queryRoot()->addPropTree("location", locationTree);
}

//---------------------------------------------------------------------------

static IDllServer * dllServer;
CriticalSection dllServerCrit;

IDllServer & queryDllServer()
{
    CriticalBlock b(dllServerCrit);
    if (!dllServer)
    {
#ifdef _CONTAINERIZED
        const char* dllserver_root = getenv("HPCC_DLLSERVER_PATH");
        assertex(dllserver_root != nullptr);
        dllServer = new SharedVolumeDllServer(dllserver_root);
#else
        const char* dllserver_root = getenv("DLLSERVER_ROOT");
        StringBuffer dir;
        if(dllserver_root == NULL)
        {
            if (envGetConfigurationDirectory("temp","dllserver","dllserver",dir)) // not sure if different instance might be better but never separated in past
                dllserver_root = dir.str();
            else
                dllserver_root = DEFAULT_SERVER_ROOTDIR;
        }
        dllServer = new DllServer(dllserver_root);
#endif
    }

    return *dllServer;
}

void closeDllServer()
{
    CriticalBlock b(dllServerCrit);
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
    //MORE: Should this test be removed?
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


