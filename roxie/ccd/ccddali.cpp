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
#include "workunit.hpp"
#include "dalienv.hpp"
#include "daclient.hpp"
#include "dautils.hpp"
#include "dllserver.hpp"
#include "ccddali.hpp"
#include "ccdfile.hpp"
#include "ccd.hpp"

#include "jencrypt.hpp"
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "workflow.hpp"
#include "mpcomm.hpp"

const char *roxieStateName = "RoxieLocalState.xml";

class CDaliPackageWatcher : public CInterface, implements ISDSSubscription, implements IDaliPackageWatcher
{
    SubscriptionId change;
    ISDSSubscription *notifier;
    StringAttr id;
    mutable CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE;
    CDaliPackageWatcher(const char *_id, const char *xpath, ISDSSubscription *_notifier)
      : id(_id)
    {
        notifier = _notifier;
        change = querySDS().subscribe(xpath, *this, true);
    }
    ~CDaliPackageWatcher()
    {
    }
    virtual void unsubscribe()
    {
        CriticalBlock b(crit);
        notifier = NULL;
        querySDS().unsubscribe(change);
    }
    virtual const char *queryName() const
    {
        return id.get();
    }
    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        CriticalBlock b(crit);
        if (notifier)
            notifier->notify(id, xpath, flags, valueLen, valueData);
    }
};

/*===============================================================================
* Roxie is not a typical Dali client - if dali is available it will use it, but
* it needs to be able to continue to serve any queries that have already been deployed
* should Dali no longer be available, including the ability to restart a roxie while
* the dali that originally provided its data is unavailable.
*
* This single-instance class is designed to help Roxie manage its dali connections.
* it connects to dali on demand, and disconnects when all links to this object are
* released - in practice that won't happen until termination as we keep an active dali
* connection as long as any QuerySets are subscribed. This class also handles the
* local caching of sufficient information retrieved from dali so that Roxie can restart
* even if dali is not available.
*
* If dali is down. Data is delivered from the cache. If dali is up, the cache is populated from
* the information retrieved from dali. We snapshot the cache at the end of each successful reload.
*
* For simplicity and safety, Roxie will not attempt to reconnect to a dali once it is
* disconnected (roxie should be manually restarted once you are confident that dali has been
* properly restored).
*
* TODO - Anything that gets an error talking to dali has to not crash...
* TODO - Any info that needs to be available when dali is down has to be retrieved via this
*      - class and cached
* TODO - dynamic files can still resolve via daliHelper (will continue to give the same answer
*      - they gave last time)? Or would it be better to fail?
*
* If we ever wanted to handle reconnects, it could possible be done by treating Dali
* coming up as the same as generating a notification on all subscriptions?
*
=================================================================================*/

class CRoxieDaliHelper : public CInterface, implements IRoxieDaliHelper
{
private:
    static bool isConnected;
    static CRoxieDaliHelper *daliHelper;  // Note - this does not own the helper
    static CriticalSection daliConnectionCrit; 
    Owned<IUserDescriptor> userdesc;

    virtual void beforeDispose()
    {
        CriticalBlock b(daliConnectionCrit);
        if (daliHelper==this)  // there is a tiny window where new dalihelper created immediately after final release
        {
            disconnect();
            daliHelper = NULL;
        }
    }

    // The cache is static since it outlives the dali connections

    static CriticalSection cacheCrit;
    static Owned<IPropertyTree> cache;

    static void initCache()
    {
        IPropertyTree *tree = createPTree("Roxie");
        tree->addPropTree("QuerySets", createPTree("QuerySets"));
        tree->addPropTree("PackageSets", createPTree("PackageSets"));
        tree->addPropTree("PackageMaps", createPTree("PackageMaps"));
        tree->addPropTree("Files", createPTree("Files"));
        cache.setown(tree);
    }

    static void loadCache()
    {
        if (!cache)
        {
            StringBuffer cacheFileName(queryDirectory);
            cacheFileName.append(roxieStateName);
            if (checkFileExists(cacheFileName))
                cache.setown(createPTreeFromXMLFile(cacheFileName));
            else
                initCache();
        }
    }

    static IPropertyTree *readCache(const char *xpath)
    {
        CriticalBlock b(cacheCrit);
        loadCache();
        return cache->getPropTree(xpath);
    }

    static void writeCache(const char *foundLoc, const char *newLoc, IPropertyTree *val)
    {
        CriticalBlock b(cacheCrit);
        cache->removeProp(foundLoc);
        if (val)
            cache->addPropTree(newLoc, LINK(val));
    }

    IPropertyTree *loadDaliTree(const char *path, const char *id)
    {
        StringBuffer xpath(path);
        if (id)
            xpath.appendf("[@id='%s']", id);
        Owned <IPropertyTree> localTree;
        if (isConnected)
        {
            Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), 0, 30*1000);
            if (conn)
            {
                Owned <IPropertyTree> daliTree = conn->getRoot();
                localTree.setown(createPTreeFromIPT(daliTree));
            }
            writeCache(xpath, path, localTree);
        }
        else
            localTree.setown(readCache(xpath));
        return localTree.getClear();
    }

public:

    IMPLEMENT_IINTERFACE;
    CRoxieDaliHelper()
    {
        if (topology)
        {
            const char *roxieUser = topology->queryProp("@ldapUser");
            const char *roxiePassword = topology->queryProp("@ldapPassword");
            if (roxieUser && *roxieUser && roxiePassword && *roxiePassword)
            {
                StringBuffer password;
                decrypt(password, roxiePassword);
                userdesc.setown(createUserDescriptor());
                userdesc->set(roxieUser, password.str());
            }
        }
    }

    static const char *getQuerySetPath(StringBuffer &buf, const char *id)
    {
        buf.appendf("QuerySets/QuerySet[@id='%s']", id);
        return buf.str();
    }

    virtual IPropertyTree *getQuerySet(const char *id)
    {
        Owned<IPropertyTree> ret = loadDaliTree("QuerySets/QuerySet", id);
        if (!ret)
        {
            ret.setown(createPTree("QuerySet"));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    static const char *getPackageSetPath(StringBuffer &buf, const char *id)
    {
        buf.appendf("PackageSets/PackageSet[@id='%s']", id);
        return buf.str();
    }

    virtual IPropertyTree *getPackageSet(const char *id)
    {
        Owned<IPropertyTree> ret = loadDaliTree("PackageSets/PackageSet", id);
        if (!ret)
        {
            ret.setown(createPTree("PackageSet"));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    static const char *getPackageMapPath(StringBuffer &buf, const char *id)
    {
        buf.appendf("PackageMaps/PackageMap[@id='%s']", id);
        return buf.str();
    }

    virtual IPropertyTree *getPackageMap(const char *id)
    {
        Owned<IPropertyTree> ret = loadDaliTree("PackageMaps/PackageMap", id);
        if (!ret)
        {
            ret.setown(createPTree("PackageMap"));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    virtual IDistributedFile *resolveLFN(const char *logicalName, bool cacheIt, bool writeAccess)
    {
        if (isConnected)
        {
            CDfsLogicalFileName lfn;
            lfn.set(logicalName);
            Owned<IDistributedFile> dfsFile = queryDistributedFileDirectory().lookup(lfn, userdesc.get(), writeAccess);
            if (dfsFile)
            {
                IDistributedSuperFile *super = dfsFile->querySuperFile();
                if (super && (0 == super->numSubFiles(true)))
                    dfsFile.clear();
            }
            if (cacheIt)
            {
                Owned<IFileDescriptor> fd;
                Owned<IPropertyTree> pt;
                if (dfsFile)
                {
                    fd.setown(dfsFile->getFileDescriptor());
                    if (fd)
                        pt.setown(fd->getFileTree());
                }
                StringBuffer xpath("Files/");
                StringBuffer lcname;
                xpath.append(lcname.append(logicalName).toLowerCase());
                writeCache(xpath.str(), xpath.str(), pt);
            }
            return dfsFile.getClear();
        }
        else
            return NULL;
    }

    virtual IFileDescriptor *resolveCachedLFN(const char *logicalName)
    {
        StringBuffer xpath("Files/");
        StringBuffer lcname;
        xpath.append(lcname.append(logicalName).toLowerCase());
        Owned<IPropertyTree> pt = readCache(xpath.str());
        if (pt)
        {
            return deserializeFileDescriptorTree(pt);
        }
        else
            return NULL;
    }

    virtual void commitCache()
    {
        if (isConnected)
        {
            CriticalBlock b(cacheCrit);
            if (!recursiveCreateDirectory(queryDirectory))
                throw MakeStringException(ROXIE_FILE_ERROR, "Unable to create directory %s", queryDirectory.str());
            VStringBuffer newCacheFileName("%s%s.new", queryDirectory.str(), roxieStateName);
            VStringBuffer oldCacheFileName("%s%s.bak", queryDirectory.str(), roxieStateName);
            VStringBuffer cacheFileName("%s%s", queryDirectory.str(), roxieStateName);
            saveXML(newCacheFileName, cache);
            Owned<IFile> oldFile = createIFile(oldCacheFileName);
            Owned<IFile> newFile = createIFile(newCacheFileName);
            Owned<IFile> cacheFile = createIFile(cacheFileName);
            if (oldFile->exists())
                oldFile->remove();
            if (cacheFile->exists())
                cacheFile->rename(oldCacheFileName);
            newFile->rename(cacheFileName);
        }
    }

    virtual IConstWorkUnit *attachWorkunit(const char *wuid, ILoadedDllEntry *source)
    {
        assertex(isConnected);
        Owned<IWorkUnitFactory> wuFactory = getWorkUnitFactory();
        Owned<IWorkUnit> w;
        StringAttr newWuid;
        if (wuid && *wuid)
        {
            w.setown(wuFactory->updateWorkUnit(wuid));
            if (!w)
                return NULL;
        }
        else
        {
            w.setown(wuFactory->createWorkUnit(NULL, NULL, NULL));
            w->getWuid(StringAttrAdaptor(newWuid));
            wuid = newWuid.get();
        }
        w->setAgentSession(myProcessSession());
        if (source)
        {
            StringBuffer wuXML;
            if (getEmbeddedWorkUnitXML(source, wuXML))
            {
                Owned<ILocalWorkUnit> localWU = createLocalWorkUnit();
                localWU->loadXML(wuXML);
                queryExtendedWU(w)->copyWorkUnit(localWU);
            }
            else
                throw MakeStringException(ROXIE_DALI_ERROR, "Failed to locate dll workunit info");
        }
        w->commit();
        w.clear();
        return wuFactory->openWorkUnit(wuid, false);
    }

    static IRoxieDaliHelper *connectToDali()
    {
        CriticalBlock b(daliConnectionCrit);
        LINK(daliHelper);
        if (daliHelper && daliHelper->isAlive())
            return daliHelper;
        else
        {
            // NOTE - if daliHelper is not NULL but isAlive returned false, then we have an overlapping connect with a final disconnect
            // In this case the beforeDispose will have taken care NOT to disconnect, isConnected will remain set, and the connect() will be a no-op.
            daliHelper = new CRoxieDaliHelper();
            if (topology && !topology->getPropBool("@lockDali", false))
                daliHelper->connect();
            return daliHelper;
        }
    }

    static void releaseCache()
    {
        CriticalBlock b(cacheCrit);
        cache.clear();
    }

    virtual IDaliPackageWatcher *getQuerySetSubscription(const char *id, ISDSSubscription *notifier)
    {
        if (isConnected)
        {
            StringBuffer xpath;
            return new CDaliPackageWatcher(id, getQuerySetPath(xpath, id), notifier);
        }
        else
            return NULL;
    }

    virtual IDaliPackageWatcher *getPackageSetSubscription(const char *id, ISDSSubscription *notifier)
    {
        if (isConnected)
        {
            StringBuffer xpath;
            return new CDaliPackageWatcher(id, getPackageSetPath(xpath, id), notifier);
        }
        else
            return NULL;
    }

    virtual IDaliPackageWatcher *getPackageMapSubscription(const char *id, ISDSSubscription *notifier)
    {
        if (isConnected)
        {
            StringBuffer xpath;
            return new CDaliPackageWatcher(id, getPackageMapPath(xpath, id), notifier);
        }
        else
            return NULL;
    }

    virtual bool connected() const
    {
        return isConnected;
    }

    // connect handles the operations generally performed by Dali clients at startup.
    virtual bool connect()
    {
        if (fileNameServiceDali.length())
        {
            if (!isConnected)
            {
                CriticalBlock b(daliConnectionCrit);
                if (!isConnected)
                {
                    try
                    {
                        // Create server group
                        Owned<IGroup> serverGroup = createIGroup(fileNameServiceDali, DALI_SERVER_PORT);

                        if (!serverGroup)
                            throw MakeStringException(ROXIE_DALI_ERROR, "Could not instantiate dali IGroup");

                        // Initialize client process
                        if (!initClientProcess(serverGroup, DCR_RoxyMaster, 0, NULL, NULL, 5000))  // wait 5 seconds
                            throw MakeStringException(ROXIE_DALI_ERROR, "Could not initialize dali client");

                        setPasswordsFromSDS();
                        CSDSServerStatus serverstatus("Roxieserver");
                        initCache();
                        isConnected = true;
                    }
                    catch(IException *e)
                    {
                        StringBuffer text;
                        e->errorMessage(text);
                        DBGLOG(ROXIE_DALI_ERROR, "Error trying to connect to dali %s: %s", fileNameServiceDali.str(), text.str());
                        e->Release();
                    }
                }
            }
        }
        else
            initMyNode(1); // Hack
        return isConnected;
    }

    virtual void disconnect()
    {
        if (isConnected)
        {
            CriticalBlock b(daliConnectionCrit);
            if (isConnected)
            {
                closeDllServer();
                closeEnvironment();
                clientShutdownWorkUnit();
                ::closedownClientProcess(); // dali client closedown
                isConnected = false;
            }
        }
    }

};

class CRoxieDllServer : public CInterface, implements IDllServer
{
    static CriticalSection crit;
    bool started;
public:
    IMPLEMENT_IINTERFACE;
    CRoxieDllServer()
    {
        started = false;
    }

    virtual IIterator * createDllIterator() { throwUnexpected(); }
    virtual void ensureAvailable(const char * name, DllLocationType location) { throwUnexpected(); }
    virtual void getDll(const char * name, MemoryBuffer & dllText) { throwUnexpected(); }
    virtual IDllEntry * getEntry(const char * name) { throwUnexpected(); }
    virtual void getLibrary(const char * name, MemoryBuffer & dllText) { throwUnexpected(); }
    virtual void getLocalLibraryName(const char * name, StringBuffer & libraryName) { throwUnexpected(); }
    virtual DllLocationType isAvailable(const char * name) { throwUnexpected(); }
    virtual void removeDll(const char * name, bool removeDlls, bool removeDirectory) { throwUnexpected(); }
    virtual void registerDll(const char * name, const char * kind, const char * dllPath) { throwUnexpected(); }

    virtual ILoadedDllEntry * loadDll(const char * name, DllLocationType location) 
    {
        if (location == DllLocationDirectory)
        {
            StringBuffer localName(queryDirectory);
            localName.append(name);
            if (checkFileExists(localName.str()))
                return createDllEntry(localName.str(), false, NULL);
        }
        CriticalBlock b(crit);
        Owned<IRoxieDaliHelper> daliHelper = connectToDali();
        if (!started)
        {
            if (!recursiveCreateDirectory(queryDirectory))
                throw MakeStringException(ROXIE_FILE_ERROR, "Unable to create directory %s", queryDirectory.str());
            initDllServer(queryDirectory);
            started = true;
        }
        return queryDllServer().loadDll(name, location);
    }
} roxieDllServer;

bool CRoxieDaliHelper::isConnected = false;
CRoxieDaliHelper * CRoxieDaliHelper::daliHelper;
CriticalSection CRoxieDaliHelper::daliConnectionCrit; 
CriticalSection CRoxieDaliHelper::cacheCrit;
Owned<IPropertyTree> CRoxieDaliHelper::cache;

CriticalSection CRoxieDllServer::crit;

IRoxieDaliHelper *connectToDali()
{
    return CRoxieDaliHelper::connectToDali();
}

extern void releaseRoxieStateCache()
{
    CRoxieDaliHelper::releaseCache();
}

extern IDllServer &queryRoxieDllServer()
{
    return roxieDllServer;
}

extern void addWuException(IConstWorkUnit *workUnit, IException *E)
{
    StringBuffer message;
    E->errorMessage(message);
    unsigned code = E->errorCode();
    OERRLOG("%u - %s", code, message.str());
    WorkunitUpdate w(&workUnit->lock());
    addExceptionToWorkunit(w, ExceptionSeverityError, "Roxie", code, message.str(), NULL, 0, 0);
}
