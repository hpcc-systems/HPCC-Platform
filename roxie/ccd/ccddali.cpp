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

#include "jlib.hpp"
#include "workunit.hpp"
#include "dalienv.hpp"
#include "daclient.hpp"
#include "dautils.hpp"
#include "dllserver.hpp"
#include "ccddali.hpp"
#include "ccdfile.hpp"
#include "ccdlistener.hpp"
#include "ccd.hpp"

#include "jencrypt.hpp"
#include "jisem.hpp"
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
    StringAttr xpath;
    mutable CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE;
    CDaliPackageWatcher(const char *_id, const char *_xpath, ISDSSubscription *_notifier)
      : id(_id), xpath(_xpath), change(0)
    {
        notifier = _notifier;
    }
    ~CDaliPackageWatcher()
    {
    }
    virtual void subscribe()
    {
        CriticalBlock b(crit);
        try
        {
            change = querySDS().subscribe(xpath, *this, true);
        }
        catch (IException *E)
        {
            // failure to subscribe implies dali is down... that's ok, we will resubscribe when we notice it come back up.
            E->Release();
        }
    }
    virtual void unsubscribe()
    {
        CriticalBlock b(crit);
        notifier = NULL;
        try
        {
            if (change)
                querySDS().unsubscribe(change);
        }
        catch (IException *E)
        {
            E->Release();
        }
        change = 0;
    }
    virtual const char *queryName() const
    {
        return id.get();
    }
    virtual void onReconnect()
    {
        Linked<CDaliPackageWatcher> me = this;  // Ensure that I am not released by the notify call (which would then access freed memory to release the critsec)
        // It's tempting to think you can avoid holding the critsec during the notify call, and that you only need to hold it while looking up notifier
        // Despite the danger of deadlocks (that requires careful code in the notifier to avoid), I think it is neccessary to hold the lock during the call,
        // as otherwise notifier may point to a deleted object.
        CriticalBlock b(crit);
        change = querySDS().subscribe(xpath, *this, true);
        if (notifier)
            notifier->notify(0, NULL, SDSNotify_None);
    }
    virtual void notify(SubscriptionId subid, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        Linked<CDaliPackageWatcher> me = this;  // Ensure that I am not released by the notify call (which would then access freed memory to release the critsec)
        Linked<ISDSSubscription> myNotifier;
        {
            CriticalBlock b(crit);
            myNotifier.set(notifier);
            // allow crit to be released, allowing this to be unsubscribed, to avoid deadlocking when other threads via notify call unsubscribe
        }
        if (myNotifier)
            myNotifier->notify(subid, xpath, flags, valueLen, valueData);
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
*
=================================================================================*/

class CRoxieDaliHelper : public CInterface, implements IRoxieDaliHelper
{
private:
    static bool isConnected;
    static CRoxieDaliHelper *daliHelper;  // Note - this does not own the helper
    static CriticalSection daliHelperCrit;
    CriticalSection daliConnectionCrit;
    Owned<IUserDescriptor> userdesc;
    InterruptableSemaphore disconnectSem;
    IArrayOf<IDaliPackageWatcher> watchers;
    CSDSServerStatus *serverStatus;

    class CRoxieDaliConnectWatcher : public Thread
    {
    private:
        CRoxieDaliHelper *owner;
        bool aborted;
    public:
        CRoxieDaliConnectWatcher(CRoxieDaliHelper *_owner) : owner(_owner)
        {
            aborted = false;
        }

        virtual int run()
        {
            while (!aborted)
            {
                if (topology && topology->getPropBool("@lockDali", false))
                {
                    Sleep(ROXIE_DALI_CONNECT_TIMEOUT);
                }
                else if (owner->connect(ROXIE_DALI_CONNECT_TIMEOUT))
                {
                    DBGLOG("roxie: CRoxieDaliConnectWatcher reconnected");
                    try
                    {
                        owner->disconnectSem.wait();
                    }
                    catch (IException *E)
                    {
                        if (!aborted)
                            EXCLOG(E, "roxie: Unexpected exception in CRoxieDaliConnectWatcher");
                        E->Release();
                    }
                }
            }
            return 0;
        }
        void stop()
        {
            aborted = true;
        }
        virtual void start()
        {
            Thread::start();
        }
        virtual void join()
        {
            if (isAlive())
                Thread::join();
        }
    } connectWatcher;

    virtual void beforeDispose()
    {
        CriticalBlock b(daliHelperCrit);
        disconnectSem.interrupt();
        connectWatcher.stop();
        if (daliHelper==this)  // there is a tiny window where new dalihelper created immediately after final release
        {
            disconnect();
            daliHelper = NULL;
        }
        connectWatcher.join();
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
            try
            {
                Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), 0, 30*1000);
                if (conn)
                {
                    Owned <IPropertyTree> daliTree = conn->getRoot();
                    localTree.setown(createPTreeFromIPT(daliTree));
                }
                writeCache(xpath, path, localTree);
                return localTree.getClear();
            }
            catch (IDaliClient_Exception *E)
            {
                if (traceLevel)
                    EXCLOG(E, "roxie: Dali connection lost");
                E->Release();
                daliHelper->disconnect();
            }
        }
        DBGLOG("LoadDaliTree(%s) - not connected - read from cache", xpath.str());
        localTree.setown(readCache(xpath));
        return localTree.getClear();
    }

    IFileDescriptor *recreateCloneSource(IFileDescriptor *srcfdesc, const char *destfilename)
    {
        Owned<IFileDescriptor> dstfdesc = createFileDescriptor(srcfdesc->getProperties());
        // calculate dest dir

        CDfsLogicalFileName dstlfn;
        if (!dstlfn.setValidate(destfilename,true))
            throw MakeStringException(-1,"Logical name %s invalid",destfilename);

        StringBuffer dstpartmask;
        getPartMask(dstpartmask,destfilename,srcfdesc->numParts());
        dstfdesc->setPartMask(dstpartmask.str());
        unsigned np = srcfdesc->numParts();
        dstfdesc->setNumParts(srcfdesc->numParts());
        dstfdesc->setDefaultDir(srcfdesc->queryProperties().queryProp("@cloneFromDir"));

        for (unsigned pn=0;pn<srcfdesc->numParts();pn++) {
            offset_t sz = srcfdesc->queryPart(pn)->queryProperties().getPropInt64("@size",-1);
            if (sz!=(offset_t)-1)
                dstfdesc->queryPart(pn)->queryProperties().setPropInt64("@size",sz);
            StringBuffer dates;
            if (srcfdesc->queryPart(pn)->queryProperties().getProp("@modified",dates))
                dstfdesc->queryPart(pn)->queryProperties().setProp("@modified",dates.str());
        }

        const char *cloneFrom = srcfdesc->queryProperties().queryProp("@cloneFrom");
        Owned<IPropertyTreeIterator> groups = srcfdesc->queryProperties().getElements("cloneFromGroup");
        ForEach(*groups)
        {
            IPropertyTree &elem = groups->query();
            const char *groupName = elem.queryProp("@groupName");
            StringBuffer dir;
            StringBuffer foreignGroup("foreign::");
            foreignGroup.append(cloneFrom).append("::").append(groupName);
            GroupType groupType;
            Owned<IGroup> group = queryNamedGroupStore().lookup(foreignGroup, dir, groupType);
            ClusterPartDiskMapSpec dmSpec;
            dmSpec.fromProp(&elem);
            if (!dmSpec.defaultBaseDir.length())
            {
                if (dir.length())
                {
                    dmSpec.setDefaultBaseDir(dir);
                }
                else
                {
                    // Due to the really weird code in dadfs, this MUST be set to match the leading portion of cloneFromDir
                    // in order to properly handle remote systems with different default directory locations
                    StringBuffer tail;
                    DFD_OS os = (getPathSepChar(srcfdesc->queryDefaultDir())=='\\')?DFD_OSwindows:DFD_OSunix;
                    makePhysicalPartName(dstlfn.get(),0,0,tail,0,os,PATHSEPSTR);  // if lfn is a::b::c, tail will be /a/b/
                    assertex(tail.length() > 1);
                    tail.setLength(tail.length()-1);   // strip off the trailing /
                    StringBuffer head(srcfdesc->queryProperties().queryProp("@cloneFromDir")); // Will end with /a/b
                    assertex(streq(head.str() + head.length() - tail.length(), tail.str()));
                    head.setLength(head.length() - tail.length()); // So strip off the end...
                    dmSpec.setDefaultBaseDir(head.str());
                }
            }
            dstfdesc->addCluster(groupName, group, dmSpec);
        }
        return dstfdesc.getClear();
    }

public:

    IMPLEMENT_IINTERFACE;
    CRoxieDaliHelper() : connectWatcher(this), serverStatus(NULL)
    {
        userdesc.setown(createUserDescriptor());
        const char *roxieUser;
        const char *roxiePassword;
        if (topology)
        {
            roxieUser = topology->queryProp("@ldapUser");
            roxiePassword = topology->queryProp("@ldapPassword");
        }
        if (!roxieUser)
            roxieUser = "roxie";
        if (!roxiePassword)
            roxiePassword = "";
        StringBuffer password;
        decrypt(password, roxiePassword);
        userdesc->set(roxieUser, password.str());
        if (fileNameServiceDali.length())
            connectWatcher.start();
        else
            initMyNode(1); // Hack
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

    virtual IPropertyTree *getPackageSets()
    {
        Owned<IPropertyTree> ret = loadDaliTree("PackageSets", NULL);
        if (!ret)
        {
            ret.setown(createPTree("PackageSets"));
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

    IFileDescriptor *checkClonedFromRemote(const char *_lfn, IFileDescriptor *fdesc, bool cacheIt)
    {
        // NOTE - we rely on the fact that  queryNamedGroupStore().lookup caches results,to avoid excessive load on remote dali
        if (_lfn && !strnicmp(_lfn, "foreign", 7)) //if need to support dali hopping should add each remote location
            return NULL;
        if (!fdesc || !fdesc->queryProperties().hasProp("@cloneFrom"))
            return NULL;
        if (fdesc->queryProperties().hasProp("cloneFromGroup") && fdesc->queryProperties().hasProp("@cloneFromDir"))
        {
            return recreateCloneSource(fdesc, _lfn);
        }
        else // Legacy mode - recently cloned files should have the extra info
        {
            SocketEndpoint cloneFrom;
            cloneFrom.set(fdesc->queryProperties().queryProp("@cloneFrom"));
            if (cloneFrom.isNull())
                return NULL;
            CDfsLogicalFileName lfn;
            lfn.set(_lfn);
            lfn.setForeign(cloneFrom, false);
            if (!connected())
                return resolveCachedLFN(lfn.get());
            Owned<IDistributedFile> cloneFile = resolveLFN(lfn.get(), cacheIt, false);
            if (cloneFile)
            {
                Owned<IFileDescriptor> cloneFDesc = cloneFile->getFileDescriptor();
                if (cloneFDesc->numParts()==fdesc->numParts())
                    return cloneFDesc.getClear();

                StringBuffer s;
                DBGLOG(ROXIE_MISMATCH, "File %s cloneFrom(%s) mismatch", _lfn, cloneFrom.getIpText(s).str());
            }
        }
        return NULL;
    }

    virtual IDistributedFile *resolveLFN(const char *logicalName, bool cacheIt, bool writeAccess)
    {
        if (isConnected)
        {
            if (traceLevel > 1)
                DBGLOG("Dali lookup %s", logicalName);
            CDfsLogicalFileName lfn;
            lfn.set(logicalName);
            Owned<IDistributedFile> dfsFile = queryDistributedFileDirectory().lookup(lfn, userdesc.get(), writeAccess, cacheIt);
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
        Owned<IWorkUnit> w = wuFactory->updateWorkUnit(wuid);
        if (!w)
            return NULL;
        w->setAgentSession(myProcessSession());
        if (source)
        {
            StringBuffer wuXML;
            if (getEmbeddedWorkUnitXML(source, wuXML))
            {
                Owned<ILocalWorkUnit> localWU = createLocalWorkUnit();
                localWU->loadXML(wuXML);
                queryExtendedWU(w)->copyWorkUnit(localWU, true);
            }
            else
                throw MakeStringException(ROXIE_DALI_ERROR, "Failed to locate dll workunit info");
        }
        w->commit();
        w.clear();
        return wuFactory->openWorkUnit(wuid, false);
    }

    virtual void noteWorkunitRunning(const char *wuid, bool running)
    {
        CriticalBlock b(daliConnectionCrit);
        if (isConnected)
        {
            assertex(serverStatus);
            if (running)
                serverStatus->queryProperties()->addProp("WorkUnit",wuid);
            else
            {
                VStringBuffer xpath("WorkUnit[.='%s']",wuid);
                serverStatus->queryProperties()->removeProp(xpath.str());
            }
            serverStatus->commitProperties();
        }
    }

    virtual void noteQueuesRunning(const char *queueNames)
    {
        CriticalBlock b(daliConnectionCrit);
        if (isConnected)
        {
            assertex(serverStatus);
            serverStatus->queryProperties()->setProp("@queue", queueNames);
            serverStatus->commitProperties();
        }
    }

    static IRoxieDaliHelper *connectToDali(unsigned waitToConnect)
    {
        CriticalBlock b(daliHelperCrit);
        LINK(daliHelper);
        if (!daliHelper || !daliHelper->isAlive())
            daliHelper = new CRoxieDaliHelper();
        if (waitToConnect && fileNameServiceDali.length() && (!topology || !topology->getPropBool("@lockDali", false)))
        {
            while (waitToConnect && !daliHelper->connected())
            {
                unsigned delay = 1000;
                if (delay > waitToConnect)
                    delay = waitToConnect;
                Sleep(delay);
                waitToConnect -= delay;
            }
        }
        return daliHelper;
    }

    static void releaseCache()
    {
        CriticalBlock b(cacheCrit);
        cache.clear();
    }

    virtual void releaseSubscription(IDaliPackageWatcher *subscription)
    {
        watchers.zap(*subscription);
        subscription->unsubscribe();
    }

    IDaliPackageWatcher *getSubscription(const char *id, const char *xpath, ISDSSubscription *notifier)
    {
        IDaliPackageWatcher *watcher = new CDaliPackageWatcher(id, xpath, notifier);
        watchers.append(*LINK(watcher));
        if (isConnected)
            watcher->subscribe();
        return watcher;
    }

    virtual IDaliPackageWatcher *getQuerySetSubscription(const char *id, ISDSSubscription *notifier)
    {
        StringBuffer xpath;
        return getSubscription(id, getQuerySetPath(xpath, id), notifier);
    }

    virtual IDaliPackageWatcher *getPackageSetsSubscription(ISDSSubscription *notifier)
    {
        StringBuffer xpath;
        return getSubscription("PackageSets", "PackageSets", notifier);
    }

    virtual IDaliPackageWatcher *getPackageMapSubscription(const char *id, ISDSSubscription *notifier)
    {
        StringBuffer xpath;
        return getSubscription(id, getPackageMapPath(xpath, id), notifier);
    }

    virtual bool connected() const
    {
        return isConnected;
    }

    // connect handles the operations generally performed by Dali clients at startup.
    virtual bool connect(unsigned timeout)
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
                    if (!initClientProcess(serverGroup, DCR_RoxyMaster, 0, NULL, NULL, timeout))
                        throw MakeStringException(ROXIE_DALI_ERROR, "Could not initialize dali client");
                    setPasswordsFromSDS();
                    serverStatus = new CSDSServerStatus("RoxieServer");
                    serverStatus->queryProperties()->setProp("@cluster", roxieName.str());
                    serverStatus->commitProperties();
                    initCache();
                    isConnected = true;
                    ForEachItemIn(idx, watchers)
                    {
                        watchers.item(idx).onReconnect();
                    }
                }
                catch(IException *e)
                {
                    ::closedownClientProcess(); // undo any partial initialization
                    StringBuffer text;
                    e->errorMessage(text);
                    DBGLOG(ROXIE_DALI_ERROR, "Error trying to connect to dali %s: %s", fileNameServiceDali.str(), text.str());
                    e->Release();
                }
            }
        }
        return isConnected;
    }

    virtual void disconnect()
    {
        if (isConnected)
        {
            CriticalBlock b(daliConnectionCrit);
            if (isConnected)
            {
                delete serverStatus;
                serverStatus = NULL;
                closeDllServer();
                closeEnvironment();
                clientShutdownWorkUnit();
                ::closedownClientProcess(); // dali client closedown
                isConnected = false;
                disconnectSem.signal();
                disconnectRoxieQueues();
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
    virtual IDllEntry * createEntry(IPropertyTree *owner, IPropertyTree *entry) { throwUnexpected(); }

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
CriticalSection CRoxieDaliHelper::daliHelperCrit;
CriticalSection CRoxieDaliHelper::cacheCrit;
Owned<IPropertyTree> CRoxieDaliHelper::cache;

CriticalSection CRoxieDllServer::crit;

IRoxieDaliHelper *connectToDali(unsigned waitToConnect)
{
    return CRoxieDaliHelper::connectToDali(waitToConnect);
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
