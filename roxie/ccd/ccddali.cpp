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

#include "jlib.hpp"
#include "workunit.hpp"
#include "environment.hpp"
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

#ifndef _CONTAINERIZED
#define ROXIE_DALI_CACHE
#endif

#ifdef ROXIE_DALI_CACHE
const char *roxieStateName = "RoxieLocalState.xml";
#endif

class CDaliPackageWatcher : public CInterface, implements ISafeSDSSubscription, implements ISDSNodeSubscription, implements IDaliPackageWatcher
{
    SubscriptionId change;
    ISafeSDSSubscription *notifier;
    StringAttr id;
    StringAttr xpath;
    mutable CriticalSection crit;
    bool isExact;
public:
    IMPLEMENT_IINTERFACE;
    CDaliPackageWatcher(const char *_id, const char *_xpath, ISafeSDSSubscription *_notifier)
      : change(0), id(_id), xpath(_xpath), isExact(false)
    {
        notifier = _notifier;
    }
    ~CDaliPackageWatcher()
    {
        if (change)
            unsubscribe();
    }
    virtual ISafeSDSSubscription *linkIfAlive() override { return isAliveAndLink() ? this : nullptr; }
    virtual void subscribe(bool exact)
    {
        CriticalBlock b(crit);
        try
        {
            if (traceLevel > 5)
                DBGLOG("Subscribing to %s, %p", xpath.get(), this);
            if (exact && queryDaliServerVersion().compare(SDS_SVER_MIN_NODESUBSCRIBE) >= 0)
            {
                isExact = true;
                change = querySDS().subscribeExact(xpath, *this, true);
            }
            else
            {
                isExact = false;
                change = querySDS().subscribe(xpath, *this, true);
            }
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
            if (traceLevel > 5)
                DBGLOG("unsubscribing from %s, %p", xpath.get(), this);
            if (change)
            {
                if (isExact)
                    querySDS().unsubscribeExact(change);
                else
                    querySDS().unsubscribe(change);
            }
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
        if (traceLevel > 5)
            DBGLOG("resubscribing to %s, %p", xpath.get(), this);
        change = querySDS().subscribe(xpath, *this, true);
        if (notifier)
            notifier->notify(0, NULL, SDSNotify_None);
    }
    virtual void notify(SubscriptionId subid, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        return notify(subid, NULL, flags, valueLen, valueData);
    }
    virtual void notify(SubscriptionId subid, const char *daliXpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        Linked<CDaliPackageWatcher> me = this;  // Ensure that I am not released by the notify call (which would then access freed memory to release the critsec)
        Linked<ISafeSDSSubscription> myNotifier;
        {
            CriticalBlock b(crit);
            if (traceLevel > 5)
                DBGLOG("Notification on %s (%s), %p", xpath.get(), daliXpath ? daliXpath : "", this);
            myNotifier.setown(notifier ? notifier->linkIfAlive() : nullptr);
            // allow crit to be released, allowing this to be unsubscribed, to avoid deadlocking when other threads via notify call unsubscribe
        }
        if (myNotifier)
            myNotifier->notify(subid, daliXpath, flags, valueLen, valueData);
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

class CRoxieDaliHelper : implements IRoxieDaliHelper, public CInterface
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
                    if (traceLevel)
                        DBGLOG("CRoxieDaliConnectWatcher reconnected");
                    try
                    {
                        owner->disconnectSem.wait();
                        Sleep(5000);   // Don't retry immediately, give Dali a chance to recover.
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

#ifdef ROXIE_DALI_CACHE
    // The cache is static since it outlives the dali connections

    static CriticalSection cacheCrit;
    static Owned<IPropertyTree> cache;

    static void initCache()
    {
        if (!oneShotRoxie)
        {
            IPropertyTree *tree = createPTree("Roxie", ipt_lowmem);
            tree->addPropTree("QuerySets");
            tree->addPropTree("PackageSets");
            tree->addPropTree("PackageMaps");
            tree->addPropTree("Files");
            cache.setown(tree);
        }
    }

    static void loadCache()
    {
        if (!cache && !oneShotRoxie)
        {
            StringBuffer cacheFileName(queryDirectory);
            cacheFileName.append(roxieStateName);
            if (checkFileExists(cacheFileName))
                cache.setown(createPTreeFromXMLFile(cacheFileName, ipt_lowmem));
            else
                initCache();
        }
    }

    static IPropertyTree *readCache(const char *xpath)
    {
        if (oneShotRoxie)
            return nullptr;
        CriticalBlock b(cacheCrit);
        loadCache();
        return cache->getPropTree(xpath);
    }

    static void writeCache(const char *foundLoc, const char *newLoc, IPropertyTree *val)
    {
        if (!oneShotRoxie)
        {
            CriticalBlock b(cacheCrit);
            if (!cache)
                initCache();
            cache->removeProp(foundLoc);
            if (val)
                cache->addPropTree(newLoc, LINK(val));
        }
    }
#else
    inline static void initCache() {}
    inline static void loadCache() {}
    inline IPropertyTree *readCache(const char *xpath) { return nullptr; }
    inline static void writeCache(const char *foundLoc, const char *newLoc, IPropertyTree *val) {}
#endif

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
                    localTree.setown(createPTreeFromIPT(daliTree, ipt_lowmem));
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
        if (oneShotRoxie)
            throw makeStringException(-1, "Error - dali not connected");
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
        unsigned np = srcfdesc->numParts();
        getPartMask(dstpartmask,destfilename, np);
        dstfdesc->setPartMask(dstpartmask.str());
        dstfdesc->setNumParts(np);
        dstfdesc->setDefaultDir(srcfdesc->queryProperties().queryProp("@cloneFromDir"));

        for (unsigned pn=0; pn<np; pn++) {
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
            queryNamedGroupStore().setRemoteTimeout(2000);
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

    static StringBuffer &normalizeName(const char *name, StringBuffer &ret)
    {
        // Ensure only chars that are accepted by jptree in an xpath element are used
        for (;;)
        {
            char c = *name++;
            if (!c)
                break;
            switch (c)
            {
            case '.':
                ret.append(".."); // Double . as we use it to escape illegal chars
                break;
            case ':':
            case '_':
            case '-':
                ret.append(c);
                break;
            default:
                if (isalnum(c))
                    ret.append(c); // Note - we COULD make the cache case-insensitive and we would be right to 99.9% if the time. But there is a weird syntax using H to force uppercase filenames...
                else
                    ret.append('.').append((unsigned) (unsigned char) c);
                break;
            }
        }
        return ret;
    }

public:

    IMPLEMENT_IINTERFACE;
    CRoxieDaliHelper() : serverStatus(NULL), connectWatcher(this)
    {
        userdesc.setown(createUserDescriptor());
        const char *roxieUser = NULL;
        const char *roxiePassword = NULL;
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

    virtual IUserDescriptor *queryUserDescriptor()
    {
        return userdesc;
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
            ret.setown(createPTree("QuerySet", ipt_lowmem));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    virtual IPropertyTree *getPackageSets()
    {
        Owned<IPropertyTree> ret = loadDaliTree("PackageSets", NULL);
        if (!ret)
        {
            ret.setown(createPTree("PackageSets", ipt_lowmem));
        }
        return ret.getClear();
    }

    static const char *getSuperFilePath(StringBuffer &buf, const char *lfn)
    {
        CDfsLogicalFileName lfnParser;
        lfnParser.set(lfn);
        if (!lfnParser.isForeign())
        {
            lfnParser.makeFullnameQuery(buf, DXB_SuperFile, true);
            return buf.str();
        }
        else
            return NULL;
    }

    virtual IPropertyTree *getPackageMap(const char *id)
    {
        Owned<IPropertyTree> ret = loadDaliTree("PackageMaps/PackageMap", id);
        if (!ret)
        {
            ret.setown(createPTree("PackageMap", ipt_lowmem));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    IFileDescriptor *checkClonedFromRemote(const char *_lfn, IFileDescriptor *fdesc, bool cacheIt, bool isPrivilegedUser)
    {
        // NOTE - we rely on the fact that  queryNamedGroupStore().lookup caches results,to avoid excessive load on remote dali
        if (_lfn && !strnicmp(_lfn, "foreign", 7)) //if need to support dali hopping should add each remote location
            return NULL;
        if (!fdesc)
            return NULL;
        const char *cloneFrom = fdesc->queryProperties().queryProp("@cloneFrom");
        if (!cloneFrom)
            return NULL;
        StringBuffer foreignLfn("foreign::");
        foreignLfn.append(cloneFrom);
        const char *cloneFromPrefix = fdesc->queryProperties().queryProp("@cloneFromPrefix");
        if (cloneFromPrefix && *cloneFromPrefix)
            foreignLfn.append("::").append(cloneFromPrefix);
        foreignLfn.append("::").append(_lfn);
        if (!connected())
            return resolveCachedLFN(foreignLfn);  // Note - cache only used when no dali connection available
        try
        {
            if (fdesc->queryProperties().hasProp("cloneFromGroup") && fdesc->queryProperties().hasProp("@cloneFromDir"))
            {
                Owned<IFileDescriptor> ret = recreateCloneSource(fdesc, _lfn);
                if (cacheIt)
                    cacheFileDescriptor(foreignLfn, ret);
                return ret.getClear();
            }
            else // Legacy mode - recently cloned files should have the extra info
            {
                if (traceLevel > 1)
                    DBGLOG("checkClonedFromRemote: Resolving %s in legacy mode", _lfn);
                Owned<IDistributedFile> cloneFile = resolveLFN(foreignLfn, cacheIt, false, isPrivilegedUser);
                if (cloneFile)
                {
                    Owned<IFileDescriptor> cloneFDesc = cloneFile->getFileDescriptor();
                    if (cloneFDesc->numParts()==fdesc->numParts())
                        return cloneFDesc.getClear();

                    DBGLOG(ROXIE_MISMATCH, "File %s cloneFrom(%s) mismatch", _lfn, cloneFrom);
                }
            }
        }
        catch (IException *E)
        {
            if (traceLevel > 3)
                EXCLOG(E);
            E->Release();  // Any failure means act as if no remote info
        }
        return NULL;
    }

    virtual IDistributedFile *resolveLFN(const char *logicalName, bool cacheIt, bool writeAccess, bool isPrivilegedUser)
    {
        if (isConnected)
        {
            unsigned start = msTick();
            CDfsLogicalFileName lfn;
            lfn.set(logicalName);
            Owned<IDistributedFile> dfsFile = queryDistributedFileDirectory().lookup(lfn, userdesc.get(), writeAccess, cacheIt,false,nullptr,isPrivilegedUser);
            if (dfsFile)
            {
                IDistributedSuperFile *super = dfsFile->querySuperFile();
                if (super && (0 == super->numSubFiles(true)))
                    dfsFile.clear();
            }
            if (cacheIt)
                cacheDistributedFile(logicalName, dfsFile);
            if (traceLevel > 1)
                DBGLOG("Dali lookup %s returned %s in %u ms", logicalName, dfsFile != NULL ? "match" : "NO match", msTick()-start);
            return dfsFile.getClear();
        }
        else
            return NULL;
    }

    virtual IFileDescriptor *resolveCachedLFN(const char *logicalName)
    {
        StringBuffer xpath("Files/F.");
        normalizeName(logicalName, xpath);
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
#ifdef ROXIE_DALI_CACHE
        if (isConnected && cache && !oneShotRoxie)
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
#endif
    }

    virtual IConstWorkUnit *attachWorkunit(const char *wuid, ILoadedDllEntry *source)
    {
        assertex(isConnected);
        Owned<IWorkUnitFactory> wuFactory = getWorkUnitFactory();
        Owned<IWorkUnit> w = wuFactory->updateWorkUnit(wuid);
        if (!w)
            return NULL;
        w->setAgentSession(myProcessSession());
        if(topology->getPropBool("@resetWorkflow", false))
            w->resetWorkflow();
        if (source)
        {
            StringBuffer wuXML;
            if (getEmbeddedWorkUnitXML(source, wuXML))
            {
                Owned<ILocalWorkUnit> localWU = createLocalWorkUnit(wuXML);
                queryExtendedWU(w)->copyWorkUnit(localWU, true, true);
            }
            else
                throw MakeStringException(ROXIE_DALI_ERROR, "Failed to locate dll workunit info");
        }
        if (topology->hasProp("@workunit"))    // This really only works properly in one-shot mode
            userdesc.set(w->queryUserDescriptor());
        w->commit();
        w.clear();
        return wuFactory->openWorkUnit(wuid);
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

    virtual StringBuffer &getDaliIp(StringBuffer &ret) const
    {
        IGroup &group = queryCoven().queryComm().queryGroup();
        Owned<INodeIterator> coven = group.getIterator();
        bool first = true;
        ForEach(*coven)
        {
            if (first)
                first = false;
            else
                ret.append(',');
            coven->query().endpoint().getUrlStr(ret);
        }
        return ret;
    }

    static IRoxieDaliHelper *connectToDali(unsigned waitToConnect)
    {
        CriticalBlock b(daliHelperCrit);
        if (!daliHelper || !daliHelper->isAliveAndLink())
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
        initializeStorageGroups(daliHelper->connected());
        return daliHelper;
    }

    static void releaseCache()
    {
#ifdef ROXIE_DALI_CACHE
        CriticalBlock b(cacheCrit);
        cache.clear();
#endif
    }

    virtual void releaseSubscription(IDaliPackageWatcher *subscription)
    {
        watchers.zap(*subscription);
        subscription->unsubscribe();
    }

    IDaliPackageWatcher *getSubscription(const char *id, const char *xpath, ISafeSDSSubscription *notifier, bool exact)
    {
        IDaliPackageWatcher *watcher = new CDaliPackageWatcher(id, xpath, notifier);
        watchers.append(*LINK(watcher));
        if (isConnected)
            watcher->subscribe(exact);
        return watcher;
    }

    virtual IDaliPackageWatcher *getQuerySetSubscription(const char *id, ISafeSDSSubscription *notifier)
    {
        StringBuffer xpath;
        return getSubscription(id, getQuerySetPath(xpath, id), notifier, false);
    }

    virtual IDaliPackageWatcher *getPackageSetsSubscription(ISafeSDSSubscription *notifier)
    {
        StringBuffer xpath;
        return getSubscription("PackageSets", "PackageSets", notifier, false);
    }

    virtual IDaliPackageWatcher *getPackageMapsSubscription(ISafeSDSSubscription *notifier)
    {
        StringBuffer xpath;
        return getSubscription("PackageMaps", "PackageMaps", notifier, false);
    }

    virtual IDaliPackageWatcher *getSuperFileSubscription(const char *lfn, ISafeSDSSubscription *notifier)
    {
        StringBuffer xpathBuf;
        const char *xpath = getSuperFilePath(xpathBuf, lfn);
        if (xpath)
            return getSubscription(lfn, xpath, notifier, true);
        else
            return NULL;
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
                    if (!initClientProcess(serverGroup, DCR_Roxie, 0, NULL, NULL, timeout))
                        throw MakeStringException(ROXIE_DALI_ERROR, "Could not initialize dali client");
                    serverStatus = new CSDSServerStatus("RoxieServer");
                    serverStatus->queryProperties()->setProp("@cluster", roxieName.str());
                    serverStatus->commitProperties();
                    isConnected = true; // Make sure this is set before the onReconnect calls, so that they refresh with info from Dali rather than from cache
                    ForEachItemIn(idx, watchers)
                    {
                        watchers.item(idx).onReconnect();
                    }
                }
                catch(IException *e)
                {
                    delete serverStatus;
                    serverStatus = NULL;
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
                isConnected = false;
                delete serverStatus;
                serverStatus = NULL;
                closeDllServer();
                closeEnvironment();
                clientShutdownWorkUnit();
                disconnectRoxieQueues();
                ::closedownClientProcess(); // dali client closedown
                disconnectSem.signal();
            }
        }
    }
protected:
    void cacheDistributedFile(const char *logicalName, IDistributedFile *dfsFile)
    {
        assertex(isConnected);
        Owned<IFileDescriptor> fd;
        if (dfsFile)
            fd.setown(dfsFile->getFileDescriptor());
        cacheFileDescriptor(logicalName, fd);
    }

    void cacheFileDescriptor(const char *logicalName, IFileDescriptor *fd)
    {
        assertex(isConnected);
        Owned<IPropertyTree> pt;
        if (fd)
            pt.setown(fd->getFileTree());
        StringBuffer xpath("Files/F.");
        normalizeName(logicalName, xpath);
        writeCache(xpath.str(), xpath.str(), pt);
    }
};

class CRoxieDllServer : implements IDllServer, public CInterface
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
            {
                try
                {
                    return createDllEntry(localName.str(), false, NULL, false);
                }
                catch (ICorruptDllException *E)
                {
                    remove(localName.str());
                    E->Release();
                }
                catch (...)
                {
                    throw;
                }
            }
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
    virtual ILoadedDllEntry * loadDllResources(const char * name, DllLocationType location) 
    {
        throwUnexpected();
    }
} roxieDllServer;

bool CRoxieDaliHelper::isConnected = false;
CRoxieDaliHelper * CRoxieDaliHelper::daliHelper;
CriticalSection CRoxieDaliHelper::daliHelperCrit;
#ifdef ROXIE_DALI_CACHE
CriticalSection CRoxieDaliHelper::cacheCrit;
Owned<IPropertyTree> CRoxieDaliHelper::cache;
#endif

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
    IERRLOG("%u - %s", code, message.str());
    WorkunitUpdate w(&workUnit->lock());
    addExceptionToWorkunit(w, SeverityError, "Roxie", code, message.str(), NULL, 0, 0, 0);
}
