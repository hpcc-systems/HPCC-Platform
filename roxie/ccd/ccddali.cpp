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
#include "ccd.hpp"

#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "workflow.hpp"

/*===============================================================================
* Roxie is not a typical Dali client - it tries to keep the dali connection open 
* only when it needs it, reconnect when necessary, and degrade gracefully should
* Dali not be available.
* This single-instance class is designed to help with that. In particular it
* - connects to dali on demand, and disconnects when all links to this object released
* - caches dali information locally so that if dali cannot be read or is not present,
*   Roxie continues to run.
* Should the caching be handled by dali client code?
=================================================================================*/

const char *roxieStateName = "RoxieLocalState.xml";

class CQuerySetWatcher : public CInterface, implements ISDSSubscription, implements IQuerySetWatcher
{
    SubscriptionId change;
    ISDSSubscription *notifier;
public:
    IMPLEMENT_IINTERFACE;
    CQuerySetWatcher(const char *xpath, ISDSSubscription *_notifier)
    {
        notifier = _notifier;
        change = querySDS().subscribe(xpath, *this, true);

    }
    virtual void unsubscribe()
    {
        querySDS().unsubscribe(change);
    }
    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
        notifier->notify(id, xpath, flags, valueLen, valueData);
    }
};

class CRoxieDaliHelper : public CInterface, implements IRoxieDaliHelper
{
private:
    static bool isConnected;
    static CRoxieDaliHelper *daliHelper;  // Note - this does not own the helper
    static CriticalSection daliConnectionCrit; 

    // connect handles the operations generally performed by Dali clients at startup.
    bool connect()
    {
        if (fileNameServiceDali.length() && !isConnected)
        {
            try
            {
                // Create server group
                Owned<IGroup> serverGroup = createIGroup(fileNameServiceDali, DALI_SERVER_PORT);

                if (!serverGroup)
                    throw MakeStringException(ROXIE_DALI_ERROR, "Could not instantiate dali IGroup");

                // Initialize client process
                if (!initClientProcess(serverGroup, DCR_RoxyMaster, 0, NULL, NULL, 60000))  // wait 1 minute
                    throw MakeStringException(ROXIE_DALI_ERROR, "Could not initialize dali client");    

                setPasswordsFromSDS();
                CSDSServerStatus serverstatus("Roxieserver");
                initDllServer(queryDirectory);
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
        return isConnected;
    }

    static void disconnect()
    {
        if (isConnected)
        {
            closeDllServer();
            closeEnvironment();
            clientShutdownWorkUnit();
            ::closedownClientProcess(); // dali client closedown
            isConnected = false;
        }
    }

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

    static void loadCache()
    {
        if (!cache)
        {
            StringBuffer cacheFileName(queryDirectory);
            cacheFileName.append(roxieStateName);
            if (checkFileExists(cacheFileName))
                cache.setown(createPTreeFromXMLFile(cacheFileName));
            else
            {
                IPropertyTree *tree = createPTree("Roxie");
                tree->addPropTree("QuerySets", createPTree("QuerySets"));
                tree->addPropTree("Files", createPTree("Files"));
                cache.setown(tree);
            }
        }
    }

    static IPropertyTree *readCache(const char *xpath)
    {
        CriticalBlock b(cacheCrit);
        loadCache();
        return cache->getPropTree(xpath);
    }

    static void writeCache(const char *xpath, IPropertyTree *val)
    {
        CriticalBlock b(cacheCrit);
        loadCache();
        if (val)
            cache->setPropTree(xpath, LINK(val));
        else
            cache->removeProp(xpath);
        StringBuffer cacheFileName(queryDirectory);
        cacheFileName.append(roxieStateName);
        saveXML(cacheFileName, cache);
    }

    IPropertyTree *loadDaliTree(const char *xpath)
    {
        Owned <IPropertyTree> localTree;
        if (isConnected)
        {
            Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), 0, 30*1000);
            if (conn)
            {
                Owned <IPropertyTree> daliTree = conn->getRoot();
                localTree.setown(createPTree(daliTree));
            }
            writeCache(xpath, localTree);
        }
        else
            localTree.setown(readCache(xpath));
        return localTree.getClear();
    }

public:

    IMPLEMENT_IINTERFACE;
    CRoxieDaliHelper()
    {
    }

    static const char *getQuerySetPath(StringBuffer &buf, const char *id)
    {
        buf.appendf("QuerySets/QuerySet[@id='%s']", id);
        return buf.str();
    }

    virtual IPropertyTree *getQuerySet(const char *id)
    {
        StringBuffer xpath;
        Owned<IPropertyTree> ret = loadDaliTree(getQuerySetPath(xpath, id));
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
        StringBuffer xpath;
        Owned<IPropertyTree> ret = loadDaliTree(getPackageSetPath(xpath, id));
        if (!ret)
        {
            ret.setown(createPTree("QuerySet"));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    virtual IPropertyTree *getRoxieState(const char *id)
    {
        // The roxie state contains a list of loaded packages, the name of the query set, the name of the active package
        StringBuffer xpath;
        xpath.appendf("RoxieStates/RoxieState[@id='%s']", id);
        Owned<IPropertyTree> ret = loadDaliTree(xpath);
        if (!ret)
        {
            ret.setown(createPTree("RoxieStates"));
            ret->setProp("@id", id);
        }
        return ret.getClear();
    }

    virtual IDistributedFile *resolveLFN(const char *logicalName)
    {
        if (isConnected)
        {
            CDfsLogicalFileName lfn;
            lfn.set(logicalName);
            Owned<IDistributedFile> dfsFile = queryDistributedFileDirectory().lookup(lfn, NULL);
            if (dfsFile)
            {
                IDistributedSuperFile *super = dfsFile->querySuperFile();
                if (super && (0 == super->numSubFiles(true)))
                    dfsFile.clear();
            }
            if (dfsFile)
            {
                Owned<IFileDescriptor> fd = dfsFile->getFileDescriptor();
                Owned<IPropertyTree> pt = fd->getFileTree();
                StringBuffer xpath("Files/");
                StringBuffer lcname;
                xpath.append(lcname.append(logicalName).toLowerCase());
                writeCache(xpath.str(), pt);
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

    virtual IConstWorkUnit *attachWorkunit(const char *wuid, ILoadedDllEntry *source)
    {
        assertex(isConnected);
        Owned<IWorkUnitFactory> wuFactory = getWorkUnitFactory();
        Owned<IWorkUnit> w = wuFactory->updateWorkUnit(wuid);
        if (!w)
            throw MakeStringException(ROXIE_DALI_ERROR, "Failed to open workunit %s", wuid);
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
        IRoxieDaliHelper *helper = LINK(daliHelper);
        if (daliHelper && daliHelper->isAlive())
            return daliHelper;
        else
        {
            // NOTE - if daliHelper is not NULL but isAlive returned false, then we have an overlapping connect with a final disconnect
            // In this case the beforeDispose will have taken care NOT to disconnect, isConnected will remain set, and the connect() will be a no-op.
            daliHelper = new CRoxieDaliHelper();
            daliHelper->connect();
            return daliHelper;
        }
    }

    static void releaseCache()
    {
        CriticalBlock b(cacheCrit);
        cache.clear();
    }

    virtual IQuerySetWatcher *getQuerySetSubscription(const char *id, ISDSSubscription *notifier)
    {
        if (isConnected)
        {
            StringBuffer xpath;
            return new CQuerySetWatcher(getQuerySetPath(xpath, id), notifier);
        }
        else
            return NULL;
    }

    virtual bool connected() const
    {
        return isConnected;
    }
};

class CRoxieDllServer : public CInterface, implements IDllServer
{
    static CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE;

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

