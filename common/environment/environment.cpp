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
#include "environment.hpp"
#include "jptree.hpp"
#include "jexcept.hpp"
#include "jiter.ipp"
#include "jmisc.hpp"
#include "jencrypt.hpp"
#include "jutil.hpp"

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"

#include <string>
#include <unordered_map>
#include <tuple>

#define SDS_LOCK_TIMEOUT  30000
#define DEFAULT_DROPZONE_INDEX      1
#define DROPZONE_BY_MACHINE_SUFFIX  "-dropzoneByMachine-"
#define DROPZONE_SUFFIX             "dropzone-"
#define MACHINE_PREFIX              "machine-"
#define SPARKTHOR_SUFFIX            "sparkthor-"

static int environmentTraceLevel = 1;
static Owned <IConstEnvironment> cache;

class CLocalEnvironment;

class CConstMachineInfoIterator : public  CSimpleInterfaceOf<IConstMachineInfoIterator>
{
public:
    CConstMachineInfoIterator();

    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IConstMachineInfo & query() override;
    virtual unsigned count() const override;

protected:
    Owned<IConstMachineInfo> curr;
    Owned<CLocalEnvironment> constEnv;
    unsigned index = 1;
    unsigned maxIndex = 0;
};

class CConstDropZoneServerInfoIterator : public CSimpleInterfaceOf<IConstDropZoneServerInfoIterator>
{
public:
    CConstDropZoneServerInfoIterator(const IConstDropZoneInfo * dropZone);

    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IConstDropZoneServerInfo & query() override;
    virtual unsigned count() const override;

protected:
    Owned<IConstDropZoneServerInfo> curr;
    Owned<CLocalEnvironment> constEnv;
    Owned<IPropertyTreeIterator> serverListIt;
    unsigned maxIndex = 0;
};

class CConstDropZoneInfoIterator : public CSimpleInterfaceOf<IConstDropZoneInfoIterator>
{
public:
    CConstDropZoneInfoIterator();

    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IConstDropZoneInfo & query() override;
    virtual unsigned count() const override;

protected:
    Owned<IConstDropZoneInfo> curr;
    Owned<CLocalEnvironment> constEnv;
    unsigned index = 1;
    unsigned maxIndex = 0;
};

class CConstSparkThorInfoIterator : public CSimpleInterfaceOf<IConstSparkThorInfoIterator>
{
public:
    CConstSparkThorInfoIterator();

    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IConstSparkThorInfo & query() override;
    virtual unsigned count() const override;

protected:
    Owned<IConstSparkThorInfo> curr;
    Owned<CLocalEnvironment> constEnv;
    unsigned index = 1;
    unsigned maxIndex = 0;
};

class CConstInstanceInfoIterator : public CSimpleInterfaceOf<IConstInstanceInfoIterator>
{
public:
    CConstInstanceInfoIterator(const CLocalEnvironment * env, IPropertyTreeIterator * itr);

    virtual bool first() override;
    virtual bool next() override;
    virtual bool isValid() override;
    virtual IConstInstanceInfo & query() override;
    virtual unsigned count() const override;

protected:
    Owned<IPropertyTreeIterator> instanceItr;
    Owned<IConstInstanceInfo> curr;
    const CLocalEnvironment* constEnv;
    unsigned index = 1;
    unsigned maxIndex = 0;
};


//==========================================================================================

class CConstInstanceInfo;

class CLocalEnvironment : implements IConstEnvironment, public CInterface
{
private:
    // NOTE - order is important - we need to construct before p and (especially) destruct after p
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTree> p;
    mutable MapStringToMyClass<IConstEnvBase> cache;
    mutable Mutex safeCache;
    mutable bool dropZoneCacheBuilt;
    mutable bool machineCacheBuilt;
    mutable bool sparkThorCacheBuilt;
    mutable bool clusterKeyNameCache;
    StringBuffer fileAccessUrl;

    struct KeyPairMapEntity
    {
        std::string publicKey, privateKey;
    };
    mutable std::unordered_map<std::string, KeyPairMapEntity> keyPairMap;
    mutable std::unordered_map<std::string, std::string> keyClusterMap;
    StringBuffer xPath;
    mutable unsigned numOfMachines;
    mutable unsigned numOfDropZones;
    mutable unsigned numOfSparkThors;


    IConstEnvBase * getCache(const char *path) const;
    void setCache(const char *path, IConstEnvBase *value) const;
    void buildMachineCache() const;
    void buildDropZoneCache() const;
    void buildSparkThorCache() const;
    void init();
    mutable bool isDropZoneRestrictionLoaded = false;
    mutable bool dropZoneRestrictionEnabled = true;


    void ensureClusterKeyMap() const // keyPairMap and keyClusterMap it alters is mutable
    {
        if (!clusterKeyNameCache)
        {
            StringBuffer keysDir;
            envGetConfigurationDirectory("keys",nullptr, nullptr, keysDir);

            Owned<IPropertyTreeIterator> keyPairIt = p->getElements("EnvSettings/Keys/KeyPair");
            ForEach(*keyPairIt)
            {
                IPropertyTree &keyPair = keyPairIt->query();
                const char *name = keyPair.queryProp("@name");
                const char *publicKeyPath = keyPair.queryProp("@publicKey");
                const char *privateKeyPath = keyPair.queryProp("@privateKey");
                if (isEmptyString(name))
                {
                    WARNLOG("skipping invalid EnvSettings/Key/KeyPair entry, name not defined");
                    continue;
                }
                if (isEmptyString(publicKeyPath) || isEmptyString(privateKeyPath))
                {
                    WARNLOG("skipping invalid EnvSettings/Key/KeyPair entry, name=%s", name);
                    continue;
                }
                StringBuffer absPublicKeyPath, absPrivateKeyPath;
                if (!isAbsolutePath(publicKeyPath))
                {
                    absPublicKeyPath.append(keysDir);
                    addPathSepChar(absPublicKeyPath);
                    absPublicKeyPath.append(publicKeyPath);
                }
                else
                    absPublicKeyPath.append(publicKeyPath);
                if (!isAbsolutePath(privateKeyPath))
                {
                    absPrivateKeyPath.append(keysDir);
                    addPathSepChar(absPrivateKeyPath);
                    absPrivateKeyPath.append(privateKeyPath);
                }
                else
                    absPrivateKeyPath.append(privateKeyPath);

                keyPairMap[name] = { absPublicKeyPath.str(), absPrivateKeyPath.str() };
            }

/* From 7.0.0 until 7.0.6, the <Keys> section of the environment required
 * the mappings to be defined as "Cluster" instead of "ClusterGroup" - See: HPCC-21192
 */
#define BKWRDCOMPAT_CLUSTER_VS_CLUSTERGROUP

            const char *groupKeysPath = "EnvSettings/Keys/ClusterGroup";

#ifdef BKWRDCOMPAT_CLUSTER_VS_CLUSTERGROUP
            for (unsigned i=0; i<2; i++) // once for std. "ClusterGroup", 2nd time for legacy "Cluster"
            {
#endif
                Owned<IPropertyTreeIterator> clusterIter = p->getElements(groupKeysPath);

#ifdef BKWRDCOMPAT_CLUSTER_VS_CLUSTERGROUP
                if (clusterIter->first() && keyClusterMap.size()) // NB: always 0 1st time around.
                {
                    WARNLOG("Invalid configuration: mixed 'Keys/ClusterGroup' definitions and legacy 'Keys/Cluster' definitions found, legacy 'Keys/Cluster' definition will be ignored.");
                    break;
                }
#endif
                ForEach(*clusterIter)
                {
                    IPropertyTree &cluster = clusterIter->query();
                    const char *clusterName = cluster.queryProp("@name");
                    if (isEmptyString(clusterName))
                    {
                        WARNLOG("skipping %s entry with no name", groupKeysPath);
                        continue;
                    }
                    if (cluster.hasProp("@keyPairName"))
                    {
                        const char *keyPairName = cluster.queryProp("@keyPairName");
                        if (isEmptyString(keyPairName))
                        {
                            WARNLOG("skipping invalid %s entry, name=%s", groupKeysPath, clusterName);
                            continue;
                        }
                        keyClusterMap[clusterName] = keyPairName;
                    }
                }
#ifdef BKWRDCOMPAT_CLUSTER_VS_CLUSTERGROUP
                groupKeysPath = "EnvSettings/Keys/Cluster";
            }
#endif
            clusterKeyNameCache = true;
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    CLocalEnvironment(IRemoteConnection *_conn, IPropertyTree *x=nullptr, const char* path="Environment");
    CLocalEnvironment(const char* path="config.xml");
    virtual ~CLocalEnvironment();

    virtual IStringVal & getName(IStringVal & str) const;
    virtual IStringVal & getXML(IStringVal & str) const;
    virtual IPropertyTree & getPTree() const;
    virtual IEnvironment& lock() const;
    virtual IConstDomainInfo * getDomain(const char * name) const;
    virtual IConstMachineInfo * getMachine(const char * name) const;
    virtual IConstMachineInfo * getMachineByAddress(const char * machineIp) const;
    virtual IConstMachineInfo * getMachineForLocalHost() const;
    virtual IConstDropZoneInfo * getDropZone(const char * name) const;
    virtual IConstInstanceInfo * getInstance(const char * type, const char * version, const char *domain) const;
    virtual CConstInstanceInfo * getInstanceByIP(const char *type, const char *version, IpAddress &ip) const;
    virtual IConstComputerTypeInfo * getComputerType(const char * name) const;
    virtual bool getRunInfo(IStringVal & path, IStringVal & dir, const char *type, const char *version, const char *machineaddr, const char *defprogname) const;
    virtual void preload();
    virtual IRemoteConnection* getConnection() const { return conn.getLink(); }

    void setXML(const char * logicalName);
    const char* getPath() const { return xPath.str(); }

    void unlockRemote();
    virtual bool isConstEnvironment() const { return true; }
    virtual void clearCache();

    virtual IConstMachineInfoIterator * getMachineIterator() const;

    virtual IConstDropZoneInfoIterator * getDropZoneIteratorByAddress(const char * address) const;
    virtual IConstDropZoneInfo * getDropZoneByAddressPath(const char * netaddress, const char *targetPath) const;

    virtual IConstDropZoneInfoIterator * getDropZoneIterator() const;

    unsigned getNumberOfMachines() const { buildMachineCache(); return numOfMachines; }
    IConstMachineInfo * getMachineByIndex(unsigned index) const;

    unsigned getNumberOfDropZones() const { buildDropZoneCache(); return numOfDropZones; }
    IConstDropZoneInfo * getDropZoneByIndex(unsigned index) const;
    bool isDropZoneRestrictionEnabled() const;

    virtual const char *getClusterKeyPairName(const char *cluster) const override
    {
        synchronized procedure(safeCache);
        ensureClusterKeyMap();
        return keyClusterMap[cluster].c_str();
    }
    virtual const char *getPublicKeyPath(const char *keyPairName) const override
    {
        synchronized procedure(safeCache);
        ensureClusterKeyMap();
        return keyPairMap[keyPairName].publicKey.c_str();
    }
    virtual const char *getPrivateKeyPath(const char *keyPairName) const override
    {
        synchronized procedure(safeCache);
        ensureClusterKeyMap();
        return keyPairMap[keyPairName].privateKey.c_str();
    }
    virtual const char *getFileAccessUrl() const
    {
        synchronized procedure(safeCache);
        return fileAccessUrl.length() ? fileAccessUrl.str() : nullptr;
    }
    virtual IConstDaFileSrvInfo *getDaFileSrvGroupInfo(const char *name) const override;

    virtual IConstSparkThorInfo *getSparkThor(const char *name) const;
    virtual IConstSparkThorInfoIterator *getSparkThorIterator() const;
    unsigned getNumberOfSparkThors() const { buildSparkThorCache(); return numOfSparkThors; }
    IConstSparkThorInfo *getSparkThorByIndex(unsigned index) const;
};

class CLockedEnvironment : implements IEnvironment, public CInterface
{
public:
    //note that order of construction/destruction is important
    Owned<CLocalEnvironment> c;
    Owned<CLocalEnvironment> env;
    Owned<CLocalEnvironment> constEnv;

    IMPLEMENT_IINTERFACE;
    CLockedEnvironment(CLocalEnvironment *_c)
    {
        Owned<IRemoteConnection> connection = _c->getConnection();

        if (connection)
        {
            constEnv.set(_c); //save original constant environment

            //we only wish to allow one party to allow updating the environment.
            //
            //create a new /NewEnvironment subtree, locked for read/write access for self and entire subtree; delete on disconnect
            //
            StringBuffer newName("/New");
            newName.append(constEnv->getPath());

            const unsigned int mode = RTM_CREATE | RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE |
                                   RTM_LOCK_SUB | RTM_DELETE_ON_DISCONNECT;
            Owned<IRemoteConnection> conn = querySDS().connect(newName.str(), myProcessSession(), mode, SDS_LOCK_TIMEOUT);

            if (conn == nullptr)
            {
                if (environmentTraceLevel > 0)
                    PrintLog("Failed to create locked environment %s", newName.str());

                throw MakeStringException(-1, "Failed to get a lock on environment /%s", newName.str());
            }

            //save the locked environment
            env.setown(new CLocalEnvironment(conn, nullptr, newName.str()));

            //get a lock on the const environment
            const unsigned int mode2 = RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_LOCK_SUB;
            Owned<IRemoteConnection> conn2 = querySDS().connect(constEnv->getPath(), myProcessSession(), mode2, SDS_LOCK_TIMEOUT);

            if (conn2 == nullptr)
            {
                if (environmentTraceLevel > 0)
                    PrintLog("Failed to lock environment %s", constEnv->getPath());

                throw MakeStringException(-1, "Failed to get a lock on environment /%s", constEnv->getPath());
            }

            //copy const environment to our member environment
            Owned<IPropertyTree> pSrc = conn2->getRoot();
            c.setown( new CLocalEnvironment(nullptr, createPTreeFromIPT(pSrc)));
            conn2->rollback();
        }
        else
        {
            c.set(_c);
        }

    }
    virtual ~CLockedEnvironment()
    {
    }

    virtual IStringVal & getName(IStringVal & str) const
            { return c->getName(str); }
    virtual IStringVal & getXML(IStringVal & str) const
            { return c->getXML(str); }
    virtual IPropertyTree & getPTree() const
            { return c->getPTree(); }
    virtual IConstDomainInfo * getDomain(const char * name) const
            { return c->getDomain(name); }
    virtual IConstMachineInfo * getMachine(const char * name) const
            { return c->getMachine(name); }
    virtual IConstMachineInfo * getMachineByAddress(const char * machineIp) const
            { return c->getMachineByAddress(machineIp); }
    virtual IConstMachineInfo * getMachineForLocalHost() const
            { return c->getMachineForLocalHost(); }
    virtual IConstDropZoneInfo * getDropZone(const char * name) const
            { return c->getDropZone(name); }
    virtual IConstInstanceInfo * getInstance(const char *type, const char *version, const char *domain) const
            { return c->getInstance(type, version, domain); }
    virtual bool getRunInfo(IStringVal & path, IStringVal & dir, const char *type, const char *version, const char *machineaddr,const char *defprogname) const
            { return c->getRunInfo(path, dir, type, version, machineaddr, defprogname); }
    virtual IConstComputerTypeInfo * getComputerType(const char * name) const
            { return c->getComputerType(name); }

    virtual IEnvironment & lock() const
            { ((CInterface*)this)->Link(); return *(IEnvironment*)this; }
    virtual void commit();
    virtual void rollback();
    virtual void setXML(const char * pstr)
            { c->setXML(pstr); }
    virtual void preload()
            { c->preload(); }
    virtual bool isConstEnvironment() const { return false; }
    virtual void clearCache() { c->clearCache(); }

    virtual IConstMachineInfoIterator * getMachineIterator() const
            { return c->getMachineIterator(); }
    virtual IConstDropZoneInfoIterator * getDropZoneIteratorByAddress(const char * address) const
            { return c->getDropZoneIteratorByAddress(address); }
    virtual IConstDropZoneInfo * getDropZoneByAddressPath(const char * netaddress, const char *targetPath) const
            { return c->getDropZoneByAddressPath(netaddress, targetPath); }
    virtual IConstDropZoneInfoIterator * getDropZoneIterator() const
            { return c->getDropZoneIterator(); }
    virtual bool isDropZoneRestrictionEnabled() const
            { return c->isDropZoneRestrictionEnabled(); }
    virtual const char *getClusterKeyPairName(const char *cluster) const override
            { return c->getClusterKeyPairName(cluster); }
    virtual const char *getPublicKeyPath(const char *keyPairName) const override
            { return c->getPublicKeyPath(keyPairName); }
    virtual const char *getPrivateKeyPath(const char *keyPairName) const override
            { return c->getPrivateKeyPath(keyPairName); }
    virtual const char *getFileAccessUrl() const
            { return c->getFileAccessUrl(); }
    virtual IConstDaFileSrvInfo *getDaFileSrvGroupInfo(const char *name) const override
            { return c->getDaFileSrvGroupInfo(name); }
    virtual IConstSparkThorInfo *getSparkThor(const char *name) const
            { return c->getSparkThor(name); }
    virtual IConstSparkThorInfoIterator *getSparkThorIterator() const
            { return c->getSparkThorIterator(); }
};

void CLockedEnvironment::commit()
{
    if (constEnv)
    {
        //get a lock on const environment momentarily
        const unsigned int mode2 = RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_LOCK_SUB;
        Owned<IRemoteConnection> conn2 = querySDS().connect(constEnv->getPath(), myProcessSession(), mode2, SDS_LOCK_TIMEOUT);

        if (conn2 == nullptr)
        {
            if (environmentTraceLevel > 0)
                PrintLog("Failed to lock environment %s", constEnv->getPath());

            throw MakeStringException(-1, "Failed to get a lock on environment /%s", constEnv->getPath());
        }

        //copy locked environment to const environment
        Owned<IPropertyTree> pSrc = &getPTree();
        Owned<IPropertyTree> pDst = conn2->queryRoot()->getBranch(nullptr);

        // JCS - I think it could (and would be more efficient if it had kept the original read lock connection to Env
        //     - instead of using NewEnv as lock point, still work on copy, then changeMode of original connect
        //     - as opposed to current scheme, where it recoonects in write mode and has to lazy fetch original env to update.

        // ensures pDst is equal to pSrc, whilst minimizing changes to pDst

        try { synchronizePTree(pDst, pSrc); }
        catch (IException *) { conn2->rollback(); throw; }
        conn2->commit();
    }
    else
    {
        Owned<IRemoteConnection> conn = c->getConnection();
        conn->commit();
    }
}

void CLockedEnvironment::rollback()
{
    if (constEnv)
    {
        //get a lock on const environment momentarily
        const unsigned int mode2 = RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_LOCK_SUB;
        Owned<IRemoteConnection> conn2 = querySDS().connect(constEnv->getPath(), myProcessSession(), mode2, SDS_LOCK_TIMEOUT);

        if (conn2 == nullptr)
        {
            if (environmentTraceLevel > 0)
                PrintLog("Failed to lock environment %s", constEnv->getPath());

            throw MakeStringException(-1, "Failed to get a lock on environment /%s", constEnv->getPath());
        }

        //copy const environment to locked environment (as it stands now) again losing any changes we made
        Owned<IPropertyTree> pSrc = conn2->getRoot();
        Owned<IPropertyTree> pDst = &getPTree();

        pDst->removeTree( pDst->queryPropTree("Hardware") );
        pDst->removeTree( pDst->queryPropTree("Software") );
        pDst->removeTree( pDst->queryPropTree("Programs") );
        pDst->removeTree( pDst->queryPropTree("Data") );

        mergePTree(pDst, pSrc);
        conn2->rollback();
    }
    else
    {
        Owned<IRemoteConnection> conn = c->getConnection();
        conn->rollback();
    }
}

//==========================================================================================
// the following class implements notification handler for subscription to dali for environment
// updates by other clients and is used by environment factory below.  This also serves as
// a sample self-contained implementation that can be easily tailored for other purposes.
//==========================================================================================
class CSdsSubscription : implements ISDSSubscription, public CInterface
{
public:
    CSdsSubscription(IEnvironmentFactory &_factory) : factory(_factory)
    {
        m_constEnvUpdated = false;
        sub_id = factory.subscribe(this);
    }
    virtual ~CSdsSubscription()
    {
        /* note that ideally, we would make this class automatically
           unsubscribe in this destructor.  However, underlying dali client
            layer (CDaliSubscriptionManagerStub) links to this object and so
            object would not get destroyed just by an application releasing it.
           The application either needs to explicitly unsubscribe or close
            the environment which unsubscribes during close down. */
    }

    void unsubscribe()
    {
        synchronized block(m_mutexEnv);
        if (sub_id)
        {
            factory.unsubscribe(sub_id);
            sub_id = 0;
        }
    }
    IMPLEMENT_IINTERFACE;

    //another client (like configenv) may have updated the environment and we got notified
    //(thanks to our subscription) but don't just reload it yet since this notification is sent on
    //another thread asynchronously and we may be actively working with the old environment.  Just
    //invoke handleEnvironmentChange() when we are ready to invalidate cache in environment factory.
    //
    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=nullptr)
    {
        DBGLOG("Environment was updated by another client of Dali server.  Invalidating cache.\n");
        synchronized block(m_mutexEnv);
        m_constEnvUpdated = true;
    }

    void handleEnvironmentChange()
    {
        synchronized block(m_mutexEnv);
        if (m_constEnvUpdated)
        {
            Owned<IConstEnvironment> constEnv = factory.openEnvironment();
            constEnv->clearCache();
            m_constEnvUpdated = false;
        }
    }

private:
    SubscriptionId sub_id;
    Mutex  m_mutexEnv;
    bool   m_constEnvUpdated;
    IEnvironmentFactory &factory;
};

//==========================================================================================

class CEnvironmentFactory : public CInterface,
                            implements IEnvironmentFactory, implements IDaliClientShutdown
{
public:
    IMPLEMENT_IINTERFACE;
    typedef ArrayOf<SubscriptionId> SubscriptionIDs;
    SubscriptionIDs subIDs;
    Mutex mutex;
    Owned<CSdsSubscription> subscription;

    CEnvironmentFactory()
    {
    }

    virtual void clientShutdown();

    virtual ~CEnvironmentFactory()
    {
        close(); //just in case it was not explicitly closed
    }

    virtual IConstEnvironment* openEnvironment()
    {
        synchronized procedure(mutex);

        if (!cache)
        {
            Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), 0, SDS_LOCK_TIMEOUT);
            if (conn)
                cache.setown(new CLocalEnvironment(conn));
        }
        if (!cache)
            throw MakeStringException(0, "Failed to get environment information");
        return cache.getLink();
    }

    virtual IEnvironment* updateEnvironment()
    {
        Owned<IConstEnvironment> pConstEnv = openEnvironment();

        synchronized procedure(mutex);
        return &pConstEnv->lock();
    }

    virtual IEnvironment * loadLocalEnvironmentFile(const char * filename)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLFile(filename, ipt_lowmem);
        Owned<CLocalEnvironment> pLocalEnv = new CLocalEnvironment(nullptr, ptree);
        return new CLockedEnvironment(pLocalEnv);
    }

    virtual IEnvironment * loadLocalEnvironment(const char * xml)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(xml, ipt_lowmem);
        Owned<CLocalEnvironment> pLocalEnv = new CLocalEnvironment(nullptr, ptree);
        return new CLockedEnvironment(pLocalEnv);
    }


    void close()
    {
        SubscriptionIDs copySubIDs;
        {
            synchronized procedure(mutex);
            cache.clear();

            //save the active subscriptions in another array
            //so they can be unsubscribed without causing deadlock
            // since ~CSdsSubscription() would ask us to unsubscribe the
            //same requiring a mutex lock (copy is a little price for this
            //normally small/empty array).
            //
            ForEachItemIn(i, subIDs)
                copySubIDs.append(subIDs.item(i));
            subIDs.kill();
        }

        //now unsubscribe all outstanding subscriptions
        //
        subscription.clear();
        ForEachItemIn(i, copySubIDs)
            querySDS().unsubscribe( copySubIDs.item(i) );
    }

    virtual SubscriptionId subscribe(ISDSSubscription* pSubHandler)
    {
        SubscriptionId sub_id = querySDS().subscribe("/Environment", *pSubHandler);

        synchronized procedure(mutex);
        subIDs.append(sub_id);
        return sub_id;
    }

    virtual void unsubscribe(SubscriptionId sub_id)
    {
        synchronized procedure(mutex);

        aindex_t i = subIDs.find(sub_id);
        if (i != NotFound)
        {
            querySDS().unsubscribe(sub_id);
            subIDs.remove(i);
        }
    }

    virtual void validateCache()
    {
        if (!subscription)
            subscription.setown( new CSdsSubscription(*this) );

        subscription->handleEnvironmentChange();
    }

private:
    IRemoteConnection* connect(const char *xpath, unsigned flags)
    {
        return querySDS().connect(xpath, myProcessSession(), flags, SDS_LOCK_TIMEOUT);
    }
};

static CEnvironmentFactory *factory=nullptr;

void CEnvironmentFactory::clientShutdown()
{
    closeEnvironment();
}

MODULE_INIT(INIT_PRIORITY_ENV_ENVIRONMENT)
{
    return true;
}

MODULE_EXIT()
{
    ::Release(factory);
}
//==========================================================================================

class CConstEnvBase : public CInterface
{
protected:
    const CLocalEnvironment* env;           // Not linked - would be circular....
                                    // That could cause problems
    Linked<IPropertyTree> root;
public:
    CConstEnvBase(const CLocalEnvironment* _env, IPropertyTree *_root)
        : env(_env), root(_root)
    {
    }

    IStringVal&     getXML(IStringVal &str) const
    {
        StringBuffer x;
        toXML(root->queryBranch("."), x);
        str.set(x.str());
        return str;
    };
    IStringVal&     getName(IStringVal &str) const
    {
        str.set(root->queryProp("@name"));
        return str;
    }
    IPropertyTree&  getPTree() const
    {
        return *LINK(root);
    }

};

#define IMPLEMENT_ICONSTENVBASE \
    virtual IStringVal&     getXML(IStringVal &str) const { return CConstEnvBase::getXML(str); } \
    virtual IStringVal&     getName(IStringVal &str) const { return CConstEnvBase::getName(str); } \
    virtual IPropertyTree&  getPTree() const { return CConstEnvBase::getPTree(); }

//==========================================================================================

class CConstDomainInfo : public CConstEnvBase, implements IConstDomainInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstDomainInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root) {}

    virtual void getAccountInfo(IStringVal &name, IStringVal &pw) const
    {
        if (root->hasProp("@username"))
            name.set(root->queryProp("@username"));
        else
            name.clear();
        if (root->hasProp("@password"))
        {
            StringBuffer pwd;
            decrypt(pwd, root->queryProp("@password"));
            pw.set(pwd.str());
        }
        else
            pw.clear();
    }

    virtual void getSnmpSecurityString(IStringVal & securityString) const
    {
        if (root->hasProp("@snmpSecurityString"))
        {
            StringBuffer sec_string;
            decrypt(sec_string, root->queryProp("@snmpSecurityString"));
            securityString.set(sec_string.str());
        }
        else
            securityString.set("");
    }

    virtual void getSSHAccountInfo(IStringVal &name, IStringVal &sshKeyFile, IStringVal& sshKeyPassphrase) const
    {
        if (root->hasProp("@username"))
            name.set(root->queryProp("@username"));
        else
            name.clear();
        if (root->hasProp("@sshKeyFile"))
            sshKeyFile.set(root->queryProp("@sshKeyFile"));
        else
            sshKeyFile.clear();
        if (root->hasProp("@sshKeyPassphrase"))
            sshKeyPassphrase.set(root->queryProp("@sshKeyPassphrase"));
        else
            sshKeyPassphrase.clear();
    }
};


//==========================================================================================

struct mapOsEnums { EnvMachineOS val; const char *str; };

static EnvMachineOS getEnum(IPropertyTree *p, const char *propname, mapOsEnums *map)
{
    const char *v = p->queryProp(propname);
    if (v && *v)
    {
        while (map->str)
        {
            if (stricmp(v, map->str)==0)
                return map->val;
            map++;
        }
        throw MakeStringException(0, "Unknown operating system: \"%s\"", v);
    }
    return MachineOsUnknown;
}


struct mapStateEnums { EnvMachineState val; const char *str; };

static EnvMachineState getEnum(IPropertyTree *p, const char *propname, mapStateEnums *map)
{
    const char *v = p->queryProp(propname);
    if (v && *v)
    {
        while (map->str)
        {
            if (stricmp(v, map->str)==0)
                return map->val;
            map++;
        }
        assertex(!"Unexpected value in getEnum");
    }
    return MachineStateUnknown;
}


mapOsEnums OperatingSystems[] = {
    { MachineOsW2K, "W2K" },
    { MachineOsSolaris, "solaris" },
    { MachineOsLinux, "linux" },
    { MachineOsSize, nullptr }
};

mapStateEnums MachineStates[] = {
    { MachineStateAvailable, "Available" },
    { MachineStateUnavailable, "Unavailable" },
    { MachineStateUnknown, "Unknown" }
};

//==========================================================================================

class CConstMachineInfo : public CConstEnvBase, implements IConstMachineInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstMachineInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root) {}

    virtual IConstDomainInfo*   getDomain() const
    {
        return env->getDomain(root->queryProp("@domain"));
    }
    virtual IStringVal&     getNetAddress(IStringVal &str) const
    {
        str.set(root->queryProp("@netAddress"));
        return str;
    }
    virtual IStringVal&     getDescription(IStringVal &str) const
    {
        UNIMPLEMENTED;
    }
    virtual unsigned getNicSpeedMbitSec() const
    {
        const char * v = root->queryProp("@nicSpeed");
        if (v && *v)
            return atoi(v);

        Owned<IConstComputerTypeInfo> type = env->getComputerType(root->queryProp("@computerType"));
        if (type)
            return type->getNicSpeedMbitSec();
        return 0;
    }
    virtual EnvMachineOS getOS() const
    {
        EnvMachineOS os = getEnum(root, "@opSys", OperatingSystems);
        if (os != MachineOsUnknown)
            return os;

        Owned<IConstComputerTypeInfo> type = env->getComputerType(root->queryProp("@computerType"));
        if (type)
            return type->getOS();
        return MachineOsUnknown;
    }
    virtual EnvMachineState getState() const
    {
        return getEnum(root, "@state", MachineStates);
    }

};

//==========================================================================================

class CConstComputerTypeInfo : public CConstEnvBase, implements IConstComputerTypeInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstComputerTypeInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root) {}

    virtual EnvMachineOS getOS() const
    {
        EnvMachineOS os = getEnum(root, "@opSys", OperatingSystems);
        if (os != MachineOsUnknown)
            return os;

        Owned<IConstComputerTypeInfo> type = env->getComputerType(root->queryProp("@computerType"));
        if (type && (type.get() != this))
            return type->getOS();
        return MachineOsUnknown;
    }
    virtual unsigned getNicSpeedMbitSec() const
    {
        const char * v = root->queryProp("@nicSpeed");
        if (v && *v)
            return atoi(v);

        Owned<IConstComputerTypeInfo> type = env->getComputerType(root->queryProp("@computerType"));
        if (type && (type.get() != this))
            return type->getNicSpeedMbitSec();
        return 0;
    }
};

//==========================================================================================

class CConstInstanceInfo : public CConstEnvBase, implements IConstInstanceInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstInstanceInfo(const CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root)
    {
    }

    virtual IConstMachineInfo * getMachine() const
    {
        return env->getMachine(root->queryProp("@computer"));
    }
    virtual IStringVal & getEndPoint(IStringVal & str) const
    {
        SCMStringBuffer ep;
        Owned<IConstMachineInfo> machine = getMachine();
        if (machine)
        {
            machine->getNetAddress(ep);
            const char *port = root->queryProp("@port");
            if (port)
                ep.s.append(':').append(port);
        }
        str.set(ep.str());
        return str;
    }
    virtual IStringVal & getExecutableDirectory(IStringVal & str) const
    {
        // this is the deploy directory so uses local path separators (I suspect this call is LEGACY now)
        SCMStringBuffer ep;
        Owned<IConstMachineInfo> machine = getMachine();
        if (machine)
        {
            machine->getNetAddress(ep);
            ep.s.insert(0, PATHSEPSTR PATHSEPSTR);
        }
        ep.s.append(PATHSEPCHAR).append(root->queryProp("@directory"));
        str.set(ep.str());
        return str;
    }
    virtual IStringVal & getDirectory(IStringVal & str) const
    {
        str.set(root->queryProp("@directory"));
        return str;
    }

    virtual bool doGetRunInfo(IStringVal & progpath, IStringVal & workdir, const char *defprogname, bool useprog) const
    {
        // this is remote path i.e. path should match *target* nodes format
        Owned<IConstMachineInfo> machine = getMachine();
        if (!machine)
            return false;
        char psep;
        bool appendexe;
        switch (machine->getOS())
        {
            case MachineOsSolaris:
            case MachineOsLinux:
                psep = '/';
                appendexe = false;
                break;
            default:
                psep = '\\';
                appendexe = true;
        }
        StringBuffer tmp;
        const char *program = useprog?root->queryProp("@program"):nullptr; // if program specified assume absolute
        if (!program||!*program)
        {
            SCMStringBuffer ep;
            machine->getNetAddress(ep);
            const char *dir = root->queryProp("@directory");
            if (dir)
            {
                if (isPathSepChar(*dir))
                    dir++;
                if (!*dir)
                    return false;
                tmp.append(psep).append(psep).append(ep.s).append(psep);
                do {
                    if (isPathSepChar(*dir))
                        tmp.append(psep);
                    else
                        tmp.append(*dir);
                    dir++;
                } while (*dir);
                if (!isPathSepChar(tmp.charAt(tmp.length()-1)))
                    tmp.append(psep);
                tmp.append(defprogname);
                size32_t l = strlen(defprogname);
                if (appendexe&&((l<5)||(stricmp(defprogname+l-4,".exe")!=0)))
                    tmp.append(".exe");
            }
            program = tmp.str();
        }
        progpath.set(program);
        const char *workd = root->queryProp("@workdir"); // if program specified assume absolute
        workdir.set(workd?workd:"");
        return true;
    }


    virtual bool getRunInfo(IStringVal & progpath, IStringVal & workdir, const char *defprogname) const
    {
        return doGetRunInfo(progpath,workdir,defprogname,true);
    }

    virtual unsigned getPort() const
    {
        return root->getPropInt("@port", 0);
    }

};

class CConstDropZoneServerInfo : public CConstEnvBase, implements IConstDropZoneServerInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstDropZoneServerInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root), prop(root) {}

    virtual StringBuffer & getName(StringBuffer & name) const
    {
        name.append(prop->queryProp("@name"));
        return name;
    }
    virtual StringBuffer & getServer(StringBuffer & server) const
    {
        server.append(prop->queryProp("@server"));
        return server;
    }

private:
    IPropertyTree * prop;
};

class CConstDropZoneInfo : public CConstEnvBase, implements IConstDropZoneInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstDropZoneInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root)
    {
        getStandardPosixPath(posixPath, root->queryProp("@directory"));
    }

    virtual IStringVal&     getComputerName(IStringVal &str) const
    {
        str.set(root->queryProp("@computer"));
        return str;
    }
    virtual IStringVal&     getDescription(IStringVal &str) const
    {
        str.set(root->queryProp("@description"));
        return str;
    }
    virtual IStringVal&     getDirectory(IStringVal &str) const
    {
        str.set(posixPath.str());
        return str;
    }
    virtual IStringVal&     getUMask(IStringVal &str) const
    {
        if (root->hasProp("@umask"))
            str.set(root->queryProp("@umask"));
        return str;
    }
    virtual bool isECLWatchVisible() const
    {
        return root->getPropBool("@ECLWatchVisible", true);
    }
    virtual IConstDropZoneServerInfoIterator * getServers() const
    {
        return new CConstDropZoneServerInfoIterator(this);
    }
private:
    StringBuffer posixPath;
};

class CConstSparkThorInfo : public CConstEnvBase, implements IConstSparkThorInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstSparkThorInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root) {}

    virtual IStringVal &getBuild(IStringVal &str) const
    {
        str.set(root->queryProp("@build"));
        return str;
    }
    virtual IStringVal &getThorClusterName(IStringVal &str) const
    {
        str.set(root->queryProp("@ThorClusterName"));
        return str;
    }
    virtual unsigned getSparkExecutorCores() const
    {
        return root->getPropInt("@SPARK_EXECUTOR_CORES", 0);
    }
    virtual unsigned long getSparkExecutorMemory() const
    {
        return readSizeSetting(root->queryProp("@SPARK_EXECUTOR_MEMORY"), 0);
    }
    virtual unsigned getSparkMasterPort() const
    {
        return root->getPropInt("@SPARK_MASTER_PORT", 0);
    }
    virtual unsigned getSparkMasterWebUIPort() const
    {
        return root->getPropInt("@SPARK_MASTER_WEBUI_PORT", 0);
    }
    virtual unsigned getSparkWorkerCores() const
    {
        return root->getPropInt("@SPARK_WORKER_CORES", 0);
    }
    virtual unsigned long getSparkWorkerMemory() const
    {
        return readSizeSetting(root->queryProp("@SPARK_WORKER_MEMORY"), 0);
    }
    virtual unsigned getSparkWorkerPort() const
    {
        return root->getPropInt("@SPARK_WORKER_PORT", 0);
    }
    virtual IConstInstanceInfoIterator *getInstanceIterator() const
    {
        return new CConstInstanceInfoIterator(env, root->getElements("Instance"));
    }
};

#if 0
//==========================================================================================

class CConstProcessInfo : public CConstEnvBase, implements IConstProcessInfo
{
    IArrayOf<IConstInstanceInfo> w;
    CArrayIteratorOf<IInterface, IIterator> it;
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstProcessInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root), it(w)
    {
        Owned<IPropertyTreeIterator> _it = root->getElements("*"); // MORE - should be instance
        for (_it->first(); _it->isValid(); _it->next())
        {
            IPropertyTree *rp = &_it->query();
            w.append(*new CConstInstanceInfo(env, rp)); // CConstInstanceInfo will link rp
        }
    }
    bool first() { return it.first(); }
    bool isValid() { return it.isValid(); }
    bool next() { return it.next(); }
    IConstInstanceInfo & query() { return (IConstInstanceInfo &) it.query();}
    virtual IConstInstanceInfo * getInstance(const char *domain)
    {
        for (int pass=0; pass<2; pass++)
        {
            ForEachItemIn(idx, w)
            {
                Owned<IConstMachineInfo> m = w.item(idx).getMachine();
                if (m)
                {
                    Owned<IConstDomainInfo> dm = m->getDomain();
                    if (dm)
                    {
                        StringBuffer thisdomain;

                        //dm->getName(StringBufferAdaptor(thisdomain)); // confuses g++
                        StringBufferAdaptor strval(thisdomain);
                        dm->getName(strval);

                        if (thisdomain.length() && strcmp(domain, thisdomain.str())==0)
                            return LINK(&w.item(idx));
                    }
                }
            }
        }
        return nullptr;
    }
};
#endif

class CConstDaFileSrvInfo : public CConstEnvBase, implements IConstDaFileSrvInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstDaFileSrvInfo(const CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root)
    {
    }
    virtual const char *getName() const override
    {
        return root->queryProp("@name");
    }
    virtual unsigned getPort() const override
    {
        return root->getPropInt("@rowServicePort");
    }
    virtual bool getSecure() const override
    {
        return root->getPropBool("@rowServiceSSL");
    }
};


//==========================================================================================

CLocalEnvironment::CLocalEnvironment(const char* environmentFile)
{
    if (environmentFile && *environmentFile)
    {
        IPropertyTree* root = createPTreeFromXMLFile(environmentFile);
        if (root)
            p.set(root);
    }

    init();
}

CLocalEnvironment::CLocalEnvironment(IRemoteConnection *_conn, IPropertyTree* root/*=nullptr*/,
                                     const char* path/*="/Environment"*/)
                                     : xPath(path)
{
    conn.set(_conn);

    if (root)
        p.set(root);
    else
        p.setown(conn->getRoot());

    init();
}

void CLocalEnvironment::init()
{
    machineCacheBuilt = false;
    dropZoneCacheBuilt = false;
    sparkThorCacheBuilt = false;
    numOfMachines = 0;
    numOfDropZones = 0;
    numOfSparkThors = 0;
    isDropZoneRestrictionLoaded = false;
    clusterKeyNameCache = false;
    ::getFileAccessUrl(fileAccessUrl);
}

CLocalEnvironment::~CLocalEnvironment()
{
    if (conn)
        conn->rollback();
}

IEnvironment& CLocalEnvironment::lock() const
{
    return *new CLockedEnvironment((CLocalEnvironment*)this);
}

IStringVal & CLocalEnvironment::getName(IStringVal & str) const
{
    synchronized procedure(safeCache);
    str.set(p->queryProp("@name"));
    return str;
}

IStringVal & CLocalEnvironment::getXML(IStringVal & str) const
{
    StringBuffer xml;
    {
        synchronized procedure(safeCache);
        toXML(p->queryBranch("."), xml);
    }
    str.set(xml.str());
    return str;
}

IPropertyTree & CLocalEnvironment::getPTree() const
{
    synchronized procedure(safeCache);
    return *LINK(p);
}

IConstEnvBase * CLocalEnvironment::getCache(const char *path) const
{
    IConstEnvBase * ret = cache.getValue(path);
    ::Link(ret);
    return ret;
}

void CLocalEnvironment::setCache(const char *path, IConstEnvBase *value) const
{
    cache.setValue(path, value);
}

IConstDomainInfo * CLocalEnvironment::getDomain(const char * name) const
{
    if (!name)
        return nullptr;
    StringBuffer xpath;
    xpath.appendf("Hardware/Domain[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return nullptr;
        cached = new CConstDomainInfo((CLocalEnvironment *) this, d);
        setCache(xpath.str(), cached);
    }
    return (IConstDomainInfo *) cached;
}

void CLocalEnvironment::buildMachineCache() const
{
    synchronized procedure(safeCache);
    if (!machineCacheBuilt)
    {
        Owned<IPropertyTreeIterator> it = p->getElements("Hardware/Computer");
        ForEach(*it)
        {
            const char *name = it->query().queryProp("@name");
            if (name)
            {
                StringBuffer x("Hardware/Computer[@name=\"");
                x.append(name).append("\"]");
                Owned<IConstEnvBase> cached = new CConstMachineInfo((CLocalEnvironment *) this, &it->query());
                cache.setValue(x.str(), cached);
            }
            const char * netAddress = it->query().queryProp("@netAddress");
            if (netAddress)
            {
                StringBuffer x("Hardware/Computer[@netAddress=\"");
                x.append(netAddress).append("\"]");
                Owned<IConstEnvBase> cached = new CConstMachineInfo((CLocalEnvironment *) this, &it->query());
                cache.setValue(x.str(), cached);

                IpAddress ip;
                ip.ipset(netAddress);
                if (ip.isLocal())
                    cache.setValue("Hardware/Computer[@netAddress=\".\"]", cached);
            }
            numOfMachines++;
            StringBuffer x("Hardware/Computer[@id=\"");
            x.append(MACHINE_PREFIX).append(numOfMachines).append("\"]");
            Owned<IConstEnvBase> cached = new CConstMachineInfo((CLocalEnvironment *) this, &it->query());
            cache.setValue(x.str(), cached);
        }
        machineCacheBuilt = true;
    }
}

void CLocalEnvironment::buildDropZoneCache() const
{
    synchronized procedure(safeCache);
    if (!dropZoneCacheBuilt)
    {
        Owned<IPropertyTreeIterator> it = p->getElements("Software/DropZone");
        ForEach(*it)
        {
            const char *name = it->query().queryProp("@name");
            if (name)
            {
                StringBuffer x("Software/DropZone[@name=\"");
                x.append(name).append("\"]");
                Owned<IConstEnvBase> cached = new CConstDropZoneInfo((CLocalEnvironment *) this, &it->query());
                cache.setValue(x.str(), cached);
            }

            numOfDropZones++;
            StringBuffer x("Software/DropZone[@id=\"");
            x.append(DROPZONE_SUFFIX).append(numOfDropZones).append("\"]");
            Owned<IConstEnvBase> cached = new CConstDropZoneInfo((CLocalEnvironment *) this, &it->query());
            cache.setValue(x.str(), cached);
        }
        dropZoneCacheBuilt = true;
    }
}

IConstComputerTypeInfo * CLocalEnvironment::getComputerType(const char * name) const
{
    if (!name)
        return nullptr;
    StringBuffer xpath;
    xpath.appendf("Hardware/ComputerType[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return nullptr;
        cached = new CConstComputerTypeInfo((CLocalEnvironment *) this, d);
        setCache(xpath.str(), cached);
    }
    return (CConstComputerTypeInfo *) cached;
}

IConstMachineInfo * CLocalEnvironment::getMachine(const char * name) const
{
    if (!name)
        return nullptr;
    buildMachineCache();
    StringBuffer xpath;
    xpath.appendf("Hardware/Computer[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return nullptr;
        cached = new CConstMachineInfo((CLocalEnvironment *) this, d);
        setCache(xpath.str(), cached);
    }
    return (CConstMachineInfo *) cached;
}

IConstMachineInfo * CLocalEnvironment::getMachineByAddress(const char * machineIp) const
{
    if (!machineIp)
        return nullptr;
    buildMachineCache();
    Owned<IPropertyTreeIterator> iter;
    StringBuffer xpath;
    xpath.appendf("Hardware/Computer[@netAddress=\"%s\"]", machineIp);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
        {
            // I suspect not in the original spirit of this but look for resolved IP
            Owned<IPropertyTreeIterator> iter = p->getElements("Hardware/Computer");
            IpAddress ip;
            ip.ipset(machineIp);
            ForEach(*iter)
            {
                IPropertyTree &computer = iter->query();
                IpAddress ip2;
                const char *ips = computer.queryProp("@netAddress");
                if (ips&&*ips)
                {
                    ip2.ipset(ips);
                    if (ip.ipequals(ip2))
                    {
                        d = &computer;
                        break;
                    }
                }
            }
        }
        if (!d)
            return nullptr;
        StringBuffer xpath1;
        xpath1.appendf("Hardware/Computer[@name=\"%s\"]", d->queryProp("@name"));
        cached = getCache(xpath1.str());
        if (!cached)
        {
            cached = new CConstMachineInfo((CLocalEnvironment *) this, d);
            setCache(xpath1.str(), cached);
            setCache(xpath.str(), cached);
        }
    }
    return (CConstMachineInfo *) cached;
}

IConstMachineInfo * CLocalEnvironment::getMachineByIndex(unsigned index) const
{
    if (!numOfMachines || (index == 0))
        return nullptr;

    buildMachineCache();
    if (index > numOfMachines)
        return nullptr;

    StringBuffer xpath("Hardware/Computer[@id=\"");
    xpath.append(MACHINE_PREFIX).append(index).append("\"]");
    synchronized procedure(safeCache);
    return (IConstMachineInfo *) getCache(xpath.str());
}

IConstMachineInfo * CLocalEnvironment::getMachineForLocalHost() const
{
    buildMachineCache();
    synchronized procedure(safeCache);
    return (CConstMachineInfo *) getCache("Hardware/Computer[@netAddress=\".\"]");
}

IConstDropZoneInfo * CLocalEnvironment::getDropZone(const char * name) const
{
    if (!name)
        return nullptr;
    buildDropZoneCache();
    VStringBuffer xpath("Software/DropZone[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    return (CConstDropZoneInfo *) getCache(xpath.str());
}

IConstDropZoneInfo * CLocalEnvironment::getDropZoneByIndex(unsigned index) const
{
    if (!numOfDropZones || (index == 0))
        return nullptr;

    buildDropZoneCache();
    if (index > numOfDropZones)
        return nullptr;

    StringBuffer xpath("Software/DropZone[@id=\"");
    xpath.append(DROPZONE_SUFFIX).append(index).append("\"]");
    synchronized procedure(safeCache);
    return (CConstDropZoneInfo *) getCache(xpath.str());
}


IConstInstanceInfo * CLocalEnvironment::getInstance(const char *type, const char *version, const char *domain) const
{
    StringBuffer xpath("Software/");
    xpath.append(type);
    if (version)
        xpath.append("[@version='").append(version).append("']");
    xpath.append("/Instance");

    synchronized procedure(safeCache);
    Owned<IPropertyTreeIterator> _it = p->getElements(xpath);
    for (_it->first(); _it->isValid(); _it->next())
    {
        IPropertyTree *rp = &_it->query();
        Owned<CConstInstanceInfo> inst = new CConstInstanceInfo(this, rp); // CConstInstanceInfo will link rp
        Owned<IConstMachineInfo> m = inst->getMachine();
        if (m)
        {
            Owned<IConstDomainInfo> dm = m->getDomain();
            if (dm)
            {
                SCMStringBuffer thisdomain;
                dm->getName(thisdomain);

                if (thisdomain.length() && strcmp(domain, thisdomain.str())==0)
                    return inst.getClear();
            }
        }
    }
    return nullptr;
}


CConstInstanceInfo * CLocalEnvironment::getInstanceByIP(const char *type, const char *version, IpAddress &ip) const
{
    StringBuffer xpath("Software/");
    xpath.append(type);
    if (version)
        xpath.append("[@version='").append(version).append("']");
    xpath.append("/Instance");

    synchronized procedure(safeCache);
    assertex(p);
    Owned<IPropertyTreeIterator> _it = p->getElements(xpath);
    assertex(_it);
    for (_it->first(); _it->isValid(); _it->next())
    {
        IPropertyTree *rp = &_it->query();
        assertex(rp);
        Owned<CConstInstanceInfo> inst = new CConstInstanceInfo(this, rp); // CConstInstanceInfo will link rp
        Owned<IConstMachineInfo> m = inst->getMachine();
        if (m)
        {
            SCMStringBuffer eps;
            m->getNetAddress(eps);
            SocketEndpoint ep(eps.str());
            if (ep.ipequals(ip))
                return inst.getClear();
        }
    }
    return nullptr;
}


void CLocalEnvironment::unlockRemote()
{
#if 0
    conn->commit(true);
    conn->changeMode(0, SDS_LOCK_TIMEOUT);
#else
    if (conn)
    {
        synchronized procedure(safeCache);
        p.clear();
        conn.setown(querySDS().connect(xPath.str(), myProcessSession(), 0, SDS_LOCK_TIMEOUT));
        p.setown(conn->getRoot());
    }
#endif
}

void CLocalEnvironment::preload()
{
    synchronized procedure(safeCache);
    p->queryBranch(".");
}

void CLocalEnvironment::setXML(const char *xml)
{
    Owned<IPropertyTree> newRoot = createPTreeFromXMLString(xml, ipt_lowmem);
    synchronized procedure(safeCache);
    Owned<IPropertyTreeIterator> it = p->getElements("*");
    ForEach(*it)
    {
        p->removeTree(&it->query());
    }

    it.setown(newRoot->getElements("*"));
    ForEach(*it)
    {
        IPropertyTree *sub = &it->get();
        p->addPropTree(sub->queryName(), sub);
    }
}

bool CLocalEnvironment::getRunInfo(IStringVal & path, IStringVal & dir, const char * tag, const char * version, const char *machineaddr, const char *defprogname) const
{
    try
    {
        // PrintLog("getExecutablePath %s %s %s", tag, version, machineaddr);

        // first see if local machine with deployed on
        SocketEndpoint ep(machineaddr);
        Owned<CConstInstanceInfo> ipinstance = getInstanceByIP(tag, version, ep);

        if (ipinstance)
        {
            StringAttr testpath;
            StringAttrAdaptor teststrval(testpath);
            if (ipinstance->doGetRunInfo(teststrval,dir,defprogname,false))
            { // this returns full string
                RemoteFilename rfn;
                rfn.setRemotePath(testpath.get());
                Owned<IFile> file = createIFile(rfn);
                if (file->exists())
                {
                    StringBuffer tmp;
                    rfn.getLocalPath(tmp);
                    path.set(tmp.str());
                    return true;
                }
            }
        }

        Owned<IConstMachineInfo> machine = getMachineByAddress(machineaddr);
        if (!machine)
        {
            LOG(MCdebugInfo, unknownJob, "Unable to find machine for %s", machineaddr);
            return false;
        }

        StringAttr targetdomain;
        Owned<IConstDomainInfo> domain = machine->getDomain();
        if (!domain)
        {
            LOG(MCdebugInfo, unknownJob, "Unable to find domain for %s", machineaddr);
            return false;
        }

        //domain->getName(StringAttrAdaptor(targetdomain)); // confuses g++
        StringAttrAdaptor strval(targetdomain);
        domain->getName(strval);

        Owned<IConstInstanceInfo> instance = getInstance(tag, version, targetdomain);
        if (!instance)
        {
            LOG(MCdebugInfo, unknownJob, "Unable to find process %s for domain %s", tag, targetdomain.get());
            return false;
        }
        return instance->getRunInfo(path,dir,defprogname);
    }
    catch (IException * e)
    {
        EXCLOG(e, "Extracting slave version");
        e->Release();
        return false;
    }
}

void CLocalEnvironment::clearCache()
{
    synchronized procedure(safeCache);
    if (conn)
    {
        p.clear();
        unsigned mode = 0;
        try
        {
            conn->reload();
        }
        catch (IException *e)
        {
            EXCLOG(e, "Failed to reload connection");
            e->Release();
            mode = conn->queryMode();
            conn.clear();
        }
        if (!conn)
            conn.setown(querySDS().connect(xPath, myProcessSession(), mode, SDS_LOCK_TIMEOUT));
        p.setown(conn->getRoot());
    }
    cache.kill();
    keyClusterMap.clear();
    keyPairMap.clear();
    init();
    resetPasswordsFromSDS();
}

IConstDropZoneInfo * CLocalEnvironment::getDropZoneByAddressPath(const char * netaddress, const char *targetFilePath) const
{
    IConstDropZoneInfo * dropZone = nullptr;
    IpAddress targetIp(netaddress);
    unsigned dropzonePathLen = _MAX_PATH + 1;

#ifdef _DEBUG
    LOG(MCdebugInfo, unknownJob, "Netaddress: '%s', targetFilePath: '%s'", netaddress, targetFilePath);
#endif
    // Check the directory path first

    Owned<IConstDropZoneInfoIterator> zoneIt = getDropZoneIterator();
    ForEach(*zoneIt)
    {
        SCMStringBuffer dropZoneDir;
        zoneIt->query().getDirectory(dropZoneDir);
        StringBuffer fullDropZoneDir(dropZoneDir.str());
        addPathSepChar(fullDropZoneDir);
        IConstDropZoneInfo * candidateDropZone = nullptr;

        if (strncmp(fullDropZoneDir, targetFilePath, fullDropZoneDir.length()) == 0)
        {
            candidateDropZone = &zoneIt->query();

            // The backward compatibility built in IConstDropZoneServerInfoIterator
            Owned<IConstDropZoneServerInfoIterator> dropzoneServerListIt = candidateDropZone->getServers();
            ForEach(*dropzoneServerListIt)
            {
                StringBuffer dropzoneServer;
                dropzoneServerListIt->query().getServer(dropzoneServer);
                // It can be a hostname or an IP -> get the IP
                IpAddress serverIP(dropzoneServer.str());

#ifdef _DEBUG
                StringBuffer serverIpString;
                serverIP.getIpText(serverIpString);
                LOG(MCdebugInfo, unknownJob, "Listed server: '%s', IP: '%s'", dropzoneServer.str(), serverIpString.str());
#endif
                if (targetIp.ipequals(serverIP))
                {
                    // OK the target is a valid machine in the server list we have a right drop zone candidate
                    // Keep this candidate drop zone if its directory path is shorter than we already have
                    if (dropzonePathLen > fullDropZoneDir.length())
                    {
                       dropzonePathLen = fullDropZoneDir.length();
                       dropZone = candidateDropZone;
                    }
                    break;
                }
            }
        }
    }

    return LINK(dropZone);
}

IConstDropZoneInfoIterator * CLocalEnvironment::getDropZoneIteratorByAddress(const char *addr) const
{
    class CByAddrIter : public CSimpleInterfaceOf<IConstDropZoneInfoIterator>
    {
        IArrayOf<IConstDropZoneInfo> matches;
        unsigned cur = NotFound;

    public:
        CByAddrIter(IConstDropZoneInfoIterator *baseIter, const char *addr)
        {
            IpAddress toMatch(addr);
            ForEach(*baseIter)
            {
                IConstDropZoneInfo &dz = baseIter->query();
                Owned<IConstDropZoneServerInfoIterator> serverIter = dz.getServers();
                ForEach(*serverIter)
                {
                    IConstDropZoneServerInfo &serverElem = serverIter->query();
                    StringBuffer serverName;
                    IpAddress serverIp(serverElem.getServer(serverName).str());
                    if (serverIp.ipequals(toMatch))
                    {
                        matches.append(*LINK(&dz));
                        break;
                    }
                }
            }
        }
        virtual bool first() override
        {
            if (0 == matches.ordinality())
            {
                cur = NotFound;
                return false;
            }
            cur = 0;
            return true;
        }
        virtual bool next() override
        {
            if (cur+1==matches.ordinality())
            {
                cur = NotFound;
                return false;
            }
            ++cur;
            return true;
        }
        virtual bool isValid() override
        {
            return NotFound != cur;
        }
        virtual IConstDropZoneInfo &query() override
        {
            assertex(NotFound != cur);
            return matches.item(cur);
        }
        virtual unsigned count() const override
        {
            return matches.ordinality();
        }
    };
    Owned<IConstDropZoneInfoIterator> baseIter = new CConstDropZoneInfoIterator();
    return new CByAddrIter(baseIter, addr);
}



IConstDropZoneInfoIterator * CLocalEnvironment::getDropZoneIterator() const
{
    return new CConstDropZoneInfoIterator();
}

IConstMachineInfoIterator * CLocalEnvironment::getMachineIterator() const
{
    return new CConstMachineInfoIterator();
}

bool CLocalEnvironment::isDropZoneRestrictionEnabled() const
{
    if (!isDropZoneRestrictionLoaded)
    {
        dropZoneRestrictionEnabled = queryEnvironmentConf().getPropBool("useDropZoneRestriction", true);
        isDropZoneRestrictionLoaded=true;
    }

    return dropZoneRestrictionEnabled;
}

IConstDaFileSrvInfo *CLocalEnvironment::getDaFileSrvGroupInfo(const char *name) const
{
    if (!name)
        return nullptr;
    VStringBuffer xpath("Software/DafilesrvGroup[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return nullptr;
        cached = new CConstDaFileSrvInfo(this, d);
        setCache(xpath.str(), cached);
    }
    return (IConstDaFileSrvInfo *) cached;
}

IConstSparkThorInfo *CLocalEnvironment::getSparkThor(const char *name) const
{
    if (isEmptyString(name))
        return nullptr;
    buildSparkThorCache();
    VStringBuffer xpath("Software/SparkThor[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    return (CConstSparkThorInfo *) getCache(xpath);
}

IConstSparkThorInfo *CLocalEnvironment::getSparkThorByIndex(unsigned index) const
{
    if (index == 0)
        return nullptr;

    buildSparkThorCache();
    if (index > numOfSparkThors)
        return nullptr;

    StringBuffer xpath("Software/SparkThor[@id=\"");
    xpath.append(SPARKTHOR_SUFFIX).append(index).append("\"]");
    synchronized procedure(safeCache);
    return (CConstSparkThorInfo *) getCache(xpath);
}

IConstSparkThorInfoIterator *CLocalEnvironment::getSparkThorIterator() const
{
    return new CConstSparkThorInfoIterator();
}

void CLocalEnvironment::buildSparkThorCache() const
{
    synchronized procedure(safeCache);
    if (sparkThorCacheBuilt)
        return;

    Owned<IPropertyTreeIterator> it = p->getElements("Software/SparkThorProcess");
    ForEach(*it)
    {
        const char *name = it->query().queryProp("@name");
        if (!isEmptyString(name))
        {
            StringBuffer x("Software/SparkThor[@name=\"");
            x.append(name).append("\"]");
            Owned<IConstEnvBase> cached = new CConstSparkThorInfo((CLocalEnvironment *) this, &it->query());
            cache.setValue(x, cached);
        }

        numOfSparkThors++;
        StringBuffer x("Software/SparkThor[@id=\"");
        x.append(SPARKTHOR_SUFFIX).append(numOfSparkThors).append("\"]");
        Owned<IConstEnvBase> cached = new CConstSparkThorInfo((CLocalEnvironment *) this, &it->query());
        cache.setValue(x, cached);
    }
    sparkThorCacheBuilt = true;
}


//==========================================================================================
// Iterators implementation

CConstMachineInfoIterator::CConstMachineInfoIterator()
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    constEnv.setown((CLocalEnvironment *)factory->openEnvironment());
    maxIndex = constEnv->getNumberOfMachines();
}

bool CConstMachineInfoIterator::first()
{
    index = 1;
    curr.setown(constEnv->getMachineByIndex(index));
    return curr != nullptr;
}
bool CConstMachineInfoIterator::next()
{
    if (index < maxIndex)
    {
        index++;
        curr.setown(constEnv->getMachineByIndex(index));
    }
    else
        curr.clear();

    return curr != nullptr;
}

bool CConstMachineInfoIterator::isValid()
{
    return curr != nullptr;
}

IConstMachineInfo & CConstMachineInfoIterator::query()
{
    return *curr;
}

unsigned CConstMachineInfoIterator::count() const
{
    return maxIndex;
}

CConstDropZoneServerInfoIterator::CConstDropZoneServerInfoIterator(const IConstDropZoneInfo * dropZone)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    constEnv.setown((CLocalEnvironment *)factory->openEnvironment());

    // For backward compatibility
    SCMStringBuffer dropZoneMachineName;
    // Returns dropzone '@computer' value if it is exists
    dropZone->getComputerName(dropZoneMachineName);

    if (0 != dropZoneMachineName.length())
    {
        // Create a ServerList for legacy element.
        Owned<IPropertyTree> legacyServerList = createPTree(ipt_lowmem);

        Owned<IConstMachineInfo> machineInfo = constEnv->getMachine(dropZoneMachineName.str());
        if (machineInfo)
        {
            SCMStringBuffer dropZoneMachineNetAddress;
            machineInfo->getNetAddress(dropZoneMachineNetAddress);

            // Create a single ServerList record related to @computer
            //<ServerList name="ServerList" server="<IP_of_@computer>"/>
            IPropertyTree *newRecord = legacyServerList->addPropTree("ServerList");
            newRecord->setProp("@name", "ServerList");
            newRecord->setProp("@server", dropZoneMachineNetAddress.str());

            maxIndex = 1;
        }
        else
        {
            // Something is terrible wrong because there is no matching machine for DropZone @computer
            maxIndex = 0;
        }
        serverListIt.setown(legacyServerList->getElements("ServerList"));
    }
    else
    {
        Owned<IPropertyTree> pSrc = &dropZone->getPTree();
        serverListIt.setown(pSrc->getElements("ServerList"));
        maxIndex = pSrc->getCount("ServerList");
    }
}

bool CConstDropZoneServerInfoIterator::first()
{
    bool hasFirst = serverListIt->first();
    if (hasFirst)
        curr.setown(new CConstDropZoneServerInfo(constEnv, &serverListIt->query()));
    else
        curr.clear();
    return hasFirst;
}

bool CConstDropZoneServerInfoIterator::next()
{
    bool hasNext = serverListIt->next();
    if (hasNext)
        curr.setown(new CConstDropZoneServerInfo(constEnv, &serverListIt->query()));
    else
        curr.clear();
    return hasNext;
}

bool CConstDropZoneServerInfoIterator::isValid()
{
    return nullptr != curr;
}

IConstDropZoneServerInfo & CConstDropZoneServerInfoIterator::query()
{
    return *curr;
}

unsigned CConstDropZoneServerInfoIterator::count() const
{
    return maxIndex;
}

//--------------------------------------------------

CConstDropZoneInfoIterator::CConstDropZoneInfoIterator()
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    constEnv.setown((CLocalEnvironment *)factory->openEnvironment());
    maxIndex = constEnv->getNumberOfDropZones();
}

bool CConstDropZoneInfoIterator::first()
{
    index = 1;
    curr.setown(constEnv->getDropZoneByIndex(index));
    return curr != nullptr;
}

bool CConstDropZoneInfoIterator::next()
{
    if (index < maxIndex)
    {
        index++;
        curr.setown(constEnv->getDropZoneByIndex(index));
    }
    else
        curr.clear();

    return curr != nullptr;
}

bool CConstDropZoneInfoIterator::isValid()
{
    return curr != nullptr;
}

IConstDropZoneInfo & CConstDropZoneInfoIterator::query()
{
    return *curr;
}

unsigned CConstDropZoneInfoIterator::count() const
{
    return maxIndex;
}

//--------------------------------------------------

CConstSparkThorInfoIterator::CConstSparkThorInfoIterator()
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory(true);
    constEnv.setown((CLocalEnvironment *)factory->openEnvironment());
    maxIndex = constEnv->getNumberOfSparkThors();
}

bool CConstSparkThorInfoIterator::first()
{
    index = 1;
    curr.setown(constEnv->getSparkThorByIndex(index));
    return curr != nullptr;
}

bool CConstSparkThorInfoIterator::next()
{
    if (index < maxIndex)
    {
        index++;
        curr.setown(constEnv->getSparkThorByIndex(index));
    }
    else
        curr.clear();

    return curr != nullptr;
}

bool CConstSparkThorInfoIterator::isValid()
{
    return curr != nullptr;
}

IConstSparkThorInfo &CConstSparkThorInfoIterator::query()
{
    return *curr;
}

unsigned CConstSparkThorInfoIterator::count() const
{
    return maxIndex;
}

//--------------------------------------------------

CConstInstanceInfoIterator::CConstInstanceInfoIterator(const CLocalEnvironment *env, IPropertyTreeIterator *itr)
    : constEnv(env)
{
    instanceItr.setown(itr);
    maxIndex = 0;
    ForEach(*instanceItr)
        maxIndex++;
}

bool CConstInstanceInfoIterator::first()
{
    index = 1;
    instanceItr->first();
    curr.setown(new CConstInstanceInfo(constEnv, &instanceItr->query()));
    return curr != nullptr;
}

bool CConstInstanceInfoIterator::next()
{
    if (index < maxIndex)
    {
        index++;
        instanceItr->next();
        curr.setown(new CConstInstanceInfo(constEnv, &instanceItr->query()));
    }
    else
        curr.clear();

    return curr != nullptr;
}

bool CConstInstanceInfoIterator::isValid()
{
    return curr != nullptr;
}

IConstInstanceInfo &CConstInstanceInfoIterator::query()
{
    return *curr;
}

unsigned CConstInstanceInfoIterator::count() const
{
    return maxIndex;
}

//==========================================================================================


static CriticalSection getEnvSect;

extern ENVIRONMENT_API IEnvironmentFactory * getEnvironmentFactory(bool update)
{
    CriticalBlock block(getEnvSect);
    if (!factory)
    {
        factory = new CEnvironmentFactory();
        addShutdownHook(*factory);
    }
    if (update)
        factory->validateCache();
    return LINK(factory);
}

extern ENVIRONMENT_API void closeEnvironment()
{
    try
    {
        CEnvironmentFactory* pFactory;
        {
            //this method is not meant to be invoked by multiple
            //threads concurrently but just in case...
            CriticalBlock block(getEnvSect);

            pFactory = factory;
            factory = nullptr;
        }
        clearPasswordsFromSDS();
        if (pFactory)
        {
            removeShutdownHook(*pFactory);
            pFactory->close();
            pFactory->Release();
        }
    }
    catch (IException *e)
    {
        EXCLOG(e);
    }
}

extern ENVIRONMENT_API unsigned long readSizeSetting(const char * sizeStr, const unsigned long defaultSize)
{
    StringBuffer buf = sizeStr;
    buf.trim();

    if (buf.isEmpty())
        return defaultSize;

    const char* ptrStart = buf;
    const char* ptrAfterDigit = ptrStart;
    while (*ptrAfterDigit && isdigit(*ptrAfterDigit))
        ptrAfterDigit++;

    if (!*ptrAfterDigit)
        return atol(buf);

    const char* ptr = ptrAfterDigit;
    while (*ptr && (ptr[0] == ' '))
        ptr++;

    char c = ptr[0];
    buf.setLength(ptrAfterDigit - ptrStart);
    unsigned long size = atol(buf);
    switch (c)
    {
    case 'k':
    case 'K':
        size *= 1000;
        break;
    case 'm':
    case 'M':
        size *= 1000000;
        break;
    case 'g':
    case 'G':
        size *= 1000000000;
        break;
    case 't':
    case 'T':
        size *= 1000000000000;
        break;
    default:
        break;
    }
    return size;
}
