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

#include "mpbase.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dasds.hpp"
#include "dalienv.hpp"

#define SDS_LOCK_TIMEOUT  30000


static int environmentTraceLevel = 1;
static Owned <IConstEnvironment> cache;

class CConstInstanceInfo;

class CLocalEnvironment : public CInterface, implements IConstEnvironment
{
private:
    // NOTE - order is important - we need to construct before p and (especially) destruct after p
    Owned<IRemoteConnection> conn;
    Owned<IPropertyTree> p;
    mutable MapStringToMyClass<IConstEnvBase> cache;
    mutable Mutex safeCache;
    mutable bool dropZoneCacheBuilt;
    mutable bool machineCacheBuilt;
   StringBuffer xPath;


    IConstEnvBase * getCache(const char *path) const;
    void setCache(const char *path, IConstEnvBase *value) const;
    void buildMachineCache() const;
    void buildDropZoneCache() const;

public:
    IMPLEMENT_IINTERFACE;

    CLocalEnvironment(IRemoteConnection *_conn, IPropertyTree *x=NULL, const char* path="Environment");
    CLocalEnvironment(const char* path="config.xml");
    virtual ~CLocalEnvironment();

    virtual IStringVal & getName(IStringVal & str) const;
    virtual IStringVal & getXML(IStringVal & str) const;
    virtual IPropertyTree & getPTree() const;
    virtual IEnvironment& lock() const;
    virtual IConstDomainInfo * getDomain(const char * name) const;
    virtual IConstMachineInfo * getMachine(const char * name) const;
    virtual IConstMachineInfo * getMachineByAddress(const char * name) const;
    virtual IConstMachineInfo * getMachineForLocalHost() const;
    virtual IConstDropZoneInfo * getDropZone(const char * name) const;
    virtual IConstDropZoneInfo * getDropZoneByComputer(const char * computer) const;
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
   
};

class CLockedEnvironment : public CInterface, implements IEnvironment
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

          if (conn == NULL)
         {
              if (environmentTraceLevel > 0)
                  PrintLog("Failed to create locked environment %s", newName.str());

              throw MakeStringException(-1, "Failed to get a lock on environment /%s", newName.str());
         }

         //save the locked environment
         env.setown(new CLocalEnvironment(conn, NULL, newName.str()));

         //get a lock on the const environment
         const unsigned int mode2 = RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_LOCK_SUB;
          Owned<IRemoteConnection> conn2 = querySDS().connect(constEnv->getPath(), myProcessSession(), mode2, SDS_LOCK_TIMEOUT);

          if (conn2 == NULL)
         {
              if (environmentTraceLevel > 0)
                  PrintLog("Failed to lock environment %s", constEnv->getPath());

              throw MakeStringException(-1, "Failed to get a lock on environment /%s", constEnv->getPath());
         }

         //copy const environment to our member environment
         Owned<IPropertyTree> pSrc = conn2->getRoot();
         c.setown( new CLocalEnvironment(NULL, createPTreeFromIPT(pSrc)));
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
   { 
       return c->getPTree();
   }
    virtual IConstDomainInfo * getDomain(const char * name) const
            { return c->getDomain(name); }
    virtual IConstMachineInfo * getMachine(const char * name) const
            { return c->getMachine(name); }
    virtual IConstMachineInfo * getMachineByAddress(const char * name) const
            { return c->getMachineByAddress(name); }
    virtual IConstMachineInfo * getMachineForLocalHost() const
            { return c->getMachineForLocalHost(); }
    virtual IConstDropZoneInfo * getDropZone(const char * name) const
            { return c->getDropZone(name); }
    virtual IConstDropZoneInfo * getDropZoneByComputer(const char * computer) const
            { return c->getDropZoneByComputer(computer); }
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

};

void CLockedEnvironment::commit()
{
   if (constEnv)
   {
      //get a lock on const environment momentarily
      const unsigned int mode2 = RTM_CREATE_QUERY | RTM_LOCK_READ | RTM_LOCK_WRITE | RTM_LOCK_SUB;
       Owned<IRemoteConnection> conn2 = querySDS().connect(constEnv->getPath(), myProcessSession(), mode2, SDS_LOCK_TIMEOUT);

       if (conn2 == NULL)
      {
           if (environmentTraceLevel > 0)
               PrintLog("Failed to lock environment %s", constEnv->getPath());

           throw MakeStringException(-1, "Failed to get a lock on environment /%s", constEnv->getPath());
      }

      //copy locked environment to const environment
      Owned<IPropertyTree> pSrc = &getPTree();
      Owned<IPropertyTree> pDst = conn2->queryRoot()->getBranch(NULL);

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

       if (conn2 == NULL)
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
class CSdsSubscription : public CInterface, implements ISDSSubscription
{
public:
   CSdsSubscription()
   { 
      m_constEnvUpdated = false;
        Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
      sub_id = envFactory->subscribe(this);
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
            Owned<IEnvironmentFactory> m_envFactory = getEnvironmentFactory();
            m_envFactory->unsubscribe(sub_id); 
            sub_id = 0; 
        } 
    }
   IMPLEMENT_IINTERFACE;

   //another client (like configenv) may have updated the environment and we got notified
   //(thanks to our subscription) but don't just reload it yet since this notification is sent on 
   //another thread asynchronously and we may be actively working with the old environment.  Just
   //invoke handleEnvironmentChange() when we are ready to invalidate cache in environment factory.
   //
    void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen=0, const void *valueData=NULL)
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
            Owned<IEnvironmentFactory> envFactory = getEnvironmentFactory();
         Owned<IConstEnvironment> constEnv = envFactory->openEnvironment();
         constEnv->clearCache();
         m_constEnvUpdated = false;
      }
   }

private:
   SubscriptionId sub_id;
   Mutex  m_mutexEnv;
   bool   m_constEnvUpdated;
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
        Owned<IPropertyTree> ptree = createPTreeFromXMLFile(filename);
        Owned<CLocalEnvironment> pLocalEnv = new CLocalEnvironment(NULL, ptree);
        return new CLockedEnvironment(pLocalEnv);
    }

    virtual IEnvironment * loadLocalEnvironment(const char * xml)
    {
        Owned<IPropertyTree> ptree = createPTreeFromXMLString(xml);
        Owned<CLocalEnvironment> pLocalEnv = new CLocalEnvironment(NULL, ptree);
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
            subscription.setown( new CSdsSubscription() );
      
        subscription->handleEnvironmentChange();
   }

private:
    IRemoteConnection* connect(const char *xpath, unsigned flags)
    {
        return querySDS().connect(xpath, myProcessSession(), flags, SDS_LOCK_TIMEOUT);
    }
};

static CEnvironmentFactory *factory=NULL;

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

struct mapEnums { EnvMachineOS val; const char *str; };

static EnvMachineOS getEnum(IPropertyTree *p, const char *propname, mapEnums *map) 
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


mapEnums OperatingSystems[] = {
    { MachineOsW2K, "W2K" },
    { MachineOsSolaris, "solaris" },
    { MachineOsLinux, "linux" },
    { MachineOsSize, NULL }
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

    virtual bool doGetRunInfo(IStringVal & progpath, IStringVal & workdir, const char *defprogname, bool useprog) const
    {
        // this is remote path i.e. path should match *target* nodes format
        Owned<IConstMachineInfo> machine = getMachine();
        if (!machine) 
            return false;
        char psep; 
        bool appendexe;
        switch (machine->getOS()) {
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
        const char *program = useprog?root->queryProp("@program"):NULL; // if program specified assume absolute 
        if (!program||!*program) {
            SCMStringBuffer ep;
            machine->getNetAddress(ep);
            const char *dir = root->queryProp("@directory");
            if (dir) {
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

class CConstDropZoneInfo : public CConstEnvBase, implements IConstDropZoneInfo
{
public:
    IMPLEMENT_IINTERFACE;
    IMPLEMENT_ICONSTENVBASE;
    CConstDropZoneInfo(CLocalEnvironment *env, IPropertyTree *root) : CConstEnvBase(env, root) {}

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
        str.set(root->queryProp("@directory"));
        return str;
    }
    virtual IStringVal&     getUMask(IStringVal &str) const
    {
        if (root->hasProp("@umask"))
            str.set(root->queryProp("@umask"));
        return str;
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
        return NULL;
    }
};
#endif

//==========================================================================================

CLocalEnvironment::CLocalEnvironment(const char* environmentFile) 
{
   if (environmentFile && *environmentFile)
   {
       IPropertyTree* root = createPTreeFromXMLFile(environmentFile);
       if (root)
           p.set(root);
   }

   machineCacheBuilt = false;
}

CLocalEnvironment::CLocalEnvironment(IRemoteConnection *_conn, IPropertyTree* root/*=NULL*/, 
                                     const char* path/*="/Environment"*/) 
                                     : xPath(path)
{
   conn.set(_conn);

   if (root)
       p.set(root);
   else
      p.setown(conn->getRoot());

    machineCacheBuilt = false;
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
        return NULL;
    StringBuffer xpath;
    xpath.appendf("Hardware/Domain[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return NULL;
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
            name = it->query().queryProp("@netAddress");
            if (name)
            {
                StringBuffer x("Hardware/Computer[@netAddress=\"");
                x.append(name).append("\"]");
                Owned<IConstEnvBase> cached = new CConstMachineInfo((CLocalEnvironment *) this, &it->query());
                cache.setValue(x.str(), cached);

                IpAddress ip;
                ip.ipset(name);
                if (ip.isLocal())
                    cache.setValue("Hardware/Computer[@netAddress=\".\"]", cached);
            }
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
            name = it->query().queryProp("@computer");
            if (name)
            {
                StringBuffer x("Software/DropZone[@computer=\"");
                x.append(name).append("\"]");
                Owned<IConstEnvBase> cached = new CConstDropZoneInfo((CLocalEnvironment *) this, &it->query());
                cache.setValue(x.str(), cached);
            }
        }
        dropZoneCacheBuilt = true;
    }
}

IConstComputerTypeInfo * CLocalEnvironment::getComputerType(const char * name) const
{
    if (!name)
        return NULL;
    StringBuffer xpath;
    xpath.appendf("Hardware/ComputerType[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return NULL;
        cached = new CConstComputerTypeInfo((CLocalEnvironment *) this, d);
        setCache(xpath.str(), cached);
    }
    return (CConstComputerTypeInfo *) cached;
}

IConstMachineInfo * CLocalEnvironment::getMachine(const char * name) const
{
    if (!name)
        return NULL;
    buildMachineCache();
    StringBuffer xpath;
    xpath.appendf("Hardware/Computer[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d)
            return NULL;
        cached = new CConstMachineInfo((CLocalEnvironment *) this, d);
        setCache(xpath.str(), cached);
    }
    return (CConstMachineInfo *) cached;
}

IConstMachineInfo * CLocalEnvironment::getMachineByAddress(const char * name) const
{
    if (!name)
        return NULL;
    buildMachineCache();
    Owned<IPropertyTreeIterator> iter;
    StringBuffer xpath;
    xpath.appendf("Hardware/Computer[@netAddress=\"%s\"]", name);
    synchronized procedure(safeCache);
    IConstEnvBase *cached = getCache(xpath.str());
    if (!cached)
    {
        IPropertyTree *d = p->queryPropTree(xpath.str());
        if (!d) {
            // I suspect not in the original spirit of this but look for resolved IP
            Owned<IPropertyTreeIterator> iter = p->getElements("Hardware/Computer");
            IpAddress ip;
            ip.ipset(name);
            ForEach(*iter) {
                IPropertyTree &computer = iter->query();
                IpAddress ip2;
                const char *ips = computer.queryProp("@netAddress");
                if (ips&&*ips) {
                    ip2.ipset(ips);
                    if (ip.ipequals(ip2)) {
                        d = &computer;
                        break;
                    }
                }
            }
        }
        if (!d) 
            return NULL;
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

IConstMachineInfo * CLocalEnvironment::getMachineForLocalHost() const
{
    buildMachineCache();
    synchronized procedure(safeCache);
    return (CConstMachineInfo *) getCache("Hardware/Computer[@netAddress=\".\"]");
}

IConstDropZoneInfo * CLocalEnvironment::getDropZone(const char * name) const
{
    if (!name)
        return NULL;
    buildDropZoneCache();
    VStringBuffer xpath("Software/DropZone[@name=\"%s\"]", name);
    synchronized procedure(safeCache);
    return (CConstDropZoneInfo *) getCache(xpath.str());
}


IConstDropZoneInfo * CLocalEnvironment::getDropZoneByComputer(const char * computer) const
{
    if (!computer)
        return NULL;
    buildDropZoneCache();
    VStringBuffer xpath("Software/DropZone[@computer=\"%s\"]", computer);
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
    return NULL;
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
    return NULL;
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
    Owned<IPropertyTree> newRoot = createPTreeFromXMLString(xml);
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
//      PrintLog("getExecutablePath %s %s %s", tag, version, machineaddr);

        // first see if local machine with deployed on
        SocketEndpoint ep(machineaddr);
        Owned<CConstInstanceInfo> ipinstance = getInstanceByIP(tag, version, ep);

        if (ipinstance)
        {
            StringAttr testpath;
            StringAttrAdaptor teststrval(testpath);
            if (ipinstance->doGetRunInfo(teststrval,dir,defprogname,false)) { // this returns full string
                RemoteFilename rfn;
                rfn.setRemotePath(testpath.get());
                Owned<IFile> file = createIFile(rfn); 
                if (file->exists()) {
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
    if (conn) {
        p.clear();
        conn->reload();
        p.setown(conn->getRoot());
    }
    cache.kill();
    machineCacheBuilt = false;
    resetPasswordsFromSDS();
}

//==========================================================================================

static CriticalSection getEnvSect;

extern ENVIRONMENT_API IEnvironmentFactory * getEnvironmentFactory()
{
    CriticalBlock block(getEnvSect);
    if (!factory)
    {
        factory = new CEnvironmentFactory();
        addShutdownHook(*factory);
    }
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
            factory = NULL;
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
