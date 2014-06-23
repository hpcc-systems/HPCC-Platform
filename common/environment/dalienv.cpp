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

#include "platform.h"

#include "jlib.hpp"
#include "jio.hpp"

#include "jmutex.hpp"
#include "jfile.hpp"
#include "jptree.hpp"

#include "dasds.hpp"
#include "daclient.hpp"

#include "environment.hpp"
#include "dalienv.hpp"
#include "rmtfile.hpp"

struct CIpInstance
{
    unsigned hash;
    IpAddress ip;
    CIpInstance(const IpAddress &_ip)
        : ip(_ip)
    {
        hash = ip.iphash();
    }
    static unsigned getHash(const char *key)
    {
        return ((const IpAddress *)key)->iphash();
    }

    bool eq(const char *key)
    {
        return ((const IpAddress *)key)->ipequals(ip);
    }
};

struct CIpPasswordInstance: public CIpInstance
{
    StringAttr password;
    StringAttr user;
    bool matched;
    CIpPasswordInstance(const IpAddress &_ip)
        : CIpInstance(_ip)
    {
        matched = false;
    }

    static void destroy(CIpPasswordInstance *i) { delete i; }
    static CIpPasswordInstance *create(const char *key) { return new CIpPasswordInstance(*(const IpAddress *)key); }
};

struct CIpPasswordHashTable: public CMinHashTable<CIpPasswordInstance>
{
};


struct CIpOsInstance: public CIpInstance
{
    EnvMachineOS            os;
    CIpOsInstance(const IpAddress &key)
        : CIpInstance(key)
    {
        os=MachineOsUnknown;
    }

    static void destroy(CIpOsInstance *i) { delete i; }
    static CIpOsInstance *create(const char *key) { return new CIpOsInstance(*(const IpAddress *)key); }
};

class CIpOsHashTable: public CMinHashTable<CIpOsInstance>
{
};



class SDSPasswordProvider : public CInterface, implements IPasswordProvider
{
public:
    SDSPasswordProvider();
    ~SDSPasswordProvider() { delete map; }
    IMPLEMENT_IINTERFACE

    virtual bool getPassword(const IpAddress & ip, StringBuffer & username, StringBuffer & password);
    void clearCache()
    {
        delete map;
        map = new CIpPasswordHashTable();
    }

protected:
    //MORE: This cache needs to be invalidated at some point...
    CIpPasswordHashTable    *map;
    Owned<IConstEnvironment> env;
    Mutex mutex;
};

SDSPasswordProvider::SDSPasswordProvider() 
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    env.setown(factory->openEnvironment());
    map = new CIpPasswordHashTable();
}

bool SDSPasswordProvider::getPassword(const IpAddress & ip, StringBuffer & username, StringBuffer & password)
{
    synchronized procedure(mutex);
    if (!env)
        return false;
    CIpPasswordInstance *match = map->find((const char *)&ip,false);
    if (!match)
    {
        match = new CIpPasswordInstance(ip);

        StringBuffer ipText;
        ip.getIpText(ipText);
        Owned<IConstMachineInfo> machine = env->getMachineByAddress(ipText.str());
        if (machine)
        {
            Owned<IConstDomainInfo> domain = machine->getDomain();
            if (domain)
            {
                SCMStringBuffer username;

                StringAttrAdaptor strval(match->password);
                domain->getAccountInfo(username, strval);

                SCMStringBuffer domainname;
                domain->getName(domainname);
                match->user.set(domainname.s.append('\\').append(username.str()).str());
                match->matched = true;
            }
        }
        map->add(match);
    }
    username.append(match->user);
    password.append(match->password);
    return match->matched;
}

static CriticalSection passwordProviderCrit;
static SDSPasswordProvider * passwordProvider = NULL;
MODULE_INIT(INIT_PRIORITY_ENV_ENVIRONMENT)
{
    return true;
}

MODULE_EXIT()
{
    clearPasswordsFromSDS();
}

void __stdcall setPasswordsFromSDS()
{
    CriticalBlock block(passwordProviderCrit);
    if (passwordProvider == NULL)
    {
        passwordProvider = new SDSPasswordProvider();
        setPasswordProvider(passwordProvider);
    }
}

void __stdcall resetPasswordsFromSDS()
{
    CriticalBlock block(passwordProviderCrit);
    if (passwordProvider)
        passwordProvider->clearCache();
}

void __stdcall clearPasswordsFromSDS()
{
    CriticalBlock block(passwordProviderCrit);
    if (passwordProvider) {
        setPasswordProvider(NULL);
        passwordProvider->Release();
        passwordProvider = NULL;
    }
}



//---------------------------------------------------------------------------

static CriticalSection ipcachesect;
static CIpOsHashTable *ipToOsCache = NULL;

EnvMachineOS queryOS(const IpAddress & ip)
{
    if (ip.isLocal()) { // we know!
#ifdef _WIN32
        return MachineOsW2K;
#else
        return MachineOsLinux;
#endif
    }
    CriticalBlock block(ipcachesect);
    EnvMachineOS ret = MachineOsUnknown;
    if (!ipToOsCache) 
        ipToOsCache = new CIpOsHashTable;
    CIpOsInstance * match = ipToOsCache->find((const char *)&ip,false);
    if (match) 
        ret = match->os;
    else {
        Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
        if (factory) {
            Owned<IConstEnvironment> env = factory->openEnvironment();
            if (env) {
                StringBuffer ipText;
                ip.getIpText(ipText);
                Owned<IConstMachineInfo> machine = env->getMachineByAddress(ipText.str());
                if (machine) 
                    ret = machine->getOS();
            }
        }
        if (ret==MachineOsUnknown) { // lets try asking dafilesrv
            SocketEndpoint ep(0,ip);
            switch (getDaliServixOs(ep)) { 
              case DAFS_OSwindows: ret = MachineOsW2K; break;
              case DAFS_OSlinux:     ret = MachineOsLinux; break;
              case DAFS_OSsolaris: ret = MachineOsSolaris; break;
            }
        }
        match = new CIpOsInstance(ip);
        match->os = ret;
        ipToOsCache->add(match);
    }
    return ret;
}



bool canAccessFilesDirectly(const IpAddress & ip)
{
    if (ip.isLocal()||ip.isNull())  // the isNull check is probably an error but saves time
        return true;                // I think usually already checked, but another can't harm
#ifdef _WIN32
    EnvMachineOS os = queryOS(ip);
    if (os==MachineOsW2K)
        return true;
    if ((os==MachineOsUnknown)&&!testDaliServixPresent(ip))
        return true;    // maybe lucky if windows
#endif
    return false; 
}

bool canSpawnChildProcess(const IpAddress & ip)
{
    //MORE: This isn't the correct implementation, but at least the calls now have the 
    //correct name.
    return canAccessFilesDirectly(ip);
}


bool canAccessFilesDirectly(const char * ipText)
{
    IpAddress ip(ipText);
    return canAccessFilesDirectly(ip);
}

bool canAccessFilesDirectly(const RemoteFilename & file)
{
    if (file.queryEndpoint().port!=0)
        return false;
#ifdef _WIN32
    if (!file.isUnixPath()) // assume any windows path can be accessed using windows share
        return true;
#endif
    return canAccessFilesDirectly(file.queryIP());
}


void setCanAccessDirectly(RemoteFilename & file)
{
    setCanAccessDirectly(file,canAccessFilesDirectly(file));
}

class CDaliEnvIntercept: public CInterface, implements IRemoteFileCreateHook
{
    bool active;
    CriticalSection crit;
public:
    IMPLEMENT_IINTERFACE;
    CDaliEnvIntercept() { active = false; }
    virtual IFile * createIFile(const RemoteFilename & filename)
    {
        CriticalBlock block(crit);
        if (active||!daliClientActive())
            return NULL;
        active = true;
        IFile * ret;
        if (canAccessFilesDirectly(filename)) 
            ret = NULL;
        else 
            ret = createDaliServixFile(filename);   
        active = false;
        return ret;
    }   
} *DaliEnvIntercept;



MODULE_INIT(INIT_PRIORITY_ENV_DALIENV)
{
    DaliEnvIntercept = new CDaliEnvIntercept;
    addIFileCreateHook(DaliEnvIntercept);
    return true;
}

MODULE_EXIT()
{
    removeIFileCreateHook(DaliEnvIntercept);
    ::Release(DaliEnvIntercept);
    delete ipToOsCache;
    ipToOsCache = NULL;
}

//---------------------------------------------------------------------------

const char * querySlaveExecutable(const char * keyName, const char * exeName, const char * version, const IpAddress &ip, StringBuffer &progpath, StringBuffer &workdir)
{
    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    StringBuffer addr;
    ip.getIpText(addr);

    StringBufferAdaptor spp(progpath);
    StringBufferAdaptor swd(workdir);
    if (!env || !env->getRunInfo(spp, swd, keyName, version, addr.str(), exeName)) {
#ifdef _DEBUG
        //printf("slave path not found\n");
        progpath.append(exeName);
#ifdef _WIN32
        progpath.append(".exe");
#endif
#else
        throw MakeStringException(1, "Could not find the location of the slave program %s for machine %s", keyName, addr.str());
#endif
    }
    // on linux check that file exists where it is supposed to be 
#ifndef _WIN32
    if (progpath.length()) {
        RemoteFilename rfn;
        SocketEndpoint ep;
        ep.ipset(ip);
        rfn.setPath(ep,progpath.str());
        Owned<IFile> file = createIFile(rfn); 
        if (!file->exists())  {
            WARNLOG("Could not find the the slave program %s for machine %s at %s", keyName, addr.str(), progpath.str());
            throw MakeStringException(1, "Could not find the slave program %s for machine %s at %s", keyName, addr.str(), progpath.str());
        }
    }
#endif
    return progpath.str();
}


bool getRemoteRunInfo(const char * keyName, const char * exeName, const char * version, const IpAddress &ip, StringBuffer &progpath, StringBuffer &workdir, INode *remotedali, unsigned timeout)
{
    // use dafilesrv to work out OS
    StringBuffer dalis;
    if (remotedali)
        remotedali->endpoint().getUrlStr(dalis);
    // first get machine by IP
    StringBuffer ips;
    ip.getIpText(ips);

    //Cannot use getEnvironmentFactory() since it is using a remotedali
    StringBuffer xpath;
    xpath.appendf("Environment/Hardware/Computer[@netAddress=\"%s\"]", ips.str());
    Owned<IPropertyTreeIterator> iter = querySDS().getElementsRaw(xpath,remotedali,timeout);
    if (!iter->first()) {
        ERRLOG("Unable to find machine for %s on dali %s", ips.str(),dalis.str());
        return false;
    }
    Owned<IPropertyTree> machine;
    machine.setown(&iter->get());
    const char *domainname = machine->queryProp("@domain");
    if (!domainname||!*domainname) {
        ERRLOG("Unable to find domain for %s on dali %s", ips.str(),dalis.str());
        return false;
    }

    xpath.clear().appendf("Environment/Software/%s",keyName);
    if (version)
        xpath.appendf("[@version='%s']",version);
    xpath.append("/Instance");
    iter.clear();
    iter.setown(querySDS().getElementsRaw(xpath,remotedali,timeout));
    ForEach(*iter) {
        IPropertyTree *inst = &iter->query();
        const char * comp = inst->queryProp("@computer");
        if (comp) {
            xpath.clear().appendf("Environment/Hardware/Computer[@name=\"%s\"]", comp);
            Owned<IPropertyTreeIterator> iter2 = querySDS().getElementsRaw(xpath,remotedali,timeout);
            if (iter2->first()) {
                Owned<IPropertyTree> machine2= &iter2->get();
                const char *domainname2 = machine2->queryProp("@domain");
                const char *ips2 = machine2->queryProp("@netAddress");
                if (ips2&&*ips2&&domainname2&&*domainname2&&(strcmp(domainname,domainname2)==0)) {
                    bool appendexe;
                    char psep;
                    SocketEndpoint ep(ips2);
                    if (getDaliServixOs(ep)==DAFS_OSwindows) {
                        psep = '\\';
                        appendexe = true;
                    }
                    else {
                        psep = '/';
                        appendexe = false;
                    }
                    StringBuffer tmp;
                    const char *program = inst->queryProp("@program"); // if program specified assume absolute 
                    if (!program||!*program) {
                        tmp.append(psep).append(psep).append(ips2).append(psep).append(inst->queryProp("@directory")).append(psep).append(exeName);
                        size32_t l = strlen(exeName);
                        if (appendexe&&((l<5)||(stricmp(exeName+l-4,".exe")!=0)))
                            tmp.append(".exe");
                        program = tmp.str();
                    }
                    progpath.set(program);
                    const char *workd = inst->queryProp("@workdir"); // if program specified assume absolute 
                    workdir.set(workd?workd:"");
                    return true;
                }
            }
        }
    }
    return false;
}

bool envGetConfigurationDirectory(const char *category, const char *component,const char *instance, StringBuffer &dirout)
{
    SessionId sessid = myProcessSession();
    if (!sessid)
        return false;

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env)
    {
        Owned<IPropertyTree> root = &env->getPTree();
        IPropertyTree * child = root->queryPropTree("Software/Directories");
        if (child)
            return getConfigurationDirectory(child,category,component,instance,dirout);
    }
    return false;
}

IPropertyTree *envGetNASConfiguration(IPropertyTree *source)
{
    if ((NULL==source) || !source->hasProp("NAS"))
        return NULL;

    // check for NAS node : <Hardware><NAS><Filter ....><Filter ....>..</NAS></Hardware>
    if (source->hasProp("NAS/Filter"))
        return createPTreeFromIPT(source->queryPropTree("NAS"));
    else
    {
        // check for 'flat' format : <Hardware><NAS ...../><NAS ..../>....</Hardware>
        Owned<IPropertyTreeIterator> nasIter = source->getElements("NAS");
        if (!nasIter->first())
            return NULL;
        Owned<IPropertyTree> nas = createPTree("NAS");
        do
        {
            IPropertyTree *filter = &nasIter->query();
            nas->addPropTree("Filter", LINK(filter));
        }
        while (nasIter->next());
        return nas.getClear();
    }
}

IPropertyTree *envGetNASConfiguration()
{
    SessionId sessid = myProcessSession();
    if (!sessid)
        return NULL;

    Owned<IEnvironmentFactory> factory = getEnvironmentFactory();
    Owned<IConstEnvironment> env = factory->openEnvironment();
    if (env)
    {
        Owned<IPropertyTree> root = &env->getPTree();
        IPropertyTree * hardware = root->queryPropTree("Hardware");
        if (hardware)
            return envGetNASConfiguration(hardware);
    }
    return NULL;
}

IPropertyTree *envGetInstallNASHooks(SocketEndpoint *myEp)
{
    Owned<IPropertyTree> nasPTree = envGetNASConfiguration();
    return envGetInstallNASHooks(nasPTree, myEp);
}

IPropertyTree *envGetInstallNASHooks(IPropertyTree *nasPTree, SocketEndpoint *myEp)
{
    IDaFileSrvHook *daFileSrvHook = queryDaFileSrvHook();
    if (!daFileSrvHook) // probably always installed
        return NULL;
    daFileSrvHook->clearFilters();
    if (!nasPTree)
        return NULL;
    return daFileSrvHook->addMyFilters(nasPTree, myEp);
}

void envInstallNASHooks(SocketEndpoint *myEp)
{
    Owned<IPropertyTree> installedFilters = envGetInstallNASHooks(myEp);
}

void envInstallNASHooks(IPropertyTree *nasPTree, SocketEndpoint *myEp)
{
    Owned<IPropertyTree> installedFilters = envGetInstallNASHooks(nasPTree, myEp);
}
