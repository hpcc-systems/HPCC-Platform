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

// Entrypoint for ThorMaster.EXE

#include "platform.h"

#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h> 
#include <stdlib.h> 

#ifdef _WIN32
#include <direct.h> 
#endif

#include "build-config.h"
#include "jlib.hpp"
#include "jdebug.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jmutex.hpp"
#include "jprop.hpp"
#include "jset.hpp"
#include "jsocket.hpp"
#include "jthread.hpp"
#include "jexcept.hpp"

#include "mpbase.hpp"
#include "mplog.hpp"

#include "daaudit.hpp"
#include "daclient.hpp"
#include "dadfs.hpp"
#include "dalienv.hpp"
#include "dasds.hpp"
#include "dllserver.hpp"

#include "rmtfile.hpp"

#include "portlist.h"

#include "thor.hpp"
#include "thorport.hpp"
#include "thormisc.hpp"

#include "thgraph.hpp"
#include "thgraphmaster.hpp"
#include "thgraphmanager.hpp"
#include "thmastermain.hpp"
#include "mawatchdog.hpp"
#include "thexception.hpp"
#include "thmem.hpp"

#define DEFAULT_QUERY_SO_DIR "sodir"
#define MAX_SLAVEREG_DELAY 60*1000*15 // 15 mins
#define SLAVEREG_VERIFY_DELAY 5*1000
#define SHUTDOWN_IN_PARALLEL 20



class CRegistryServer : public CSimpleInterface
{
    unsigned msgDelay, slavesRegistered;
    CriticalSection crit;
    bool stopped;

    static CriticalSection regCrit;
    static CRegistryServer *registryServer;

    class CDeregistrationWatch : implements IThreaded
    {
        CThreaded threaded;
        CRegistryServer &registry;
        bool running;
    public:
        CDeregistrationWatch(CRegistryServer &_registry) : threaded("CDeregistrationWatch"), registry(_registry), running(false) { }
        ~CDeregistrationWatch()
        {
            stop();
        }
        void start() { threaded.init(this); }
        void stop()
        {
            if (running)
            {
                running = false;
                queryWorldCommunicator().cancel(NULL, MPTAG_THORREGISTRATION);
                threaded.join();
            }
        }
        virtual void main()
        {
            running = true;
            loop
            {
                INode *senderNode;
                CMessageBuffer msg;
                if (!queryWorldCommunicator().recv(msg, NULL, MPTAG_THORREGISTRATION, &senderNode))
                    return;
                rank_t sender = queryClusterGroup().rank(senderNode);
                SocketEndpoint ep = senderNode->endpoint();
                StringBuffer url;
                ep.getUrlStr(url);
                if (RANK_NULL == sender)
                {
                    PROGLOG("Node %s trying to deregister is not part of this cluster", url.str());
                    continue;
                }
                RegistryCode code;
                msg.read((int &)code);
                if (!rc_deregister == code)
                    throwUnexpected();
                registry.deregisterNode(sender);
            }
            running = false;
        }
    } deregistrationWatch;
public:
    Linked<CMasterWatchdogBase> watchdog;
    IBitSet *status;

    CRegistryServer()  : deregistrationWatch(*this), stopped(false)
    {
        status = createBitSet();
        msgDelay = SLAVEREG_VERIFY_DELAY;
        slavesRegistered = 0;
        if (globals->getPropBool("@watchdogEnabled"))
            watchdog.setown(createMasterWatchdog(globals->getPropBool("@useUDPWatchdog")));
        else
            globals->setPropBool("@watchdogProgressEnabled", false);
        CriticalBlock b(regCrit);
        registryServer = this;
    }
    ~CRegistryServer()
    {
        CriticalBlock b(regCrit);
        registryServer = NULL;
        stop();
        if (watchdog)
            watchdog->stop();
        shutdown();
        status->Release();
    }
    static CRegistryServer *getRegistryServer()
    {
        CriticalBlock b(regCrit);
        return LINK(registryServer);
    }
    void deregisterNode(unsigned slave)
    {
        const SocketEndpoint &ep = queryClusterGroup().queryNode(slave).endpoint();
        StringBuffer url;
        ep.getUrlStr(url);
        if (!status->test(slave-1))
        {
            PROGLOG("Slave %d (%s) trying to unregister, but not currently registered", slave, url.str());
            return;
        }
        PROGLOG("Slave %d (%s) unregistered", slave, url.str());
        status->set(slave-1, false);
        --slavesRegistered;
        if (watchdog)
            watchdog->removeSlave(ep);
        abortThor(MakeThorOperatorException(TE_AbortException, "The machine %s and/or the slave was shutdown. Aborting Thor", url.str()));
    }
    void registerNode(unsigned slave)
    {
        SocketEndpoint ep = queryClusterGroup().queryNode(slave).endpoint();
        StringBuffer url;
        ep.getUrlStr(url);
        if (status->test(slave-1))
        {
            PROGLOG("Slave %d (%s) already registered, rejecting", slave, url.str());
            return;
        }
        PROGLOG("Slave %d (%s) registered", slave, url.str());
        status->set(slave-1);
        if (watchdog)
            watchdog->addSlave(ep);
        ++slavesRegistered;
    }
    bool connect()
    {
        LOG(MCdebugProgress, thorJob, "Waiting for %d slaves to register", queryClusterWidth());
        unsigned timeWaited = 0;
        unsigned connected = 0;
        unsigned slaves = queryClusterWidth();
        Owned<IBitSet> connectedSet = createBitSet();
        loop
        {
            CTimeMon tm(msgDelay);
            UnsignedArray todo;
            unsigned s = 0;
            while ((s=connectedSet->scan(s, false)) < slaves)
                todo.append(s++);
            Owned<IShuffledIterator> shuffled = createShuffledIterator(todo.ordinality());
            ForEach(*shuffled) 
            {
                s = todo.item(shuffled->get());
                unsigned remaining;
                if (tm.timedout(&remaining))
                    break;
                PROGLOG("Verifying connection to slave %d", s+1);
                if (queryWorldCommunicator().verifyConnection(&queryClusterGroup().queryNode(s+1), remaining))
                {
                    StringBuffer str;
                    PROGLOG("verified connection with %s", queryClusterGroup().queryNode(s+1).endpoint().getUrlStr(str.clear()).str());
                    ++connected;
                    connectedSet->set(s);
                }
                if (stopped)
                    return false;
            }
            timeWaited += tm.elapsed();
            if (connected == slaves)
                break;
            else
            {
                if (timeWaited >= MAX_SLAVEREG_DELAY)
                    throw MakeThorException(TE_AbortException, "Have waited over %d minutes for all slaves to connect, quitting.", MAX_SLAVEREG_DELAY/1000/60);
                unsigned outstanding = slaves - connected;
                PROGLOG("Still Waiting for minimum %d slaves to connect", outstanding);
                if ((outstanding) <= 5)
                {
                    unsigned s=0;
                    loop
                    {
                        unsigned ns = connectedSet->scan(s, false);
                        if (ns<s || ns >= slaves)
                            break;
                        s = ns+1;
                        StringBuffer str;
                        PROGLOG("waiting for slave %d (%s)", s, queryClusterGroup().queryNode(s).endpoint().getUrlStr(str.clear()).str());
                    }
                }
                msgDelay = (unsigned) ((float)msgDelay * 1.5);
                if (timeWaited+msgDelay > MAX_SLAVEREG_DELAY)
                    msgDelay = MAX_SLAVEREG_DELAY - timeWaited;
            }
        }
        
        PROGLOG("Slaves connected, initializing..");
        CMessageBuffer msg;
        msg.append(THOR_VERSION_MAJOR).append(THOR_VERSION_MINOR);
        queryClusterGroup().serialize(msg);
        globals->serialize(msg);
        msg.append(masterSlaveMpTag);
        if (!queryClusterComm().send(msg, RANK_ALL_OTHER, MPTAG_THORREGISTRATION, MP_ASYNC_SEND))
        {
            PROGLOG("Failed to initialize slaves");
            return false;
        }
        PROGLOG("Initialization sent to slave group");
        try
        {
            while (slavesRegistered < slaves)
            {
                rank_t sender;
                CMessageBuffer msg;
                if (!queryClusterComm().recv(msg, RANK_ALL, MPTAG_THORREGISTRATION, &sender, MAX_SLAVEREG_DELAY))
                {
                    PROGLOG("Slaves not responding to cluster initialization: ");
                    unsigned s=0;
                    loop
                    {
                        unsigned ns = status->scan(s, false);
                        if (ns<s || ns >= slaves)
                            break;
                        s = ns+1;
                        StringBuffer str;
                        PROGLOG("Slave %d (%s)", s, queryClusterGroup().queryNode(s).endpoint().getUrlStr(str.clear()).str());
                    }
                    throw MakeThorException(TE_AbortException, "Slaves failed to respond to cluster initialization");
                }
                StringBuffer str;
                PROGLOG("Registration confirmation from %s", queryClusterGroup().queryNode(sender).endpoint().getUrlStr(str).str());
                if (msg.length())
                {
                    Owned<IException> e = deserializeException(msg);
                    EXCLOG(e, "Registration error");
                    if (TE_FailedToRegisterSlave == e->errorCode())
                    {
                        setExitCode(0); // to avoid thor auto-recycling
                        return false;
                    }
                }
                registerNode(sender);
            }
            PROGLOG("Slaves initialized");
            unsigned s=0;
            for (; s<slaves; s++)
            {
                CMessageBuffer msg;
                if (!queryClusterComm().send(msg, s+1, MPTAG_THORREGISTRATION))
                {
                    PROGLOG("Failed to acknowledge slave %d registration", s+1);
                    return false;
                }
            }
            deregistrationWatch.start();
            return true;
        }
        catch (IException *e)
        {
            EXCLOG(e, "Slave registration exception");
            e->Release();
        }
        shutdown();
        return false;   
    }
    void stop()
    {
        if (stopped)
            return;
        stopped = true;
        deregistrationWatch.stop();
        queryWorldCommunicator().cancel(NULL, MPTAG_THORREGISTRATION);
    }
    void shutdown()
    {
        CriticalBlock block(crit);
        unsigned i=0;
        for (; i<queryClusterWidth(); i++)
        {
            if (status->test(i))
            {
                status->set(i, false);
                SocketEndpoint ep = queryClusterGroup().queryNode(i+1).endpoint();
                if (watchdog)
                    watchdog->removeSlave(ep);
                CMessageBuffer msg;
                msg.append((unsigned)Shutdown);
                try
                {
                    queryClusterComm().send(msg, i+1, masterSlaveMpTag, MP_ASYNC_SEND);
                }
                catch (IMP_Exception *e) { e->Release(); }
                catch (IException *e)
                {
                    EXCLOG(e, "Shutting down slave");
                    e->Release();
                }
            }
        }
    }
};

CriticalSection CRegistryServer::regCrit;
CRegistryServer *CRegistryServer::registryServer = NULL;


//
//////////////////

bool checkClusterRelicateDAFS(IGroup *grp)
{
    // check the dafilesrv is running (and right version) 
    unsigned start = msTick();
    PROGLOG("Checking cluster replicate nodes");
    SocketEndpointArray epa;
    grp->getSocketEndpoints(epa);
    ForEachItemIn(i1,epa) {
        epa.item(i1).port = getDaliServixPort();
    }
    SocketEndpointArray failures;
    UnsignedArray failedcodes;
    StringArray failedmessages;
    validateNodes(epa,NULL,NULL,true,NULL,0,failures,failedcodes,failedmessages);
    ForEachItemIn(i,failures) {
        SocketEndpoint ep(failures.item(i));
        ep.port = 0;
        StringBuffer ips;
        ep.getIpText(ips);
        FLLOG(MCoperatorError, thorJob, "VALIDATE FAILED(%d) %s : %s",failedcodes.item(i),ips.str(),failedmessages.item(i));
    }
    PROGLOG("Cluster replicate nodes check completed in %dms",msTick()-start);
    return (failures.ordinality()==0);
}



static bool auditStartLogged = false;

static bool firstCtrlC = true;
bool ControlHandler() 
{ 
    if (firstCtrlC)
    {
        LOG(MCdebugProgress, thorJob, "CTRL-C detected");
        firstCtrlC = false;
        {
            Owned<CRegistryServer> registry = CRegistryServer::getRegistryServer();
            if (registry)
                registry->stop();
        }
        abortThor();
    }
    else
    {
        LOG(MCdebugProgress, thorJob, "2nd CTRL-C detected - terminating process");

        if (auditStartLogged)
        {
            auditStartLogged = false;
            LOG(daliAuditLogCat,",Progress,Thor,Terminate,%s,%s,%s,ctrlc",
                queryServerStatus().queryProperties()->queryProp("@thorname"),
                queryServerStatus().queryProperties()->queryProp("@nodeGroup"),
                queryServerStatus().queryProperties()->queryProp("@queue"));
        }
        queryLogMsgManager()->flushQueue(10*1000);
#ifdef _WIN32
        TerminateProcess(GetCurrentProcess(), 1);
#else
        //MORE- verify this
        kill(getpid(), SIGKILL);
#endif
        _exit(1);
    }
    return false; 
} 


#include "thactivitymaster.hpp"
int main( int argc, char *argv[]  )
{
#if defined(WIN32) && defined(_DEBUG)
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag( tmpFlag );
#endif

    loadMasters(); // actually just a dummy call to ensure dll linked
    InitModuleObjects();
    NoQuickEditSection xxx;
    {
        Owned<IFile> iFile = createIFile("thor.xml");
        globals = iFile->exists() ? createPTree(*iFile, ipt_caseInsensitive) : createPTree("Thor", ipt_caseInsensitive);
    }
    globals->setProp("@masterBuildTag", BUILD_TAG);
    char **pp = argv+1;
    while (*pp)
        loadCmdProp(globals, *pp++);

    setIORetryCount(globals->getPropInt("Debug/@ioRetries")); // default == 0 == off
    StringBuffer daliServer;
    if (!globals->getProp("@DALISERVERS", daliServer)) 
    {
        LOG(MCerror, thorJob, "No Dali server list specified in THOR.XML (DALISERVERS=iport,iport...)\n");
        return 0; // no recycle
    }

    SocketEndpoint thorEp;
    const char *master = globals->queryProp("@MASTER");
    if (master)
    {
        thorEp.set(master);
        thorEp.setLocalHost(thorEp.port);
    }
    else
        thorEp.setLocalHost(0);

    setMasterPortBase(thorEp.port); // both same
    thorEp.port = getMasterPortBase();

    // Remove sentinel asap
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);

    setMachinePortBase(thorEp.port);
    unsigned slavePort = globals->hasProp("@slaveport")?atoi(globals->queryProp("@slaveport")):THOR_BASESLAVE_PORT;

    EnableSEHtoExceptionMapping(); 
#ifndef __64BIT__
    Thread::setDefaultStackSize(0x10000);   // NB under windows requires linker setting (/stack:)
#endif
    const char *thorname = NULL;
    StringBuffer nodeGroup, logUrl;
    Owned<IPerfMonHook> perfmonhook;

    ILogMsgHandler *logHandler;
    try
    {
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(globals, "thor");
            lf->setName("thormaster");//override default filename
            lf->setCreateAliasFile(false);
            logHandler = lf->beginLogging();
            createUNCFilename(lf->queryLogFileSpec(), logUrl, false);
        }
        LOG(MCdebugProgress, thorJob, "Opened log file %s", logUrl.toCharArray());
        LOG(MCdebugProgress, thorJob, "Build %s", BUILD_TAG);
        globals->setProp("@logURL", logUrl.str());

        Owned<IGroup> serverGroup = createIGroup(daliServer.str(), DALI_SERVER_PORT);

        unsigned retry = 0;
        loop {
            try {
                unsigned port = getFixedPort(TPORT_mp);
                LOG(MCdebugProgress, thorJob, "calling initClientProcess Port %d", port);
                initClientProcess(serverGroup, DCR_ThorMaster, port);
                break;
            }
            catch (IJSOCK_Exception *e) { 
                if ((e->errorCode()!=JSOCKERR_port_in_use))
                    throw;
                FLLOG(MCexception(e), thorJob, e,"InitClientProcess");
                if (retry++>10) 
                    throw;
                e->Release();
                LOG(MCdebugProgress, thorJob, "Retrying");
                Sleep(retry*2000);  
            }
        }

        setPasswordsFromSDS();

        if (globals->getPropBool("@enableSysLog",true))
            UseSysLogForOperatorMessages();

        thorname = globals->queryProp("@name");
        if (!thorname)
        {
            PROGLOG("No 'name' setting, defaulting to \"local\"");
            thorname = "local";
            globals->setProp("@name", thorname);
        }
        Owned<IGroup> thorGroup;
        OwnedIFile iFile = createIFile("thorgroup");
        if (iFile->exists())
        {
            PROGLOG("Found file 'thorgroup', using to form thor group");
            OwnedIFileIO iFileIO = iFile->open(IFOread);
            MemoryBuffer slaveListMb;
            size32_t sz = (size32_t)iFileIO->size();
            if (sz)
            {
                void *b = slaveListMb.reserveTruncate(sz);
                verifyex(sz = iFileIO->read(0, sz, b));
                
                IArrayOf<INode> nodes;
                nodes.append(*LINK(queryMyNode()));

                const char *n = (const char *)b;
                const char *e = n + sz;
                loop
                {
                    while (n < e && isspace(*n)) n++;
                    if (n == e || !*n) break;
                    const char *start = n;
                    while (!isspace(*n)) n++;
                    StringBuffer nodeStr;
                    nodeStr.append(n-start, start);
                    SocketEndpoint ep = nodeStr.str();
                    ep.port = getFixedPort(ep.port, TPORT_mp);
                    INode *node = createINode(ep);
                    nodes.append(*node);
                }
                assertex(nodes.ordinality());
                thorGroup.setown(createIGroup(nodes.ordinality(), nodes.getArray()));
            }
            else
            {
                ERRLOG("'thorgroup' file is empty");
                return 0; // no recycle
            }
        }
        if (!globals->getProp("@nodeGroup", nodeGroup)) {
            nodeGroup.append(thorname);
            globals->setProp("@nodeGroup", thorname);
        }        
        if (!thorGroup)
        {
            thorGroup.setown(queryNamedGroupStore().lookup(nodeGroup.str()));
            if (!thorGroup)
            {
                ERRLOG("Named group '%s' not found", nodeGroup.str());
                return 0; // no recycle
            }
            else
            {
                IArrayOf<INode> nodes;
                nodes.append(*LINK(queryMyNode()));
                unsigned r=0;
                for (; r<thorGroup->ordinality(); r++)
                {
                    SocketEndpoint ep = thorGroup->queryNode(r).endpoint();
                    if (!ep.port)
                        ep.port = getFixedPort(slavePort, TPORT_mp);
                    nodes.append(*createINode(ep));
                }
                thorGroup.setown(createIGroup(nodes.ordinality(), nodes.getArray()));
            }
        }
        setClusterGroup(thorGroup);
        if (globals->getPropBool("@replicateOutputs")&&globals->getPropBool("@validateDAFS",true)&&!checkClusterRelicateDAFS(thorGroup)) {
            FLLOG(MCoperatorError, thorJob, "ERROR: Validate failure(s) detected, exiting Thor");
            return globals->getPropBool("@validateDAFSretCode"); // default is no recycle!
        }

        if (globals->getPropBool("@useNASTranslation", true))
        {
            Owned<IPropertyTree> nasConfig = envGetNASConfiguration();
            if (nasConfig)
                globals->setPropTree("NAS", nasConfig.getLink()); // for use by slaves
            Owned<IPropertyTree> masterNasFilters = envGetInstallNASHooks(nasConfig, &thorEp);
        }
        
        HardwareInfo hdwInfo;
        getHardwareInfo(hdwInfo);
        globals->setPropInt("@masterTotalMem", hdwInfo.totalMemory);
        unsigned gmemSize = globals->getPropInt("@globalMemorySize"); // in MB
        if (0 == gmemSize)
        {
            unsigned maxMem = hdwInfo.totalMemory;
#ifdef _WIN32
            if (maxMem > 2048)
                maxMem = 2048;
#else
#ifndef __64BIT__
            if (maxMem > 2048)
            {
            	// 32 bit OS doesn't handle whole physically installed RAM
                maxMem = 2048;
            }
#endif
#endif
            gmemSize = maxMem * 3 / 4; // default to 75% of total
            unsigned perSlaveSize = gmemSize;
            unsigned slavesPerNode = globals->getPropInt("@slavesPerNode", 1);
            if (slavesPerNode>1)
            {
                PROGLOG("Sharing globalMemorySize(%d MB), between %d slave. %d MB each", perSlaveSize, slavesPerNode, perSlaveSize / slavesPerNode);
                perSlaveSize /= slavesPerNode;
            }
            globals->setPropInt("@globalMemorySize", perSlaveSize);
        }
        else if (gmemSize >= hdwInfo.totalMemory)
        {
            // should prob. error here
        }
        unsigned gmemSizeMaster = globals->getPropInt("@masterMemorySize", gmemSize); // in MB
        bool gmemAllowHugePages = globals->getPropBool("@heapUseHugePages", false);

        // if @masterMemorySize and @globalMemorySize unspecified gmemSize will be default based on h/w
        globals->setPropInt("@masterMemorySize", gmemSizeMaster);

        PROGLOG("Global memory size = %d MB", gmemSizeMaster);
        roxiemem::setTotalMemoryLimit(gmemAllowHugePages, ((memsize_t)gmemSizeMaster) * 0x100000, 0, NULL);

        const char * overrideBaseDirectory = globals->queryProp("@thorDataDirectory");
        const char * overrideReplicateDirectory = globals->queryProp("@thorReplicateDirectory");
        StringBuffer datadir;
        StringBuffer repdir;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"data","thor",globals->queryProp("@name"),datadir))
            overrideBaseDirectory = datadir.str();
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"mirror","thor",globals->queryProp("@name"),repdir))
            overrideReplicateDirectory = repdir.str();
        if (overrideBaseDirectory&&*overrideBaseDirectory)
            setBaseDirectory(overrideBaseDirectory, false);
        if (overrideReplicateDirectory&&*overrideBaseDirectory)
            setBaseDirectory(overrideReplicateDirectory, true);
        StringBuffer tempDirStr;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"temp","thor",globals->queryProp("@name"), tempDirStr))
            globals->setProp("@thorTempDirectory", tempDirStr.str());
        else
            tempDirStr.append(globals->queryProp("@thorTempDirectory"));
        logDiskSpace(); // Log before temp space is cleared
        StringBuffer tempPrefix("thtmp");
        tempPrefix.append(getMasterPortBase()).append("_");
        SetTempDir(tempDirStr.str(), tempPrefix.str(), true);

        char thorPath[1024];
        if (!GetCurrentDirectory(1024, thorPath)) {
            ERRLOG("ThorMaster::main: Current directory path too big, setting it to null");
            thorPath[0] = 0;
        }
        unsigned l = strlen(thorPath);
        if (l) { thorPath[l] = PATHSEPCHAR; thorPath[l+1] = '\0'; }
        globals->setProp("@thorPath", thorPath);

        StringBuffer soDir, soPath;
        if (getConfigurationDirectory(globals->queryPropTree("Directories"),"query","thor",globals->queryProp("@name"),soDir))
            globals->setProp("@query_so_dir", soDir.str());
        else if (!globals->getProp("@query_so_dir", soDir)) {
            globals->setProp("@query_so_dir", DEFAULT_QUERY_SO_DIR); 
            soDir.append(DEFAULT_QUERY_SO_DIR);
        }
        if (isAbsolutePath(soDir.str()))
            soPath.append(soDir);
        else
        {
            soPath.append(thorPath);
            addPathSepChar(soPath);
            soPath.append(soDir);
        }
        addPathSepChar(soPath);
        globals->setProp("@query_so_dir", soPath.str());
        recursiveCreateDirectory(soPath.str());

        startLogMsgParentReceiver();    
        connectLogMsgManagerToDali();
        if (globals->getPropBool("@cache_dafilesrv_master",false))
            setDaliServixSocketCaching(true); // speeds up deletes under linux

        unsigned pinterval = globals->getPropInt("@system_monitor_interval",1000*60);
        if (pinterval) {
            perfmonhook.setown(createThorMemStatsPerfMonHook());
            startPerformanceMonitor(pinterval,PerfMonStandard,perfmonhook);
        }
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), thorJob, e,"ThorMaster");
        e->Release();
        return -1;
    }
    StringBuffer queueName;
    SCMStringBuffer _queueNames;
    const char *thorName = globals->queryProp("@name");
    if (!thorName) thorName = "thor";
    getThorQueueNames(_queueNames, thorName);
    queueName.set(_queueNames.str());

    try {
        CSDSServerStatus &serverStatus = openThorServerStatus();

        Owned<CRegistryServer> registry = new CRegistryServer();
        StringBuffer thorEpStr;
        LOG(MCdebugProgress, thorJob, "ThorMaster version %d.%d, Started on %s", THOR_VERSION_MAJOR,THOR_VERSION_MINOR,thorEp.getUrlStr(thorEpStr).toCharArray());
        LOG(MCdebugProgress, thorJob, "Thor name = %s, queue = %s, nodeGroup = %s",thorname,queueName.str(),nodeGroup.str());

        serverStatus.queryProperties()->setProp("@thorname", thorname);
        serverStatus.queryProperties()->setProp("@cluster", nodeGroup.str()); // JCSMORE rename
        serverStatus.queryProperties()->setProp("LogFile", logUrl.str()); // LogFile read by eclwatch (possibly)
        serverStatus.queryProperties()->setProp("@nodeGroup", nodeGroup.str());
        serverStatus.queryProperties()->setProp("@queue", queueName.str());
        serverStatus.commitProperties();

        addAbortHandler(ControlHandler);
        masterSlaveMpTag = allocateClusterMPTag();

        writeSentinelFile(sentinelFile);

        if (registry->connect())
        {
            PROGLOG("verifying mp connection to rest of cluster");
            if (!queryClusterComm().verifyAll())
                ERRLOG("Failed to connect to all nodes");
            else
                PROGLOG("verified mp connection to rest of cluster");

            LOG(daliAuditLogCat, ",Progress,Thor,Startup,%s,%s,%s,%s",nodeGroup.str(),thorname,queueName.str(),logUrl.str());
            auditStartLogged = true;

            thorMain(logHandler);
            LOG(daliAuditLogCat, ",Progress,Thor,Terminate,%s,%s,%s",thorname,nodeGroup.str(),queueName.str());
        }
        else
            PROGLOG("Registration aborted");
        LOG(MCdebugProgress, thorJob, "ThorMaster terminated OK");
    }
    catch (IException *e) 
    {
        FLLOG(MCexception(e), thorJob, e,"ThorMaster");
        e->Release();
    }
    disconnectLogMsgManagerFromDali();
    closeThorServerStatus();
    if (globals) globals->Release();
    PROGLOG("Thor closing down 6");

    stopPerformanceMonitor();
    PROGLOG("Thor closing down 5");
    PROGLOG("Thor closing down 4");
    closeDllServer();
    PROGLOG("Thor closing down 3");
    closeEnvironment();
    PROGLOG("Thor closing down 2");
    closedownClientProcess();
    PROGLOG("Thor closing down 1");
    UseSysLogForOperatorMessages(false);
    releaseAtoms(); // don't know why we can't use a module_exit to destruct this...

    return queryExitCode();
}
