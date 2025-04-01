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

// Entrypoint for ThorSlave.EXE

#include "platform.h"

#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h> 
#include <stdlib.h> 

#include "jlib.hpp"
#include "jcontainerized.hpp"
#include "jdebug.hpp"
#include "jexcept.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jprop.hpp"
#include "jthread.hpp"

#include "thormisc.hpp"
#include "slavmain.hpp"
#include "thorport.hpp"
#include "thexception.hpp"
#include "thmem.hpp"
#include "thbuf.hpp"

#include "mpbase.hpp"
#include "mplog.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "slave.hpp"
#include "portlist.h"
#include "dafdesc.hpp"
#include "rmtfile.hpp"

#ifdef _CONTAINERIZED
#include "dafsserver.hpp"
#endif

// #define USE_MP_LOG

static INode *masterNode = NULL;
MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}
MODULE_EXIT()
{
    ::Release(masterNode);
}

#ifdef _DEBUG
USE_JLIB_ALLOC_HOOK;
#endif

static SocketEndpoint slfEp;
static unsigned mySlaveNum;

static const unsigned defaultStrandBlockSize = 512;
static const unsigned defaultForceNumStrands = 0;

static const char **cmdArgs;
static ILogMsgHandler *logHandler = nullptr;

static void replyError(unsigned errorCode, const char *errorMsg)
{
    SocketEndpoint myEp = queryMyNode()->endpoint();
    StringBuffer str("Node '");
    myEp.getEndpointHostText(str);
    str.append("' exception: ").append(errorMsg);
    Owned<IException> e = MakeStringException(errorCode, "%s", str.str());
    CMessageBuffer msg;
    serializeException(e, msg);
    queryNodeComm().send(msg, 0, MPTAG_THORREGISTRATION);
}

static void setSlaveAffinity(unsigned processOnNode)
{
    const char * affinity = globals->queryProp("@affinity");
    if (affinity)
        setProcessAffinity(affinity);
    else if (globals->getPropBool("@autoAffinity", true))
    {
        const char * nodes = globals->queryProp("@autoNodeAffinityNodes");
        unsigned slavesPerNode = globals->getPropInt("@slavesPerNode", 1);
        setAutoAffinity(processOnNode, slavesPerNode, nodes);
    }

    //The default policy is to allocate from the local node, so restricting allocations to the current sockets
    //may not buy much once the affinity is set up.  It also means it will fail if there is no memory left on
    //this socket - even if there is on others.
    //Therefore it is not recommended unless you have maybe several independent thors running on the same machines
    //with exclusive access to memory.
    if (globals->getPropBool("@numaBindLocal", false))
        bindMemoryToLocalNodes();
}

static std::atomic<bool> isRegistered {false};

static bool RegisterSelf(SocketEndpoint &masterEp)
{
    StringBuffer slfStr;
    StringBuffer masterStr;
    PROGLOG("registering %s - master %s",slfEp.getEndpointHostText(slfStr).str(),masterEp.getEndpointHostText(masterStr).str());
    try
    {
        SocketEndpoint ep = masterEp;
        ep.port = getFixedPort(getMasterPortBase(), TPORT_mp);
        Owned<INode> masterNode = createINode(ep);
        CMessageBuffer msg;
        msg.append(mySlaveNum);
        if (isContainerized())
        {
            msg.append(k8s::queryMyPodName());
            msg.append(k8s::queryMyContainerName());
        }
        queryWorldCommunicator().send(msg, masterNode, MPTAG_THORREGISTRATION);
        if (!queryWorldCommunicator().recv(msg, masterNode, MPTAG_THORREGISTRATION))
            return false;
        DBGLOG("Initialization received");
        unsigned vmajor, vminor;
        msg.read(vmajor);
        msg.read(vminor);
        Owned<IGroup> processGroup = deserializeIGroup(msg);
        Owned<IPropertyTree> masterComponentConfig = createPTree(msg);
        Owned<IPropertyTree> masterGlobalConfig = createPTree(msg);
        mySlaveNum = (unsigned)processGroup->rank(queryMyNode());
        assertex(NotFound != mySlaveNum);
        mySlaveNum++; // 1 based;

        unsigned configSlaveNum = globals->getPropInt("@slavenum", NotFound);
        if (NotFound == configSlaveNum)
            globals->setPropInt("@slavenum", mySlaveNum);
        else
            assertex(mySlaveNum == configSlaveNum);

        Owned<IPropertyTree> mergedComponentConfig = createPTreeFromIPT(globals);
        mergeConfiguration(*mergedComponentConfig, *masterComponentConfig);
        if (masterComponentConfig->hasProp("logging/@thorworkerdetail"))
        {
            unsigned workerDetailLevel = masterComponentConfig->getPropInt("logging/@thorworkerdetail");
            mergedComponentConfig->setPropInt("logging/@detail", workerDetailLevel);
            ILogMsgFilter *existingLogFilter = queryLogMsgManager()->queryMonitorFilter(logHandler);
            dbgassertex(existingLogFilter);
            if (existingLogFilter->queryMaxDetail() != workerDetailLevel)
                verifyex(queryLogMsgManager()->changeMonitorFilterOwn(logHandler, getCategoryLogMsgFilter(existingLogFilter->queryAudienceMask(), existingLogFilter->queryClassMask(), workerDetailLevel)));
        }
        replaceComponentConfig(mergedComponentConfig, masterGlobalConfig);
        globals.set(mergedComponentConfig);
#ifdef _DEBUG
        unsigned holdSlave = globals->getPropInt("@holdSlave", NotFound);
        if (mySlaveNum == holdSlave)
        {
            DBGLOG("Thor slave %u paused for debugging purposes, attach and set held=false to release", mySlaveNum);
            bool held = true;
            while (held)
                Sleep(5);
        }
#endif
        unsigned channelsPerSlave = globals->getPropInt("@channelsPerSlave", 1);
        unsigned localThorPortInc = globals->getPropInt("@localThorPortInc", DEFAULT_WORKERPORTINC);
        unsigned slaveBasePort = globals->getPropInt("@slaveport", DEFAULT_THORWORKERPORT);
        setupCluster(masterNode, processGroup, channelsPerSlave, slaveBasePort, localThorPortInc);

        if (vmajor != THOR_VERSION_MAJOR || vminor != THOR_VERSION_MINOR)
        {
            replyError(TE_FailedToRegisterSlave, "Thor master/slave version mismatch");
            return false;
        }

        if (!applyResourcedCPUAffinity(globals->queryPropTree("workerResources")))
        {
            // NB: autoAffinity/affinity only applicable in the absence of workerResources.cpu
            setSlaveAffinity(globals->getPropInt("@slaveprocessnum"));
        }

        StringBuffer xpath;
        getExpertOptPath(nullptr, xpath); // 'expert' in container world, or 'Debug' in bare-metal
        ensurePTree(globals, xpath);
        unsigned numStrands, blockSize;
        workunitGraphCacheEnabled = getExpertOptBool("workunitGraphCacheEnabled", workunitGraphCacheEnabled);
        getExpertOptPath("forceNumStrands", xpath.clear());
        if (globals->hasProp(xpath))
            numStrands = globals->getPropInt(xpath);
        else
        {
            numStrands = defaultForceNumStrands;
            globals->setPropInt(xpath, defaultForceNumStrands);
        }
        getExpertOptPath("strandBlockSize", xpath.clear());
        if (globals->hasProp(xpath))
            blockSize = globals->getPropInt(xpath);
        else
        {
            blockSize = defaultStrandBlockSize;
            globals->setPropInt(xpath, defaultStrandBlockSize);
        }
        DBGLOG("Strand defaults: numStrands=%u, blockSize=%u", numStrands, blockSize);

        const char *_masterBuildTag = globals->queryProp("@masterBuildTag");
        const char *masterBuildTag = _masterBuildTag?_masterBuildTag:"no build tag";
        PROGLOG("Master build: %s", masterBuildTag);
        if (!_masterBuildTag || 0 != strcmp(hpccBuildInfo.buildTag, _masterBuildTag))
        {
            StringBuffer errStr("Thor master/slave build mismatch, master = ");
            errStr.append(masterBuildTag).append(", slave = ").append(hpccBuildInfo.buildTag);
            OERRLOG("%s", errStr.str());
#ifndef _DEBUG
            replyError(TE_FailedToRegisterSlave, errStr.str());
            return false;
#endif
        }
        readUnderlyingType<mptag_t>(msg, managerWorkerMpTag);
        readUnderlyingType<mptag_t>(msg, kjServiceMpTag);

        msg.clear();
        if (!queryNodeComm().send(msg, 0, MPTAG_THORREGISTRATION))
            return false;
        DBGLOG("Registration confirmation sent");

        if (!queryNodeComm().recv(msg, 0, MPTAG_THORREGISTRATION))
            return false;
        DBGLOG("Registration confirmation receipt received");

        ::masterNode = LINK(masterNode);

        DBGLOG("verifying mp connection to rest of cluster");
        if (!queryNodeComm().verifyAll())
            OERRLOG("Failed to connect to all nodes");
        else
            DBGLOG("verified mp connection to rest of cluster");
        PROGLOG("registered %s",slfStr.str());
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), e,"slave registration error");
        e->Release();
        return false;
    }
    isRegistered = true;
    return true;
}

static bool jobListenerStopped = true;

bool UnregisterSelf(IException *e)
{
    if (!hasMPServerStarted())
        return false;

    if (!isRegistered)
        return false;

    StringBuffer slfStr;
    slfEp.getEndpointHostText(slfStr);
    DBGLOG("Unregistering slave : %s", slfStr.str());
    try
    {
        CMessageBuffer msg;
        msg.append(rc_deregister);
        serializeException(e, msg); // NB: allows exception to be NULL
        if (!queryWorldCommunicator().send(msg, masterNode, MPTAG_THORREGISTRATION, 60*1000))
        {
            IERRLOG("Failed to unregister slave : %s", slfStr.str());
            return false;
        }
        DBGLOG("Unregistered slave : %s", slfStr.str());
        isRegistered = false;
        return true;
    }
    catch (IException *e)
    {
        if (!jobListenerStopped)
            IERRLOG(e, "slave unregistration error");
        e->Release();
    }
    return false;
}

bool ControlHandler(ahType type)
{
    static bool recvdSig = false;
    if (recvdSig)
    {
        if (ahInterrupt == type)
            _exit(128+SIGINT);
        else
            _exit(128+SIGTERM);
    }
    recvdSig = true;
    raiseSignalInFuture(SIGTERM, 20);

    if (ahInterrupt == type)
        DBGLOG("CTRL-C detected");
    else if (!jobListenerStopped)
        DBGLOG("SIGTERM detected");

    bool unregOK = false;
    if (!jobListenerStopped)
    {
        if (masterNode)
            unregOK = UnregisterSelf(NULL);
        abortSlave();
    }
    if (recvShutdown)
        return false;
    return !unregOK;
}

void usage()
{
    printf("usage: thorslave  MASTER=ip:port SLAVE=.:port DALISERVERS=ip:port\n");
    exit(1);
}

#ifdef _WIN32
class CReleaseMutex : public CSimpleInterface, public Mutex
{
public:
    CReleaseMutex(const char *name) : Mutex(name) { }
    ~CReleaseMutex() { if (owner) unlock(); }
}; 
#endif


void startSlaveLog()
{
    if (!isContainerized())
    {
        StringBuffer fileName("thorslave");
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(globals->queryProp("@logDir"), "thor");
        StringBuffer slaveNumStr;
        lf->setPostfix(slaveNumStr.append(mySlaveNum).str());
        lf->setCreateAliasFile(false);
        lf->setName(fileName.str());//override default filename
        logHandler = lf->beginLogging();
#ifndef _DEBUG 
        // keep duplicate logging output to stderr to aide debugging
        queryLogMsgManager()->removeMonitor(queryStderrLogMsgHandler());
#endif

        DBGLOG("Opened log file %s", lf->queryLogFileSpec());
    }
    else
    {
        setupContainerizedLogMsgHandler();
        logHandler = queryStderrLogMsgHandler();
        StringBuffer wuid;
        if (getComponentConfigSP()->getProp("@workunit", wuid))
        {
            LogMsgJobId thorJobId = queryLogMsgManager()->addJobId(wuid);
            setDefaultJobId(thorJobId);
        }
    }

    //setupContainerizedStorageLocations();
    PROGLOG("Build %s", hpccBuildInfo.buildTag);
}

int main( int argc, const char *argv[]  )
{
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

#if defined(WIN32) && defined(_DEBUG)
    int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    tmpFlag |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag( tmpFlag );
#endif

    InitModuleObjects();

    addAbortHandler(ControlHandler);
    EnableSEHtoExceptionMapping();

    dummyProc();
#ifndef __64BIT__
    // Restrict stack sizes on 32-bit systems
    Thread::setDefaultStackSize(0x10000);   // NB under windows requires linker setting (/stack:)
#endif

#ifdef _WIN32
    Owned<CReleaseMutex> globalNamedMutex;
#endif 

    globals.setown(createPTree("Thor"));
    unsigned multiThorMemoryThreshold = 0;

    Owned<IException> unregisterException;
    try
    {
        if (argc==1)
        {
            usage();
            return 1;
        }
        cmdArgs = argv+1;
#ifdef _CONTAINERIZED
        globals.setown(loadConfiguration(thorDefaultConfigYaml, argv, "thor", "THOR", nullptr, nullptr, nullptr, false));
        // pickup the default logging level from the thor default config yaml
        if (globals->hasProp("logging/@thorworkerdetail"))
        {
            unsigned workerDetailLevel = globals->getPropInt("logging/@thorworkerdetail");
            globals->setPropInt("logging/@detail", workerDetailLevel);
            // NB: may be overridden by Thor config settings during RegisterSelf
        }
#else
        globals.setown(loadConfiguration(globals, nullptr, argv, "thor", "THOR", nullptr, nullptr, nullptr, false));
#endif

        // NB: the thor configuration is serialized from the manager and only available after RegisterSelf
        // Until that point, only properties on the command line are available.

        const char *master = globals->queryProp("@master");
        if (!master)
            usage();

        mySlaveNum = globals->getPropInt("@slavenum", NotFound);
        if (!isContainerized() && (NotFound == mySlaveNum))
            throw makeStringException(0, "Slave number not specified (@slavenum)");
        startSlaveLog(); // configures 'logHandler'

        // In container world, SLAVE= will not be used
        const char *slave = globals->queryProp("@slave");
        if (slave)
        {
            slfEp.set(slave);
            localHostToNIC(slfEp);
        }
        else 
            slfEp.setLocalHost(0);

        // TBD: use new config/init system for generic handling of init settings vs command line overrides
        if (0 == slfEp.port) // assume default from config if not on command line
            slfEp.port = globals->getPropInt("@slaveport", THOR_BASESLAVE_PORT);

        startMPServer(DCR_ThorSlave, slfEp.port, false, true);
        if (0 == slfEp.port)
            slfEp.port = queryMyNode()->endpoint().port;
        setMachinePortBase(slfEp.port);

#ifdef USE_MP_LOG
        startLogMsgParentReceiver();
        DBGLOG("MPServer started on port %d", getFixedPort(TPORT_mp));
#endif

        SocketEndpoint masterEp(master);
        localHostToNIC(masterEp);
        setMasterPortBase(masterEp.port);

        if (RegisterSelf(masterEp))
        {
            if (globals->getPropBool("@MPChannelReconnect"))
                getMPServer()->setOpt(mpsopt_channelreopen, "true");

            // slaveDaliClient option deprecated, but maintained for compatibility
            if (getExpertOptBool("allowDaliAccess", getExpertOptBool("slaveDaliClient")))
                enableThorSlaveAsDaliClient();

            IDaFileSrvHook *daFileSrvHook = queryDaFileSrvHook();
            if (daFileSrvHook) // probably always installed
                daFileSrvHook->addFilters(globals->queryPropTree("NAS"), &slfEp);

            enableForceRemoteReads(); // forces file reads to be remote reads if they match environment setting 'forceRemotePattern' pattern.

            StringBuffer thorPath;
            globals->getProp("@thorPath", thorPath);
            recursiveCreateDirectory(thorPath.str());
            int err = _chdir(thorPath.str());
            if (err)
            {
                IException *e = makeErrnoExceptionV(-1, "Failed to change dir to '%s'", thorPath.str());
                OERRLOG(e);
                throw e;
            }

// Initialization from globals
            setIORetryCount((unsigned)getExpertOptInt64("ioRetries")); // default == 0 == off

            StringBuffer str;
            if (globals->getProp("@externalProgDir", str.clear()))
            {
                int err = _mkdir(str.str());
                if (err)
                {
                    IException *e = makeErrnoExceptionV(-1, "Failed to create dir '%s'", str.str());
                    OERRLOG(e);
                    throw e;
                }
            }
            else
                globals->setProp("@externalProgDir", thorPath);

#ifndef _CONTAINERIZED
            const char * overrideBaseDirectory = globals->queryProp("@thorDataDirectory");
            const char * overrideReplicateDirectory = globals->queryProp("@thorReplicateDirectory");
            StringBuffer datadir;
            StringBuffer repdir;
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"data","thor",globals->queryProp("@name"),datadir))
                overrideBaseDirectory = datadir.str();
            if (getConfigurationDirectory(globals->queryPropTree("Directories"),"mirror","thor",globals->queryProp("@name"),repdir))
                overrideReplicateDirectory = repdir.str();
            if (!isEmptyString(overrideBaseDirectory))
                setBaseDirectory(overrideBaseDirectory, false);
            if (!isEmptyString(overrideReplicateDirectory))
                setBaseDirectory(overrideReplicateDirectory, true);
#endif

            if (!isContainerized() && getConfigurationDirectory(globals->queryPropTree("Directories"),"query","thor",globals->queryProp("@name"),str.clear()))
                globals->setProp("@query_so_dir", str.str());
            else
                globals->getProp("@query_so_dir", str.clear());
            if (str.length())
            {
                if (getExpertOptBool("dllsToSlaves", true))
                {
                    StringBuffer uniqSoPath;
                    if (PATHSEPCHAR == str.charAt(str.length()-1))
                        uniqSoPath.append(str.length()-1, str.str());
                    else
                        uniqSoPath.append(str);
                    uniqSoPath.append("_").append(getMachinePortBase());
                    str.swapWith(uniqSoPath);
                    globals->setProp("@query_so_dir", str.str());
                }
                DBGLOG("Using querySo directory: %s", str.str());
                recursiveCreateDirectory(str.str());
            }

            useMemoryMappedRead(globals->getPropBool("@useMemoryMappedRead"));

            PROGLOG("ThorSlave Version LCR - %d.%d started",THOR_VERSION_MAJOR,THOR_VERSION_MINOR);
#ifdef _WIN32
            ULARGE_INTEGER userfree;
            ULARGE_INTEGER total;
            ULARGE_INTEGER free;
            if (GetDiskFreeSpaceEx("c:\\",&userfree,&total,&free)&&total.QuadPart) {
                unsigned pc = (unsigned)(free.QuadPart*100/total.QuadPart);
                PROGLOG("Total disk space = %" I64F "d k", total.QuadPart/1000);
                PROGLOG"Free  disk space = %" I64F "d k", free.QuadPart/1000);
                PROGLOG"%d%% disk free\n",pc);
            }
#endif
     
            multiThorMemoryThreshold = globals->getPropInt("@multiThorMemoryThreshold")*0x100000;
            if (multiThorMemoryThreshold) {
                StringBuffer lgname;
                if (!globals->getProp("@multiThorResourceGroup",lgname))
                    globals->getProp("@nodeGroup",lgname);
                if (lgname.length()) {
                    Owned<ILargeMemLimitNotify> notify = createMultiThorResourceMutex(lgname.str());
                    setMultiThorMemoryNotify(multiThorMemoryThreshold,notify);
                    PROGLOG("Multi-Thor resource limit for %s set to %" I64F "d",lgname.str(),(__int64)multiThorMemoryThreshold);
                }   
                else
                    multiThorMemoryThreshold = 0;
            }

#ifndef _CONTAINERIZED
            unsigned pinterval = globals->getPropInt("@system_monitor_interval",1000*60);
            if (pinterval)
                startPerformanceMonitor(pinterval, PerfMonStandard, nullptr);
#endif

#ifdef _CONTAINERIZED
            class CServerThread : public CSimpleInterfaceOf<IThreaded>
            {
                CThreaded threaded;
                Owned<IRemoteFileServer> dafsInstance;
            public:
                CServerThread() : threaded("CServerThread")
                {
                    dafsInstance.setown(createRemoteFileServer());
                    threaded.init(this, false);
                }
                ~CServerThread()
                {
                    DBGLOG("Stopping dafilesrv");
                    dafsInstance->stop();
                    threaded.join();
                }
            // IThreaded
                virtual void threadmain() override
                {
                    SocketEndpoint listenEp(DAFILESRV_PORT);
                    try
                    {
                        DBGLOG("Starting dafilesrv");
                        dafsInstance->run(nullptr, SSLNone, listenEp);
                    }
                    catch (IException *e)
                    {
                        IERRLOG(e, "dafilesrv error");
                        throw;
                    }
                }
            };
            OwnedPtr<CServerThread> dafsThread;
            if (globals->getPropBool("@_dafsStorage"))
                dafsThread.setown(new CServerThread);
#endif
            installDefaultFileHooks(globals);
            slaveMain(jobListenerStopped, logHandler);
        }

        DBGLOG("ThorSlave terminated OK");
    }
    catch (IException *e) 
    {
        if (!jobListenerStopped)
            FLLOG(MCexception(e), e,"ThorSlave");
        unregisterException.setown(e);
    }
#ifndef _CONTAINERIZED
    stopPerformanceMonitor();
#endif

    if (multiThorMemoryThreshold)
        setMultiThorMemoryNotify(0,NULL);
    roxiemem::releaseRoxieHeap();

    if (!recvShutdown && unregisterException.get())
        UnregisterSelf(unregisterException);

    // slaveDaliClient option deprecated, but maintained for compatibility
    if (getExpertOptBool("allowDaliAccess", getExpertOptBool("slaveDaliClient")))
        disableThorSlaveAsDaliClient();

#ifdef USE_MP_LOG
    stopLogMsgReceivers();
#endif
    stopMPServer(!recvShutdown);
    releaseAtoms(); // don't know why we can't use a module_exit to destruct these...

    ExitModuleObjects(); // not necessary, atexit will call, but good for leak checking
    return 0;
}

