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

#include <platform.h>
#include <signal.h>
#include <jlib.hpp>
#include <jio.hpp>
#include <jmisc.hpp>
#include <jqueue.tpp>
#include <jsocket.hpp>
#include <jlog.hpp>
#include <jprop.hpp>
#include <jfile.hpp>
#include <jencrypt.hpp>
#include "jutil.hpp"
#include <build-config.h>

#include "dalienv.hpp"
#include "rmtfile.hpp"
#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdserver.hpp"
#include "ccdlistener.hpp"
#include "ccdsnmp.hpp"
#include "thorplugin.hpp"

#if defined (__linux__)
#include <sys/syscall.h>
#include "ioprio.h"
#endif

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

//=================================================================================

bool shuttingDown = false;
unsigned numChannels;
unsigned callbackRetries = 3;
unsigned callbackTimeout = 5000;
unsigned lowTimeout = 10000;
unsigned highTimeout = 2000;
unsigned slaTimeout = 2000;
unsigned numServerThreads = 30;
unsigned numSlaveThreads = 30;
bool prestartSlaveThreads = false;
unsigned numRequestArrayThreads = 5;
unsigned headRegionSize;
unsigned ccdMulticastPort;
bool enableHeartBeat = true;
unsigned parallelLoopFlowLimit = 100;
unsigned perChannelFlowLimit = 10;
time_t startupTime;
unsigned statsExpiryTime = 3600;
unsigned miscDebugTraceLevel = 0;  // separate trace settings purely for debugging specific items (i.e. all possible locations to look for files at startup)
unsigned readTimeout = 300;
unsigned indexReadChunkSize = 60000;
unsigned maxBlockSize = 10000000;
unsigned maxLockAttempts = 5;
bool pretendAllOpt = false;
bool traceStartStop = false;
bool traceServerSideCache = false;
bool defaultTimeActivities = true;
bool defaultTraceEnabled = false;
unsigned defaultTraceLimit = 10;
unsigned watchActivityId = 0;
unsigned testSlaveFailure = 0;
unsigned restarts = 0;
IRecordLayoutTranslator::Mode fieldTranslationEnabled = IRecordLayoutTranslator::NoTranslation;
bool mergeSlaveStatistics = true;
PTreeReaderOptions defaultXmlReadFlags = ptr_ignoreWhiteSpace;
bool runOnce = false;

unsigned udpMulticastBufferSize = 262142;
bool roxieMulticastEnabled = true;

IPropertyTree *topology;
MapStringTo<int> *preferredClusters;
StringBuffer topologyFile;
CriticalSection ccdChannelsCrit;
IPropertyTree* ccdChannels;
StringArray allQuerySetNames;

bool allFilesDynamic;
bool lockSuperFiles;
bool useRemoteResources;
bool checkFileDate;
bool lazyOpen;
bool localSlave;
bool ignoreOrphans;
bool doIbytiDelay = true; 
unsigned initIbytiDelay; // In MillSec
unsigned minIbytiDelay;  // In MillSec
bool copyResources;
bool enableKeyDiff = true;
bool chunkingHeap = true;
bool logFullQueries;
bool blindLogging = false;
bool debugPermitted = true;
bool checkCompleted = true;
unsigned preabortKeyedJoinsThreshold = 100;
unsigned preabortIndexReadsThreshold = 100;
bool preloadOnceData;
bool reloadRetriesFailed;
bool selfTestMode = false;

int backgroundCopyClass = 0;
int backgroundCopyPrio = 0;

unsigned memoryStatsInterval = 0;
memsize_t defaultMemoryLimit;
unsigned defaultTimeLimit[3] = {0, 0, 0};
unsigned defaultWarnTimeLimit[3] = {0, 5000, 5000};
unsigned defaultThorConnectTimeout;

unsigned defaultParallelJoinPreload = 0;
unsigned defaultPrefetchProjectPreload = 10;
unsigned defaultConcatPreload = 0;
unsigned defaultFetchPreload = 0;
unsigned defaultFullKeyedJoinPreload = 0;
unsigned defaultKeyedJoinPreload = 0;
unsigned dafilesrvLookupTimeout = 10000;
bool defaultCheckingHeap = false;
unsigned defaultStrandBlockSize = 512;
unsigned defaultForceNumStrands = 0;
unsigned defaultHeapFlags = roxiemem::RHFnofragment;

unsigned slaveQueryReleaseDelaySeconds = 60;
unsigned coresPerQuery = 0;

unsigned logQueueLen;
unsigned logQueueDrop;
bool useLogQueue;
bool fastLaneQueue;
unsigned mtu_size = 1400; // upper limit on outbound buffer size - allow some header room too
StringBuffer fileNameServiceDali;
StringBuffer roxieName;
bool trapTooManyActiveQueries;
unsigned maxEmptyLoopIterations;
unsigned maxGraphLoopIterations;
bool probeAllRows;
bool steppingEnabled = true;
bool simpleLocalKeyedJoins = true;

unsigned __int64 minFreeDiskSpace = 1024 * 0x100000;  // default to 1 GB
unsigned socketCheckInterval = 5000;

StringBuffer logDirectory;
StringBuffer pluginDirectory;
StringBuffer queryDirectory;
StringBuffer codeDirectory;
StringBuffer tempDirectory;

ClientCertificate clientCert;
bool useHardLink;

unsigned maxFileAge[2] = {0xffffffff, 60*60*1000}; // local files don't expire, remote expire in 1 hour, by default
unsigned minFilesOpen[2] = {2000, 500};
unsigned maxFilesOpen[2] = {4000, 1000};

unsigned myNodeIndex = (unsigned) -1;
SocketEndpoint ownEP;
HardwareInfo hdwInfo;
unsigned parallelAggregate;
bool inMemoryKeysEnabled = true;
unsigned serverSideCacheSize = 0;

bool nodeCachePreload = false;
unsigned nodeCacheMB = 100;
unsigned leafCacheMB = 50;
unsigned blobCacheMB = 0;

unsigned roxiePort = 0;
Owned<IPerfMonHook> perfMonHook;

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    topology = NULL;
    ccdChannels = NULL;

    return true;
}

MODULE_EXIT()
{
    ::Release(topology);
    ::Release(ccdChannels);
}

//=========================================================================================
//////////////////////////////////////////////////////////////////////////////////////////////
extern "C" void caughtSIGPIPE(int sig)
{
    DBGLOG("Caught sigpipe %d", sig);
}

extern "C" void caughtSIGHUP(int sig)
{
    DBGLOG("Caught sighup %d", sig);
}


extern "C" void caughtSIGALRM(int sig)
{
    DBGLOG("Caught sigalrm %d", sig);
}

extern "C" void caughtSIGTERM(int sig)
{
    DBGLOG("Caught sigterm %d", sig);
}




void init_signals()
{
//  signal(SIGTERM, caughtSIGTERM);
#ifndef _WIN32
    signal(SIGPIPE, caughtSIGPIPE);
    signal(SIGHUP, caughtSIGHUP);
    signal(SIGALRM, caughtSIGALRM);

#endif
}   

//=========================================================================================

class Waiter : public CInterface, implements IAbortHandler
{
    Semaphore aborted;
public:
    IMPLEMENT_IINTERFACE;

    bool wait(unsigned timeout)
    {
        return aborted.wait(timeout);
    }
    void wait()
    {
        aborted.wait();
    }
    bool onAbort()
    {
        aborted.signal();
        roxieMetrics.clear();
#ifdef _DEBUG
        return false; // we want full leak checking info
#else
        return true; // we don't care - just exit as fast as we can
#endif
    }
} waiter;

void closedown()
{
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
    waiter.onAbort();
}

void getAccessList(const char *aclName, const IPropertyTree *topology, IPropertyTree *aclInfo)
{
    StringBuffer xpath;
    xpath.append("ACL[@name='").append(aclName).append("']");
    if (aclInfo->queryPropTree(xpath))
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - recursive ACL definition of %s", aclName);
    aclInfo->addPropTree("ACL")->setProp("@name", aclName);

    Owned<IPropertyTree> acl = topology->getPropTree(xpath.str());
    if (!acl)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - acl %s not found", aclName);

    Owned<IPropertyTreeIterator> access = acl->getElements("Access");
    ForEach(*access)
    {
        IPropertyTree &child = access->query();
        const char *base = child.queryProp("@base");
        if (base)
            getAccessList(base, topology, aclInfo);
        else
            aclInfo->addPropTree(child.queryName(), LINK(&child));
    }
    aclInfo->removeProp(xpath);
}

bool ipMatch(IpAddress &ip)
{
    return ip.isLocal();
}

void addSlaveChannel(unsigned channel, unsigned level)
{
    StringBuffer xpath;
    xpath.appendf("RoxieSlaveProcess[@channel=\"%d\"]", channel);
    if (ccdChannels->hasProp(xpath.str()))
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - channel %d repeated", channel);
    IPropertyTree *ci = ccdChannels->addPropTree("RoxieSlaveProcess");
    ci->setPropInt("@channel", channel);
    ci->setPropInt("@subChannel", numSlaves[channel]);
}

void addChannel(unsigned nodeNumber, unsigned channel, unsigned level)
{
    numSlaves[channel]++;
    if (nodeNumber == myNodeIndex && channel > 0)
    {
        assertex(channel <= numChannels);
        assertex(!replicationLevel[channel]);
        replicationLevel[channel] = level;
        addSlaveChannel(channel, level);
    }
    if (!localSlave)
    {
        addEndpoint(channel, getNodeAddress(nodeNumber), ccdMulticastPort);
    }
}

extern void doUNIMPLEMENTED(unsigned line, const char *file)
{
    throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "UNIMPLEMENTED at %s:%d", sanitizeSourceFile(file), line);
}

void FatalError(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    StringBuffer errMsg;
    errMsg.valist_appendf(format, args);
    va_end(args);
    Owned<IException> E = MakeStringException(MSGAUD_operator, ROXIE_INTERNAL_ERROR, "%s", errMsg.str());
    EXCLOG(E, "Fatal error");
    Sleep(5000);
    _exit(1);
}

// If changing these, please change roxie.cpp's roxie_server_usage() as well
static void roxie_common_usage(const char * progName)
{
    StringBuffer program;
    program.append(progName);
    getFileNameOnly(program, false);
    // Things that are also relevant to stand-alone executables
    printf("Usage: %s [options]\n", program.str());
    printf("\nOptions:\n");
    printf("\t--daliServers=[host1,...]\t: List of Dali servers to use\n");
    printf("\t--tracelevel=[integer]\t: Amount of information to dump on logs\n");
    printf("\t--stdlog=[boolean]\t: Standard log format (based on tracelevel)\n");
    printf("\t--logfile\t: Outputs to logfile, rather than stdout\n");
    printf("\t--help|-h\t: This message\n");
    printf("\n");
}

class MAbortHandler : implements IExceptionHandler
{
    unsigned dummy; // to avoid complaints about an empty class...
public:
    MAbortHandler() : dummy(0) {};

    virtual bool fireException(IException *e)
    {
        ForEachItemIn(idx, socketListeners)
        {
            socketListeners.item(idx).stopListening();
        }
        return false; // It returns to excsighandler() to abort!
    }
} abortHandler;

#ifdef _WIN32
int myhook(int alloctype, void *, size_t nSize, int p1, long allocSeq, const unsigned char *file, int line)
{
    // Handy place to put breakpoints when tracking down obscure memory leaks...
    if (nSize==68 && !file)
    {
        DBGLOG("memory hook matched");
    }
    return true;
}
#endif

void saveTopology()
{
    // Write back changes that have been made via certain control:xxx changes, so that they survive a roxie restart
    // Note that they are overwritten when Roxie is manually stopped/started via hpcc-init service - these changes
    // are only intended to be temporary for the current session
    try
    {
        saveXML(topologyFile.str(), topology);
    }
    catch (IException *E)
    {
        // If we can't save the topology, then tough. Carry on without it. Changes will not survive an unexpected roxie restart
        EXCLOG(E, "Error saving topology file");
        E->Release();
    }
}

class CHpccProtocolPluginCtx : implements IHpccProtocolPluginContext, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    virtual int ctxGetPropInt(const char *propName, int defaultValue) const
    {
        return topology->getPropInt(propName, defaultValue);
    }
    virtual bool ctxGetPropBool(const char *propName, bool defaultValue) const
    {
        return topology->getPropBool(propName, defaultValue);
    }
    virtual const char *ctxQueryProp(const char *propName) const
    {
        return topology->queryProp(propName);
    }
};

int STARTQUERY_API start_query(int argc, const char *argv[])
{
    EnableSEHtoExceptionMapping();
    setTerminateOnSEH();
    init_signals();
    // We need to do the above BEFORE we call InitModuleObjects
    InitModuleObjects();
    init_signals();

    // stand alone usage only, not server
    for (unsigned i=0; i<(unsigned)argc; i++)
    {
        if (stricmp(argv[i], "--help")==0 ||
            stricmp(argv[i], "-h")==0)
        {
            roxie_common_usage(argv[0]);
            return EXIT_SUCCESS;
        }
    }

    #ifdef _USE_CPPUNIT
    if (argc>=2 && stricmp(argv[1], "-selftest")==0)
    {
        selfTestMode = true;
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_prefix);
        CppUnit::TextUi::TestRunner runner;
        if (argc==2)
        {
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
            runner.addTest( registry.makeTest() );
        }
        else 
        {
            // MORE - maybe add a 'list' function here?
            for (int name = 2; name < argc; name++)
            {
                if (stricmp(argv[name], "-q")==0)
                {
                    traceLevel = 0;
                    roxiemem::setMemTraceLevel(0);
                    removeLog();
                }
                else
                {
                    CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry(argv[name]);
                    runner.addTest( registry.makeTest() );
                }
            }
        }
        bool wasSucessful = runner.run( "", false );
        releaseAtoms();
        return wasSucessful;
    }
#endif
#ifdef _DEBUG
#ifdef _WIN32
    _CrtSetAllocHook(myhook);
#endif
#endif

#ifndef __64BIT__
    // Restrict stack sizes on 32-bit systems
    Thread::setDefaultStackSize(0x10000);   // NB under windows requires linker setting (/stack:)
#endif
    srand( (unsigned)time( NULL ) );
    ccdChannels = createPTree("Channels", ipt_lowmem);

    char currentDirectory[_MAX_DIR];
    if (!getcwd(currentDirectory, sizeof(currentDirectory)))
        throw MakeStringException(ROXIE_INTERNAL_ERROR, "getcwd failed (%d)", errno);

    codeDirectory.set(currentDirectory);
    addNonEmptyPathSepChar(codeDirectory);
    try
    {
        Owned<IProperties> globals = createProperties(true);
        for (int i = 1; i < argc; i++)
            globals->loadProp(argv[i], true);

        Owned<IFile> sentinelFile = createSentinelTarget();
        removeSentinelFile(sentinelFile);

        if (globals->hasProp("--topology"))
            globals->getProp("--topology", topologyFile);
        else
            topologyFile.append(codeDirectory).append(PATHSEPCHAR).append("RoxieTopology.xml");

        if (checkFileExists(topologyFile.str()))
        {
            DBGLOG("Loading topology file %s", topologyFile.str());
            topology = createPTreeFromXMLFile(topologyFile.str(), ipt_lowmem);
            saveTopology();
        }
        else
        {
            if (globals->hasProp("--topology"))
            {
                // Explicitly-named topology file SHOULD exist...
                throw MakeStringException(ROXIE_INVALID_TOPOLOGY, "topology file %s not found", topologyFile.str());
            }
            topology=createPTreeFromXMLString(
                "<RoxieTopology allFilesDynamic='1' localSlave='1' resolveLocally='1'>"
                " <RoxieFarmProcess/>"
                " <RoxieServerProcess netAddress='.'/>"
                "</RoxieTopology>"
                , ipt_lowmem
                );
            int port = globals->getPropInt("--port", 9876);
            topology->setPropInt("RoxieFarmProcess/@port", port);
            topology->setProp("@daliServers", globals->queryProp("--daliServers"));
            topology->setProp("@traceLevel", globals->queryProp("--traceLevel"));
            topology->setPropInt("@allFilesDynamic", globals->getPropInt("--allFilesDynamic", 1));
            topology->setProp("@memTraceLevel", globals->queryProp("--memTraceLevel"));
        }
        if (topology->hasProp("PreferredCluster"))
        {
            preferredClusters = new MapStringTo<int>(true);
            Owned<IPropertyTreeIterator> clusters = topology->getElements("PreferredCluster");
            ForEach(*clusters)
            {
                IPropertyTree &child = clusters->query();
                const char *name = child.queryProp("@name");
                int priority = child.getPropInt("@priority", 100);
                if (name && *name)
                    preferredClusters->setValue(name, priority);
            }
        }
        topology->getProp("@name", roxieName);
        if (roxieName.length())
            setStatisticsComponentName(SCTroxie, roxieName, true);
        else
            setStatisticsComponentName(SCTroxie, "roxie", true);

        Owned<const IQueryDll> standAloneDll;
        if (globals->hasProp("--loadWorkunit"))
        {
            StringBuffer workunitName;
            globals->getProp("--loadWorkunit", workunitName);
            standAloneDll.setown(createQueryDll(workunitName));
        }
        else
        {
            Owned<ILoadedDllEntry> dll = createExeDllEntry(argv[0]);
            if (checkEmbeddedWorkUnitXML(dll))
            {
                standAloneDll.setown(createExeQueryDll(argv[0]));
                runOnce = globals->getPropInt("--port", 0) == 0;
            }
        }

        traceLevel = topology->getPropInt("@traceLevel", runOnce ? 0 : 1);
        if (traceLevel > MAXTRACELEVEL)
            traceLevel = MAXTRACELEVEL;
        udpTraceLevel = topology->getPropInt("@udpTraceLevel", runOnce ? 0 : 1);
        roxiemem::setMemTraceLevel(topology->getPropInt("@memTraceLevel", runOnce ? 0 : 1));
        soapTraceLevel = topology->getPropInt("@soapTraceLevel", runOnce ? 0 : 1);
        miscDebugTraceLevel = topology->getPropInt("@miscDebugTraceLevel", 0);

        Linked<IPropertyTree> directoryTree = topology->queryPropTree("Directories");
        if (!directoryTree)
        {
            Owned<IPropertyTree> envFile = getHPCCEnvironment();
            if (envFile)
                directoryTree.set(envFile->queryPropTree("Software/Directories"));
        }
        if (directoryTree)
        {
            getConfigurationDirectory(directoryTree, "query", "roxie", roxieName, queryDirectory);
            for (unsigned replicationLevel = 0; replicationLevel < MAX_REPLICATION_LEVELS; replicationLevel++)
            {
                StringBuffer dataDir;
                StringBuffer dirId("data");
                if (replicationLevel)
                    dirId.append(replicationLevel+1);
                if (getConfigurationDirectory(directoryTree, dirId, "roxie", roxieName, dataDir))
                    setBaseDirectory(dataDir, replicationLevel, DFD_OSdefault);
            }
        }
        directoryTree.clear();

        //Logging stuff
        if (globals->getPropBool("--stdlog", traceLevel != 0) || topology->getPropBool("@forceStdLog", false))
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_thread | MSGFIELD_prefix);
        else
            removeLog();
        if (globals->hasProp("--logfile"))
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(topology, "roxie");
            lf->setMaxDetail(TopDetail);
            lf->beginLogging();
            logDirectory.set(lf->queryLogDir());
#ifdef _DEBUG
            unsigned useLogQueue = topology->getPropBool("@useLogQueue", false);
#else
            unsigned useLogQueue = topology->getPropBool("@useLogQueue", true);
#endif
            if (useLogQueue)
            {
                unsigned logQueueLen = topology->getPropInt("@logQueueLen", 512);
                unsigned logQueueDrop = topology->getPropInt("@logQueueDrop", 32);
                queryLogMsgManager()->enterQueueingMode();
                queryLogMsgManager()->setQueueDroppingLimit(logQueueLen, logQueueDrop);
            }
            if (globals->getPropBool("--enableSysLog",true))
                UseSysLogForOperatorMessages();
        }

        roxieMetrics.setown(createRoxieMetricsManager());

        Owned<IPropertyTreeIterator> userMetrics = topology->getElements("./UserMetric");
        ForEach(*userMetrics)
        {
            IPropertyTree &metric = userMetrics->query();
            const char *name = metric.queryProp("@name");
            const char *regex= metric.queryProp("@regex");
            if (name && regex)
                roxieMetrics->addUserMetric(name, regex);
            else
                throw MakeStringException(ROXIE_INTERNAL_ERROR, "Invalid UserMetric element in topology file - name or regex missing");
        }

        restarts = globals->getPropInt("--restarts", 0);
        const char *preferredSubnet = topology->queryProp("@preferredSubnet");
        if (preferredSubnet)
        {
            const char *preferredSubnetMask = topology->queryProp("@preferredSubnetMask");
            if (!preferredSubnetMask) preferredSubnetMask = "255.255.255.0";
            if (!setPreferredSubnet(preferredSubnet, preferredSubnetMask))
                throw MakeStringException(ROXIE_INTERNAL_ERROR, "Error setting preferred subnet %s mask %s", preferredSubnet, preferredSubnetMask);
        }
        if (restarts)
        {
            if (traceLevel)
                DBGLOG("Roxie restarting: restarts = %d build = %s", restarts, BUILD_TAG);
            setStartRuid(restarts);
        }
        else
        {
            if (traceLevel)
            {
                DBGLOG("Roxie starting, build = %s", BUILD_TAG);
            }
        }

        headRegionSize = topology->getPropInt("@headRegionSize", 50);
        ccdMulticastPort = topology->getPropInt("@multicastPort", CCD_MULTICAST_PORT);
        statsExpiryTime = topology->getPropInt("@statsExpiryTime", 3600);
        roxiemem::setMemTraceSizeLimit((memsize_t) topology->getPropInt64("@memTraceSizeLimit", 0));
        callbackRetries = topology->getPropInt("@callbackRetries", 3);
        callbackTimeout = topology->getPropInt("@callbackTimeout", 5000);
        lowTimeout = topology->getPropInt("@lowTimeout", 10000);
        highTimeout = topology->getPropInt("@highTimeout", 2000);
        slaTimeout = topology->getPropInt("@slaTimeout", 2000);
        parallelLoopFlowLimit = topology->getPropInt("@parallelLoopFlowLimit", 100);
        perChannelFlowLimit = topology->getPropInt("@perChannelFlowLimit", 10);
        copyResources = topology->getPropBool("@copyResources", true);
        useRemoteResources = topology->getPropBool("@useRemoteResources", true);
        checkFileDate = topology->getPropBool("@checkFileDate", true);
        const char *lazyOpenMode = topology->queryProp("@lazyOpen");
        if (!lazyOpenMode || stricmp(lazyOpenMode, "smart")==0)
            lazyOpen = (restarts > 0);
        else
            lazyOpen = topology->getPropBool("@lazyOpen", false);
        bool useNasTranslation = topology->getPropBool("@useNASTranslation", true);
        if (useNasTranslation)
        {
            Owned<IPropertyTree> nas = envGetNASConfiguration(topology);
            envInstallNASHooks(nas);
        }
        localSlave = topology->getPropBool("@localSlave", false);
        doIbytiDelay = topology->getPropBool("@doIbytiDelay", true);
        minIbytiDelay = topology->getPropInt("@minIbytiDelay", 2);
        initIbytiDelay = topology->getPropInt("@initIbytiDelay", 50);
        allFilesDynamic = topology->getPropBool("@allFilesDynamic", false);
        lockSuperFiles = topology->getPropBool("@lockSuperFiles", false);
        ignoreOrphans = topology->getPropBool("@ignoreOrphans", true);
        chunkingHeap = topology->getPropBool("@chunkingHeap", true);
        readTimeout = topology->getPropInt("@readTimeout", 300);
        logFullQueries = topology->getPropBool("@logFullQueries", false);
        debugPermitted = topology->getPropBool("@debugPermitted", true);
        blindLogging = topology->getPropBool("@blindLogging", false);
        if (!blindLogging)
            logExcessiveSeeks = true;
        preloadOnceData = topology->getPropBool("@preloadOnceData", true);
        reloadRetriesFailed  = topology->getPropBool("@reloadRetriesSuspended", true);
#if defined(__linux__) && defined(SYS_ioprio_set)
        const char *backgroundCopyClassString = topology->queryProp("@backgroundCopyClass");
        if (!isEmptyString(backgroundCopyClassString))
        {
            if (strieq(backgroundCopyClassString, "best-effort"))
                backgroundCopyClass = IOPRIO_CLASS_BE;
            else if (strieq(backgroundCopyClassString, "idle"))
                backgroundCopyClass = IOPRIO_CLASS_IDLE;
            else if (strieq(backgroundCopyClassString, "none"))
                backgroundCopyClass = IOPRIO_CLASS_NONE;
            else
                DBGLOG("Invalid backgroundCopyClass %s specified - ignored", backgroundCopyClassString);
        }
        backgroundCopyPrio = topology->getPropInt("@backgroundCopyPrio", 0);
        if (backgroundCopyPrio >= IOPRIO_BE_NR)
        {
            DBGLOG("Invalid backgroundCopyPrio %d specified - using %d", backgroundCopyPrio, (int) (IOPRIO_BE_NR-1));
            backgroundCopyPrio = IOPRIO_BE_NR-1;
        }
        else if (backgroundCopyPrio < 0)
        {
            DBGLOG("Invalid backgroundCopyPrio %d specified - using 0", backgroundCopyPrio);
            backgroundCopyPrio = 0;
        }
#endif
        linuxYield = topology->getPropBool("@linuxYield", false);
        traceSmartStepping = topology->getPropBool("@traceSmartStepping", false);
        useMemoryMappedIndexes = topology->getPropBool("@useMemoryMappedIndexes", false);
        flushJHtreeCacheOnOOM = topology->getPropBool("@flushJHtreeCacheOnOOM", true);
        fastLaneQueue = topology->getPropBool("@fastLaneQueue", true);
        udpOutQsPriority = topology->getPropInt("@udpOutQsPriority", 0);
        udpSnifferEnabled = topology->getPropBool("@udpSnifferEnabled", true);
        udpInlineCollation = topology->getPropBool("@udpInlineCollation", false);
        udpInlineCollationPacketLimit = topology->getPropInt("@udpInlineCollationPacketLimit", 50);
        udpSendCompletedInData = topology->getPropBool("@udpSendCompletedInData", false);
        udpRetryBusySenders = topology->getPropInt("@udpRetryBusySenders", 0);

        // Historically, this was specified in seconds. Assume any value <= 10 is a legacy value specified in seconds!
        udpMaxRetryTimedoutReqs = topology->getPropInt("@udpMaxRetryTimedoutReqs", 0);
        udpRequestToSendTimeout = topology->getPropInt("@udpRequestToSendTimeout", 0);
        if (udpRequestToSendTimeout<=10)
            udpRequestToSendTimeout *= 1000;
        if (udpRequestToSendTimeout == 0)
        {
            if (slaTimeout)
                udpRequestToSendTimeout = (slaTimeout*3) / 4;
            else
                udpRequestToSendTimeout = 5000;
        }
        // MORE: might want to check socket buffer sizes against sys max here instead of udp threads ?
        udpSnifferReadThreadPriority = topology->getPropInt("@udpSnifferReadThreadPriority", 3);
        udpSnifferSendThreadPriority = topology->getPropInt("@udpSnifferSendThreadPriority", 3);

        udpMulticastBufferSize = topology->getPropInt("@udpMulticastBufferSize", 262142);
        udpFlowSocketsSize = topology->getPropInt("@udpFlowSocketsSize", 131072);
        udpLocalWriteSocketSize = topology->getPropInt("@udpLocalWriteSocketSize", 1024000);
        
        roxieMulticastEnabled = topology->getPropBool("@roxieMulticastEnabled", true);   // enable use of multicast for sending requests to slaves
        if (udpSnifferEnabled && !roxieMulticastEnabled)
        {
            DBGLOG("WARNING: ignoring udpSnifferEnabled setting as multicast not enabled");
            udpSnifferEnabled = false;
        }

        int ttlTmp = topology->getPropInt("@multicastTTL", 1);
        if (ttlTmp < 0)
        {
            multicastTTL = 1;
            WARNLOG("multicastTTL value (%d) invalid, must be >=0, resetting to %u", ttlTmp, multicastTTL);
        }
        else if (ttlTmp > 255)
        {
            multicastTTL = 255;
            WARNLOG("multicastTTL value (%d) invalid, must be <=%u, resetting to maximum", ttlTmp, multicastTTL);
        }
        else
            multicastTTL = ttlTmp;

        indexReadChunkSize = topology->getPropInt("@indexReadChunkSize", 60000);
        numSlaveThreads = topology->getPropInt("@slaveThreads", 30);
        numServerThreads = topology->getPropInt("@serverThreads", 30);
        numRequestArrayThreads = topology->getPropInt("@requestArrayThreads", 5);
        maxBlockSize = topology->getPropInt("@maxBlockSize", 10000000);
        maxLockAttempts = topology->getPropInt("@maxLockAttempts", 5);
        enableHeartBeat = topology->getPropBool("@enableHeartBeat", true);
        checkCompleted = topology->getPropBool("@checkCompleted", true);
        prestartSlaveThreads = topology->getPropBool("@prestartSlaveThreads", false);
        preabortKeyedJoinsThreshold = topology->getPropInt("@preabortKeyedJoinsThreshold", 100);
        preabortIndexReadsThreshold = topology->getPropInt("@preabortIndexReadsThreshold", 100);
        defaultMemoryLimit = (memsize_t) topology->getPropInt64("@defaultMemoryLimit", 0);
        defaultTimeLimit[0] = (unsigned) topology->getPropInt64("@defaultLowPriorityTimeLimit", 0);
        defaultTimeLimit[1] = (unsigned) topology->getPropInt64("@defaultHighPriorityTimeLimit", 0);
        defaultTimeLimit[2] = (unsigned) topology->getPropInt64("@defaultSLAPriorityTimeLimit", 0);
        defaultWarnTimeLimit[0] = (unsigned) topology->getPropInt64("@defaultLowPriorityTimeWarning", 0);
        defaultWarnTimeLimit[1] = (unsigned) topology->getPropInt64("@defaultHighPriorityTimeWarning", 0);
        defaultWarnTimeLimit[2] = (unsigned) topology->getPropInt64("@defaultSLAPriorityTimeWarning", 0);
        defaultThorConnectTimeout = (unsigned) topology->getPropInt64("@defaultThorConnectTimeout", 60);

        defaultXmlReadFlags = topology->getPropBool("@defaultStripLeadingWhitespace", true) ? ptr_ignoreWhiteSpace : ptr_none;
        defaultParallelJoinPreload = topology->getPropInt("@defaultParallelJoinPreload", 0);
        defaultConcatPreload = topology->getPropInt("@defaultConcatPreload", 0);
        defaultFetchPreload = topology->getPropInt("@defaultFetchPreload", 0);
        defaultFullKeyedJoinPreload = topology->getPropInt("@defaultFullKeyedJoinPreload", 0);
        defaultKeyedJoinPreload = topology->getPropInt("@defaultKeyedJoinPreload", 0);
        defaultPrefetchProjectPreload = topology->getPropInt("@defaultPrefetchProjectPreload", 10);
        defaultStrandBlockSize = topology->getPropInt("@defaultStrandBlockSize", 512);
        defaultForceNumStrands = topology->getPropInt("@defaultForceNumStrands", 0);
        defaultCheckingHeap = topology->getPropBool("@checkingHeap", false);  // NOTE - not in configmgr - too dangerous!

        slaveQueryReleaseDelaySeconds = topology->getPropInt("@slaveQueryReleaseDelaySeconds", 60);
        coresPerQuery = topology->getPropInt("@coresPerQuery", 0);

        diskReadBufferSize = topology->getPropInt("@diskReadBufferSize", 0x10000);
        fieldTranslationEnabled = IRecordLayoutTranslator::NoTranslation;
        const char *val = topology->queryProp("@fieldTranslationEnabled");
        if (val)
        {
            if (strieq(val, "payload"))
                fieldTranslationEnabled = IRecordLayoutTranslator::TranslatePayload;
            else if (strToBool(val))
                fieldTranslationEnabled = IRecordLayoutTranslator::TranslateAll;
        }

        pretendAllOpt = topology->getPropBool("@ignoreMissingFiles", false);
        memoryStatsInterval = topology->getPropInt("@memoryStatsInterval", 60);
        roxiemem::setMemoryStatsInterval(memoryStatsInterval);
        pingInterval = topology->getPropInt("@pingInterval", 0);
        socketCheckInterval = topology->getPropInt("@socketCheckInterval", runOnce ? 0 : 5000);
        memsize_t totalMemoryLimit = (memsize_t) topology->getPropInt64("@totalMemoryLimit", 0);
        bool allowHugePages = topology->getPropBool("@heapUseHugePages", false);
        bool allowTransparentHugePages = topology->getPropBool("@heapUseTransparentHugePages", true);
        bool retainMemory = topology->getPropBool("@heapRetainMemory", false);
        if (!totalMemoryLimit)
            totalMemoryLimit = 1024 * 0x100000;  // 1 Gb;
        roxiemem::setTotalMemoryLimit(allowHugePages, allowTransparentHugePages, retainMemory, totalMemoryLimit, 0, NULL, NULL);

        traceStartStop = topology->getPropBool("@traceStartStop", false);
        traceServerSideCache = topology->getPropBool("@traceServerSideCache", false);
        defaultTimeActivities = topology->getPropBool("@timeActivities", true);
        defaultTraceEnabled = topology->getPropBool("@traceEnabled", false);
        defaultTraceLimit = topology->getPropInt("@traceLimit", 10);
        clientCert.certificate.set(topology->queryProp("@certificateFileName"));
        clientCert.privateKey.set(topology->queryProp("@privateKeyFileName"));
        clientCert.passphrase.set(topology->queryProp("@passphrase"));
        useHardLink = topology->getPropBool("@useHardLink", false);
        maxFileAge[false] = topology->getPropInt("@localFilesExpire", (unsigned) -1);
        maxFileAge[true] = topology->getPropInt("@remoteFilesExpire", 60*60*1000);
        minFilesOpen[false] = topology->getPropInt("@minLocalFilesOpen", 2000);
        minFilesOpen[true] = topology->getPropInt("@minRemoteFilesOpen", 500);
        maxFilesOpen[false] = topology->getPropInt("@maxLocalFilesOpen", 4000);
        maxFilesOpen[true] = topology->getPropInt("@maxRemoteFilesOpen", 1000);
        dafilesrvLookupTimeout = topology->getPropInt("@dafilesrvLookupTimeout", 10000);
        setRemoteFileTimeouts(dafilesrvLookupTimeout, 0);
        topology->getProp("@daliServers", fileNameServiceDali);
        trapTooManyActiveQueries = topology->getPropBool("@trapTooManyActiveQueries", true);
        maxEmptyLoopIterations = topology->getPropInt("@maxEmptyLoopIterations", 1000);
        maxGraphLoopIterations = topology->getPropInt("@maxGraphLoopIterations", 1000);
        mergeSlaveStatistics = topology->getPropBool("@mergeSlaveStatistics", true);

        enableKeyDiff = topology->getPropBool("@enableKeyDiff", true);

        // MORE: Get parms from topology after it is populated from Hardware/computer types section in configenv
        //       Then if does not match and based on desired action in topolgy, either warn, or fatal exit or .... etc
        //       Also get prim path and sec from topology
#ifdef _WIN32
        getHardwareInfo(hdwInfo, "C:", "D:");
#else // linux
        getHardwareInfo(hdwInfo, "/c$", "/d$");
#endif
        if (traceLevel)
        {
            DBGLOG("Current Hardware Info: CPUs=%i, speed=%i MHz, Mem=%i MB , primDisk=%i GB, primFree=%i GB, secDisk=%i GB, secFree=%i GB, NIC=%i", 
              hdwInfo.numCPUs, hdwInfo.CPUSpeed, hdwInfo.totalMemory, 
              hdwInfo.primDiskSize, hdwInfo.primFreeSize, hdwInfo.secDiskSize, hdwInfo.secFreeSize, hdwInfo.NICSpeed);
        }
        parallelAggregate = topology->getPropInt("@parallelAggregate", 0);
        if (!parallelAggregate)
            parallelAggregate = hdwInfo.numCPUs;
        if (!parallelAggregate)
            parallelAggregate = 1;
        simpleLocalKeyedJoins = topology->getPropBool("@simpleLocalKeyedJoins", true);
        inMemoryKeysEnabled = topology->getPropBool("@inMemoryKeysEnabled", true);
        serverSideCacheSize = topology->getPropInt("@serverSideCacheSize", 0);

        setKeyIndexCacheSize((unsigned)-1); // unbound
        nodeCachePreload = topology->getPropBool("@nodeCachePreload", false);
        setNodeCachePreload(nodeCachePreload);
        nodeCacheMB = topology->getPropInt("@nodeCacheMem", 100); 
        setNodeCacheMem(nodeCacheMB * 0x100000);
        leafCacheMB = topology->getPropInt("@leafCacheMem", 50);
        setLeafCacheMem(leafCacheMB * 0x100000);
        blobCacheMB = topology->getPropInt("@blobCacheMem", 0);
        setBlobCacheMem(blobCacheMB * 0x100000);

        unsigned __int64 affinity = topology->getPropInt64("@affinity", 0);
        updateAffinity(affinity);

        minFreeDiskSpace = topology->getPropInt64("@minFreeDiskSpace", (1024 * 0x100000)); // default to 1 GB
        if (topology->getPropBool("@jumboFrames", false))
        {
            mtu_size = 9000;    // upper limit on outbound buffer size - allow some header room too
            roxiemem::setDataAlignmentSize(0x2000);
        }
        else
        {
            mtu_size = 1400;    // upper limit on outbound buffer size - allow some header room too
            roxiemem::setDataAlignmentSize(0x400);
        }
        unsigned pinterval = topology->getPropInt("@systemMonitorInterval",1000*60);
        perfMonHook.setown(roxiemem::createRoxieMemStatsPerfMonHook());  // Note - we create even if pinterval is 0, as can be enabled via control message
        if (pinterval)
            startPerformanceMonitor(pinterval, PerfMonStandard, perfMonHook);


        topology->getProp("@pluginDirectory", pluginDirectory);
        if (pluginDirectory.length() == 0)
            pluginDirectory.append(codeDirectory).append("plugins");
        getAdditionalPluginsPath(pluginDirectory, codeDirectory);
        if (queryDirectory.length() == 0)
        {
            topology->getProp("@queryDir", queryDirectory);
            if (queryDirectory.length() == 0)
                queryDirectory.append(codeDirectory).append("queries");
        }
        addNonEmptyPathSepChar(queryDirectory);
        queryFileCache().start();
        getTempFilePath(tempDirectory, "roxie", topology);

#ifdef _WIN32
        topology->addPropBool("@linuxOS", false);
#else
        topology->addPropBool("@linuxOS", true);
#endif
        allQuerySetNames.appendListUniq(topology->queryProp("@querySets"), ",");
        Owned<IPropertyTreeIterator> roxieServers = topology->getElements("./RoxieServerProcess");
        ForEach(*roxieServers)
        {
            IPropertyTree &roxieServer = roxieServers->query();
            const char *iptext = roxieServer.queryProp("@netAddress");
            unsigned nodeIndex = addRoxieNode(iptext);
            if (getNodeAddress(nodeIndex).isLocal())
                myNodeIndex = nodeIndex;
            if (traceLevel > 3)
                DBGLOG("Roxie server %u is at %s", nodeIndex, iptext);
        }
        if (myNodeIndex == -1)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - current node is not in server list");

        // Set multicast base addresses - must be done before generating slave channels

        if (roxieMulticastEnabled && !localSlave)
        {
            if (topology->queryProp("@multicastBase"))
                multicastBase.ipset(topology->queryProp("@multicastBase"));
            else
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - multicastBase not set");
            if (topology->queryProp("@multicastLast"))
                multicastLast.ipset(topology->queryProp("@multicastLast"));
            else
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - multicastLast not set");
        }

        // Generate the slave channels
        unsigned numDataCopies = topology->getPropInt("@numDataCopies", 1);
        if (!numDataCopies)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numDataCopies should be > 0");
        unsigned channelsPerNode = topology->getPropInt("@channelsPerNode", 1);
        unsigned numNodes = getNumNodes();
        const char *slaveConfig = topology->queryProp("@slaveConfig");
        if (!slaveConfig)
            slaveConfig = "simple";
        if (strnicmp(slaveConfig, "cyclic", 6) == 0)
        {
            numChannels = numNodes;
            unsigned cyclicOffset = topology->getPropInt("@cyclicOffset", 1);
            for (unsigned i=0; i<numNodes; i++)
            {
                // Note this code is a little confusing - easy to get the cyclic offset backwards
                // cyclic offset means node n+offset has copy 2 for channel n, so node n has copy 2 for channel n-offset
                int channel = (int)i+1;
                for (unsigned copy=0; copy<numDataCopies; copy++)
                {
                    if (channel < 1)
                        channel = channel + numNodes;
                    addChannel(i, channel, copy);
                    channel -= cyclicOffset;
                }
            }
        }
        else if (strnicmp(slaveConfig, "overloaded", 10) == 0)
        {
            if (!channelsPerNode)
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - channelsPerNode should be > 0");
            numChannels = numNodes * channelsPerNode;
            for (unsigned i=0; i<numNodes; i++)
            {
                int channel = (int)(i+1);
                for (unsigned copy=0; copy<channelsPerNode; copy++)
                {
                    addChannel(i, channel, copy);
                    channel += numNodes;
                }
            }
        }
        else    // 'Full redundancy' or 'simple' mode
        {
            if (numNodes % numDataCopies)
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels not an integer");
            numChannels = numNodes / numDataCopies;
            int channel = 1;
            for (unsigned i=0; i<numNodes; i++)
            {
                addChannel(i, channel, 0);
                channel++;
                if ((unsigned)channel > numChannels)
                    channel = 1;
            }
        }
        if (!numChannels)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels calculated at 0");
        if (numChannels > 1 && localSlave)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - localSlave requires single channel (%d channels specified)", numChannels);
        // Now we know all the channels, we can open and subscribe the multicast channels
        if (!localSlave)
            openMulticastSocket();

        setDaliServixSocketCaching(true);  // enable daliservix caching
        loadPlugins();
        createDelayedReleaser();
        globalPackageSetManager = createRoxiePackageSetManager(standAloneDll.getClear());
        globalPackageSetManager->load();
        unsigned snifferChannel = numChannels+2; // MORE - why +2 not +1 ??
        ROQ = createOutputQueueManager(snifferChannel, numSlaveThreads);
        ROQ->setHeadRegionSize(headRegionSize);
        ROQ->start();
        Owned<IPacketDiscarder> packetDiscarder = createPacketDiscarder();
#if defined(WIN32) && defined(_DEBUG) && defined(_DEBUG_HEAP_FULL)
        int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
        tmpFlag |= _CRTDBG_CHECK_ALWAYS_DF;
        _CrtSetDbgFlag( tmpFlag );
#endif
        EnableSEHtoExceptionMapping();
        setSEHtoExceptionHandler(&abortHandler);
        Owned<IHpccProtocolPluginContext> protocolCtx = new CHpccProtocolPluginCtx();
        if (runOnce)
        {
            if (globals->hasProp("-wu"))
            {
                Owned<IHpccProtocolListener> roxieServer = createRoxieWorkUnitListener(1, false);
                try
                {
                    VStringBuffer x("-%s", argv[0]);
                    roxieServer->runOnce(x);
                    fflush(stdout);  // in windows if output is redirected results don't appear without flushing
                }
                catch (IException *E)
                {
                    EXCLOG(E);
                    E->Release();
                }
            }
            else
            {
                Owned<IHpccProtocolPlugin> protocolPlugin = loadHpccProtocolPlugin(protocolCtx, NULL);
                Owned<IHpccProtocolListener> roxieServer = protocolPlugin->createListener("runOnce", createRoxieProtocolMsgSink(getNodeAddress(myNodeIndex), 0, 1, false), 0, 0, NULL);
                try
                {
                    const char *format = globals->queryProp("format");
                    if (!format)
                    {
                        if (globals->hasProp("-xml"))
                            format = "xml";
                        else if (globals->hasProp("-csv"))
                            format = "csv";
                        else if (globals->hasProp("-raw"))
                            format = "raw";
                        else
                            format = "ascii";
                    }
                    StringBuffer query;
                    query.appendf("<roxie format='%s'/>", format);
                    roxieServer->runOnce(query.str()); // MORE - should use the wu listener instead I suspect
                    fflush(stdout);  // in windows if output is redirected results don't appear without flushing
                }
                catch (IException *E)
                {
                    EXCLOG(E);
                    E->Release();
                }
            }
        }
        else
        {
            Owned<IPropertyTreeIterator> roxieFarms = topology->getElements("./RoxieFarmProcess");
            ForEach(*roxieFarms)
            {
                IPropertyTree &roxieFarm = roxieFarms->query();
                unsigned listenQueue = roxieFarm.getPropInt("@listenQueue", DEFAULT_LISTEN_QUEUE_SIZE);
                unsigned numThreads = roxieFarm.getPropInt("@numThreads", numServerThreads);
                unsigned port = roxieFarm.getPropInt("@port", ROXIE_SERVER_PORT);
                unsigned requestArrayThreads = roxieFarm.getPropInt("@requestArrayThreads", 5);
                // NOTE: farmer name [@name=] is not copied into topology
                const IpAddress &ip = getNodeAddress(myNodeIndex);
                if (!roxiePort)
                {
                    roxiePort = port;
                    ownEP.set(roxiePort, ip);
                }
                bool suspended = roxieFarm.getPropBool("@suspended", false);
                Owned <IHpccProtocolListener> roxieServer;
                if (port)
                {
                    const char *protocol = roxieFarm.queryProp("@protocol");
                    StringBuffer certFileName;
                    StringBuffer keyFileName;
                    StringBuffer passPhraseStr;
                    if (protocol && streq(protocol, "ssl"))
                    {
#ifdef _USE_OPENSSL
                        const char *certFile = roxieFarm.queryProp("@certificateFileName");
                        if (!certFile)
                            throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing certificateFileName tag", port);
                        if (isAbsolutePath(certFile))
                            certFileName.append(certFile);
                        else
                            certFileName.append(codeDirectory.str()).append(certFile);
                        if (!checkFileExists(certFileName.str()))
                            throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing certificateFile (%s)", port, certFileName.str());

                        const char *keyFile = roxieFarm.queryProp("@privateKeyFileName");
                        if (!keyFile)
                            throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing privateKeyFileName tag", port);
                        if (isAbsolutePath(keyFile))
                            keyFileName.append(keyFile);
                        else
                            keyFileName.append(codeDirectory.str()).append(keyFile);
                        if (!checkFileExists(keyFileName.str()))
                            throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing privateKeyFile (%s)", port, keyFileName.str());

                        const char *passPhrase = roxieFarm.queryProp("@passphrase");
                        if (!isEmptyString(passPhrase))
                            decrypt(passPhraseStr, passPhrase);
#else
                        WARNLOG("Skipping Roxie SSL Farm Listener on port %d : OpenSSL disabled in build", port);
                        continue;
#endif
                    }
                    const char *soname =  roxieFarm.queryProp("@so");
                    const char *config  = roxieFarm.queryProp("@config");
                    Owned<IHpccProtocolPlugin> protocolPlugin = ensureProtocolPlugin(*protocolCtx, soname);
                    roxieServer.setown(protocolPlugin->createListener(protocol ? protocol : "native", createRoxieProtocolMsgSink(ip, port, numThreads, suspended), port, listenQueue, config, certFileName.str(), keyFileName.str(), passPhraseStr.str()));
                }
                else
                    roxieServer.setown(createRoxieWorkUnitListener(numThreads, suspended));

                IHpccProtocolMsgSink *sink = roxieServer->queryMsgSink();
                const char *aclName = roxieFarm.queryProp("@aclName");
                if (aclName && *aclName)
                {
                    Owned<IPropertyTree> aclInfo = createPTree("AccessInfo", ipt_lowmem);
                    getAccessList(aclName, topology, aclInfo);
                    Owned<IPropertyTreeIterator> accesses = aclInfo->getElements("Access");
                    ForEach(*accesses)
                    {
                        IPropertyTree &access = accesses->query();
                        try
                        {
                            sink->addAccess(access.getPropBool("@allow", true), access.getPropBool("@allowBlind", true), access.queryProp("@ip"), access.queryProp("@mask"), access.queryProp("@query"), access.queryProp("@error"), access.getPropInt("@errorCode"));
                        }
                        catch (IException *E)
                        {
                            StringBuffer s, x;
                            E->errorMessage(s);
                            E->Release();
                            toXML(&access, x, 0, 0);
                            throw MakeStringException(ROXIE_ACL_ERROR, "Error in access statement %s: %s", x.str(), s.str());
                        }
                    }
                }
                socketListeners.append(*roxieServer.getLink());
                time(&startupTime);
                roxieServer->start();
            }
            writeSentinelFile(sentinelFile);
            DBGLOG("Startup completed - LPT=%u APT=%u", queryNumLocalTrees(), queryNumAtomTrees());
            DBGLOG("Waiting for queries");
            if (pingInterval)
                startPingTimer();
            LocalIAbortHandler abortHandler(waiter);
            waiter.wait();
        }
        shuttingDown = true;
        if (pingInterval)
            stopPingTimer();
        setSEHtoExceptionHandler(NULL);
        while (socketListeners.isItem(0))
        {
            socketListeners.item(0).stop(1000);
            socketListeners.remove(0);
        }
        packetDiscarder->stop();
        packetDiscarder.clear();
        ROQ->stop();
        ROQ->join();
        ROQ->Release();
        ROQ = NULL;
    }
    catch (IException *E)
    {
        StringBuffer x;
        DBGLOG("EXCEPTION: (%d): %s", E->errorCode(), E->errorMessage(x).str());
        E->Release();
    }

    roxieMetrics.clear();
    stopPerformanceMonitor();
    ::Release(globalPackageSetManager);
    globalPackageSetManager = NULL;
    stopDelayedReleaser();
    cleanupPlugins();
    closeMulticastSockets();
    releaseSlaveDynamicFileCache();
    releaseRoxieStateCache();
    setDaliServixSocketCaching(false);  // make sure it cleans up or you get bogus memleak reports
    setNodeCaching(false); // ditto
    perfMonHook.clear();
    strdup("Make sure leak checking is working");
    UseSysLogForOperatorMessages(false);
    ExitModuleObjects();
    releaseAtoms();
    strdup("Make sure leak checking is working");
#ifdef _WIN32
#ifdef _DEBUG
#if 1
    StringBuffer leakFileDir(logDirectory.str());
    leakFileDir.append("roxieleaks.log");
    HANDLE h = CreateFile(leakFileDir.str(), GENERIC_READ|GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, 0, 0);
    _CrtSetReportMode( _CRT_WARN, _CRTDBG_MODE_FILE|_CRTDBG_MODE_DEBUG);
    _CrtSetReportFile( _CRT_WARN, h);
    _CrtSetReportMode( _CRT_ERROR, _CRTDBG_MODE_FILE|_CRTDBG_MODE_DEBUG);
    _CrtSetReportFile( _CRT_ERROR, h);
    _CrtSetReportMode( _CRT_ASSERT, _CRTDBG_MODE_FILE|_CRTDBG_MODE_DEBUG);
    _CrtSetReportFile( _CRT_ASSERT, h);
//  _CrtDumpMemoryLeaks(); if you uncomment these lines you get to see the leaks sooner (so can look in debugger at full memory) 
//   CloseHandle(h); but there will be additional leaks reported that are not really leaks
#endif
#endif
#endif
    return 0;
}
