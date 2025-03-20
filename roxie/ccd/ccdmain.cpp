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
#include <algorithm>
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
#include "jsecrets.hpp"
#include "udptopo.hpp"

#include "rtlformat.hpp"

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
#include "hpccconfig.hpp"
#include "udpsha.hpp"

#ifdef _USE_OPENSSL
#include "securesocket.hpp"
#endif

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
unsigned callbackRetries = 3;
unsigned callbackTimeout = 5000;
unsigned lowTimeout = 10000;
unsigned highTimeout = 2000;
unsigned slaTimeout = 2000;
unsigned numServerThreads = 30;
unsigned numAgentThreads = 30;
bool prestartAgentThreads = false;
unsigned numRequestArrayThreads = 5;
bool blockedLocalAgent = true;
bool acknowledgeAllRequests = true;
unsigned packetAcknowledgeTimeout = 100;
cycle_t dynPriorityAdjustCycles = 0;   // default off (0)
bool traceThreadStartDelay = true;
int adjustBGThreadNiceValue = 5;
unsigned ccdMulticastPort;
bool enableHeartBeat = true;
unsigned parallelLoopFlowLimit = 100;
unsigned perChannelFlowLimit = 10;
time_t startupTime;
unsigned statsExpiryTime = 3600;
unsigned miscDebugTraceLevel = 0;      // separate trace settings purely for debugging specific items (i.e. all possible locations to look for files at startup)
bool traceRemoteFiles = false;
bool traceStrands = false;
unsigned readTimeout = 300;            // timeout (in ms) for reading input data blocks in roxiepipe mode
unsigned indexReadChunkSize = 60000;
unsigned maxBlockSize = 10000000;
unsigned maxLockAttempts = 5;
bool pretendAllOpt = false;
bool traceStartStop = false;
bool traceActivityCharacteristics = false;
unsigned actResetLogPeriod = 300;
bool delaySubchannelPackets = false;    // For debugging/testing purposes only
bool defaultTimeActivities = true;
bool defaultTraceEnabled = false;
bool traceTranslations = true;
unsigned IBYTIbufferSize = 0;
unsigned IBYTIbufferLifetime = 50;  // In milliseconds
unsigned defaultTraceLimit = 10;
unsigned watchActivityId = 0;
unsigned testAgentFailure = 0;
RelaxedAtomic<unsigned> restarts;
RecordTranslationMode fieldTranslationEnabled = RecordTranslationMode::PayloadRemoveOnly;
bool mergeAgentStatistics = true;
PTreeReaderOptions defaultXmlReadFlags = ptr_ignoreWhiteSpace;
bool runOnce = false;
bool oneShotRoxie = false;
unsigned minPayloadSize = 800;

unsigned udpMulticastBufferSize = 262142;
#if !defined(_CONTAINERIZED) && !defined(SUBCHANNELS_IN_HEADER)
bool roxieMulticastEnabled = true;
#else
unsigned myChannel;
#endif

IPropertyTree *topology;
MapStringTo<int> *preferredClusters;
StringBuffer topologyFile;
CriticalSection ccdChannelsCrit;
StringArray allQuerySetNames;

bool alwaysTrustFormatCrcs;
bool allFilesDynamic;
bool lockSuperFiles;
bool useRemoteResources;
bool checkFileDate;
bool lazyOpen;
bool localAgent = false;
bool encryptInTransit;
bool ignoreOrphans;
bool doIbytiDelay = true; 
bool copyResources;
bool chunkingHeap = true;
bool logFullQueries;
bool alwaysSendSummaryStats = false;
bool blindLogging = false;
bool debugPermitted = true;
bool checkCompleted = true;
unsigned preabortKeyedJoinsThreshold = 100;
unsigned preabortIndexReadsThreshold = 100;
bool preloadOnceData;
bool reloadRetriesFailed;
bool selfTestMode = false;
bool defaultCollectFactoryStatistics = true;
bool defaultExecuteDependenciesSequentially = false;
bool defaultStartInputsSequentially = false;
bool defaultNoSeekBuildIndex = false;
unsigned parallelQueryLoadThreads = 0;               // Number of threads to use for parallel loading of queries. 0 means don't (may cause CPU starvation on other vms)
bool alwaysFailOnLeaks = false;
bool ignoreFileDateMismatches = false;
bool ignoreFileSizeMismatches = false;
int fileTimeFuzzySeconds = 0;
SinkMode defaultSinkMode = SinkMode::Automatic;
unsigned continuationCompressThreshold = 1024;

bool useOldTopology = false;

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
bool defaultDisableLocalOptimizations = false;
unsigned defaultStrandBlockSize = 512;
unsigned defaultForceNumStrands = 0;
unsigned defaultHeapFlags = roxiemem::RHFnone;

unsigned agentQueryReleaseDelaySeconds = 60;
unsigned coresPerQuery = 0;

unsigned logQueueLen;
unsigned logQueueDrop;
bool useLogQueue;
bool fastLaneQueue;
unsigned mtu_size = 1400; // upper limit on outbound buffer size - allow some header room too
StringBuffer fileNameServiceDali;
StringBuffer roxieName;
StringBuffer allowedPipePrograms;
#ifdef _CONTAINERIZED
StringBuffer defaultPlane;
StringBuffer defaultIndexBuildPlane;
#endif
bool trapTooManyActiveQueries;
unsigned maxEmptyLoopIterations;
unsigned maxGraphLoopIterations;
bool steppingEnabled = true;
bool simpleLocalKeyedJoins = true;
bool adhocRoxie = false;
bool limitWaitingWorkers = false;

unsigned __int64 minFreeDiskSpace = 1024 * 0x100000;  // default to 1 GB
unsigned socketCheckInterval = 5000;

unsigned cacheReportPeriodSeconds = 5*60;
stat_type minimumInterestingActivityCycles;

StringBuffer logDirectory;
StringBuffer pluginDirectory;
StringBuffer queryDirectory;
StringBuffer codeDirectory;
StringBuffer spillDirectory;
StringBuffer tempDirectory;

ClientCertificate clientCert;
bool useHardLink;

unsigned __int64 maxFileAgeNS[2] = {0xffffffffffffffffULL, 60*60*1000*1000000ULL}; // local files don't expire, remote expire in 1 hour, by default
unsigned minFilesOpen[2] = {2000, 500};
unsigned maxFilesOpen[2] = {4000, 1000};

SocketEndpoint debugEndpoint;
HardwareInfo hdwInfo;
unsigned parallelAggregate;
bool inMemoryKeysEnabled = true;

unsigned nodeCacheMB = 100;
unsigned leafCacheMB = 50;
unsigned blobCacheMB = 0;

unsigned roxiePort = 0;
ISyncedPropertyTree *roxiePortTlsClientConfig = nullptr;

#ifndef _CONTAINERIZED
Owned<IPerfMonHook> perfMonHook;
#endif

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    topology = NULL;
    roxiePortTlsClientConfig = nullptr;

    return true;
}

MODULE_EXIT()
{
    ::Release(roxiePortTlsClientConfig);
    ::Release(topology);
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

static Semaphore closedDown;

void closedown()
{
    waiter.onAbort();
    closedDown.wait();
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
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
    printf("  --[xml|csv|raw]           : Output format (default ascii)\n");
    printf("  --daliServers=[host1,...] : List of Dali servers to use\n");
    printf("  --tracelevel=[integer]    : Amount of information to dump on logs\n");
    printf("  --stdlog=[boolean]        : Standard log format (based on tracelevel)\n");
    printf("  --logfile=[filename]      : Outputs to logfile, rather than stdout\n");
    printf("  --help|-h                 : This message\n");
    printf("\n");
}

class MAbortHandler : implements IExceptionHandler
{
public:
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

void saveTopology(bool lockDali)
{
    // Write back changes that have been made via control:(un)lockDali changes, so that they survive a roxie restart
    // Note that they are overwritten when Roxie is manually stopped/started via hpcc-init service - these changes
    // are only intended to be temporary for the current session
    if (!useOldTopology)
        return;
    try
    {
        Owned<IPTree> tempTopology = createPTreeFromXMLFile(topologyFile.str(), ipt_caseInsensitive);
        tempTopology->setPropBool("@lockDali", lockDali);
        saveXML(topologyFile.str(), tempTopology);
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

static std::vector<RoxieEndpointInfo> myRoles;
static std::vector<std::pair<unsigned, unsigned>> agentChannels;

void *leakChecker = nullptr;   // Used to deliberately leak an allocation to ensure leak checking is working

unsigned __int64 currentTopologyHash = 0;
unsigned __int64 originalTopologyHash = 0;

hash64_t getTopologyHash()
{
    StringBuffer xml;
    toXML(topology, xml, 0, XML_SortTags);
    return rtlHash64Data(xml.length(), xml.str(), 707018);
}

#ifndef _CONTAINERIZED
void readStaticTopology()
{
    // If dynamicServers not set, we read a list of all servers form the topology file, and deduce which ones are on which channel
    // and the total number of channels
    std::vector<RoxieEndpointInfo> allRoles;
    IpAddressArray nodeTable;
    unsigned numNodes = topology->getCount("./RoxieServerProcess");
    if (!numNodes && oneShotRoxie)
    {
        if (topology->getPropBool("expert/@addDummyNode", false))
        {
            // Special config for testing some multinode things on a single node
            topology->addPropTree("RoxieServerProcess")->setProp("@netAddress", ".");
            topology->addPropTree("RoxieServerProcess")->setProp("@netAddress", "192.0.2.0");  // A non-existent machine (this address is reserved for documentation)
            numNodes = 2;
            localAgent = false;
            topology->setPropInt("@numChannels", 2);
            numChannels = 2;
            topology->setPropInt("@numDataCopies", 2);
            topology->setPropInt("@channelsPerNode", 2);
            topology->setProp("@agentConfig", "cyclic");
        }
        else if (oneShotRoxie)
        {
            topology->addPropTree("RoxieServerProcess")->setProp("@netAddress", ".");
            numNodes = 1;
        }
    }
    Owned<IPropertyTreeIterator> roxieServers = topology->getElements("./RoxieServerProcess");

    bool myNodeSet = false;
    unsigned calcNumChannels = 0;
    ForEach(*roxieServers)
    {
        IPropertyTree &roxieServer = roxieServers->query();
        const char *iptext = roxieServer.queryProp("@netAddress");
        IpAddress ip(iptext);
        if (ip.isNull())
            throw MakeStringException(ROXIE_UDP_ERROR, "Could not resolve address %s", iptext);
        if (ip.isLocal() && !myNodeSet)
        {
            myNodeSet = true;
            myNode.setIp(ip);
            myAgentEP.set(ccdMulticastPort, myNode.getIpAddress());
        }
        ForEachItemIn(idx, nodeTable)
        {
            if (ip.ipequals(nodeTable.item(idx)))
                throw MakeStringException(ROXIE_UDP_ERROR, "Duplicated node %s in RoxieServerProcess list", iptext);
        }
        nodeTable.append(ip);
        Owned<IPropertyTreeIterator> roxieFarms = topology->getElements("./RoxieFarmProcess");
        ForEach(*roxieFarms)
        {
            IPropertyTree &roxieFarm = roxieFarms->query();
            unsigned port = roxieFarm.getPropInt("@port", ROXIE_SERVER_PORT);
            RoxieEndpointInfo server = {RoxieEndpointInfo::RoxieServer, 0, { (unsigned short) port, ip }, 0};
            allRoles.push_back(server);
        }
    }
    if (!myNodeSet)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - current node is not in server list");

    // Generate the agent channels

    unsigned numDataCopies = topology->getPropInt("@numDataCopies", 1);
    if (!numDataCopies)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numDataCopies should be > 0");
    unsigned channelsPerNode = topology->getPropInt("@channelsPerNode", 1);
    const char *agentConfig = topology->queryProp("@agentConfig");
    if (!agentConfig)
        agentConfig = topology->queryProp("@slaveConfig");  // legacy name
    if (!agentConfig)
        agentConfig = "simple";

    if (strnicmp(agentConfig, "cyclic", 6) == 0)
    {
        calcNumChannels = numNodes;
        unsigned cyclicOffset = topology->getPropInt("@cyclicOffset", 1);
        for (unsigned copy=0; copy<numDataCopies; copy++)
        {
            // Note this code is a little confusing - easy to get the cyclic offset backwards
            // cyclic offset means node n+offset has copy 2 for channel n, so node n has copy 2 for channel n-offset
            for (unsigned i=0; i<numNodes; i++)
            {
                int channel = (int)i+1 - (copy * cyclicOffset);
                while (channel < 1)
                    channel = channel + numNodes;
                RoxieEndpointInfo agent = {RoxieEndpointInfo::RoxieAgent, (unsigned) channel, { (unsigned short) ccdMulticastPort, nodeTable.item(i) }, copy};
                allRoles.push_back(agent);
            }
        }
    }
    else if (strnicmp(agentConfig, "overloaded", 10) == 0)
    {
        if (!channelsPerNode)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - channelsPerNode should be > 0");
        calcNumChannels = numNodes * channelsPerNode;
        for (unsigned copy=0; copy<channelsPerNode; copy++)
        {
            for (unsigned i=0; i<numNodes; i++)
            {
                unsigned channel = i+1;
                RoxieEndpointInfo agent = {RoxieEndpointInfo::RoxieAgent, channel, { (unsigned short) ccdMulticastPort, nodeTable.item(i) }, copy};
                allRoles.push_back(agent);
                channel += numNodes;
            }
        }
    }
    else    // 'Full redundancy' or 'simple' mode
    {
        if (numNodes % numDataCopies)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels not an integer");
        calcNumChannels = numNodes / numDataCopies;
        unsigned channel = 1;
        for (unsigned i=0; i<numNodes; i++)
        {
            RoxieEndpointInfo agent = {RoxieEndpointInfo::RoxieAgent, channel, { (unsigned short) ccdMulticastPort, nodeTable.item(i) }, 0 };
            allRoles.push_back(agent);
            channel++;
            if (channel > calcNumChannels)
                channel = 1;
        }
    }
    if (numChannels && numChannels != calcNumChannels)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels calculated at %u but specified as %u", calcNumChannels, numChannels);
    if (!calcNumChannels)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels calculated at 0");
    if (calcNumChannels > 1 && localAgent)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - localAgent requires single channel (%d channels specified)", calcNumChannels);
    numChannels = calcNumChannels;
    createStaticTopology(allRoles, traceLevel);
}
#endif

int CCD_API roxie_main(int argc, const char *argv[], const char * defaultYaml)
{
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

    EnableSEHtoExceptionMapping();
    setTerminateOnSEH();
    init_signals();
    // We need to do the above BEFORE we call InitModuleObjects
    try
    {
        InitModuleObjects();
    }
    catch (IException *E)
    {
        EXCLOG(E);
        E->Release();
        return EXIT_FAILURE;
    }
    init_signals();

    for (unsigned i=0; i<(unsigned)argc; i++)
    {
        if (stricmp(argv[i], "--help")==0 ||
            stricmp(argv[i], "-h")==0)
        {
            roxie_common_usage(argv[0]);
            return EXIT_SUCCESS;
        }
        else if (strsame(argv[i], "-xml"))  // Back compatibility
            argv[i] = "--xml";
        else if (strsame(argv[i], "-csv"))
            argv[i] = "--csv";
        else if (strsame(argv[i], "-raw"))
            argv[i] = "--raw";
    }

    #ifdef _USE_CPPUNIT
    if (argc>=2 && (stricmp(argv[1], "-selftest")==0 || stricmp(argv[1], "--selftest")==0))
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

    char currentDirectory[_MAX_DIR];
    if (!getcwd(currentDirectory, sizeof(currentDirectory)))
        throw MakeStringException(ROXIE_INTERNAL_ERROR, "getcwd failed (%d)", errno);

    codeDirectory.set(currentDirectory);
    addNonEmptyPathSepChar(codeDirectory);
    PerfTracer startupTracer;
    try
    {
        Owned<IFile> sentinelFile = createSentinelTarget();
        removeSentinelFile(sentinelFile);

        topologyFile.append(codeDirectory).append(PATHSEPCHAR).append("RoxieTopology.xml");
        useOldTopology = checkFileExists(topologyFile.str());
        topology = loadConfiguration(useOldTopology ? nullptr : defaultYaml, argv, "roxie", "ROXIE", topologyFile, nullptr, "@netAddress");
        saveTopology(topology->getPropBool("@lockDali", false));
        originalTopologyHash = currentTopologyHash = getTopologyHash();

        // Any settings we read from topology that must NOT be overridden in workunit debug fields should be read at this point, before the following section
        getAllowedPipePrograms(allowedPipePrograms, true);

        // Allow workunit debug fields to override most roxie configuration values, for testing/debug purposes.

        topology->getProp("@daliServers", fileNameServiceDali);
        const char *wuid = topology->queryProp("@workunit");
        if (wuid)
        {
            Owned<IRoxieDaliHelper> daliHelper;
            Owned<IConstWorkUnit> wu;
            daliHelper.setown(connectToDali(ROXIE_DALI_CONNECT_TIMEOUT));
            wu.setown(daliHelper->attachWorkunit(wuid));
            Owned<IStringIterator> debugValues = &wu->getDebugValues();
            ForEach (*debugValues)
            {
                StringBuffer debugStr;
                SCMStringBuffer valueStr;
                StringBufferAdaptor aDebugStr(debugStr);
                debugValues->str(aDebugStr);
                if (startsWith(debugStr, "roxie:"))
                {
                    wu->getDebugValue(debugStr.str(), valueStr);
                    debugStr.replaceString("roxie:", "@");
                    topology->setProp(debugStr.str(), valueStr.str());
                }
            }
        }

        if (topology->getPropBool("expert/@profileStartup", false))
        {
            double interval = topology->getPropReal("expert/@profileStartupInterval", 0.2);
            startupTracer.setInterval(interval);
            startupTracer.start();
        }
        localAgent = topology->getPropBool("@localAgent", topology->getPropBool("@localSlave", localAgent));  // legacy name
        encryptInTransit = topology->getPropBool("@encryptInTransit", false) && !localAgent;
        if (encryptInTransit)
            initSecretUdpKey();
        const char *topos = topology->queryProp("@topologyServers");
        StringArray topoValues;
        if (topos)
            topoValues.appendList(topos, ",", true);

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
#ifdef _CONTAINERIZED
        try
        {
            getDefaultStoragePlane(defaultPlane);
            getDefaultIndexBuildStoragePlane(defaultIndexBuildPlane);
        }
        catch (IException *E)
        {
#ifdef _DEBUG            
            E->Release();   // Useful for some local testing to be able to ignore these configuration errors
#else
            throw;
#endif
        }
#endif
        installDefaultFileHooks(topology);

        Owned<const IQueryDll> standAloneDll;
        if (wuid)
            setDefaultJobName(wuid);
        if (topology->hasProp("@loadWorkunit"))
        {
            StringBuffer workunitName;
            topology->getProp("@loadWorkunit", workunitName);
            standAloneDll.setown(createQueryDll(workunitName));
        }
        else
        {
            Owned<ILoadedDllEntry> dll = createExeDllEntry(argv[0]);
            if (containsEmbeddedWorkUnit(dll))
                standAloneDll.setown(createExeQueryDll(argv[0]));
        }
        if (standAloneDll || wuid)
        {
            oneShotRoxie = true;
            if (wuid)
                DBGLOG("Starting roxie - wuid=%s", wuid);
            if (topology->getPropBool("@server", false))
            {
#ifdef _CONTAINERIZED
                if (!topology->getCount("services"))
                {
                    // Makes debugging easier...
                    IPropertyTree *service = topology->addPropTree("services");
                    service->setProp("@name", "query");
                    service->setPropInt("@port", ROXIE_SERVER_PORT);
                }
#else
                if (!topology->getCount("RoxieFarmProcess"))
                {
                    // Makes debugging easier...
                    IPropertyTree *service = topology->addPropTree("RoxieFarmProcess");
                    service->setPropInt("@port", topology->getPropInt("@port", 9876));
                }
#endif
                runOnce = false;
            }
            else
                runOnce = true;
        }

        numChannels = topology->getPropInt("@numChannels", 0);
#ifdef _CONTAINERIZED
        if (!numChannels)
            throw makeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels not set");
#endif
        const char *channels = topology->queryProp("@channels");
        if (channels)
        {
            StringArray channelSpecs;
            channelSpecs.appendList(channels, ",", true);
            ForEachItemIn(idx, channelSpecs)
            {
                char *tail = nullptr;
                unsigned channel = strtoul(channelSpecs.item(idx), &tail, 10);
                unsigned repl = 0;
                if (*tail==':')
                {
                    tail++;
                    repl = atoi(tail);
                }
                else if (*tail)
                    throw makeStringExceptionV(ROXIE_INTERNAL_ERROR, "Invalid channel specification %s", channels);
                agentChannels.push_back(std::pair<unsigned, unsigned>(channel, repl));
            }
#ifdef _CONTAINERIZED
            if (agentChannels.size() != 1)
                throw makeStringExceptionV(ROXIE_INTERNAL_ERROR, "Invalid channel specification %s - single channel expected", channels);
            myChannel = agentChannels[0].first;
            if (myChannel > numChannels)
                throw makeStringExceptionV(ROXIE_INTERNAL_ERROR, "Invalid channel specification %s - value out of range", channels);
#endif
        }
#ifdef _CONTAINERIZED
        else if (oneShotRoxie)
        {
            for (unsigned channel = 1; channel <= numChannels; channel++)
                agentChannels.push_back(std::pair<unsigned, unsigned>(channel, 0));
        }
#endif

        if (!topology->hasProp("@resolveLocally"))
            topology->setPropBool("@resolveLocally", !topology->hasProp("@daliServers"));

        traceLevel = topology->getPropInt("@traceLevel", runOnce ? 0 : 1);
        if (traceLevel > MAXTRACELEVEL)
            traceLevel = MAXTRACELEVEL;
        if (traceLevel && topology->hasProp("logging/@disabled"))
            topology->setPropBool("logging/@disabled", false);
        udpStatsReportInterval = topology->getPropInt("@udpStatsReportInterval", traceLevel ? 60000 : 0);
        udpTraceFlow = topology->getPropBool("@udpTraceFlow", false);
        udpTraceTimeouts = topology->getPropBool("@udpTraceTimeouts", false);
        udpTraceLevel = topology->getPropInt("@udpTraceLevel", runOnce ? 0 : 1);
        roxiemem::setMemTraceLevel(topology->getPropInt("@memTraceLevel", runOnce ? 0 : 1));
        soapTraceLevel = topology->getPropInt("@soapTraceLevel", runOnce ? 0 : 1);
        if (topology->hasProp("@soapLogSepString"))
        {
            StringBuffer tmpSepString;
            topology->getProp("@soapLogSepString", tmpSepString);
            setSoapSepString(tmpSepString.str());
        }
        miscDebugTraceLevel = topology->getPropInt("@miscDebugTraceLevel", 0);
        traceRemoteFiles = topology->getPropBool("@traceRemoteFiles", false);
        testAgentFailure = topology->getPropInt("expert/@testAgentFailure", testAgentFailure);

        Linked<IPropertyTree> directoryTree = topology->queryPropTree("Directories");
#ifndef _CONTAINERIZED
        if (!directoryTree)
        {
            Owned<IPropertyTree> envFile = getHPCCEnvironment();
            if (envFile)
                directoryTree.set(envFile->queryPropTree("Software/Directories"));
        }
#endif
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

        // Tracing feature flags
        TraceFlags traceLevelFlag = traceLevel ? TraceFlags::Standard : TraceFlags::None;
        updateTraceFlags(loadTraceFlags(topology, roxieTraceOptions, traceLevelFlag | traceRoxieActiveQueries), true);

        //Logging stuff
#ifndef _CONTAINERIZED
        if (topology->getPropBool("@stdlog", traceLevel != 0) || topology->getPropBool("@forceStdLog", false))
        {
            if (topology->getPropBool("@minlog", false))
                queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_prefix);
            else
                queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_milliTime | MSGFIELD_thread | MSGFIELD_prefix);
        }
        else
            removeLog();
        if (topology->hasProp("@logfile"))
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
            if (topology->getPropBool("@enableSysLog",true))
                UseSysLogForOperatorMessages();
        }
#else
        setupContainerizedLogMsgHandler();
#endif

        roxieMetrics.setown(createRoxieMetricsManager());
        if (topology->getPropBool("@updateSecretsInBackground", !runOnce))
            startSecretUpdateThread(0);

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

        restarts = topology->getPropInt("@restarts", 0);
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
                DBGLOG("Roxie restarting: restarts = %d build = %s", restarts.load(), hpccBuildInfo.buildTag);
            setStartRuid(restarts);
        }
        else
        {
            if (traceLevel)
            {
                DBGLOG("Roxie starting, build = %s", hpccBuildInfo.buildTag);
            }
        }

        minPayloadSize = topology->getPropInt("@minPayloadSize", minPayloadSize);
        blockedLocalAgent = topology->getPropBool("@blockedLocalAgent", blockedLocalAgent);
        acknowledgeAllRequests = topology->getPropBool("@acknowledgeAllRequests", acknowledgeAllRequests);
        packetAcknowledgeTimeout = topology->getPropInt("@packetAcknowledgeTimeout", packetAcknowledgeTimeout);
        unsigned dynAdjustMsec = topology->getPropInt("@dynPriorityAdjustTime", 0);
        if (dynAdjustMsec)
            dynPriorityAdjustCycles = dynAdjustMsec * (queryOneSecCycles() / 1000ULL);
        traceThreadStartDelay = topology->getPropBool("@traceThreadStartDelay", traceThreadStartDelay);
        adjustBGThreadNiceValue = topology->getPropInt("@adjustBGThreadNiceValue", adjustBGThreadNiceValue);
        if (adjustBGThreadNiceValue < 0)
            adjustBGThreadNiceValue = 0;
        if (adjustBGThreadNiceValue > 19)
            adjustBGThreadNiceValue = 19;
        ccdMulticastPort = topology->getPropInt("@multicastPort", CCD_MULTICAST_PORT);
        statsExpiryTime = topology->getPropInt("@statsExpiryTime", 3600);
        roxiemem::setMemTraceSizeLimit((memsize_t) topology->getPropInt64("@memTraceSizeLimit", 0));
        callbackRetries = topology->getPropInt("@callbackRetries", 3);
        callbackTimeout = topology->getPropInt("@callbackTimeout", 5000);
        lowTimeout = topology->getPropInt("@lowTimeout", 10000);
        highTimeout = topology->getPropInt("@highTimeout", 2000);
        slaTimeout = topology->getPropInt("@slaTimeout", 2000);
        parallelLoopFlowLimit = topology->getPropInt("@parallelLoopFlowLimit", parallelLoopFlowLimit);
        perChannelFlowLimit = topology->getPropInt("@perChannelFlowLimit", 10);
        copyResources = (!oneShotRoxie) && topology->getPropBool("@copyResources", true);
        useRemoteResources = oneShotRoxie || topology->getPropBool("@useRemoteResources", !isContainerized());
        checkFileDate = topology->getPropBool("@checkFileDate", true);
        const char *lazyOpenMode = topology->queryProp("@lazyOpen");
        if (!lazyOpenMode || stricmp(lazyOpenMode, "smart")==0)
            lazyOpen = (restarts > 0);
        else
            lazyOpen = topology->getPropBool("@lazyOpen", false);
#ifndef _CONTAINERIZED
        bool useNasTranslation = topology->getPropBool("@useNASTranslation", true);
        if (useNasTranslation)
        {
            Owned<IPropertyTree> nas = envGetNASConfiguration(topology);
            envInstallNASHooks(nas);
        }
#endif
        doIbytiDelay = topology->getPropBool("@doIbytiDelay", true);
        minIbytiDelay = topology->getPropInt("@minIbytiDelay", 2);
        initIbytiDelay = topology->getPropInt("@initIbytiDelay", 50);
        alwaysTrustFormatCrcs = topology->getPropBool("@alwaysTrustFormatCrcs", true);
        allFilesDynamic = topology->getPropBool("@allFilesDynamic", false);
        lockSuperFiles = topology->getPropBool("@lockSuperFiles", false);
        ignoreOrphans = topology->getPropBool("@ignoreOrphans", true);
        chunkingHeap = topology->getPropBool("@chunkingHeap", true);
        readTimeout = topology->getPropInt("@readTimeout", 300);
        logFullQueries = topology->getPropBool("@logFullQueries", false);
        alwaysSendSummaryStats = topology->getPropBool("expert/@alwaysSendSummaryStats", alwaysSendSummaryStats);
        debugPermitted = topology->getPropBool("@debugPermitted", true);
        blindLogging = topology->getPropBool("@blindLogging", false);
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
        traceStrands = topology->getPropBool("@traceStrands", false);

        useMemoryMappedIndexes = topology->getPropBool("@useMemoryMappedIndexes", false);
        flushJHtreeCacheOnOOM = topology->getPropBool("@flushJHtreeCacheOnOOM", true);
        fastLaneQueue = topology->getPropBool("@fastLaneQueue", true);
        udpOutQsPriority = topology->getPropInt("@udpOutQsPriority", 0);
        udpSendTraceThresholdMs = topology->getPropInt("@udpSendTraceThresholdMs", udpSendTraceThresholdMs);

        //See the head of udptrr.cpp for details of the following options
        udpPermitTimeout = topology->getPropInt("@udpPermitTimeout", udpPermitTimeout);
        udpRequestTimeout = topology->getPropInt("@udpRequestTimeout", udpRequestTimeout);
        udpFlowAckTimeout = topology->getPropInt("@udpFlowAckTimeout", udpFlowAckTimeout);
        if (!udpFlowAckTimeout)
        {
            udpFlowAckTimeout = 5;
            if (!localAgent)
                DBGLOG("Bad or missing value for udpFlowAckTimeout - using %u", udpFlowAckTimeout);
        }

        udpMaxPermitDeadTimeouts = topology->getPropInt("@udpMaxPermitDeadTimeouts", udpMaxPermitDeadTimeouts);
        udpRequestDeadTimeout = topology->getPropInt("@udpRequestDeadTimeout", udpRequestDeadTimeout);

        if (!localAgent)
        {
            if (!udpMaxPermitDeadTimeouts)
                ERRLOG("Bad value for udpMaxPermitDeadTimeouts - using %u", udpMaxPermitDeadTimeouts);
            if (!udpRequestDeadTimeout)
                ERRLOG("Bad value for udpRequestDeadTimeout - using %u", udpRequestDeadTimeout);
        }

        updDataSendTimeout = topology->getPropInt("@udpDataSendTimeout", updDataSendTimeout);
        udpResendDelay = topology->getPropInt("@udpResendDelay", udpResendDelay);
        udpMaxClientPercent = topology->getPropInt("@udpMaxClientPercent", udpMaxClientPercent);

        // MORE: might want to check socket buffer sizes against sys max here instead of udp threads ?

#ifdef _CONTAINERIZED
        udpMulticastBufferSize = topology->getPropInt("@udpAgentBufferSize", 262142);
#else
        udpMulticastBufferSize = topology->getPropInt("@udpMulticastBufferSize", 262142);
#endif

        udpFlowSocketsSize = topology->getPropInt("@udpFlowSocketsSize", udpFlowSocketsSize);
        udpLocalWriteSocketSize = topology->getPropInt("@udpLocalWriteSocketSize", udpLocalWriteSocketSize);
#if !defined(_CONTAINERIZED) && !defined(SUBCHANNELS_IN_HEADER)
        roxieMulticastEnabled = topology->getPropBool("@roxieMulticastEnabled", true);   // enable use of multicast for sending requests to agents
#endif

        udpResendLostPackets = topology->getPropBool("@udpResendLostPackets", true);
        udpAssumeSequential = topology->getPropBool("@udpAssumeSequential", false);
        udpMaxPendingPermits = topology->getPropInt("@udpMaxPendingPermits", udpMaxPendingPermits);
        udpResendAllMissingPackets = topology->getPropBool("@udpResendAllMissingPackets", udpResendAllMissingPackets);
        udpAdjustThreadPriorities = topology->getPropBool("@udpAdjustThreadPriorities", udpAdjustThreadPriorities);
        udpAllowAsyncPermits = topology->getPropBool("@udpAllowAsyncPermits", udpAllowAsyncPermits);
        udpMinSlotsPerSender = topology->getPropInt("@udpMinSlotsPerSender", udpMinSlotsPerSender);
        udpRemoveDuplicatePermits = topology->getPropBool("@udpRemoveDuplicatePermits", udpRemoveDuplicatePermits);
        udpEncryptOnSendThread = topology->getPropBool("expert/@udpEncryptOnSendThread", udpEncryptOnSendThread);

        unsigned __int64 defaultNetworkSpeed = 10 * U64C(0x40000000); // 10Gb/s
        unsigned __int64 networkSpeed = topology->getPropInt64("@udpNetworkSpeed", defaultNetworkSpeed);   // only used to sanity check the different udp options
        unsigned udpQueueSize = topology->getPropInt("@udpQueueSize", UDP_QUEUE_SIZE);
        unsigned udpSendQueueSize = topology->getPropInt("@udpSendQueueSize", UDP_SEND_QUEUE_SIZE);
        sanityCheckUdpSettings(udpQueueSize, udpSendQueueSize, numChannels, networkSpeed);

        int ttlTmp = topology->getPropInt("@multicastTTL", 1);
        if (ttlTmp < 0)
        {
            multicastTTL = 1;
            IWARNLOG("multicastTTL value (%d) invalid, must be >=0, resetting to %u", ttlTmp, multicastTTL);
        }
        else if (ttlTmp > 255)
        {
            multicastTTL = 255;
            IWARNLOG("multicastTTL value (%d) invalid, must be <=%u, resetting to maximum", ttlTmp, multicastTTL);
        }
        else
            multicastTTL = ttlTmp;

        bool recordStartupEvents = topology->getPropBool("expert/@recordStartupEvents", false);
        if (topology->getPropBool("@recordAllEvents", false) || recordStartupEvents)
        {
            const char * recordEventOptions = topology->queryProp("@recordEventOptions");
            const char * optRecordEventFilename = topology->queryProp("@recordEventFilename");
            startRoxieEventRecording(recordEventOptions, optRecordEventFilename);
        }

        workunitGraphCacheEnabled = topology->getPropBool("expert/@workunitGraphCacheEnabled", workunitGraphCacheEnabled);

        indexReadChunkSize = topology->getPropInt("@indexReadChunkSize", 60000);
        numAgentThreads = topology->getPropInt("@agentThreads", topology->getPropInt("@slaveThreads", 30));  // legacy name
        numServerThreads = topology->getPropInt("@serverThreads", 30);
        numRequestArrayThreads = topology->getPropInt("@requestArrayThreads", 5);
        maxBlockSize = topology->getPropInt("@maxBlockSize", 10000000);
        maxLockAttempts = topology->getPropInt("@maxLockAttempts", 5);
        enableHeartBeat = topology->getPropBool("@enableHeartBeat", true);
        checkCompleted = topology->getPropBool("@checkCompleted", true);
        prestartAgentThreads = topology->getPropBool("@prestartAgentThreads", topology->getPropBool("@prestartSlaveThreads", false));  // legacy name
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
        continuationCompressThreshold = (unsigned) topology->getPropInt64("@continuationCompressThreshold", 1024);

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
        defaultDisableLocalOptimizations = topology->getPropBool("@disableLocalOptimizations", false);  // NOTE - not in configmgr - too dangerous!

        agentQueryReleaseDelaySeconds = topology->getPropInt("@agentQueryReleaseDelaySeconds", topology->getPropInt("@slaveQueryReleaseDelaySeconds", 60));  // legacy name
        coresPerQuery = topology->getPropInt("@coresPerQuery", 0);

        fieldTranslationEnabled = RecordTranslationMode::PayloadRemoveOnly;
        const char *val = topology->queryProp("@fieldTranslationEnabled");
        if (val)
            fieldTranslationEnabled = getTranslationMode(val, false);

        pretendAllOpt = topology->getPropBool("@ignoreMissingFiles", false);
        memoryStatsInterval = topology->getPropInt("@memoryStatsInterval", 60);
        roxiemem::setMemoryStatsInterval(memoryStatsInterval);
        socketCheckInterval = topology->getPropInt("@socketCheckInterval", runOnce ? 0 : 5000);
        const char *totalMemoryString = topology->queryProp("@totalMemoryLimit");
        memsize_t totalMemoryLimit = totalMemoryString ? friendlyStringToSize(totalMemoryString) : 0;
        bool allowHugePages = topology->getPropBool("@heapUseHugePages", false);
        bool allowTransparentHugePages = topology->getPropBool("@heapUseTransparentHugePages", true);
        bool retainMemory = topology->getPropBool("@heapRetainMemory", false);
        // NB: This could be in a isContainerized(), but the resource sections only apply to containerized setups
        memsize_t maxTotalMemoryLimit = 0;
        constexpr float roxieMemResourcedMemoryPct = 75.0;
        const char *resourcedMemory = topology->queryProp("resources/@memory");
        if (isEmptyString(resourcedMemory))
        {
            if (topology->getPropBool("@server"))
                resourcedMemory = topology->queryProp("serverResources/@memory");
            else
                resourcedMemory = topology->queryProp("channelResources/@memory");
        }
        if (!isEmptyString(resourcedMemory))
        {
            maxTotalMemoryLimit = friendlyStringToSize(resourcedMemory);
            maxTotalMemoryLimit = maxTotalMemoryLimit / 100.0 * roxieMemResourcedMemoryPct;
        }
        bool lockMemory = topology->getPropBool("@heapLockMemory", true);
        if (!totalMemoryLimit)
            totalMemoryLimit = 1024 * 0x100000; // 1 Gb
        if (maxTotalMemoryLimit && (totalMemoryLimit > maxTotalMemoryLimit))
        {
            LOG(MCoperatorWarning, "roxie.totalMemoryLimit(%zu) is greater than %.1f%% of resources/@memory, limiting to %zu", totalMemoryLimit, roxieMemResourcedMemoryPct, maxTotalMemoryLimit);
            totalMemoryLimit = maxTotalMemoryLimit;
        }
        roxiemem::setTotalMemoryLimit(allowHugePages, allowTransparentHugePages, retainMemory, lockMemory, totalMemoryLimit, 0, NULL, NULL);
        roxiemem::setMemoryOptions(topology);

        traceStartStop = topology->getPropBool("@traceStartStop", false);
        traceActivityCharacteristics = topology->getPropBool("@traceActivityCharacteristics", traceActivityCharacteristics);
        actResetLogPeriod = topology->getPropInt("@actResetLogPeriod", 300);
        watchActivityId = topology->getPropInt("@watchActivityId", 0);
        delaySubchannelPackets = topology->getPropBool("@delaySubchannelPackets", false);
        IBYTIbufferSize = topology->getPropInt("@IBYTIbufferSize", roxieMulticastEnabled ? 0 : 10);
        IBYTIbufferLifetime = topology->getPropInt("@IBYTIbufferLifetime", initIbytiDelay);
        traceTranslations = topology->getPropBool("@traceTranslations", true);
        defaultTimeActivities = topology->getPropBool("@timeActivities", true);
        defaultTraceEnabled = topology->getPropBool("@traceEnabled", false);
        defaultTraceLimit = topology->getPropInt("@traceLimit", 10);
        clientCert.certificate.set(topology->queryProp("@certificateFileName"));
        clientCert.privateKey.set(topology->queryProp("@privateKeyFileName"));
        clientCert.passphrase.set(topology->queryProp("@passphrase"));
        useHardLink = topology->getPropBool("@useHardLink", false);
        unsigned temp = topology->getPropInt("@localFilesExpire", (unsigned) -1);
        if (temp && temp != (unsigned) -1)
            maxFileAgeNS[false] = milliToNano(temp);
        else
            maxFileAgeNS[false] = (unsigned __int64) -1;
        temp = topology->getPropInt("@remoteFilesExpire", 60*60*1000);
        if (temp && temp != (unsigned) -1)
            maxFileAgeNS[true] = milliToNano(temp);
        else
            maxFileAgeNS[true] = (unsigned __int64) -1;
        maxFilesOpen[false] = topology->getPropInt("@maxLocalFilesOpen", 20000);
        maxFilesOpen[true] = topology->getPropInt("@maxRemoteFilesOpen", 1000);
        minFilesOpen[false] = topology->getPropInt("@minLocalFilesOpen", maxFilesOpen[false]/2);
        if (minFilesOpen[false] >= maxFilesOpen[false])
           throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid settings - minLocalFilesOpen should be less than maxLocalFilesOpen");
        minFilesOpen[true] = topology->getPropInt("@minRemoteFilesOpen", maxFilesOpen[true]/2);
        if (minFilesOpen[true] >= maxFilesOpen[true])
           throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid settings - minRemoteFilesOpen should be less than maxRemoteFilesOpen");
        dafilesrvLookupTimeout = topology->getPropInt("@dafilesrvLookupTimeout", 10000);
        setRemoteFileTimeouts(dafilesrvLookupTimeout, 0);
        trapTooManyActiveQueries = topology->getPropBool("@trapTooManyActiveQueries", true);
        maxEmptyLoopIterations = topology->getPropInt("@maxEmptyLoopIterations", 1000);
        maxGraphLoopIterations = topology->getPropInt("@maxGraphLoopIterations", 1000);
        mergeAgentStatistics = topology->getPropBool("@mergeAgentStatistics", topology->getPropBool("@mergeSlaveStatistics", true));  // legacy name
        defaultCollectFactoryStatistics = topology->getPropBool("@collectFactoryStatistics", true);
        defaultExecuteDependenciesSequentially = topology->getPropBool("@executeDependenciesSequentially", defaultExecuteDependenciesSequentially);
        defaultStartInputsSequentially = topology->getPropBool("@startInputsSequentially", defaultStartInputsSequentially);
        defaultNoSeekBuildIndex = topology->getPropBool("@noSeekBuildIndex", isContainerized());
        parallelQueryLoadThreads = topology->getPropInt("@parallelQueryLoadThreads", parallelQueryLoadThreads);
        if (!parallelQueryLoadThreads)
            parallelQueryLoadThreads = 1;
        alwaysFailOnLeaks = topology->getPropBool("@alwaysFailOnLeaks", false);
        ignoreFileDateMismatches = topology->getPropBool("@ignoreFileDateMismatches", false);
        ignoreFileSizeMismatches = topology->getPropBool("@ignoreFileSizeMismatches", false);
        fileTimeFuzzySeconds = topology->getPropInt("@fileTimeFuzzySeconds", 0);
        const char *sinkModeText = topology->queryProp("@sinkMode");
        if (sinkModeText)
            defaultSinkMode = getSinkMode(sinkModeText);
        limitWaitingWorkers = topology->getPropBool("@limitWaitingWorkers", limitWaitingWorkers);

        cacheReportPeriodSeconds = topology->getPropInt("@cacheReportPeriodSeconds", 5*60);
        setLegacyAES(topology->getPropBool("expert/@useLegacyAES", false));

        // NB: these directories will have been setup by topology earlier
        const char *primaryDirectory = queryBaseDirectory(grp_unknown, 0);
        const char *secondaryDirectory = queryBaseDirectory(grp_unknown, 1);

        // MORE: Get parms from topology after it is populated from Hardware/computer types section in configenv
        //       Then if does not match and based on desired action in topolgy, either warn, or fatal exit or .... etc
        getHardwareInfo(hdwInfo, primaryDirectory, secondaryDirectory);

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

        setKeyIndexCacheSize((unsigned)-1); // unbound
        nodeCacheMB = topology->getPropInt("@nodeCacheMem", 100); 
        setNodeCacheMem(nodeCacheMB * 0x100000ULL);
        leafCacheMB = topology->getPropInt("@leafCacheMem", 50);
        setLeafCacheMem(leafCacheMB * 0x100000ULL);
        blobCacheMB = topology->getPropInt("@blobCacheMem", 0);
        setBlobCacheMem(blobCacheMB * 0x100000ULL);
        if (topology->hasProp("@nodeFetchThresholdNs"))
            setNodeFetchThresholdNs(topology->getPropInt64("@nodeFetchThresholdNs"));
        setIndexWarningThresholds(topology);

        unsigned __int64 affinity = topology->getPropInt64("@affinity", 0);
        updateAffinity(affinity);

        unsigned __int64 minimumInterestingActivityMs = topology->getPropInt64("@minimumInterestingActivityMs", 10);
        minimumInterestingActivityCycles = nanosec_to_cycle(minimumInterestingActivityMs * 1'000'000);

        minFreeDiskSpace = topology->getPropInt64("@minFreeDiskSpace", (1024 * 0x100000)); // default to 1 GB
        mtu_size = topology->getPropInt("@mtuPayload", 0);
        if (mtu_size)
        {
            if (mtu_size < 1400 || mtu_size > 9000)
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid settings - mtuPayload should be between 1400 and 9000");
            unsigned alignment = 0x400;
            while (alignment*2 < mtu_size)
                alignment *= 2;
            roxiemem::setDataAlignmentSize(alignment);  // smallest power of two under mtuSize

        }
        else if (topology->getPropBool("@jumboFrames", false))
        {
            mtu_size = 9000;    // upper limit on outbound buffer size - allow some header room too
            roxiemem::setDataAlignmentSize(0x2000);
        }
        else
        {
            mtu_size = 1400;    // upper limit on outbound buffer size - allow some header room too
            roxiemem::setDataAlignmentSize(0x400);
        }
#ifndef _CONTAINERIZED
        perfMonHook.setown(roxiemem::createRoxieMemStatsPerfMonHook());  // Note - we create even if pinterval is 0, as can be enabled via control message
        unsigned pinterval = topology->getPropInt("@systemMonitorInterval",1000*60);
        if (pinterval)
            startPerformanceMonitor(pinterval, PerfMonStandard, perfMonHook);
#endif

        topology->getProp("@pluginDirectory", pluginDirectory);
        StringBuffer packageDirectory;
        getPackageFolder(packageDirectory);
        if (pluginDirectory.length() == 0 && packageDirectory.length() != 0)
        {
            pluginDirectory.append(packageDirectory).append("plugins");
        }
        getAdditionalPluginsPath(pluginDirectory, packageDirectory);
        if (queryDirectory.length() == 0)
        {
            topology->getProp("@queryDir", queryDirectory);
            if (queryDirectory.length() == 0)
                queryDirectory.append(codeDirectory).append("queries");
        }
        addNonEmptyPathSepChar(queryDirectory);
        getSpillFilePath(spillDirectory, "roxie", topology);
        getTempFilePath(tempDirectory, "roxie", topology);

#ifdef _WIN32
        topology->addPropBool("@linuxOS", false);
#else
        topology->addPropBool("@linuxOS", true);
#endif
#ifdef _CONTAINERIZED
        allQuerySetNames.append(roxieName);
#else
        allQuerySetNames.appendListUniq(topology->queryProp("@querySets"), ",");
#endif

#ifdef _CONTAINERIZED
        IpAddress myIP(".");
        myNode.setIp(myIP);
        if (traceLevel)
        {
            StringBuffer s;
            DBGLOG("My node ip=%s", myIP.getHostText(s).str());
        }
        if (topology->getPropBool("@server", true))
        {
            Owned<IPropertyTreeIterator> roxieFarms = topology->getElements("./services");
            ForEach(*roxieFarms)
            {
                IPropertyTree &roxieFarm = roxieFarms->query();
                unsigned port = roxieFarm.getPropInt("@port", roxieFarm.getPropInt("@servicePort", ROXIE_SERVER_PORT));
                RoxieEndpointInfo me = {RoxieEndpointInfo::RoxieServer, 0, { (unsigned short) port, myIP }, 0};
                myRoles.push_back(me);
            }
        }
        for (std::pair<unsigned, unsigned> channel: agentChannels)
        {
            myAgentEP.set(ccdMulticastPort, myIP);
            RoxieEndpointInfo me = { RoxieEndpointInfo::RoxieAgent, channel.first, myAgentEP, channel.second };
            myRoles.push_back(me);
        }
#else
        // Set multicast base addresses - must be done before generating agent channels
        if (roxieMulticastEnabled && !localAgent)
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
        readStaticTopology();
#endif
        // Now we know all the channels, we can open and subscribe the multicast channels
        if (!localAgent)
        {
            openMulticastSocket();
            if (roxieMulticastEnabled)
                setMulticastEndpoints(numChannels);
        }
        setDaliServixSocketCaching(true);  // enable daliservix caching
        enableForceRemoteReads(); // forces file reads to be remote reads if they match environment setting 'forceRemotePattern' pattern.

        if (!oneShotRoxie)
        {
            queryFileCache().start();
            loadPlugins();
        }
#ifdef _CONTAINERIZED
        initializeTopology(topoValues, myRoles);
        writeSentinelFile(sentinelFile);
#endif
        configurePreferredPlanes();
        createDelayedReleaser();
        CCycleTimer loadPackageTimer;
        globalPackageSetManager = createRoxiePackageSetManager(standAloneDll.getClear());
        globalPackageSetManager->load();
        if (traceLevel)
            DBGLOG("Loading all packages took %ums", loadPackageTimer.elapsedMs());

        ROQ = createOutputQueueManager(numAgentThreads, encryptInTransit);
        ROQ->start();
        Owned<IPacketDiscarder> packetDiscarder = createPacketDiscarder();
#if defined(WIN32) && defined(_DEBUG) && defined(_DEBUG_HEAP_FULL)
        int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
        tmpFlag |= _CRTDBG_CHECK_ALWAYS_DF;
        _CrtSetDbgFlag( tmpFlag );
#endif

        //MORE: I'm not sure where this should go, or how it fits in.  Possibly the function needs to be split in two.
        initializeStoragePlanes(false, true);

        EnableSEHtoExceptionMapping();
        setSEHtoExceptionHandler(&abortHandler);
        Owned<IHpccProtocolPluginContext> protocolCtx = new CHpccProtocolPluginCtx();
        if (runOnce)
        {
            // Avoid delaying the release of packages or queries - otherwise stand-alone queries can take a while to terminate
            PerfTracer oneShotTracer;
            bool traceOneShot = topology->getPropBool("expert/@perftrace", false); // MORE - check wu options too?
            if (traceOneShot)
                oneShotTracer.start();
            agentQueryReleaseDelaySeconds = 0;
            if (wuid)
            {
                Owned<IHpccProtocolListener> roxieServer = createRoxieWorkUnitListener(1, false);
                try
                {
                    roxieServer->runOnce(wuid);
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
                Owned<IHpccProtocolListener> roxieServer = protocolPlugin->createListener("runOnce", createRoxieProtocolMsgSink(myNode.getIpAddress(), 0, 1, false), 0, 0, nullptr, nullptr);
                try
                {
                    const char *format = topology->queryProp("@format");
                    if (!format)
                    {
                        if (topology->getPropBool("@xml", false))
                            format = "xml";
                        else if (topology->getPropBool("@csv", false))
                            format = "csv";
                        else if (topology->getPropBool("@raw", false))
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
            if (traceOneShot)
            {
                oneShotTracer.stop();
                const char *fname = topology->queryProp("expert/@perftraceFileName");
                if (!fname)
                    fname = "perftrace.svg";
                Owned<IFile> iFile = createIFile(fname);
                try
                {
                    Owned<IFileIO> iFileIO = iFile->open(IFOcreate);
                    if (iFileIO)
                    {
                        StringBuffer &svg = oneShotTracer.queryResult();
                        iFileIO->write(0, svg.length(), svg.str());
                        DBGLOG("Flame graph for query written to %s", fname);
                    }
                }
                catch (IException *E)
                {
                    VStringBuffer msg("Failed to write flame graph to %s", fname);
                    EXCLOG(E, msg);
                    ::Release(E);
                }
            }
        }
        else
        {
            try
            {
#ifdef _CONTAINERIZED
                Owned<IPropertyTreeIterator> roxieFarms = topology->getElements("./services");
#else
                Owned<IPropertyTreeIterator> roxieFarms = topology->getElements("./RoxieFarmProcess");
#endif
                ForEach(*roxieFarms)
                {
                    IPropertyTree &roxieFarm = roxieFarms->query();
                    unsigned listenQueue = roxieFarm.getPropInt("@listenQueue", DEFAULT_LISTEN_QUEUE_SIZE);
                    unsigned numThreads = roxieFarm.getPropInt("@numThreads", 0);
                    if (!numThreads)
                        numThreads = numServerThreads;
                    unsigned port = roxieFarm.getPropInt("@port", roxieFarm.getPropInt("@servicePort", ROXIE_SERVER_PORT));
                    const char *protocol = roxieFarm.queryProp("@protocol");
                    bool serviceTLS = roxieFarm.getPropBool("@tls") || (protocol && streq(protocol, "ssl"));
                    //unsigned requestArrayThreads = roxieFarm.getPropInt("@requestArrayThreads", 5);
                    // NOTE: farmer name [@name=] is not copied into topology
                    const IpAddress ip = myNode.getIpAddress();
                    if (!roxiePort)
                    {
                        roxiePort = port;
                        if (serviceTLS)
                        {
#ifdef _USE_OPENSSL
                            const char *certIssuer = roxieFarm.queryProp("@issuer");
                            if (isEmptyString(certIssuer))
                                certIssuer = roxieFarm.getPropBool("@public", true) ? "public" : "local";
                            roxiePortTlsClientConfig = createIssuerTlsConfig(certIssuer, nullptr, true, roxieFarm.getPropBool("@selfSigned"), true, false);
#endif
                        }
                        debugEndpoint.set(roxiePort, ip);
                    }
                    bool suspended = roxieFarm.getPropBool("@suspended", false);
                    Owned <IHpccProtocolListener> roxieServer;
                    if (port)
                    {
                        StringBuffer certFileName;
                        StringBuffer keyFileName;
                        StringBuffer passPhraseStr;
                        Owned<const ISyncedPropertyTree> tlsConfig;
                        if (serviceTLS)
                        {
#ifdef _USE_OPENSSL
                            protocol = "ssl";
                            const char *certIssuer = roxieFarm.queryProp("@issuer");
                            if (isEmptyString(certIssuer))
                                certIssuer = roxieFarm.getPropBool("@public", true) ? "public" : "local";
                            bool disableMtls = roxieFarm.getPropBool("@disableMtls", false);
                            tlsConfig.setown(getIssuerTlsSyncedConfig(certIssuer, roxieFarm.queryProp("trusted_peers"), disableMtls));
                            if (!tlsConfig || !tlsConfig->isValid())
                            {
                                if (isContainerized())
                                    throw MakeStringException(ROXIE_FILE_ERROR, "TLS secret for issuer %s not found", certIssuer);

                                const char *passPhrase = roxieFarm.queryProp("@passphrase");
                                if (!isEmptyString(passPhrase))
                                    passPhraseStr.append(passPhrase); // NB passphrase is decrypted in CSecureSocketContext::createNewContext()

                                const char *certFile = roxieFarm.queryProp("@certificateFileName");
                                if (!certFile)
                                    throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing certificateFileName tag", port);
                                if (isAbsolutePath(certFile))
                                    certFileName.append(certFile);
                                else
                                    certFileName.append(codeDirectory.str()).append(certFile);

                                const char *keyFile = roxieFarm.queryProp("@privateKeyFileName");
                                if (!keyFile)
                                    throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing privateKeyFileName tag", port);
                                if (isAbsolutePath(keyFile))
                                    keyFileName.append(keyFile);
                                else
                                    keyFileName.append(codeDirectory.str()).append(keyFile);

                                if (!checkFileExists(certFileName.str()))
                                    throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing certificateFile (%s)", port, certFileName.str());

                                if (!checkFileExists(keyFileName.str()))
                                    throw MakeStringException(ROXIE_FILE_ERROR, "Roxie SSL Farm Listener on port %d missing privateKeyFile (%s)", port, keyFileName.str());

                                Owned<IPropertyTree> staticConfig = createSecureSocketConfig(certFileName, keyFileName, passPhraseStr, false);
                                tlsConfig.setown(createSyncedPropertyTree(staticConfig));
                            }
                            else
                                DBGLOG("Roxie service, port(%d) TLS issuer (%s)", port, certIssuer);

#else
                            OWARNLOG("Skipping Roxie SSL Farm Listener on port %d : OpenSSL disabled in build", port);
                            continue;
#endif
                        }
                        const char *soname =  roxieFarm.queryProp("@so");
                        const char *config  = roxieFarm.queryProp("@config");
                        // NB: leaks - until we fix bug in ensureProtocolPlugin() whereby some paths return a linked object and others do not
                        IHpccProtocolPlugin *protocolPlugin = ensureProtocolPlugin(*protocolCtx, soname);
                        roxieServer.setown(protocolPlugin->createListener(protocol ? protocol : "native", createRoxieProtocolMsgSink(ip, port, numThreads, suspended), port, listenQueue, config, tlsConfig));
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

                if (!topology->getPropBool("@disableCachePrewarming", false))
                    queryFileCache().loadSavedOsCacheInfo();
                queryFileCache().startCacheReporter();
#ifdef _CONTAINERIZED
                // Roxie indicates readiness when all channels report ready
                publishTopology(traceLevel, myRoles);
#else
                writeSentinelFile(sentinelFile);
#endif
                DBGLOG("Startup completed - LPT=%u APT=%u", queryNumLocalTrees(), queryNumAtomTrees());
                if (recordStartupEvents)
                    stopRoxieEventRecording(nullptr);

                if (topology->getPropBool("expert/@profileStartup", false))
                {
                    const char *fname = topology->queryProp("expert/@profileStartupFileName");
                    if (!fname)
                        fname = "startuptrace.svg";
                    startupTracer.stop();
                    Owned<IFile> iFile = createIFile(fname);
                    try
                    {
                        Owned<IFileIO> iFileIO = iFile->open(IFOcreate);
                        if (iFileIO)
                        {
                            StringBuffer &svg = startupTracer.queryResult();
                            iFileIO->write(0, svg.length(), svg.str());
                            DBGLOG("Flame graph for startup written to %s", fname);
                        }
                    }
                    catch (IException *E)
                    {
                        VStringBuffer msg("Failed to write flame graph for startup to %s", fname);
                        EXCLOG(E, msg);
                        ::Release(E);
                    }
                }
                DBGLOG("Waiting for queries");
                LocalIAbortHandler abortHandler(waiter);
                waiter.wait();
            }
            catch (IException *E)
            {
                StringBuffer x;
                IERRLOG("EXCEPTION: (%d): %s", E->errorCode(), E->errorMessage(x).str());
                E->Release();
            }
        }
        DBGLOG("Roxie closing down");
        logCacheState();
        shuttingDown = true;
        stopTopoThread();
        ::Release(globalPackageSetManager);
        globalPackageSetManager = NULL;
        setSEHtoExceptionHandler(NULL);
        while (socketListeners.isItem(0))
        {
            socketListeners.item(0).stop();
            socketListeners.remove(0);
        }
        packetDiscarder->stop();
        packetDiscarder.clear();
        ROQ->stop();
        ROQ->join();
        ROQ->Release();
        ROQ = NULL;
        stopDelayedReleaser();
        closedDown.signal();
    }
    catch (IException *E)
    {
        StringBuffer x;
        IERRLOG("EXCEPTION: (%d): %s", E->errorCode(), E->errorMessage(x).str());
        if (!queryLogMsgManager()->isActiveMonitor(queryStderrLogMsgHandler()))
            fprintf(stderr, "EXCEPTION: (%d): %s\n", E->errorCode(), x.str());
        E->Release();
    }

    stopSecretUpdateThread();
    roxieMetrics.clear();
#ifndef _CONTAINERIZED
    stopPerformanceMonitor();
#endif
    stopRoxieEventRecording(nullptr);
    cleanupPlugins();
    unloadHpccProtocolPlugin();
    closeMulticastSockets();
    releaseAgentDynamicFileCache();
    releaseRoxieStateCache();
    setDaliServixSocketCaching(false);  // make sure it cleans up or you get bogus memleak reports
    setNodeCaching(false); // ditto
#ifndef _CONTAINERIZED
    perfMonHook.clear();
#endif
    leakChecker = strdup("Make sure leak checking is working");
    roxiemem::releaseRoxieHeap();
    UseSysLogForOperatorMessages(false);
    ExitModuleObjects();
    releaseAtoms();
    leakChecker = strdup("Make sure leak checking is working");
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
    leakChecker = nullptr;
    return 0;
}

// These defaults only apply when roxie is linked into a standalone executable
// Note that the defaults for roxie executable (in whatever mode) are set in roxie.cpp

static constexpr const char * standaloneDefaultYaml = R"!!(
version: "1.0"
roxie:
  allFilesDynamic: true
  localAgent: true
  numChannels: 1
  queueNames: roxie.roxie
  traceLevel: 0
  server: false
  logging:
    disabled: true
)!!";

int STARTQUERY_API start_query(int argc, const char *argv[])
{
    return roxie_main(argc, argv, standaloneDefaultYaml);
}
