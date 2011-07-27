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
#include <build-config.h>

#include "rmtfile.hpp"
#include "ccd.hpp"
#include "ccdquery.hpp"
#include "ccdstate.hpp"
#include "ccdqueue.ipp"
#include "ccdserver.hpp"
#include "ccdsnmp.hpp"
#include "thorplugin.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#endif

//=================================================================================

bool shuttingDown = false;
unsigned numChannels;
unsigned numActiveChannels;
unsigned callbackRetries = 3;
unsigned callbackTimeout = 500;
unsigned lowTimeout = 10000;
unsigned highTimeout = 2000;
unsigned slaTimeout = 2000;
unsigned numServerThreads = 30;
unsigned numSlaveThreads = 30;
unsigned numRequestArrayThreads = 5;
unsigned headRegionSize;
bool enableHeartBeat = true;
unsigned keyedJoinFlowLimit = 1000;
unsigned parallelLoopFlowLimit = 100;
unsigned perChannelFlowLimit = 10;
time_t startupTime;
unsigned statsExpiryTime = 3600;
unsigned miscDebugTraceLevel = 0;  // separate trace settings purely for debugging specific items (i.e. all possible locations to look for files at startup)
unsigned readTimeout = 300;
unsigned indexReadChunkSize = 60000;
unsigned smartSteppingChunkRows = 100;
unsigned maxBlockSize = 10000000;
unsigned maxLockAttempts = 5;
bool checkVersion = true;
bool deleteUnneededFiles = true;
bool checkPrimaries = true;
bool pretendAllOpt = false;
bool traceStartStop = false;
bool traceServerSideCache = false;
bool timeActivities = true;
unsigned watchActivityId = 0;
unsigned testSlaveFailure = 0;
unsigned restarts = 0;
bool heapSort = false;
bool insertionSort = false;
bool fieldTranslationEnabled = false;
bool syncCluster = false;  // should we sync an out of sync cluster (always send a trap)
bool useTreeCopy = true;
bool mergeSlaveStatistics = true;
bool defaultStripLeadingWhitespace = true;
bool runOnce = false;

unsigned udpMulticastBufferSize = 262142;
bool roxieMulticastEnabled = true;

IPropertyTree* topology;
CriticalSection ccdChannelsCrit;
IPropertyTree* ccdChannels;

bool crcResources;
bool useRemoteResources;
bool checkFileDate;
bool lazyOpen;
bool localSlave;
bool doIbytiDelay = true; 
unsigned initIbytiDelay; // In MillSec
unsigned minIbytiDelay;  // In MillSec
bool copyResources;
bool enableKeyDiff = true;
bool enableForceKeyDiffCopy = true;
bool chunkingHeap = true;
bool logFullQueries;
bool blindLogging = false;
bool debugPermitted = true;
bool checkCompleted = true;
unsigned preabortKeyedJoinsThreshold = 100;
unsigned preabortIndexReadsThreshold = 100;

unsigned memoryStatsInterval = 0;
memsize_t defaultMemoryLimit;
unsigned defaultTimeLimit[3] = {0, 0, 0};
unsigned defaultWarnTimeLimit[3] = {0, 5000, 5000};

int defaultCheckingHeap = 0;
unsigned defaultParallelJoinPreload = 0;
unsigned defaultPrefetchProjectPreload = 10;
unsigned defaultConcatPreload = 0;
unsigned defaultFetchPreload = 0;
unsigned defaultFullKeyedJoinPreload = 0;
unsigned defaultKeyedJoinPreload = 0;
unsigned dafilesrvLookupTimeout = 10000;

ILogMsgHandler * logFileHandler;
unsigned logQueueLen;
unsigned logQueueDrop;
bool useLogQueue;
bool fastLaneQueue;
unsigned mtu_size = 1400; // upper limit on outbound buffer size - allow some header room too
StringBuffer fileNameServiceDali;
StringBuffer roxieName;
bool trapTooManyActiveQueries;
bool allowRoxieOnDemand;
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
StringBuffer baseDataDirectory;

ClientCertificate clientCert;
bool useHardLink;

unsigned maxFileAge[2] = {0xffffffff, 60*60*1000}; // local files don't expire, remote expire in 1 hour, by default
unsigned minFilesOpen[2] = {2000, 500};
unsigned maxFilesOpen[2] = {4000, 1000};

SocketEndpoint ownEP;
SocketEndpointArray allRoxieServers;
HardwareInfo hdwInfo;
unsigned parallelAggregate;
bool inMemoryKeysEnabled = true;
unsigned serverSideCacheSize = 0;

bool nodeCachePreload = false;
unsigned nodeCacheMB = 100;
unsigned leafCacheMB = 50;
unsigned blobCacheMB = 0;

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

void getAccessList(const char *aclName, IPropertyTree *topology, IPropertyTree *serverInfo)
{
    StringBuffer xpath;
    xpath.append("ACL[@name='").append(aclName).append("']");
    if (serverInfo->queryPropTree(xpath))
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - recursive ACL definition of %s", aclName);
    Owned<IPropertyTree> X = createPTree("ACL");
    X->setProp("@name", aclName);
    serverInfo->addPropTree("ACL", X.getClear());

    Owned<IPropertyTree> acl = topology->getPropTree(xpath.str());
    if (!acl)
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - acl %s not found", aclName);

    Owned<IPropertyTreeIterator> access = acl->getElements("Access");
    ForEach(*access)
    {
        IPropertyTree &child = access->query();
        const char *base = child.queryProp("@base");
        if (base)
            getAccessList(base, topology, serverInfo);
        else
            serverInfo->addPropTree(child.queryName(), LINK(&child));
    }
    serverInfo->removeProp(xpath);
}

void addServerChannel(const char *dataDirectory, unsigned port, unsigned threads, const char *access, IPropertyTree *topology)
{
    if (!ownEP.port)
        ownEP.set(port, queryHostIP());
    Owned<IPropertyTreeIterator> servers = ccdChannels->getElements("RoxieServerProcess");
    ForEach(*servers)
    {
        IPropertyTree &f = servers->query();
        if (strcmp(f.queryProp("@dataDirectory"), dataDirectory) != 0)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - Roxie server dataDirectory respecified");
        if (f.getPropInt("@port", 0) == port)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - Roxie server port repeated");
    }
    IPropertyTree *ci = createPTree("RoxieServerProcess");
    ci->setProp("@dataDirectory", dataDirectory);
    ci->setPropInt("@port", port);
    ci->setPropInt("@numThreads", threads);
    if (access && *access)
    {
        getAccessList(access, topology,  ci);
    }
    ccdChannels->addPropTree("RoxieServerProcess", ci);
}

bool ipMatch(IpAddress &ip)
{
    return ip.isLocal();
}

void addSlaveChannel(unsigned channel, const char *dataDirectory, bool suspended)
{
    StringBuffer xpath;
    xpath.appendf("RoxieSlaveProcess[@channel=\"%d\"]", channel);
    if (ccdChannels->hasProp(xpath.str()))
        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - channel %d repeated", channel);
    IPropertyTree *ci = createPTree("RoxieSlaveProcess");
    ci->setPropInt("@channel", channel);
    ci->setPropBool("@suspended", suspended);
    ci->setPropInt("@subChannel", numSlaves[channel]);
    suspendedChannels[channel] = suspended;
    ci->setProp("@dataDirectory", dataDirectory);
    ccdChannels->addPropTree("RoxieSlaveProcess", ci);
}

void addChannel(unsigned channel, const char *dataDirectory, bool isMe, bool suspended, IpAddress& slaveIp)
{
    numSlaves[channel]++;
    if (isMe && channel > 0 && channel <= numChannels)
    {
        addSlaveChannel(channel, dataDirectory, suspended);
    }
    if (!localSlave)
    {
        addEndpoint(channel, slaveIp, CCD_MULTICAST_PORT);
    }
}

void ensureDirectory(StringBuffer &dir)
{
    StringBuffer absolute;
    recursiveCreateDirectory(dir.str());
    makeAbsolutePath(dir.str(), absolute);
    dir.clear().append(absolute);
    addPathSepChar(dir);
}

extern void doUNIMPLEMENTED(unsigned line, const char *file)
{
    throw MakeStringException(ROXIE_UNIMPLEMENTED_ERROR, "UNIMPLEMENTED at %s:%d", file, line);
}

void FatalError(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    StringBuffer errMsg;
    errMsg.valist_appendf(format, args);
    va_end(args);
    Owned<IException> E = MakeStringException(MSGAUD_operator, ROXIE_INTERNAL_ERROR, errMsg.str());
    EXCLOG(E, "Fatal error");
    Sleep(5000);
    _exit(1);
}

class MAbortHandler : implements IExceptionHandler
{
    unsigned x;
public:
    virtual bool fireException(IException *e)
    {
        ForEachItemIn(idx, socketListeners)
        {
            socketListeners.item(idx).stopListening();
        }
        return false;
    }
} abortHandler;

#ifdef _WIN32
int myhook(int alloctype, void *, size_t nSize, int p1, long allocSeq, const unsigned char *file, int line)
{
    // Handy place to put breakpoints when tracking down obscure memory leaks...
    if (nSize==68 && !file)
    {
        int a = 1;
    }
    return true;
}
#endif

static class RoxieRowCallbackHook : implements IRtlRowCallback
{
public:
    virtual void releaseRow(const void * row) const
    {
        ReleaseRoxieRow(row);
    }
    virtual void releaseRowset(unsigned count, byte * * rowset) const
    {
        if (rowset)
        {
            if (!roxiemem::HeapletBase::isShared(rowset))
            {
                byte * * finger = rowset;
                while (count--)
                    ReleaseRoxieRow(*finger++);
            }
            ReleaseRoxieRow(rowset);
        }
    }
    virtual void * linkRow(const void * row) const
    {
        if (row) 
            LinkRoxieRow(row);
        return const_cast<void *>(row);
    }
    virtual byte * * linkRowset(byte * * rowset) const
    {
        if (rowset)
            LinkRoxieRow(rowset);
        return const_cast<byte * *>(rowset);
    }
} callbackHook;

int STARTQUERY_API start_query(int argc, const char *argv[])
{
    InitModuleObjects();
    getDaliServixPort();
    init_signals();

    #ifdef _USE_CPPUNIT
    if (argc>=2 && stricmp(argv[1], "-selftest")==0)
    {
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_prefix);
        CppUnit::TextUi::TestRunner runner;
        if (argc==2)
        {
            CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
            runner.addTest( registry.makeTest() );
        }
        else 
        {
            for (int name = 2; name < argc; name++)
            {
                CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry(argv[name]);
                runner.addTest( registry.makeTest() );
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
    Thread::setDefaultStackSize(0x10000);   // NB under windows requires linker setting (/stack:)
#endif
    srand( (unsigned)time( NULL ) );
    ccdChannels = createPTree("Channels");
    EnableSEHtoExceptionMapping();
    setTerminateOnSEH();

    char currentDirectory[_MAX_DIR];
    if (!getcwd(currentDirectory, sizeof(currentDirectory)))
        throw MakeStringException(ROXIE_INTERNAL_ERROR, "getcwd failed (%d)", errno);

    codeDirectory.set(currentDirectory);
    ensureDirectory(codeDirectory);
    try
    {
        Owned<IProperties> globals = createProperties(true);
        for (int i = 1; i < argc; i++)
            globals->loadProp(argv[i], true);

        // sentinelFile should be removed as early as possible to avoid infinite restarts, if there is a problem restarting
        // sentinelFilename will exist in directory where executable exists
        Owned<IFile> sentinelFile;
        if (globals->hasProp("sentinel"))
        {
            StringBuffer sentinelFilename;
            sentinelFilename.append(codeDirectory).append(globals->queryProp("sentinel"));
            sentinelFile.setown(createIFile(sentinelFilename.str()));
            sentinelFile->remove();
        }

        StringBuffer topologyFile;
        if (globals->hasProp("topology"))
            globals->getProp("topology", topologyFile);
        else
            topologyFile.append(codeDirectory).append(PATHSEPCHAR).append("RoxieTopology.xml");

        if (checkFileExists(topologyFile.str()))
        {
            DBGLOG("Loading topology file %s", topologyFile.str());
            topology = createPTreeFromXMLFile(topologyFile.str());
        }
        else
        {
            if (globals->hasProp("topology"))
            {
                // Explicitly-named topology file SHOULD exist...
                throw MakeStringException(ROXIE_INVALID_TOPOLOGY, "topology file %s not found", topologyFile.str());
            }
            topology=createPTreeFromXMLString(
                "<RoxieTopology numChannels='1' localSlave='1'>"
                 "<RoxieServerProcess dataDirectory='.' netAddress='.'/>"
                 "<RoxieSlaveProcess dataDirectory='.' netAddress='.' channel='1'/>"
                "</RoxieTopology>"
                );
            int port = globals->getPropInt("port", 9876);
            topology->setPropInt("RoxieServerProcess/@port", port);
            topology->setProp("@daliServers", globals->queryProp("daliServers"));
            topology->setProp("@traceLevel", globals->queryProp("traceLevel"));
        }

        topology->getProp("@name", roxieName);
        Owned<const IQueryDll> standAloneDll;
        if (globals->hasProp("loadWorkunit"))
        {
            StringBuffer workunitName;
            globals->getProp("loadWorkunit", workunitName);
            standAloneDll.setown(createQueryDll(workunitName));
        }
        else
        {
            Owned<ILoadedDllEntry> dll = createExeDllEntry(argv[0]);
            if (checkEmbeddedWorkUnitXML(dll))
            {
                standAloneDll.setown(createExeQueryDll(argv[0]));
                runOnce = globals->getPropInt("port", 0) == 0;
            }
        }

        traceLevel = topology->getPropInt("@traceLevel", runOnce ? 0 : 1);
        if (traceLevel > MAXTRACELEVEL)
            traceLevel = MAXTRACELEVEL;
        udpTraceLevel = topology->getPropInt("@udpTraceLevel", runOnce ? 0 : 1);
        roxiemem::memTraceLevel = topology->getPropInt("@memTraceLevel", runOnce ? 0 : 1);
        soapTraceLevel = topology->getPropInt("@soapTraceLevel", runOnce ? 0 : 1);
        miscDebugTraceLevel = topology->getPropInt("@miscDebugTraceLevel", 0);

        IPropertyTree *directoryTree = topology->queryPropTree("Directories");
        if (directoryTree)
        {
            getConfigurationDirectory(directoryTree,"log","roxie", roxieName, logDirectory);
            getConfigurationDirectory(directoryTree,"query","roxie", roxieName, queryDirectory);
        }
        // get the log directory
        if (!logDirectory.length())
        {
            topology->getProp("@logDir", logDirectory);
            if (!logDirectory.length())
                logDirectory.append(codeDirectory).append("logs");
        }
        ensureDirectory(logDirectory);

        StringBuffer initialFileName;
        StringBuffer timeDiffInfo;
        StringBuffer logFileParam;
        bool timescan = false;
        bool timediff = false;
        bool haslogfile = false;
        if (globals->getPropBool("stdlog", traceLevel != 0) || topology->getPropBool("@forceStdLog", false))
            queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time | MSGFIELD_thread | MSGFIELD_prefix);
        else
            removeLog();
        if (globals->hasProp("logfile"))
        {
            initialFileName.appendf("%sroxie.",logDirectory.str());

            haslogfile = true;
            StringBuffer log;
            globals->getProp("logfile", log);
            logFileParam.append(log);
            
            // If it's in the form mm_dd_yyyy_hh_mm_ss, and we have rolled over, then use rolled-over form
            unsigned year,month,day,hour,min,sec;
            
            StringBuffer log_format;
            log_format.append("%2u_%2u_%4u_%2u_%2u_%2u");
                
            if (sscanf(log.str(), log_format.str(), &month, &day, &year, &hour, &min, &sec)==6)
            {
                timescan = true;
                time_t tNow;
                time(&tNow);
                struct tm ltNow;
                localtime_r(&tNow, &ltNow);
                ltNow.tm_mon = ltNow.tm_mon +1;  // localtime returns month (0 - 11 : 0 = January) */
                ltNow.tm_year += 1900; // localtime returns  Year less 1900 */
                if(ltNow.tm_mday != day || ltNow.tm_mon != month || ltNow.tm_year != year)
                {
                    timediff = true;
                    timeDiffInfo.appendf("current_day = %d  day = %d  current_month = %d  month = %d   current_year = %d  year = %d", ltNow.tm_mday, day, ltNow.tm_mon, month, ltNow.tm_year, year);
                    addFileTimestamp(log.clear(), true); // this call adds a '.' at the beginning
                }
            }
            else
                DBGLOG("no date match");

            initialFileName.appendf("%s.log", (log[0] !='.') ? log.str() : (log.str()+1)); // make sure 'log' does not start with a '.'
        }
        else  // if not started via start scripts (usually by developer)
        {
            initialFileName.appendf("%sroxie.",logDirectory.str());
            StringBuffer log;
            addFileTimestamp(log.clear(), false); // this call adds a '.' at the beginning
            initialFileName.appendf("%s.log", (log[0] !='.') ? log.str() : (log.str()+1)); // make sure 'log' does not start with a '.'
        }

        // build the base log file name using the logDir setting in the topology file
        StringBuffer logBaseName(logDirectory.str());
        logBaseName.append("roxie");

        // set the alias name 
        StringBuffer logAliasName(logBaseName.str());
        logAliasName.append(".log");

#ifdef _DEBUG
        useLogQueue = topology->getPropBool("@useLogQueue", false);
#else
        useLogQueue = topology->getPropBool("@useLogQueue", true);
#endif
        logQueueLen = topology->getPropInt("@logQueueLen", 512);
        logQueueDrop = topology->getPropInt("@logQueueDrop", 32);

        ILogMsgFilter * filter = getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail, false);
        logFileHandler = getRollingFileLogMsgHandler(logBaseName.str(), ".log", MSGFIELD_STANDARD, true, true, initialFileName.length() ? initialFileName.str() : NULL, logAliasName.str());
        queryLogMsgManager()->addMonitorOwn(logFileHandler, filter);
        if (useLogQueue)
        {
            queryLogMsgManager()->enterQueueingMode();
            queryLogMsgManager()->setQueueDroppingLimit(logQueueLen, logQueueDrop);
        }
        if (globals->getPropBool("enableSysLog",true))
            UseSysLogForOperatorMessages();
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

        restarts = globals->getPropInt("restarts", 0);
        const char *preferredSubnet = topology->queryProp("@preferredSubnet");
        if (preferredSubnet)
        {
            const char *preferredSubnetMask = topology->queryProp("@preferredSubnetMask");
            if (!preferredSubnetMask) preferredSubnetMask = "255.255.255.0";
            if (!setPreferredSubnet(preferredSubnet, preferredSubnetMask))
                throw MakeStringException(ROXIE_INTERNAL_ERROR, "Error setting preferred subnet %s mask %s", preferredSubnet, preferredSubnetMask);
        }
        bool multiHostMode = globals->hasProp("host");
        unsigned myHostNumber = globals->getPropInt("host", 0);
        if (restarts)
        {
            if (traceLevel)
                DBGLOG("Roxie restarting: restarts = %d", restarts);
            setStartRuid(restarts);
        }
        else
        {
            if (traceLevel)
            {
                DBGLOG("Roxie starting");
                CBuildVersion::log();
            }
        }

        if (traceLevel)
            DBGLOG("%s log = %s originalLogName = %s logBaseName = %s  alias = %s  haslogfile = %d  timescan = %d   timediff = %d %s", BUILD_TAG, initialFileName.str(), logFileParam.str(), logBaseName.str(), logAliasName.str(), haslogfile, timescan, timediff, (timeDiffInfo.length()) ? timeDiffInfo.str() : " ");

        bool isCCD = false;

        headRegionSize = topology->getPropInt("@headRegionSize", 50);
        numChannels = topology->getPropInt("@numChannels", 0);
        numActiveChannels = topology->getPropInt("@numActiveChannels", numChannels);
        statsExpiryTime = topology->getPropInt("@statsExpiryTime", 3600);
        roxiemem::memTraceSizeLimit = topology->getPropInt("@memTraceSizeLimit", 0);
        callbackRetries = topology->getPropInt("@callbackRetries", 3);
        callbackTimeout = topology->getPropInt("@callbackTimeout", 5000);
        lowTimeout = topology->getPropInt("@lowTimeout", 10000);
        highTimeout = topology->getPropInt("@highTimeout", 2000);
        slaTimeout = topology->getPropInt("@slaTimeout", 2000);
        keyedJoinFlowLimit = topology->getPropInt("@keyedJoinFlowLimit", 1000);
        parallelLoopFlowLimit = topology->getPropInt("@parallelLoopFlowLimit", 100);
        perChannelFlowLimit = topology->getPropInt("@perChannelFlowLimit", 10);
        copyResources = topology->getPropBool("@copyResources", true);
        useRemoteResources = topology->getPropBool("@useRemoteResources", true);
        checkFileDate = topology->getPropBool("@checkFileDate", true);
        const char *lazyOpenMode = topology->queryProp("@lazyOpen");
        if (!lazyOpenMode)
            lazyOpen = false;
        else if (stricmp(lazyOpenMode, "smart")==0)
            lazyOpen = (restarts > 0);
        else
            lazyOpen = topology->getPropBool("@lazyOpen", false);
        localSlave = topology->getPropBool("@localSlave", false);
        doIbytiDelay = topology->getPropBool("@doIbytiDelay", true);
        minIbytiDelay = topology->getPropInt("@minIbytiDelay", 2);
        initIbytiDelay = topology->getPropInt("@initIbytiDelay", 50);
        crcResources = topology->getPropBool("@crcResources", false);
        chunkingHeap = topology->getPropBool("@chunkingHeap", true);
        readTimeout = topology->getPropInt("@readTimeout", 300);
        logFullQueries = topology->getPropBool("@logFullQueries", false);
        debugPermitted = topology->getPropBool("@debugPermitted", true);
        blindLogging = topology->getPropBool("@blindLogging", false);
        if (!blindLogging)
            logExcessiveSeeks = true;
        linuxYield = topology->getPropBool("@linuxYield", true);
        traceSmartStepping = topology->getPropBool("@traceSmartStepping", false);
        useMemoryMappedIndexes = topology->getPropBool("@useMemoryMappedIndexes", false);
        traceJHtreeAllocations = topology->getPropBool("@traceJHtreeAllocations", false);
        flushJHtreeCacheOnOOM = topology->getPropBool("@flushJHtreeCacheOnOOM", true);
        fastLaneQueue = topology->getPropBool("@fastLaneQueue", true);
        udpOutQsPriority = topology->getPropInt("@udpOutQsPriority", 0);
        udpSnifferEnabled = topology->getPropBool("@udpSnifferEnabled", true);
        udpInlineCollation = topology->getPropBool("@udpInlineCollation", false);
        udpInlineCollationPacketLimit = topology->getPropInt("@udpInlineCollationPacketLimit", 50);
        udpSendCompletedInData = topology->getPropBool("@udpSendCompletedInData", false);
        udpRetryBusySenders = topology->getPropInt("@udpRetryBusySenders", 0);
        udpMaxRetryTimedoutReqs = topology->getPropInt("@udpMaxRetryTimedoutReqs", 0);
        udpRequestToSendTimeout = topology->getPropInt("@udpRequestToSendTimeout", 5);
        // MORE: think of a better way/value/check maybe/and/or based on Roxie server timeout
        if (udpRequestToSendTimeout == 0)
            udpRequestToSendTimeout = 5; 
        // This is not added to deployment\xmlenv\roxie.xsd on purpose
        enableSocketMaxSetting = topology->getPropBool("@enableSocketMaxSetting", false);
        // MORE: might want to check socket buffer sizes against sys max here instead of udp threads ?
        udpMulticastBufferSize = topology->getPropInt("@udpMulticastBufferSize", 262142);
        udpFlowSocketsSize = topology->getPropInt("@udpFlowSocketsSize", 131072);
        udpLocalWriteSocketSize = topology->getPropInt("@udpLocalWriteSocketSize", 1024000);
        
        roxieMulticastEnabled = topology->getPropBool("@roxieMulticastEnabled", true);   // enable use of multicast for sending requests to slaves
        if (udpSnifferEnabled && !roxieMulticastEnabled)
            DBGLOG("WARNING: ignoring udpSnifferEnabled setting as multicast not enabled");

        indexReadChunkSize = topology->getPropInt("@indexReadChunkSize", 60000);
        smartSteppingChunkRows = topology->getPropInt("@smartSteppingChunkRows", 100);
        numSlaveThreads = topology->getPropInt("@slaveThreads", 30);
        numServerThreads = topology->getPropInt("@serverThreads", 30);
        numRequestArrayThreads = topology->getPropInt("@requestArrayThreads", 5);
        maxBlockSize = topology->getPropInt("@maxBlockSize", 10000000);
        maxLockAttempts = topology->getPropInt("@maxLockAttempts", 5);
        enableHeartBeat = topology->getPropBool("@enableHeartBeat", true);
        checkCompleted = topology->getPropBool("@checkCompleted", true);
        preabortKeyedJoinsThreshold = topology->getPropInt("@preabortKeyedJoinsThreshold", 100);
        preabortIndexReadsThreshold = topology->getPropInt("@preabortIndexReadsThreshold", 100);
        defaultMemoryLimit = (memsize_t) topology->getPropInt64("@defaultMemoryLimit", 0);
        defaultTimeLimit[0] = (unsigned) topology->getPropInt64("@defaultLowPriorityTimeLimit", 0);
        defaultTimeLimit[1] = (unsigned) topology->getPropInt64("@defaultHighPriorityTimeLimit", 0);
        defaultTimeLimit[2] = (unsigned) topology->getPropInt64("@defaultSLAPriorityTimeLimit", 0);
        defaultWarnTimeLimit[0] = (unsigned) topology->getPropInt64("@defaultLowPriorityTimeWarning", 0);
        defaultWarnTimeLimit[1] = (unsigned) topology->getPropInt64("@defaultHighPriorityTimeWarning", 0);
        defaultWarnTimeLimit[2] = (unsigned) topology->getPropInt64("@defaultSLAPriorityTimeWarning", 0);

        defaultStripLeadingWhitespace = topology->getPropBool("@defaultStripLeadingWhitespace", true);
        defaultParallelJoinPreload = topology->getPropInt("@defaultParallelJoinPreload", 0);
        defaultConcatPreload = topology->getPropInt("@defaultConcatPreload", 0);
        defaultFetchPreload = topology->getPropInt("@defaultFetchPreload", 0);
        defaultFullKeyedJoinPreload = topology->getPropInt("@defaultFullKeyedJoinPreload", 0);
        defaultKeyedJoinPreload = topology->getPropInt("@defaultKeyedJoinPreload", 0);
        defaultPrefetchProjectPreload = topology->getPropInt("@defaultPrefetchProjectPreload", 10);
        fieldTranslationEnabled = topology->getPropBool("@fieldTranslationEnabled", false);

        defaultCheckingHeap = topology->getPropInt("@checkingHeap", 0);
        checkVersion = topology->getPropBool("@checkVersion", true);
        deleteUnneededFiles = topology->getPropBool("@deleteUnneededFiles", true);
        checkPrimaries = topology->getPropBool("@checkPrimaries", true);
        pretendAllOpt = topology->getPropBool("@ignoreMissingFiles", false);
        memoryStatsInterval = topology->getPropInt("@memoryStatsInterval", 60);
        roxiemem::setMemoryStatsInterval(memoryStatsInterval);
        pingInterval = topology->getPropInt("@pingInterval", 0);
        socketCheckInterval = topology->getPropInt("@socketCheckInterval", 5000);
        memsize_t totalMemoryLimit = (memsize_t) topology->getPropInt64("@totalMemoryLimit", 0);
        if (!totalMemoryLimit)
            totalMemoryLimit = 1024 * 0x100000;  // 1 Gb;
        roxiemem::setTotalMemoryLimit(totalMemoryLimit);
        rtlSetReleaseRowHook(&callbackHook);

        traceStartStop = topology->getPropBool("@traceStartStop", false);
        traceServerSideCache = topology->getPropBool("@traceServerSideCache", false);
        timeActivities = topology->getPropBool("@timeActivities", true);
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
        topology->getProp("@daliServers", fileNameServiceDali);
        trapTooManyActiveQueries = topology->getPropBool("@trapTooManyActiveQueries", true);
        allowRoxieOnDemand = topology->getPropBool("@allowRoxieOnDemand", false);
        maxEmptyLoopIterations = topology->getPropInt("@maxEmptyLoopIterations", 1000);
        maxGraphLoopIterations = topology->getPropInt("@maxGraphLoopIterations", 1000);
        useTreeCopy = topology->getPropBool("@useTreeCopy", true);
        mergeSlaveStatistics = topology->getPropBool("@mergeSlaveStatistics", true);

        syncCluster = topology->getPropBool("@syncCluster", false);  // should we sync an out of sync cluster (always send a trap)
        enableKeyDiff = topology->getPropBool("@enableKeyDiff", true);
        enableForceKeyDiffCopy = topology->getPropBool("@enableForceKeyDiffCopy", false);

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
        if (pinterval)
            startPerformanceMonitor(pinterval);


        topology->getProp("@pluginDirectory", pluginDirectory);
        if (pluginDirectory.length() == 0)
            pluginDirectory.append(codeDirectory).append("plugins");
        ensureDirectory(pluginDirectory);

        if (queryDirectory.length() == 0)
        {
            topology->getProp("@queryDir", queryDirectory);
            if (queryDirectory.length() == 0)
                queryDirectory.append(codeDirectory).append("queries");
        }

        ensureDirectory(queryDirectory);

        baseDataDirectory.append(topology->queryProp("@baseDataDir"));
        queryFileCache().start();

#ifdef _WIN32
        topology->addPropBool("@linuxOS", false);
#else
        topology->addPropBool("@linuxOS", true);
#endif

        if (!numChannels)
            throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - numChannels attribute must be specified");

        bool myIPadded = false;
        Owned<IPropertyTreeIterator> roxieServers = topology->getElements("./RoxieServerProcess");
        ForEach(*roxieServers)
        {
            IPropertyTree &roxieServer = roxieServers->query();
            const char *iptext = roxieServer.queryProp("@netAddress");
            unsigned nodeIndex = addRoxieNode(iptext);
            unsigned port = roxieServer.getPropInt("@port", ROXIE_SERVER_PORT);
            if (iptext && port)
            {
                SocketEndpoint ep(iptext, port);
                unsigned roxieServerHost = roxieServer.getPropInt("@multihost", 0);
                if (ipMatch(ep) && ((roxieServerHost == myHostNumber) || (myHostNumber==-1)))
                {
                    if (baseDataDirectory.length() == 0)  // if not set by other topology settings default to this ...
                        baseDataDirectory.append(roxieServer.queryProp("@baseDataDirectory"));
                    unsigned numThreads = roxieServer.getPropInt("@numThreads", numServerThreads);
                    const char *aclName = roxieServer.queryProp("@aclName");
                    addServerChannel(roxieServers->query().queryProp("@dataDirectory"), port, numThreads, aclName, topology);
                    if (!myIPadded || (myHostNumber==-1))
                    {
                        myNodeIndex = nodeIndex;
                        allRoxieServers.append(ep);
                        myIPadded = true;
                    }
                }
                else if (multiHostMode || !roxieServerHost)
                {
                    bool found = false;
                    ForEachItemIn(idx, allRoxieServers)
                    {
                        if (multiHostMode)
                        {
                            if (ep.equals(allRoxieServers.item(idx)))
                            {
                                found = true;
                                break;
                            }
                        }
                        else
                        {
                            if (ep.ipequals(allRoxieServers.item(idx)))
                            {
                                found = true;
                                break;
                            }
                        }
                    }
                    if (!found)
                        allRoxieServers.append(ep);
                }
            }
            else
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - missing netAddress or port specification on RoxieServerProcess element");
        }
        if (!localSlave)
        {
            if (roxieMulticastEnabled)
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
        }
        Owned<IPropertyTreeIterator> slaves = topology->getElements("./RoxieSlaveProcess");
        unsigned slaveCount = 0;
        IpAddress *primaries = new IpAddress[numChannels+1];    // check each channel has a different primary, if possible. Leaks on fatal errors, but who cares
        ForEach(*slaves)
        {
            IPropertyTree &slave = slaves->query();
            const char *iptext = slave.queryProp("@netAddress");
            if (iptext)
            {
                unsigned nodeIndex = addRoxieNode(iptext);
                IpAddress slaveIp(iptext);
                bool isMe = ipMatch(slaveIp) && slave.getPropInt("@multihost", 0) == myHostNumber;
                bool suspended = slave.getPropBool("@suspended", false);
                unsigned channel = slave.getPropInt("@channel", 0);
                if (!channel)
                    channel = slave.getPropInt("@channels", 0); // legacy support
                const char *dataDirectory = slave.queryProp("@dataDirectory");
                if (channel && channel <= numChannels)
                {
                    if (isMe)
                        isCCD = true;
                    if (!numSlaves[channel])
                    {
                        primaries[channel] = slaveIp;
                        slaveCount++;
                    }
                    addChannel(channel, dataDirectory, isMe, suspended, slaveIp);
                    if (isMe)
                        joinMulticastChannel(channel);
                }
                else
                    throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - missing or invalid channel attribute on RoxieSlaveProcess element");
            }
            else
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - missing netAddress attribute on RoxieSlaveProcess element");
        }
        if (numActiveChannels)
            joinMulticastChannel(0); // all slaves also listen on channel 0

        for (unsigned n = 1; n < numActiveChannels; n++)
        {
            if (primaries[n].isNull())
                throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - no slaves for channel %d", n);
            if (checkPrimaries)
            {
                for (unsigned m = n+1; m <= numChannels; m++)
                    if (primaries[n].ipequals(primaries[m]))
                    {
                        StringBuffer s;
                        primaries[n].getIpText(s);
                        throw MakeStringException(MSGAUD_operator, ROXIE_INVALID_TOPOLOGY, "Invalid topology file - slave %s is primary for multiple channels", s.str());
                    }
            }
        }
        delete [] primaries;

        unsigned snifferChannel = numChannels+2; // MORE - why +2 not +1 ??
        ROQ = createOutputQueueManager(snifferChannel, isCCD ? numSlaveThreads : 1);
        try
        {
            createResourceManagers(standAloneDll, numChannels);
        }
        catch(IException *E)
        {
            EXCLOG(E, "No configuration could be loaded");
            controlSem.interrupt();
            throw; // MORE - this may not be appropriate.
        }
        Owned<IPacketDiscarder> packetDiscarder = createPacketDiscarder();

        setDaliServixSocketCaching(true);  // enable daliservix caching
        ROQ->setHeadRegionSize(headRegionSize);
        ROQ->start();
#if defined(WIN32) && defined(_DEBUG) && defined(_DEBUG_HEAP_FULL)
        int tmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
        tmpFlag |= _CRTDBG_CHECK_ALWAYS_DF;
        _CrtSetDbgFlag( tmpFlag );
#endif
        setSEHtoExceptionHandler(&abortHandler);
        if (runOnce)
        {
            Owned <IRoxieSocketListener> roxieServer = createRoxieSocketListener(0, 1, 0, false);
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
                roxieServer->runOnce(query.str());
            }
            catch (IException *E)
            {
                EXCLOG(E);
                E->Release();
            }
        }
        else
        {
            Owned<IPropertyTreeIterator> it = ccdChannels->getElements("RoxieServerProcess");
            ForEach(*it)
            {
                // MORE - there are assumptions that everyone is a server (in deployment)
                IPropertyTree &serverInfo = it->query();
                unsigned port = serverInfo.getPropInt("@port", -1);
                bool suspended = serverInfo.getPropBool("@suspended", false);
                unsigned numThreads = serverInfo.getPropInt("@numThreads", -1);
                unsigned listenQueue = serverInfo.getPropInt("@listenQueue", DEFAULT_LISTEN_QUEUE_SIZE);
                Owned <IRoxieSocketListener> roxieServer = createRoxieSocketListener(port, numThreads, listenQueue, suspended);
                Owned<IPropertyTreeIterator> accesses = serverInfo.getElements("Access");
                ForEach(*accesses)
                {
                    IPropertyTree &access = accesses->query();
                    try
                    {
                        roxieServer->addAccess(access.getPropBool("@allow", true), access.getPropBool("@allowBlind", true), access.queryProp("@ip"), access.queryProp("@mask"), access.queryProp("@query"), access.queryProp("@error"), access.getPropInt("@errorCode"));
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
                socketListeners.append(*roxieServer.getLink());
                time(&startupTime);
                roxieServer->start();
            }
            // if we got here, the initial load is ok, so create the "sentinel file" for re-runs from the script
            // put in its own "scope" to force the flush
            if (sentinelFile)
            {
                DBGLOG("Creating sentinel file %s", sentinelFile->queryFilename());
                Owned<IFileIO> fileIO = sentinelFile->open(IFOcreate);
                fileIO->write(0, 5, "rerun");
            }
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
    allRoxieServers.kill();
    stopPerformanceMonitor();
    cleanupResourceManagers();
    closeMulticastSockets();
    releaseSlaveDynamicFileCache();
    releaseDiffFileInfoCache();
    releaseRoxieStateCache();
    setDaliServixSocketCaching(false);  // make sure it cleans up or you get bogus memleak reports
    setNodeCaching(false); // ditto
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
