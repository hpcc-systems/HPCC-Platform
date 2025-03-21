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

#include "platform.h"
#include "thirdparty.h"
#include "portlist.h"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jptree.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "mplog.hpp"
#include "dasess.hpp"
#include "dasds.hpp"
#include "dafdesc.hpp"
#include "daclient.hpp"
#include "environment.hpp"
#include "dllserver.hpp"
#include "rmtfile.hpp"

#include "saserver.hpp"
#include "saarch.hpp"
#include "saverify.hpp"
#include "saxref.hpp"
#include "sadbghk.hpp"
#include "saqmon.hpp"
#include "sacoalescer.hpp"
#include "sacmd.hpp"

extern void LDStest();

#define SASHAVERSION "1.4"

#define DEFAULT_PERF_REPORT_DELAY (60*5)

Owned<IPropertyTree> serverConfig;
static IArrayOf<ISashaServer> servers;
static std::atomic<unsigned> StopSuspendCount{0};
static bool stopped = false;
static Semaphore stopSem;

const char *sashaProgramName;

CSuspendAutoStop::CSuspendAutoStop() { StopSuspendCount++; }
CSuspendAutoStop::~CSuspendAutoStop() { StopSuspendCount--; }

#ifdef _CONTAINERIZED
const char *service = nullptr;
#else

static void AddServers()
{
    // order significant
    servers.append(*createSashaArchiverServer());
//  servers.append(*createSashaVerifierServer());
    servers.append(*createSashaSDSCoalescingServer());
    servers.append(*createSashaXrefServer());
    servers.append(*createSashaDaFSMonitorServer());
    servers.append(*createSashaQMonitorServer());
    servers.append(*createSashaFileExpiryServer()); 
    // add new servers here
}
#endif

static void stopServer()
{
    ForEachItemInRev(i,servers)
    {
        ISashaServer &server=servers.item(i);
        LOG(MCprogress, "Stopping %d",i);
        server.stop();
    }
    ForEachItemInRev(j,servers)
    {
        servers.remove(j);      // ensure correct order for destruction
    }
}


static bool actionOnAbort()
{
    LOG(MCprogress, "Stop signalled");
    if (stopped)
    {
        LOG(MCprogress, "Previously marked stopped. Killing process..");
        queryLogMsgManager()->flushQueue(10*1000);
#ifdef _WIN32
        TerminateProcess(GetCurrentProcess(), 1);
#else
        kill(getpid(), SIGTERM);
#endif
    }
    else
    {
        requestStop(NULL);
        return false;
    }
    return true;
}

void requestStop(IException *e)
{
    if (e)
        LOG(MCoperatorError, e, "SASERVER: Unexpected exception, saserver terminating");
    LOG(MCprogress, "Stop requested");
    stopSem.signal();
}

void stopSashaServer(const char *eps,unsigned short port)
{
    SocketEndpoint ep;
    if (eps&&*eps)
        ep.set(eps,port);
    else
        ep.setLocalHost(port);
    
    Owned<INode> node = createINode(ep);
    IInterCommunicator & comm=queryWorldCommunicator();
    Owned<ISashaCommand> cmd = createSashaCommand();
    cmd->setAction(SCA_STOP);
    if (!queryWorldCommunicator().verifyConnection(node,1000)) {
        PROGLOG("Sasha Server already stopped");
        return;
    }
    PROGLOG("Sasha Server being stopped");
    cmd->send(node,1000*60);    
    while (queryWorldCommunicator().verifyConnection(node,500)) {
        PROGLOG("Waiting for Sasha Server to stop....");
        Sleep(2000);
    }
}

void usage()
{
    printf("Usage: SASERVER   (all configuration parameters in sashaconf.xml)\n"
           "or:    SASERVER --coalesce [--force]\n"
           "or:    SASERVER --stop\n");
}

class CSashaCmdThread : public CInterface, implements IPooledThread
{
    Owned<ISashaCommand> cmd;
public:
    IMPLEMENT_IINTERFACE;
    virtual void init(void *param) override
    {
        cmd.setown((ISashaCommand *)param);
    }
    virtual void threadmain() override
    {
        StringAttrArray out;
        StringAttrArray outres;
        if (cmd->getAction()==SCA_STOP)
        {
            if (!stopped)
            {
                stopped = true;
                cmd->cancelaccept();
                PROGLOG("Request to stop received");
            }
        }
        else if (cmd->getAction()==SCA_GETVERSION) 
            cmd->addId(SASHAVERSION); // should be a result probably but keep bwd compatible
        else 
        {
#ifdef _CONTAINERIZED
                if (strieq(service, "coalescer"))
                {
                    if (cmd->getAction()==SCA_COALESCE_SUSPEND)
                        suspendCoalescingServer();
                    else if (cmd->getAction()==SCA_COALESCE_RESUME)
                        resumeCoalescingServer();
                }
                else if (strieq(service, "wu-archiver") || strieq(service, "dfuwu-archiver"))
                {
                    switch (cmd->getAction())
                    {
                        case SCA_ARCHIVE:
                        case SCA_BACKUP:
                        case SCA_RESTORE:
                        case SCA_LIST: 
                        case SCA_GET: 
                        case SCA_WORKUNIT_SERVICES_GET: 
                        case SCA_LISTDT:
                        {
                            if (!processArchiverCommand(cmd)) 
                                OWARNLOG("Command %d not handled",cmd->getAction());
                            break;
                        }
                        default:
                            OWARNLOG("Unrecognised command: %d", cmd->getAction());
                            break;
                    }
                }
#else
            if (cmd->getAction()==SCA_COALESCE_SUSPEND)
                suspendCoalescingServer();
            else if (cmd->getAction()==SCA_COALESCE_RESUME)
                resumeCoalescingServer();
            else if (cmd->getAction()==SCA_XREF) 
                processXRefRequest(cmd);
            else if (!processArchiverCommand(cmd)) 
                OWARNLOG("Command %d not handled",cmd->getAction());
#endif
        }
        if (cmd->getAction()==SCA_WORKUNIT_SERVICES_GET)
            cmd->WUSreply();
        else
            cmd->reply();
    }
    virtual bool stop() override
    { 
        return true; 
    }
    virtual bool canReuse() const override
    { 
        return false; 
    }
};


void SashaMain()
{
    IInterCommunicator & comm=queryWorldCommunicator();
    unsigned start = msTick();
    unsigned timeout = serverConfig->getPropInt("@autoRestartInterval")*1000*60*60;
    class cCmdPoolFactory : public CInterface, implements IThreadFactory
    {
    public:
        IMPLEMENT_IINTERFACE;
        IPooledThread *createNew()
        {
            return new CSashaCmdThread;
        }
    } factory;
    Owned<IThreadPool> threadpool = createThreadPool("sashaCmdPool",&factory, false, nullptr);
    CMessageBuffer mb;
    while (!stopped) {
        try {
            Owned<ISashaCommand> cmd = createSashaCommand();
            if (cmd->accept(5*60*1000)) {
                threadpool->start(cmd.getClear());
            }
            else if (stopped) {
                PROGLOG("Sasha stopping");
                break;
            }
            else if (!verifyCovenConnection(30*1000)) {
                PROGLOG("Dali stopped");
                stopped = true;
            }
            else if (timeout&&(timeout<msTick()-start)) {
                if (StopSuspendCount==0) {
                    PROGLOG("Auto Restart");
                    stopped = true;
                }
                else
                    PROGLOG("Waiting to Auto Restart %d",msTick()-start-timeout);
            }

        }
        catch (IException *e)
        {
            EXCLOG(e, "SashaMain");
            e->Release();
        }
    }
    if (!threadpool->joinAll(true,1000*60))
        PROGLOG("Stopping join timed out");
}

static struct CRequestStop : implements IExceptionHandler
{
    virtual bool fireException(IException *e)
    {
        requestStop(e);
        return true;
    }
} exceptionStopHandler;


static constexpr const char * defaultYaml = R"!!(
version: 1.0
sasha:
  name: sasha
)!!";

int main(int argc, const char* argv[])
{
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    sashaProgramName = argv[0];

#ifndef __64BIT__
    // Restrict stack sizes on 32-bit systems
    Thread::setDefaultStackSize(0x20000);
#endif
    setDaliServixSocketCaching(true);
    bool stop = false;
    bool coalescer = false;
    bool force = false;
#ifndef _DEBUG
    NoQuickEditSection x;
#endif

    bool enableSNMP = false;

    try
    {
        serverConfig.setown(loadConfiguration(defaultYaml, argv, "sasha", "SASHA", "sashaconf.xml", nullptr));

        Owned<IFile> sentinelFile;

        stop = serverConfig->hasProp("@stop");
        if (!stop)
        {
            coalescer = serverConfig->hasProp("@coalesce");
            if (coalescer)
                force = serverConfig->hasProp("@force");
            else
            {
                sentinelFile.setown(createSentinelTarget());
                removeSentinelFile(sentinelFile);
            }
        }

    #ifndef _CONTAINERIZED
        StringBuffer logname;
        StringBuffer logdir;
        if (!stop)
        {
            Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(serverConfig, "sasha");
            lf->setName("saserver");//override default filename
            if (coalescer)
                lf->setPostfix("coalesce");
            lf->setMaxDetail(TopDetail);
            lf->beginLogging();
        }
    #else
        setupContainerizedLogMsgHandler();
    #endif
        DBGLOG("Build %s", hpccBuildInfo.buildTag);

        unsigned short port = serverConfig->getPropInt("service/@port", serverConfig->getPropInt("@port"));
        if (!port)
        {
            if (!stop && !coalescer)
                port = DEFAULT_SASHA_PORT;
        }

#ifdef _CONTAINERIZED
        if (!stop && !coalescer)
        {
            // NB: if this service is within the Dali pod, then must write sentinal before trying to connect to Dali,
            // because Dali will not receive any traffic until all containers are ready
            if (serverConfig->getPropBool("@inDaliPod"))
                writeSentinelFile(sentinelFile);
        }
#endif
        StringBuffer daliServer;
        if (!serverConfig->getProp("@daliServers", daliServer))
            serverConfig->getProp("@DALISERVERS", daliServer); // @DALISERVERS legacy/bare-metal
        if (0 == daliServer.length())
        {
            PROGLOG("DALISERVERS not specified in sashaconf.xml");
            return 1;
        }
        Owned<IGroup> serverGroup = createIGroupRetry(daliServer.str(), DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_SashaServer, port, nullptr, nullptr, MP_WAIT_FOREVER, true);

        if (stop)
            stopSashaServer((argc>2)?argv[2]:"", DEFAULT_SASHA_PORT);
        else
        {
            startLogMsgParentReceiver();    // for auditing
            connectLogMsgManagerToDali();

            if (serverConfig->getPropInt("@enableSysLog"),true)
                UseSysLogForOperatorMessages();
            if (coalescer)
            {
#ifdef _CONTAINERIZED
                coalesceDatastore(serverConfig, force);
#else
                coalesceDatastore(serverConfig->queryPropTree("Coalescer"), force);
#endif
            }
            else
            {
                addAbortHandler(actionOnAbort);
                initializeStoragePlanes(true, true);
#ifdef _CONTAINERIZED
                service = serverConfig->queryProp("@service");
                if (isEmptyString(service))
                    throw makeStringException(0, "'service' undefined");

                /*
                 * NB: for the time being both wu-archiver and dfuwu-archive can handle
                 * requests for either standard workunits or dfuworkunits
                 * 
                 */

                if (strieq(service, "coalescer"))
                    servers.append(*createSashaSDSCoalescingServer());
                else if (strieq(service, "wu-archiver"))
                    servers.append(*createSashaWUArchiverServer());
                else if (strieq(service, "dfuwu-archiver"))
                    servers.append(*createSashaDFUWUArchiverServer());
                else if (strieq(service, "dfurecovery-archiver"))
                    servers.append(*createSashaDFURecoveryArchiverServer());
                else if (strieq(service, "cachedwu-remover"))
                    servers.append(*createSashaCachedWURemoverServer());
                else if (strieq(service, "file-expiry"))
                    servers.append(*createSashaFileExpiryServer());
                else if (strieq(service, "thor-qmon"))
                    servers.append(*createSashaQMonitorServer());
                else if (strieq(service, "debugplane-housekeeping"))
                    servers.append(*createSashaDebugPlaneHousekeepingServer());
                //else if (strieq(service, "xref")) // TODO
                //    servers.append(*createSashaXrefServer());
                else
                    throw makeStringExceptionV(0, "Unrecognised 'service': %s", service);
#else
                startPerformanceMonitor(serverConfig->getPropInt("@perfReportDelay", DEFAULT_PERF_REPORT_DELAY)*1000);
                AddServers();
#endif

                StringBuffer eps;
                PROGLOG("SASERVER starting on %s",queryMyNode()->endpoint().getEndpointHostText(eps).str());

                ForEachItemIn(i1,servers)
                {
                    ISashaServer &server=servers.item(i1);
                    server.start();
                }
                ForEachItemIn(i2,servers)
                {
                    ISashaServer &server=servers.item(i2);
                    server.ready();
                }
                class CStopThread : implements IThreaded
                {
                    CThreaded threaded;
                public:
                    CStopThread() : threaded("CStopThread") { threaded.init(this, false); }
                    ~CStopThread() { threaded.join(); }
                    virtual void threadmain() override
                    {
                        stopSem.wait();
                        if (!stopped)
                        {
                            stopped = true;
                            IInterCommunicator &comm=queryWorldCommunicator();
                            comm.cancel(NULL, MPTAG_SASHA_REQUEST);
                        }
                    }
                } *stopThread = new CStopThread;
                addThreadExceptionHandler(&exceptionStopHandler);
#ifdef _CONTAINERIZED
                if (!serverConfig->getPropBool("@inDaliPod"))
                    writeSentinelFile(sentinelFile);
#else
                writeSentinelFile(sentinelFile);
#endif
                SashaMain();
                removeThreadExceptionHandler(&exceptionStopHandler);

                stopSem.signal();
                delete stopThread;

                PROGLOG("SASERVER exiting");
#ifndef _CONTAINERIZED
                stopPerformanceMonitor();
#endif
            }
        }
    }
    catch(IException *e){ 
        EXCLOG(e, "Sasha Server Exception: ");
#ifndef _CONTAINERIZED
        stopPerformanceMonitor();
#endif
        e->Release();
    }
    catch (const char *s) {
        OWARNLOG("Sasha: %s",s);
    }

    if (!stop && !coalescer)
    {
        stopServer();
    }
    else if (stop)
        Sleep(2000);    // give time to stop
    serverConfig.clear();
    try
    {
        closeDllServer();
        closeEnvironment();
        closedownClientProcess();
    }
    catch (IException *) {  // dali may be down
    }
    UseSysLogForOperatorMessages(false);
    releaseAtoms();
    return 0;
}


    
