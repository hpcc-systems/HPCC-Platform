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

#include "build-config.h"
#include "platform.h"
#include "thirdparty.h"
#include "portlist.h"
#include "jlib.hpp"
#include "jlog.ipp"
#include "jptree.hpp"
#include "jmisc.hpp"

#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "mplog.hpp"
#include "dasess.hpp"
#include "dasds.hpp"
#include "daclient.hpp"
#include "dalienv.hpp"
#include "dllserver.hpp"
#include "rmtfile.hpp"

#include "saserver.hpp"
#include "saarch.hpp"
#include "saverify.hpp"
#include "saxref.hpp"
#include "saqmon.hpp"
#include "sacoalescer.hpp"
#include "sacmd.hpp"

extern void LDStest();

#define SASHAVERSION "1.4"

#define DEFAULT_PERF_REPORT_DELAY (60*5)

Owned<IPropertyTree> serverConfig;
static IArrayOf<ISashaServer> servers;
static ILogMsgHandler * fileMsgHandler=NULL;
static CSDSServerStatus * SashaServerStatus=NULL;
static atomic_t StopSuspendCount = ATOMIC_INIT(0);
static bool stopped = false;
static Semaphore stopSem;

const char *sashaProgramName;

CSuspendAutoStop::CSuspendAutoStop() { atomic_inc(&StopSuspendCount); }
CSuspendAutoStop::~CSuspendAutoStop() { atomic_dec(&StopSuspendCount); }

void setMsgLevel(unsigned level)
{
    if (!fileMsgHandler)
        return;
    ILogMsgFilter *filter = getSwitchLogMsgFilterOwn(getComponentLogMsgFilter(3), getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, level, true), getDefaultLogMsgFilter());
    queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
    queryLogMsgManager()->changeMonitorFilterOwn(fileMsgHandler, filter);
}

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

static void stopServer()
{
    ForEachItemInRev(i,servers)
    {
        ISashaServer &server=servers.item(i);
        LOG(MCprogress, unknownJob, "Stopping %d",i);
        server.stop();
    }
    ForEachItemInRev(j,servers)
    {
        servers.remove(j);      // ensure correct order for destruction
    }
}


static bool actionOnAbort()
{
    LOG(MCprogress, unknownJob, "Stop signalled");
    if (stopped)
    {
        LOG(MCprogress, unknownJob, "Previously marked stopped. Killing process..");
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
        LOG(MCoperatorError, unknownJob, e, "SASERVER: Unexpected exception, saserver terminating");
    LOG(MCprogress, unknownJob, "Stop requested");
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
    printf("Usage: SASERVER     -- (all configuration parameters in sashaconf.xml)\n"
           "or:    SASERVER STOP\n");
}

class CSashaCmdThread : public CInterface, implements IPooledThread
{
    Owned<ISashaCommand> cmd;
public:
    IMPLEMENT_IINTERFACE;
    void init(void *param)
    {
        cmd.setown((ISashaCommand *)param);
    }
    void main()
    {
        StringAttrArray out;
        StringAttrArray outres;
        if (cmd->getAction()==SCA_STOP) {
            if (!stopped) {
                stopped = true;
                cmd->cancelaccept();
                PROGLOG("Request to stop received");
            }
        }
        else if (cmd->getAction()==SCA_GETVERSION) 
            cmd->addId(SASHAVERSION); // should be a result probably but keep bwd compatible
        else if (cmd->getAction()==SCA_COALESCE_SUSPEND)
            suspendCoalescingServer();
        else if (cmd->getAction()==SCA_COALESCE_RESUME)
            resumeCoalescingServer();
        else if (cmd->getAction()==SCA_XREF) 
            processXRefRequest(cmd);
        else if (!processArchiverCommand(cmd)) 
            WARNLOG("Command %d not handled",cmd->getAction());
        if (cmd->getAction()==SCA_WORKUNIT_SERVICES_GET)
            cmd->WUSreply();
        else
            cmd->reply();
    }
    bool stop() 
    { 
        return true; 
    }
    bool canReuse() 
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
    Owned<IThreadPool> threadpool = createThreadPool("sachaCmdPool",&factory);
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
            else if (!verifyCovenConnection(5*60*1000)) {
                PROGLOG("Dali stopped");
                stopped = true;
            }
            else if (timeout&&(timeout<msTick()-start)) {
                if (atomic_read(&StopSuspendCount)==0) {
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




int main(int argc, const char* argv[])
{
    InitModuleObjects();
    EnableSEHtoExceptionMapping();

    sashaProgramName = argv[0];

#ifndef __64BIT__
    Thread::setDefaultStackSize(0x20000);
#endif
    setDaliServixSocketCaching(true);
    bool stop = false;
    bool coalescer = false;
    bool force = false;
    if (argc>1) {
        if (stricmp(argv[1],"STOP")==0)
            stop = true;
        else if (stricmp(argv[1],"COALESCE")==0) {
            coalescer = true;
            force = (argc>2)&&(stricmp(argv[2],"FORCE")==0);
        }
        else {
            usage();
            return 1;
        }
    }
#ifndef _DEBUG
    NoQuickEditSection x;
#endif

    OwnedIFile ifile = createIFile("sashaconf.xml");
    serverConfig.setown(ifile->exists()?createPTreeFromXMLFile("sashaconf.xml"):createPTree());
    StringBuffer daliServer;
    if (!serverConfig->getProp("@DALISERVERS", daliServer)||(daliServer.length()==0)) {
        printf("DALISERVERS not specified in sashaconf.xml");
        return 1;
    }

    StringBuffer logname;
    StringBuffer logdir;
    if (!getConfigurationDirectory(serverConfig->queryPropTree("Directories"),"log","sasha",serverConfig->queryProp("@name"),logdir))
        serverConfig->getProp("@logDir", logdir);
    if (logdir.length() && recursiveCreateDirectory(logdir.str()))
        logname.append(logdir);
    else
    {
        char cwd[1024];
        GetCurrentDirectory(1024, cwd);
        logname.append(cwd);
    }
    addPathSepChar(logname);

    if (coalescer) {
        logname.append("saserver.coalesce");
        addFileTimestamp(logname, true);
        logname.append(".log");
        appendLogFile(logname.str(),0,false);
    }
    else if (!stop) {
        logname.append("saserver");
        StringBuffer aliaslogname(logname);
        aliaslogname.append(".log");
        fileMsgHandler = getRollingFileLogMsgHandler(logname.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliaslogname.str());
        queryLogMsgManager()->addMonitorOwn(fileMsgHandler, getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, TopDetail));
        DBGLOG("Build %s", BUILD_TAG);
    }

    bool enableSNMP = false;

    Owned<IPerfMonHook> perfMonHook;
    try {


        unsigned short port = (stop||coalescer)?0:DEFAULT_SASHA_PORT;
        Owned<IGroup> serverGroup = createIGroup(daliServer.str(),DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_SashaServer, port, NULL, NULL, MP_WAIT_FOREVER);
        setPasswordsFromSDS(); 
        if (!stop&!coalescer) {
            startLogMsgParentReceiver();    // for auditing
            connectLogMsgManagerToDali();
        }
            
        if (stop) {
            stopSashaServer((argc>2)?argv[2]:"",DEFAULT_SASHA_PORT);
        }
        else {
            if (serverConfig->getPropInt("@enableSysLog"),true)
                UseSysLogForOperatorMessages();
            if (coalescer) {
                coalesceDatastore(force);
            }
            else {
                startPerformanceMonitor(serverConfig->getPropInt("@perfReportDelay", DEFAULT_PERF_REPORT_DELAY)*1000, PerfMonStandard, perfMonHook);
                AddServers();
                addAbortHandler(actionOnAbort);

                StringBuffer eps;
                PROGLOG("SASERVER starting on %s",queryMyNode()->endpoint().getUrlStr(eps).str());

                ForEachItemIn(i1,servers)
                {
                    ISashaServer &server=servers.item(i1);
                    server.start();
                }

                SashaServerStatus = new CSDSServerStatus("SashaServer");

                ForEachItemIn(i2,servers)
                {
                    ISashaServer &server=servers.item(i2);
                    server.ready();
                }
                class CStopThread : implements IThreaded
                {
                    CThreaded threaded;
                public:
                    CStopThread() : threaded("CStopThread") { threaded.init(this); } 
                    ~CStopThread() { threaded.join(); }
                    virtual void main()
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
                SashaMain();
                removeThreadExceptionHandler(&exceptionStopHandler);

                stopSem.signal();
                delete stopThread;

                PROGLOG("SASERVER exiting");
                stopPerformanceMonitor();
            }
            delete SashaServerStatus;
            SashaServerStatus = NULL;
        }
    }
    catch(IException *e){ 
        EXCLOG(e, "Sasha Server Exception: ");
        stopPerformanceMonitor();
        e->Release();
    }
    catch (const char *s) {
        WARNLOG("Sasha: %s",s);
    }

    if (!stop)
    {
        stopServer();
    }
    else if (stop)
        Sleep(2000);    // give time to stop
    serverConfig.clear();
    try {
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


    
