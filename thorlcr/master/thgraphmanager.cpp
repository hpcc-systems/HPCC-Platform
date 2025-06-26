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

#include <chrono>
#include <future>
#include <string>
#include <unordered_set>

#include "platform.h"
#include <math.h>
#include "jarray.hpp"
#include "jcontainerized.hpp"
#include "jfile.hpp"
#include "jmutex.hpp"
#include "jlog.hpp"
#include "jsecrets.hpp"
#include "rmtfile.hpp"

#include "portlist.h"
#include "wujobq.hpp"
#include "daclient.hpp"
#include "daqueue.hpp"
#include "dastats.hpp"

#include "thgraphmaster.ipp"
#include "thorport.hpp"

#include "thmem.hpp"
#include "thmastermain.hpp"
#include "thexception.hpp"
#include "thcodectx.hpp"

#include "daaudit.hpp"
#include "dadfs.hpp"
#include "dafdesc.hpp"
#include "dautils.hpp"
#include "dllserver.hpp"
#include "eclhelper.hpp"
#include "swapnodelib.hpp"
#include "thactivitymaster.ipp"
#include "thdemonserver.hpp"
#include "thgraphmanager.hpp"
#include "roxiehelper.hpp"
#include "securesocket.hpp"
#include "environment.hpp"
#include "anawu.hpp"
#include "workunit.hpp"

static const StatisticsMapping podStatistics({StNumPods});


void relayWuidException(IConstWorkUnit *workunit, const IException *exception)
{
    switch (workunit->getState())
    {
        case WUStateWait: // if already in wait state, then an exception has already been relayed
        case WUStateAborting:
        case WUStateAborted:
        case WUStateFailed:
            break;
        default:
        {
            Owned<IWorkUnit> wu = &workunit->lock();
            WUState state = wu->getState();
            switch (state)
            {
                case WUStateWait:
                case WUStateAborting:
                case WUStateAborted:
                case WUStateFailed:
                    break;
                default:
                {
                    Owned<IWUException> we = wu->createException();
                    we->setSeverity(SeverityError);
                    StringBuffer errStr;
                    exception->errorMessage(errStr);
                    we->setExceptionMessage(errStr);
                    we->setExceptionSource("thormasterexception");
                    we->setExceptionCode(exception->errorCode());
                    WUState newState = (WUStateRunning == state) ? WUStateWait : WUStateFailed;
                    wu->setState(newState);
                    break;
                }
            }
        }
    }
}

class CJobManager : public CSimpleInterface, implements IJobManager, implements IExceptionHandler
{
    bool stopped, handlingConversation;
    Owned<IConversation> conversation;
    StringAttr queueName;
    CriticalSection replyCrit, jobCrit;
    CFifoFileCache querySoCache;
    Owned<IJobQueue> jobq;
    ICopyArrayOf<CJobMaster> jobs;
    Owned<IException> exitException;
    std::atomic<int> postMortemCaptureInProgress{0};
    double thorRate{0};
    const char * thorName{nullptr};

    class CPodInfo
    {
        unsigned wfid = 0;
        StringAttr wuid, graphName;

        unsigned __int64 min = 0;
        unsigned __int64 max = 0;
        unsigned minNode = 0;
        unsigned maxNode = 0;
        double stdDev = 0;
        CRuntimeSummaryStatisticCollection podStats;
        std::vector<std::string> nodeNames; // ordered list of the unique node names
        bool collectAttempted = false;
        
    public:
        CPodInfo() : podStats(podStatistics)
        {
        }
        void setContext(unsigned _wfid, const char *_wuid, const char *_graphName)
        {
            wfid = _wfid;
            wuid.set(_wuid);
            graphName.set(_graphName);
        }
        bool hasStdDev() const
        {
            return 0 != stdDev;
        }
        void ensureCollected()
        {
            // collate pod distribution
            if (collectAttempted)
                return;
            collectAttempted = true;
            try
            {
                VStringBuffer selector("thorworker-job-%s-%s", wuid.get(), graphName.get());
                std::vector<std::vector<std::string>> pods = k8s::getPodNodes(selector.toLowerCase());
                std::unordered_map<std::string, unsigned> podPerNodeCounts;
                for (const auto &podNode: pods)
                {
                    const std::string &node = podNode[1]; // pod is 1st item, node is 2nd
                    podPerNodeCounts[node]++; // NB: if doesn't exist is created with default value of 0 1st
                }
                for (const auto &node: podPerNodeCounts)
                {
                    podStats.mergeStatistic(StNumPods, node.second, nodeNames.size());
                    nodeNames.push_back(node.first);
                }
                stdDev = podStats.queryStdDevInfo(StNumPods, min, max, minNode, maxNode);
            }
            catch (IException *e)
            {
                IERRLOG(e);
                e->Release();
            }
        }
        void report(IWorkUnit *wu)
        {
            // issue warning and publish pod distribution stats
            Owned<IStatisticGatherer> collector = createGlobalStatisticGatherer(wu);
            StatsScopeId wfidScopeId(SSTworkflow, wfid);
            StatsScopeId graphScopeId(graphName);
            collector->beginScope(wfidScopeId);
            collector->beginScope(graphScopeId);
            podStats.recordStatistics(*collector, false);

            StringBuffer scopeStr;
            wfidScopeId.getScopeText(scopeStr).append(':');
            graphScopeId.getScopeText(scopeStr);
            Owned<IException> e = makeStringExceptionV(-1, "%s: Degraded performance. Worker pods are unevenly distributed over nodes. StdDev=%.2f. min node(%s) has %" I64F "u pods, max node(%s) has %" I64F "u pods", scopeStr.str(), stdDev, nodeNames[minNode].c_str(), min, nodeNames[maxNode].c_str(), max);
            reportExceptionToWorkunit(*wu, e);
        }
    } podInfo;
    
    Owned<IDeMonServer> demonServer;
    std::atomic<unsigned> activeTasks;
    StringAttr          currentWuid;
    ILogMsgHandler *logHandler;

    CJobMaster *getCurrentJob() { CriticalBlock b(jobCrit); return jobs.ordinality() ? &OLINK(jobs.item(0)) : NULL; }
    bool executeGraph(IConstWorkUnit &workunit, const char *graphName, const SocketEndpoint &agentEp);
    void addJob(CJobMaster &job) { CriticalBlock b(jobCrit); jobs.append(job); }
    void removeJob(CJobMaster &job) { CriticalBlock b(jobCrit); jobs.zap(job); }

    class CThorDebugListener : public CSimpleInterfaceOf<IInterface>, implements IThreaded
    {
    protected:
        CThreaded threaded;
        unsigned port;
        Owned<ISocket> sock;
        CJobManager &mgr;
    private:
        std::atomic<bool> running;
    public:
        CThorDebugListener(CJobManager &_mgr) : threaded("CThorDebugListener", this), mgr(_mgr)
        {
            unsigned defaultThorDebugPort = getFixedPort(getMasterPortBase(), TPORT_debug);
            port = globals->getPropInt("DebugPort", defaultThorDebugPort);
            running = true;
            threaded.start(false);
        }
        ~CThorDebugListener()
        {
            running = false;
            if (sock)
                sock->cancel_accept();
            threaded.join();
        }
        virtual unsigned getPort() const { return port; }

        virtual void processDebugCommand(CSafeSocket &ssock, StringBuffer &rawText)
        {
            Owned<IPropertyTree> queryXml;
            try
            {
                queryXml.setown(createPTreeFromXMLString(rawText.str(), ipt_caseInsensitive, (PTreeReaderOptions)(ptr_ignoreWhiteSpace|ptr_ignoreNameSpaces)));
            }
            catch (IException *E)
            {
                StringBuffer s;
                IWARNLOG("processDebugCommand: Invalid XML received from %s:%s", E->errorMessage(s).str(), rawText.str());
                throw;
            }

            Linked<CJobMaster> job = mgr.getCurrentJob();
            if (!job)
                throw MakeStringException(5300, "Command not available when no job active");
            const char *graphId = job->queryGraphName();
            if (!graphId)
                throw MakeStringException(5300, "Command not available when no graph active");

            const char *command = queryXml->queryName();
            if (!command) throw MakeStringException(5300, "Invalid debug command");

            FlushingStringBuffer response(&ssock, false, MarkupFmt_XML, false, false, queryDummyContextLogger());
            response.startDataset("Debug", NULL, (unsigned) -1);

            if (strieq(command, "print"))
            {
                const char *edgeId = queryXml->queryProp("@edgeId");
                if (!edgeId) throw MakeStringException(5300, "Debug command requires edgeId");
                response.appendf("<print graphId='%s' edgeId='%s'>", graphId, edgeId);
                auto responseFunc = [&response](unsigned worker, MemoryBuffer &mb)
                {
                    StringAttr row;
                    mb.read(row);
                    response.append(row);
                };
                constexpr unsigned maxTimeMs = 30000; // should be more than enough
                job->issueWorkerDebugCmd(rawText.str(), 0, responseFunc, maxTimeMs);
                response.append("</print>");
            }
            else if (strieq(command, "quit"))
            {
                DBGLOG("ABORT detected from user during debug session");
                Owned<IException> e = MakeThorException(TE_WorkUnitAborting, "User signalled abort during debug session");
                job->fireException(e);
                response.appendf("<quit state='quit'/>");
            }
            else
                throw makeStringExceptionV(5300, "Command '%s' not supported by Thor", command);

            response.flush(true);
        }
        virtual void threadmain() override
        {
            sock.setown(ISocket::create(port));
            while (running)
            {
                try
                {
                    Owned<ISocket> client = sock->accept(true);
                    // TLS TODO: secure_accept() on Thor debug socket if globally configured for mtls ...
                    if (client)
                    {
                        client->set_linger(-1);
                        CSafeSocket ssock(client.getClear());
                        StringBuffer rawText;
                        IpAddress peer;
                        bool continuationNeeded;
                        bool isStatus;

                        ssock.querySocket()->getPeerAddress(peer);
                        DBGLOG("Reading debug command from socket...");
                        if (!ssock.readBlocktms(rawText, WAIT_FOREVER, NULL, continuationNeeded, isStatus, 1024*1024))
                        {
                            DBGLOG("No data reading query from socket");
                            continue;
                        }
                        assertex(!continuationNeeded);
                        assertex(!isStatus);

                        try
                        {
                            processDebugCommand(ssock,rawText);
                        }
                        catch (IException *E)
                        {
                            StringBuffer s;
                            ssock.sendException("Thor", E->errorCode(), E->errorMessage(s), false, queryDummyContextLogger());
                            E->Release();
                        }
                        // Write terminator
                        unsigned replyLen = 0;
                        ssock.write(&replyLen, sizeof(replyLen));
                    }
                }
                catch (IException *E)
                {
                    IERRLOG(E);
                    E->Release();
                }
                catch (...)
                {
                    DBGLOG("Unexpected exception in CThorDebugListener");
                }
            }
        }
    };
    Owned<CThorDebugListener> debugListener;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CJobManager(ILogMsgHandler *logHandler);
    ~CJobManager();

    bool doit(IConstWorkUnit *workunit, const char *graphName, const SocketEndpoint &agentep);
    void reply(IConstWorkUnit *workunit, const char *wuid, IException *e, const SocketEndpoint &agentep, bool allDone);

    void run();
    bool execute(IConstWorkUnit *workunit, const char *wuid, const char *graphName, const SocketEndpoint &agentep);
    IException *queryExitException() { return exitException; }
    void clearExitException() { exitException.clear(); }

// IExceptionHandler
    bool fireException(IException *e);

// IJobManager
    virtual void stop();
    virtual void replyException(CJobMaster &job, IException *e);
    virtual void setWuid(const char *wuid, const char *cluster=NULL);
    virtual IDeMonServer *queryDeMonServer() { return demonServer; }
    virtual void fatal(IException *e);
    virtual void addCachedSo(const char *name);
    virtual void updateWorkUnitLog(IWorkUnit &workunit);
    virtual void setExceptionCtx(IThorException *e);
    virtual void deltaPostmortemInProgress(int v);
};

// CJobManager impl.

CJobManager::CJobManager(ILogMsgHandler *_logHandler) : logHandler(_logHandler)
{
    stopped = handlingConversation = false;
    addThreadExceptionHandler(this);
    if (globals->getPropBool("@watchdogEnabled"))
        demonServer.setown(createDeMonServer());
    else
        globals->setPropBool("@watchdogProgressEnabled", false);
    activeTasks = 0;
    setJobManager(this);
    debugListener.setown(new CThorDebugListener(*this));

    StringBuffer soPath;
    globals->getProp("@query_so_dir", soPath);
    StringBuffer soPattern("*.");
#ifdef _WIN32
    soPattern.append("dll");
#else
    soPattern.append("so");
#endif
    querySoCache.init(soPath.str(), DEFAULT_QUERYSO_LIMIT, soPattern);
    thorRate = getThorRate(queryNodeClusterWidth());
    thorName = globals->queryProp("@name");
    if (!thorName) thorName = "thor";
}

CJobManager::~CJobManager()
{
    setJobManager(NULL);
    removeThreadExceptionHandler(this);
}

void CJobManager::stop()
{
    if (!stopped)
    {
        DBGLOG("Stopping jobManager");
        stopped = true;
        if (jobq)
        {
            jobq->cancelWaitStatsChange();
            jobq->cancelAcceptConversation();
        }
        if (conversation && !handlingConversation)
            conversation->cancel();
    }
}

void CJobManager::fatal(IException *e)
{
    try
    {
        // crude mechanism to wait if post-mortem capture if it is in progress (it shouldn't be, but want to know if it is and wait a short while)
        CTimeMon tm(30000);
        if (postMortemCaptureInProgress)
        {
            PROGLOG("Waiting for post-mortem capture to complete");
            while (true)
            {
                MilliSleep(5000);
                if (0 == postMortemCaptureInProgress)
                    break;
                else if (tm.timedout())
                {
                    PROGLOG("Timed out waiting for post-mortem capture to complete. Continuing to terminate");
                    break;
                }
            }
        }

        IArrayOf<CJobMaster> jobList;
        {
            CriticalBlock b(jobCrit);
            ForEachItemIn(j, jobs)
                jobList.append(*LINK(&jobs.item(j)));
        }
        ForEachItemIn(j, jobList)
            replyException(jobList.item(j), e);
        jobList.kill();

        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryDeMonServer()->endGraphs();
        setWuid(NULL); // deactivate workunit status (Shouldn't this logic belong outside of thor?)
    }
    catch (IException *e)
    {
        IERRLOG(e);
        e->Release();
    }
    catch (...)
    {
        IERRLOG("Unknown exception in CJobManager::fatal");
    }
    auditThorSystemEvent("Terminate", {"exception"});

    queryLogMsgManager()->flushQueue(10*1000);

    // exit code -1 in bare-metal to recycle
    // exit code 0 to prevent unwanted pod Error status
    _exit(isContainerized() ? 0 : -1);
}

void CJobManager::updateWorkUnitLog(IWorkUnit &workunit)
{
#ifndef _CONTAINERIZED
    StringBuffer log, logUrl, slaveLogPattern;
    logHandler->getLogName(log);
    createUNCFilename(log, logUrl, false);
    slaveLogPattern.set(THORSLAVELOGSEARCHSTR).append(SLAVEIDSTR);
    const char *ptr = strstr(log, THORMASTERLOGSEARCHSTR);
    dbgassertex(ptr);
    slaveLogPattern.append(ptr + strlen(THORMASTERLOGSEARCHSTR) - 1); //Keep the '.' at the end of the THORMASTERLOGSEARCHSTR.
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(workunit.queryClusterName());
    unsigned numberOfSlaves = clusterInfo->getNumberOfSlaveLogs();
    workunit.addProcess("Thor", globals->queryProp("@name"), 0, numberOfSlaves, slaveLogPattern, false, logUrl.str());
#endif
}

void CJobManager::setExceptionCtx(IThorException *e)
{
    if (nullptr != e->queryGraphName()) // already set
        return;
    Owned<CJobMaster> job = getCurrentJob();
    if (!job)
        return;
    e->setGraphInfo(job->queryGraphName(), job->queryCurrentSubGraphId());
}

void CJobManager::deltaPostmortemInProgress(int v)
{
    postMortemCaptureInProgress += v;
    assertex(postMortemCaptureInProgress >= 0);
}


#define IDLE_RESTART_PERIOD (8*60) // 8 hours
class CIdleShutdown : public CSimpleInterface, implements IThreaded
{
    unsigned timeout;
    Semaphore sem;
    CThreaded threaded;
public:
    CIdleShutdown(unsigned _timeout) : timeout(_timeout*60000), threaded("CIdleShutdown") { threaded.init(this, false); }
    ~CIdleShutdown() { stop(); threaded.join(); }
    virtual void threadmain() override
    {
        if (!sem.wait(timeout)) // feeling neglected, restarting..
        {
            Owned<IThorException> te = MakeThorException(TE_IdleRestart, "Thor has been idle for %d minutes, restarting", timeout/60000);
            abortThor(te, TEC_Idle, false);
        }
    }
    void stop() { sem.signal(); }
};

bool CJobManager::fireException(IException *e)
{
    IArrayOf<CJobMaster> jobList;
    {
        CriticalBlock b(jobCrit);
        ForEachItemIn(j, jobs)
            jobList.append(*LINK(&jobs.item(j)));
    }
    ForEachItemIn(j, jobList)
        jobList.item(j).fireException(e);
    jobList.kill();
    return true;
}

bool CJobManager::execute(IConstWorkUnit *workunit, const char *wuid, const char *graphName, const SocketEndpoint &agentep)
{
    Owned<IException> exception;
    TraceFlags startTraceFlags = queryTraceFlags();
    COnScopeExit restoreTraceFlags([&](){ updateTraceFlags(startTraceFlags, true); });

    try
    {
        unsigned defaultConfigLogLevel = getComponentConfigSP()->getPropInt("logging/@detail", DefaultDetail);
        unsigned maxLogDetail = workunit->getDebugValueInt("maxlogdetail", defaultConfigLogLevel);
        ILogMsgFilter *existingLogHandler = queryLogMsgManager()->queryMonitorFilter(logHandler);
        dbgassertex(existingLogHandler);
        if (existingLogHandler->queryMaxDetail() != maxLogDetail)
            verifyex(queryLogMsgManager()->changeMonitorFilterOwn(logHandler, getCategoryLogMsgFilter(existingLogHandler->queryAudienceMask(), existingLogHandler->queryClassMask(), maxLogDetail)));
        updateTraceFlags(wuLoadTraceFlags(workunit, thorTraceOptions, startTraceFlags), true);

        if (!workunit) // check workunit is available and ready to run.
            throw MakeStringException(0, "Could not locate workunit %s", wuid);
        if (workunit->getCodeVersion() == 0)
            throw makeStringException(0, "Attempting to execute a workunit that hasn't been compiled");
        if ((workunit->getCodeVersion() > ACTIVITY_INTERFACE_VERSION) || (workunit->getCodeVersion() < MIN_ACTIVITY_INTERFACE_VERSION))
            throw MakeStringException(0, "Workunit was compiled for eclagent interface version %d, this thor (%s) requires version %d..%d", workunit->getCodeVersion(), globals->queryProp("@name"), MIN_ACTIVITY_INTERFACE_VERSION, ACTIVITY_INTERFACE_VERSION);
        if (workunit->getCodeVersion() == 652)
        {
            // Any workunit compiled using eclcc 7.12.0-7.12.18 is not compatible
            StringBuffer buildVersion, eclVersion;
            workunit->getBuildVersion(StringBufferAdaptor(buildVersion), StringBufferAdaptor(eclVersion));
            const char *version = strstr(buildVersion, "7.12.");
            if (version)
            {
                const char *point = version + strlen("7.12.");
                unsigned pointVer = atoi(point);
                if (pointVer <= 18)
                    throw MakeStringException(0, "Workunit was compiled by eclcc version %s which is not compatible with this thor (%s)", buildVersion.str(), globals->queryProp("@name"));
            }
        }

        if (debugListener)
        {
            WorkunitUpdate wu(&workunit->lock());
            StringBuffer sb;
            queryHostIP().getHostText(sb);
            wu->setDebugAgentListenerIP(sb); //tells debugger what IP to write commands to
            wu->setDebugAgentListenerPort(debugListener->getPort());
        }
        workunitGraphCacheEnabled = getExpertOptBool("workunitGraphCacheEnabled", workunitGraphCacheEnabled);
        return doit(workunit, graphName, agentep);
    }
    catch (IException *e)
    {
        IThorException *te = QUERYINTERFACE(e, IThorException);
        if (te && tea_shutdown==te->queryAction())
            stopped = true;
        exception.setown(e);
    }
    catch (CATCHALL)
    {
        exception.setown(makeStringException(0, "Unknown exception"));
    }
    reply(workunit, wuid, exception, agentep, false);
    return false;
}

void CJobManager::run()
{
    DBGLOG("Listening for graph");

    setWuid(NULL);
#ifndef _CONTAINERIZED
    SCMStringBuffer _queueNames;
    getThorQueueNames(_queueNames, thorName);
    queueName.set(_queueNames.str());
#endif

    jobq.setown(createJobQueue(queueName.get()));

    PROGLOG("verifying mp connection to all slaves");
    Owned<IMPServer> mpServer = getMPServer();
    Owned<ICommunicator> comm = mpServer->createCommunicator(&queryClusterGroup());
    if (!comm->verifyAll(false, 1000*60*30, 1000*60))
        throwStringExceptionV(0, "Failed to connect to all slaves");
    else
        PROGLOG("verified mp connection to all slaves");

    class CThorListener : public CSimpleInterface, implements IThreaded
    {
        CThreaded threaded;
        mptag_t mptag;
        bool stopped = false;
    public:
        CThorListener(mptag_t _mptag) : threaded("CDaliConnectionValidator"), mptag(_mptag)
        {
            threaded.init(this, false);
        }
        ~CThorListener() { stop(); threaded.join(); }
        void stop()
        {
            stopped = true;
            queryWorldCommunicator().cancel(NULL, mptag);
        }
        virtual void threadmain() override
        {
            for (;;)
            {
                CMessageBuffer msg;
                if (!queryWorldCommunicator().recv(msg, NULL, mptag))
                    break;

                StringAttr cmd;
                msg.read(cmd);
                if (0 == stricmp("stop", cmd))
                {
                    bool stopCurrentJob;
                    msg.read(stopCurrentJob);
                    abortThor(NULL, TEC_Clean, stopCurrentJob);
                    break;
                }
                else
                    IWARNLOG("Unknown cmd = %s", cmd.get());
            }
        }
    } stopThorListener(MPTAG_THOR);
    StringBuffer exclusiveLockName;
    Owned<IDaliMutex> exclLockDaliMutex;
    if (globals->getProp("@multiThorExclusionLockName",exclusiveLockName))
    {
        if (exclusiveLockName.length())
        {
            PROGLOG("Multi-Thor exclusive lock defined: %s", exclusiveLockName.str());
            exclLockDaliMutex.setown(createDaliMutex(exclusiveLockName.str()));
        }
    }
    bool jobQConnected = false;
    while (!stopped)
    {
        handlingConversation = false;
        conversation.clear();
        SocketEndpoint masterEp(getMasterPortBase());
        StringBuffer url;
        PROGLOG("ThorLCR(%s) available, waiting on queue %s",masterEp.getEndpointHostText(url).str(),queueName.get());

        struct CLock
        {
            IDaliMutex *lock;
            StringAttr name;
            CLock() : lock(NULL) { }
            ~CLock()
            {
                clear();
            }
            void set(IDaliMutex *_lock, const char *_name)
            {
                lock = _lock;
                name.set(_name);
                PROGLOG("Took exclusive lock %s", name.get());
            }
            void clear()
            {
                if (lock)
                {
                    IDaliMutex *_lock = lock;
                    lock = NULL;
                    _lock->leave();
                    PROGLOG("Cleared exclusive lock: %s", name.get());
                }
            }
        } daliLock;

        Owned<IJobQueueItem> item;
        {
            CIdleShutdown idleshutdown(globals->getPropInt("@idleRestartPeriod", IDLE_RESTART_PERIOD));
            CCycleTimer waitTimer;
            if (exclLockDaliMutex.get())
            {
                for (;;)
                {
                    while (!stopped && !jobq->ordinality()) // this is avoid tight loop when nothing on q.
                    {
                        if (jobQConnected)
                        {
                            jobq->disconnect();
                            jobQConnected = false;
                        }
                        jobq->waitStatsChange(1000);
                    }
                    if (stopped)
                        break;
                    unsigned connected, waiting, enqueued;

                    if (exclLockDaliMutex->enter(5000))
                    {
                        daliLock.set(exclLockDaliMutex, exclusiveLockName);
                        if (jobq->ordinality())
                        {
                            if (!jobQConnected)
                            {
                                jobq->connect(true);
                                jobQConnected = true;
                            }
                            // NB: this is expecting to get an item without delay, timeout JIC.
                            unsigned t = msTick();
                            Owned<IJobQueueItem> _item = jobq->dequeue(30*1000);
                            unsigned e = msTick() - t;
                            StringBuffer msg;
                            if (_item.get())
                                msg.append("Jobqueue item retrieved");
                            else
                                msg.append("Nothing found on jobq::dequeue");
                            if (e>=5000)
                                msg.append(" - acceptConversation took ").append(e/1000).append(" secs");
                            PROGLOG("%s", msg.str());
                            if (_item.get())
                            {
                                if (_item->isValidSession())
                                {
                                    SocketEndpoint ep = _item->queryEndpoint();
                                    ep.port = _item->getPort();
                                    Owned<IConversation> acceptconv;
#if defined(_USE_OPENSSL)
                                    if (queryMtls())
                                        acceptconv.setown(createSingletonSecureSocketConnection(ep.port,&ep));
                                    else
#endif
                                        acceptconv.setown(createSingletonSocketConnection(ep.port,&ep));
                                    if (acceptconv->connect(60*1000)) // shouldn't need that long
                                    {
                                        acceptconv->set_keep_alive(true);
                                        item.setown(_item.getClear());
                                        conversation.setown(acceptconv.getClear());
                                    }
                                    break;
                                }
                            }
                        }
                        daliLock.clear();
                    }
                    else
                    {
                        jobq->getStats(connected, waiting, enqueued);
                        if (enqueued)
                            PROGLOG("Exclusive lock %s in use. Queue state (connected=%d, waiting=%d, enqueued=%d)", exclusiveLockName.str(), connected ,waiting, enqueued);
                    }
                }
            }
            else
            {
                if (!jobQConnected)
                {
                    jobq->connect(true);
                    jobQConnected = true;
                }
                IJobQueueItem *_item;
                conversation.setown(jobq->acceptConversation(_item,30*1000));    // 30s priority transition delay
                item.setown(_item);
            }

            __uint64 waitTimeNs = waitTimer.elapsedNs();
            double expenseWait = calcCostNs(thorRate, waitTimeNs);
            cost_type costWait = money2cost_type(expenseWait);

            if (item)
                recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorName } }, { StNumAccepts, StNumWaits, StTimeWaitSuccess, StCostWait }, { 1, 1, waitTimeNs, costWait });
            else
                recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorName } }, { StNumWaits, StTimeWaitFailure, StCostWait }, { 1, waitTimeNs, costWait });
        }
        if (!conversation.get()||!item.get())
        {
            if (!stopped)
                setExitCode(0);
            DBGLOG("acceptConversation aborted - terminating");
            break;
        }
        StringAttr graphName, wuid;
        const char *wuidGraph = item->queryWUID(); // actually <wfid>/<wuid>/<graphName>
        StringArray sArray;
        sArray.appendList(wuidGraph, "/");
        assertex(3 == sArray.ordinality());
        unsigned wfid = atoi(sArray.item(0));
        wuid.set(sArray.item(1));
        graphName.set(sArray.item(2));

        handlingConversation = true;
        SocketEndpoint agentep;
        try
        {
            MemoryBuffer msg;
            masterEp.serialize(msg);  // only used for tracing
            if (!conversation->send(msg))
            {
                IWARNLOG("send conversation failed");
                continue;
            }
            if (!conversation->recv(msg.clear(),60*1000))
            {
                IWARNLOG("recv conversation failed");
                continue;
            }
            agentep.deserialize(msg);
        }
        catch (IException *e)
        {
            FLLOG(MCoperatorWarning, e, "CJobManager::run");
            continue;
        }
        Owned<IWorkUnitFactory> factory;
        Owned<IConstWorkUnit> workunit;
        Owned<IException> exception;
        bool allDone = false;
        try
        {
            factory.setown(getWorkUnitFactory());
            workunit.setown(factory->openWorkUnit(wuid));

            {
                Owned<IWorkUnit> w = &workunit->lock();
                addTimeStamp(w, wfid, graphName, StWhenDequeued);
            }

            allDone = execute(workunit, wuid, graphName, agentep);
            daliLock.clear();
            reply(workunit, wuid, NULL, agentep, allDone);
        }
        catch (IException *e)
        {
            IThorException *te = QUERYINTERFACE(e, IThorException);
            if (te && tea_shutdown==te->queryAction())
                stopped = true;
            exception.setown(e);
        }
        catch (CATCHALL)
        {
            exception.setown(makeStringException(0, "Unknown exception"));
        }
        reply(workunit, wuid, exception, agentep, false);

        // reset for next job
        setProcessAborted(false);
    }
    jobq.clear();
}

bool CJobManager::doit(IConstWorkUnit *workunit, const char *graphName, const SocketEndpoint &agentep)
{
    StringBuffer s;
    StringAttr wuid(workunit->queryWuid());
    StringAttr user(workunit->queryUser());

    JobNameScope activeJobName(wuid);
    CCycleTimer graphTimer;
    DBGLOG("Processing wuid=%s, graph=%s from agent: %s", wuid.str(), graphName, agentep.getEndpointHostText(s).str());
    auditThorJobEvent("Start", wuid, graphName, user);

    Owned<IException> e;
    bool allDone = false;
    try
    {
        allDone = executeGraph(*workunit, graphName, agentep);
    }
    catch (IException *_e) { e.setown(_e); }
    auditThorJobEvent("Stop", wuid, graphName, user);

    __uint64 executeTimeNs = graphTimer.elapsedNs();
    double expenseExecute = calcCostNs(thorRate, executeTimeNs);
    cost_type costExecute = money2cost_type(expenseExecute);
    const char * username = user.str();
    WUState state = workunit->getState();
    bool aborted = (state == WUStateAborted) || (state == WUStateAborting);
    bool failed = (state == WUStateFailed) || e || !allDone;
    if (aborted)
        recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorName }, { "user", username } }, { StNumAborts, StTimeLocalExecute, StCostAbort }, { 1, executeTimeNs, costExecute });
    else if (failed)
        recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorName }, { "user", username } }, { StNumFailures, StTimeLocalExecute, StCostExecute }, { 1, executeTimeNs, costExecute });
    else
        recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorName }, { "user", username } }, { StTimeLocalExecute, StCostExecute }, { executeTimeNs, costExecute });
        //TBD: Ideally this time would be spread over all the timeslots when this graph was executing

    if (e.get()) throw e.getClear();
    return allDone;
}


void CJobManager::setWuid(const char *wuid, const char *cluster)
{
    currentWuid.set(wuid);
    try
    {
        if (wuid && *wuid)
        {
            queryServerStatus().queryProperties()->setProp("WorkUnit", wuid);
            queryServerStatus().queryProperties()->setProp("Cluster", cluster);
        }
        else
        {
            queryServerStatus().queryProperties()->removeProp("WorkUnit");
            queryServerStatus().queryProperties()->removeProp("Cluster");
        }
        queryServerStatus().commitProperties();
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), e, "WARNING: Failed to set wuid in SDS:");
        e->Release();
    }
    catch (CATCHALL)
    {
        FLLOG(MCerror, "WARNING: Failed to set wuid in SDS: Unknown error");
    }
}

void CJobManager::replyException(CJobMaster &job, IException *e)
{
    reply(&job.queryWorkUnit(), job.queryWorkUnit().queryWuid(), e, job.queryAgentEp(), false);
}

void CJobManager::reply(IConstWorkUnit *workunit, const char *wuid, IException *e, const SocketEndpoint &agentep, bool allDone)
{
    CriticalBlock b(replyCrit);
#ifdef _CONTAINERIZED
    // JCSMORE ignore pause/resume cases for now.
    if (e)
    {
        if (!exitException)
        {
            exitException.set(e);
            relayWuidException(workunit, e);
        }
        return;
    }
#else
    workunit->forceReload();
    if (!conversation)
        return;
    StringBuffer s;
    if (e) {
        s.append("Posting exception: ");
        e->errorMessage(s);
    }
    else
        s.append("Posting OK");
    s.append(" to agent ");
    agentep.getEndpointHostText(s);
    s.append(" for workunit(").append(wuid).append(")");
    PROGLOG("%s", s.str());
    MemoryBuffer replyMb;
    workunit->forceReload();
    if (!allDone && (WUActionPause == workunit->getAction() || WUActionPauseNow == workunit->getAction()))
    {
        replyMb.append((unsigned)DAMP_THOR_REPLY_PAUSED);
        if (e)
        {
            // likely if WUActionPauseNow, shouldn't happen if WUActionPause
            IERRLOG(e, "Exception at time of pause");
            replyMb.append(true);
            serializeException(e, replyMb);
        }
        else
            replyMb.append(false);
    }
    else if (e)
    {
        IThorException *te = QUERYINTERFACE(e, IThorException);
        if (te)
        {
            switch (te->errorCode())
            {
            case TE_CostExceeded:
            case TE_WorkUnitAborting:
                replyMb.append((unsigned)DAMP_THOR_REPLY_ABORT);
                break;
            default:
                replyMb.append((unsigned)DAMP_THOR_REPLY_ERROR);
                break;
            }
        }
        else
            replyMb.append((unsigned)DAMP_THOR_REPLY_ERROR);
        serializeException(e, replyMb);
    }
    else
        replyMb.append((unsigned)DAMP_THOR_REPLY_GOOD);
    if (!conversation->send(replyMb)) {
        s.clear();
        IERRLOG("Failed to reply to agent %s",agentep.getEndpointHostText(s).str());
    }
    conversation.clear();
    handlingConversation = false;

    //GH->JCS Should this be using getEnvironmentFactory()->openEnvironment()?
    Owned<IRemoteConnection> conn = querySDS().connect("/Environment", myProcessSession(), RTM_LOCK_READ, MEDIUMTIMEOUT);
    if (checkThorNodeSwap(globals->queryProp("@name"),e?wuid:NULL,(unsigned)-1))
        abortThor(e, TEC_Swap, false);
#endif
}

bool CJobManager::executeGraph(IConstWorkUnit &workunit, const char *graphName, const SocketEndpoint &agentEp)
{
    timestamp_type startTs = getTimeStampNowValue();
    {
        Owned<IWorkUnit> wu = &workunit.lock();
        wu->setTracingValue("ThorBuild", hpccBuildInfo.buildTag);
#ifndef _CONTAINERIZED
        updateWorkUnitLog(*wu);
#endif
    }
    workunit.forceReload();
    StringAttr wuid(workunit.queryWuid());
    cycle_t startCycles = get_cycles_now();

    Owned<IConstWUQuery> query = workunit.getQuery();
    SCMStringBuffer soName;
    query->getQueryDllName(soName);
    unsigned version = query->getQueryDllCrc();
    query.clear();

    bool sendSo = false;
    Owned<ILoadedDllEntry> querySo;
    StringBuffer soPath;
    if (!getExpertOptBool("saveQueryDlls"))
    {
        DBGLOG("Loading query name: %s", soName.str());
        querySo.setown(queryDllServer().loadDll(soName.str(), DllLocationLocal));
        soPath.append(querySo->queryName());
    }
    else
    {
        globals->getProp("@query_so_dir", soPath);
        StringBuffer compoundPath;
        compoundPath.append(soPath.str());
        soPath.append(soName.str());
        getCompoundQueryName(compoundPath, soName.str(), version);
        if (querySoCache.isAvailable(compoundPath.str()))
            DBGLOG("Using existing local dll: %s", compoundPath.str()); // It is assumed if present here then _still_ present on slaves from previous send.
        else
        {
            MemoryBuffer file;
            queryDllServer().getDll(soName.str(), file);
            DBGLOG("Saving dll: %s", compoundPath.str());
            OwnedIFile out = createIFile(compoundPath.str());
            try
            {
                out->setCreateFlags(S_IRWXU);
                OwnedIFileIO io = out->open(IFOcreate);
                io->write(0, file.length(), file.toByteArray());
                io->close();
                io.clear();
            }
            catch (IException *e)
            {
                FLLOG(MCexception(e), e, "Failed to write query dll - ignoring!");
                e->Release();
            }
            sendSo = getExpertOptBool("dllsToSlaves", true);
        }
        querySo.setown(createDllEntry(compoundPath.str(), false, NULL, false));
        soPath.swapWith(compoundPath);
    }

    SCMStringBuffer eclstr;
    StringAttr user(workunit.queryUser());

    PROGLOG("Started wuid=%s, user=%s, graph=%s, query=%s", wuid.str(), user.str(), graphName, soPath.str());

    Owned<CJobMaster> job = createThorGraph(graphName, workunit, querySo, sendSo, agentEp);
    unsigned wfid = job->getWfid();
    StringBuffer graphScope;
    graphScope.append(WorkflowScopePrefix).append(wfid).append(":").append(graphName);
    DBGLOG("Graph %s created", graphName);
    DBGLOG("Running graph=%s", job->queryGraphName());
    addJob(*job);
    bool allDone = false;
    Owned<IException> exception;
    Owned<IFatalHandler> fatalHdlr;
    try
    {
        struct CounterBlock
        {
            std::atomic<unsigned> &counter;
            CounterBlock(std::atomic<unsigned> &_counter) : counter(_counter) { ++counter; }
            ~CounterBlock() { --counter; }
        } cBlock(activeTasks);

        if (isContainerized())
        {
            podInfo.setContext(wfid, wuid, graphName);
            podInfo.ensureCollected(); // will collect pod info the 1st run only, since they remain the same for subsequent jobs this Thor runs
        }

        {
            Owned<IWorkUnit> wu = &workunit.lock();
            wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTgraph, graphScope, StWhenStarted, NULL, startTs, 1, 0, StatsMergeAppend);
            //Could use addTimeStamp(wu, SSTgraph, graphName, StWhenStarted, wfid) if start time could be this point
            wu->setState(WUStateRunning);
            wu->setEngineSession(myProcessSession());
            VStringBuffer version("%d.%d", THOR_VERSION_MAJOR, THOR_VERSION_MINOR);
            wu->setDebugValue("ThorVersion", version.str(), true);

            if (isContainerized() && podInfo.hasStdDev())
                podInfo.report(wu);
        }
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryDeMonServer()->loadExistingAggregates(workunit);

        setWuid(workunit.queryWuid(), workunit.queryClusterName());

        allDone = job->go();

        Owned<IWorkUnit> wu = &workunit.lock();
        unsigned __int64 graphTimeNs = cycle_to_nanosec(get_cycles_now()-startCycles);
        StringBuffer graphTimeStr;
        formatGraphTimerLabel(graphTimeStr, graphName);

        updateWorkunitStat(wu, SSTgraph, graphName, StTimeElapsed, graphTimeStr, graphTimeNs, wfid);

        addTimeStamp(wu, SSTgraph, graphName, StWhenFinished, wfid);
        cost_type cost = money2cost_type(calculateThorCost(nanoToMilli(graphTimeNs), queryNodeClusterWidth()));
        if (cost)
            wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTgraph, graphScope, StCostExecute, NULL, cost, 1, 0, StatsMergeReplace);
        if (globals->getPropBool("@watchdogProgressEnabled"))
            queryDeMonServer()->updateAggregates(wu);
        // clear engine session, otherwise agent may consider a failure beyond this point for an unrelated job caused by this instance
        wu->setEngineSession(-1);

        removeJob(*job);
    }
    catch (IException *e)
    {
        exception.setown(ThorWrapException(e, "CJobManager::executeGraph"));
        e->Release();
    }
    job->endJob();
    removeJob(*job);
    if (exception)
    {
        setWuid(nullptr);
        throw exception.getClear();
    }
    fatalHdlr.setown(job->clearFatalHandler());
    job.clear();
    PROGLOG("Finished wuid=%s, graph=%s", wuid.str(), graphName);

    fatalHdlr->clear();

    runWorkunitAnalyser(workunit, getComponentConfigSP(), graphName, false, calculateThorCostPerHour(queryNodeClusterWidth()));
    setWuid(NULL);
    return allDone;
}

void CJobManager::addCachedSo(const char *name)
{
    querySoCache.add(name);
}

static int exitCode = -1;
void setExitCode(int code) { exitCode = code; }
int queryExitCode() { return exitCode; }

static Owned<IJobQueue> thorQueue; // used if multiJobLinger is in use, and here so abortThor can cancel
static unsigned aborting = 99;
void abortThor(IException *e, unsigned errCode, bool abortCurrentJob)
{
    if (-1 == queryExitCode()) setExitCode(errCode);
    Owned<CJobManager> jM = ((CJobManager *)getJobManager());
    Owned<IThorException> te;
    if (0 == aborting)
    {
        aborting = 1;
        if (errCode != TEC_Clean)
        {
            if (e)
                te.setown(MakeThorException(e));
            else
                te.setown(MakeThorException(TE_AbortException, "THOR ABORT"));
            Owned<IJobManager> mgr = getJobManager();
            if (mgr)
                mgr->setExceptionCtx(te);
            e = te;
            DBGLOG(e, "abortThor");
        }
        DBGLOG("abortThor called");
        if (jM)
            jM->stop();
        if (thorQueue)
        {
            Owned<IJobQueue> queue = thorQueue.getLink();
            queue->cancelAcceptConversation();
        }
    }
    if (2 > aborting && abortCurrentJob)
    {
        aborting = 2;
        DBGLOG("aborting any current active job");
        if (jM)
        {
            if (!e)
            {
                te.setown(MakeThorException(TE_AbortException, "THOR ABORT"));
                e = te;
            }
            jM->fireException(e);
        }
        if (errCode == TEC_Clean)
        {
            DBGLOG("Removing sentinel upon normal shutdown");
            Owned<IFile> sentinelFile = createSentinelTarget();
            removeSentinelFile(sentinelFile);
        }
    }
}

#define DEFAULT_VERIFYDALI_POLL 5*60 // secs
class CDaliConnectionValidator : public CSimpleInterface, implements IThreaded
{
    bool stopped;
    unsigned pollDelay;
    Semaphore poll;
    CThreaded threaded;
public:
    CDaliConnectionValidator(unsigned _pollDelay) : threaded("CDaliConnectionValidator") { pollDelay = _pollDelay*1000; stopped = false; threaded.init(this, false); }
    ~CDaliConnectionValidator() { stop(); threaded.join(); }
    virtual void threadmain() override
    {
        for (;;)
        {
            poll.wait(pollDelay);
            if (stopped) break;
            if (!verifyCovenConnection(pollDelay)) // use poll delay time for verify connection timeout
            {
                Owned<IThorException> te = MakeThorOperatorException(TE_AbortException, "Detected lost connectivity with dali server, aborting thor");
                abortThor(te, TEC_DaliDown);
                break;
            }
        }
    }
    void stop()
    {
        stopped = true;
        poll.signal();
    }
};

static CSDSServerStatus *serverStatus = NULL;
CSDSServerStatus &queryServerStatus() { return *serverStatus; }

CSDSServerStatus &openThorServerStatus()
{
    assertex(!serverStatus);
    serverStatus = new CSDSServerStatus("ThorMaster");
    return *serverStatus;
}
void closeThorServerStatus()
{
    if (serverStatus)
    {
        delete serverStatus;
        serverStatus = NULL;
    }
}

/*
 * Waits on recv for another wuid/graph to run.
 * Return values:
 * -2 = reply to client failed
 * -1 = recv failed/timedout
 *  0 = unrecognised format, or wuid mismatch
 *  1 = success. new graph/wuid received.
 */
static int recvNextGraph(unsigned timeoutMs, const char *wuid, StringBuffer &retWfid, StringBuffer &retWuid, StringBuffer &retGraphName, unsigned __int64 priority)
{
    StringBuffer next;
    CMessageBuffer msg;
    if (thorQueue)
    {
        Owned<IJobQueueItem> item = thorQueue->dequeuePriority(priority, timeoutMs);
        if (!item)
            return -1;
        next.set(item->queryWUID());
    }
    else
    {
        if (!queryWorldCommunicator().recv(msg, NULL, MPTAG_THOR, nullptr, timeoutMs))
            return -1;
        msg.read(next);
    }

    // validate
    StringArray sArray;
    sArray.appendList(next, "/");
    if (3 == sArray.ordinality())
    {
        if (!thorQueue)
        {
            if (wuid && !streq(sArray.item(1), wuid))
                return 0; // mismatch/ignore
            msg.clear().append(true);
            if (!queryWorldCommunicator().reply(msg, 60*1000)) // should be quick!
                return -2; // failed to reply to client
        }
    }
    else
    {
        IWARNLOG("Unrecognised job format received: %s", next.str());
        return 0; // unrecognised format, ignore
    }
    retWfid.set(sArray.item(0));
    retWuid.set(sArray.item(1));
    retGraphName.set(sArray.item(2));
    return 1; // success
}


static std::vector<CConnectedWorkerDetail> connectedWorkers;
void publishPodNames(IWorkUnit *workunit, const char *graphName, const std::vector<CConnectedWorkerDetail> *_connectedWorkers)
{
    // skip if Thor manager already published (implying worker pods already published too)
    // NB: this will always associate the new 'graphName' with the manager pod meta info.
    if (workunit->setContainerizedProcessInfo("Thor", globals->queryProp("@name"), k8s::queryMyPodName(), k8s::queryMyContainerName(), graphName, nullptr))
    {
        if (_connectedWorkers)
        {
            assertex(connectedWorkers.empty());
            connectedWorkers = *_connectedWorkers;
        }
        else
        {
            assertex(!connectedWorkers.empty());
        }
        for (unsigned workerNum=0; workerNum<connectedWorkers.size(); workerNum++)
        {
            const CConnectedWorkerDetail &worker = connectedWorkers[workerNum];
            workunit->setContainerizedProcessInfo("ThorWorker", globals->queryProp("@name"), worker.podName.c_str(), worker.containerName.c_str(), nullptr, std::to_string(workerNum+1).c_str());
        }
    }
}

static void auditThorSystemEventBuilder(std::string &msg, const char *eventName, std::initializer_list<const char*> args)
{
    msg += std::string(",Progress,Thor,") + eventName + "," + getComponentConfigSP()->queryProp("@name");
    for (auto arg : args)
        msg += "," + std::string(arg);
    if (isContainerized())
        msg += std::string(",") + k8s::queryMyPodName() + "," + k8s::queryMyContainerName();
    else
    {
        const char *nodeGroup = queryServerStatus().queryProperties()->queryProp("@nodeGroup");
        const char *queueName = queryServerStatus().queryProperties()->queryProp("@queue");
        msg += std::string(",") + nodeGroup + "," + queueName;
    }
}

void auditThorSystemEvent(const char *eventName)
{
    std::string msg;
    auditThorSystemEventBuilder(msg, eventName, {});
    LOG(MCauditInfo, "%s", msg.c_str());
}

void auditThorSystemEvent(const char *eventName, std::initializer_list<const char*> args)
{
    std::string msg;
    auditThorSystemEventBuilder(msg, eventName, args);
    LOG(MCauditInfo, "%s", msg.c_str());
}

void auditThorJobEvent(const char *eventName, const char *wuid, const char *graphName, const char *user)
{
    std::string msg;
    auditThorSystemEventBuilder(msg, eventName, { wuid, graphName, nullText(user) });
    LOG(MCauditInfo, "%s", msg.c_str());
}

void thorMain(ILogMsgHandler *logHandler, const char *wuid, const char *graphName)
{
    aborting = 0;
    unsigned multiThorMemoryThreshold = globals->getPropInt("@multiThorMemoryThreshold")*0x100000;
    try
    {
        Owned<CDaliConnectionValidator> daliConnectValidator = new CDaliConnectionValidator(globals->getPropInt("@verifyDaliConnectionInterval", DEFAULT_VERIFYDALI_POLL));
        Owned<ILargeMemLimitNotify> notify;
        if (multiThorMemoryThreshold)
        {
            StringBuffer ngname;
            if (!globals->getProp("@multiThorResourceGroup",ngname))
                globals->getProp("@nodeGroup",ngname);
            if (ngname.length())
            {
                notify.setown(createMultiThorResourceMutex(ngname.str(),serverStatus));
                setMultiThorMemoryNotify(multiThorMemoryThreshold,notify);
                PROGLOG("Multi-Thor resource limit for %s set to %" I64F "d",ngname.str(),(__int64)multiThorMemoryThreshold);
            }
            else
                multiThorMemoryThreshold = 0;
        }
        initFileManager();
        CThorResourceMaster masterResource;
        setIThorResource(masterResource);

        enableForceRemoteReads(); // forces file reads to be remote reads if they match environment setting 'forceRemotePattern' pattern.

        bool disableQueuePriority = getComponentConfigSP()->getPropBool("expert/@disableQueuePriority");
        Owned<CJobManager> jobManager = new CJobManager(logHandler);
        const char * thorname = globals->queryProp("@name");
        double thorRate = getThorRate(queryNodeClusterWidth());  // This doesn't feel quite right to call a global function to get the width, but ok for now.
        try
        {
            if (!isContainerized())
                jobManager->run();
            else
            {
                unsigned lingerPeriod = globals->getPropInt("@lingerPeriod", defaultThorLingerPeriod)*1000;
                dbgassertex(lingerPeriod>=1000); // NB: the schema or the default ensure the linger period is non-zero
                bool multiJobLinger = globals->getPropBool("@multiJobLinger", defaultThorMultiJobLinger);
                StringBuffer instance("thorinstance_"); // only used when multiJobLinger = false

                // NB: in k8s a Thor instance is explicitly started to run a specific wuid/graph
                // it will not listen/receive another job/graph until the 1st explicit request the job
                // started to is complete.

                if (multiJobLinger)
                {
                    StringBuffer queueNames;
                    getClusterThorQueueName(queueNames, globals->queryProp("@name"));
                    if (disableQueuePriority)
                    {
                        queueNames.append(",");
                        getClusterLingerThorQueueName(queueNames, globals->queryProp("@name"));
                    }
                    PROGLOG("multiJobLinger: on. Queue names: %s", queueNames.str());
                    thorQueue.setown(createJobQueue(queueNames));
                    thorQueue->connect(false);
                }

                StringBuffer currentWfId; // not filled/not used until recvNextGraph() is called.
                if (!multiJobLinger)
                {
                    // We avoid using getEndpointHostText here and get an IP instead, because the client pod communicating directly with this Thor manager,
                    // will not have the ability to resolve this pods hostname.
                    queryMyNode()->endpoint().getEndpointIpText(instance);
                }
                StringBuffer currentGraphName(graphName);
                StringBuffer currentWuid(wuid);

                CTimeMon lingerTimer(lingerPeriod); // NB: reset after it actually runs a job

                // baseImageVersion corresponds to the helm chart image version
                const char *baseImageVersion = getenv("baseImageVersion");
                // runtimeImageVersion will either match baseImageVersion, or have been set in the yaml to the runtime "platformVersion"
                const char *runtimeImageVersion = getenv("runtimeImageVersion");
                bool platformVersioningAvailable = true;
                if (isEmptyString(baseImageVersion) || isEmptyString(runtimeImageVersion))
                {
                    IWARNLOG("baseImageVersion or runtimeImageVersion missing from environment");
                    platformVersioningAvailable = false;
                }
                while (true)
                {
                    {
                        Owned<IWorkUnitFactory> factory;
                        Owned<IConstWorkUnit> workunit;
                        factory.setown(getWorkUnitFactory());
                        workunit.setown(factory->openWorkUnit(currentWuid));
                        if (!workunit)
                        {
                            IWARNLOG("Discarding unknown wuid: wuid=%s, graph=%s", currentWuid.str(), currentGraphName.str());
                            currentWuid.clear();
                        }
                        else
                        {
                            SessionId agentSessionID = workunit->getAgentSession();
                            if (agentSessionID <= 0)
                            {
                                IWARNLOG("Discarding job with invalid sessionID: wuid=%s, graph=%s (sessionID=%" I64F "d)", currentWuid.str(), currentGraphName.str(), agentSessionID);
                                currentWuid.clear();
                            }
                            else if (querySessionManager().sessionStopped(agentSessionID, 0))
                            {
                                IWARNLOG("Discarding agentless job: wuid=%s, graph=%s", currentWuid.str(), currentGraphName.str());
                                currentWuid.clear();
                            }
                            else
                            {
                                bool runJob = true;
                                if (platformVersioningAvailable)
                                {
                                    SCMStringBuffer jobVersion;
                                    workunit->getDebugValue("platformVersion", jobVersion);
                                    if (jobVersion.length())
                                    {
                                        if (!streq(jobVersion.str(), runtimeImageVersion))
                                            runJob = false;
                                    }
                                    else if (!streq(baseImageVersion, runtimeImageVersion))
                                    {
                                        // This is a custom runtime version, which did not specify a jobVersion,
                                        // meaning it intended to use the regular baseImageVersion.
                                        // Therefore we mismatch
                                        runJob = false;
                                    }
                                    if (!runJob) // version mismatch, delay and queue for other instance to take
                                    {
                                        assertex(thorQueue); // it should never be possible for a non-lingering Thor to have a mismatch

                                        // This Thor has picked up a job that has submitted with a different #option platformVersion.
                                        // requeue it, and wait a bit, so that it can either be picked up by an existing compatible Thor, or
                                        // an agent

                                        VStringBuffer job("%s/%s/%s", currentWfId.str(), currentWuid.str(), currentGraphName.str());
                                        Owned<IJobQueueItem> item = createJobQueueItem(job);
                                        item->setOwner(workunit->queryUser());
                                        item->setPriority(workunit->getPriorityValue());
                                        thorQueue->enqueue(item.getClear());
                                        currentWuid.clear();
                                        constexpr unsigned pauseSecs = 10;
                                        if (jobVersion.length())
                                            WARNLOG("Job=%s requeued due to version mismatch (this Thor version=%s, Job version=%s). Pausing for %u seconds", job.str(), runtimeImageVersion, jobVersion.str(), pauseSecs);
                                        else
                                            WARNLOG("Job=%s requeued due to version mismatch (this Thor version=%s, Job version not specified, uses original helm image version=%s). Pausing for %u seconds", job.str(), runtimeImageVersion, baseImageVersion, pauseSecs);
                                        MilliSleep(pauseSecs*1000);
                                    }
                                }
                                if (runJob)
                                {
                                    JobNameScope activeJobName(currentWuid.str());
                                    saveWuidToFile(currentWuid);
                                    VStringBuffer msg("Executing: wuid=%s, graph=%s", currentWuid.str(), currentGraphName.str());
                                    if (platformVersioningAvailable && !streq(baseImageVersion, runtimeImageVersion))
                                        msg.appendf(" (custom runtime version=%s)", runtimeImageVersion);
                                    PROGLOG("%s", msg.str());

                                    {
                                        Owned<IWorkUnit> wu = &workunit->lock();
                                        publishPodNames(wu, currentGraphName, nullptr);
                                    }
                                    SocketEndpoint dummyAgentEp;
                                    jobManager->execute(workunit, currentWuid, currentGraphName, dummyAgentEp);

                                    Owned<IWorkUnit> w = &workunit->lock();
                                    if (!multiJobLinger)
                                        w->setDebugValue(instance, "1", true);

                                    if (jobManager->queryExitException())
                                    {
                                        // NB: exitException has already been relayed.
                                        jobManager->clearExitException();
                                    }
                                    else
                                    {
                                        switch (w->getState())
                                        {
                                            case WUStateRunning:
                                                w->setState(WUStateWait);
                                                break;
                                            case WUStateAborting:
                                            case WUStateAborted:
                                            case WUStateFailed:
                                                break;
                                            default:
                                                w->setState(WUStateFailed);
                                                break;
                                        }
                                    }
                                    saveWuidToFile(""); // clear wuid file. Signifies that no wuid is running.
                                    lingerTimer.reset(lingerPeriod);
                                }
                            }
                        }
                    }

                    currentGraphName.clear();
                    unsigned lingerRemaining;
                    if (lingerTimer.timedout(&lingerRemaining))
                        break;
                    PROGLOG("Lingering time left: %.2f", ((float)lingerRemaining)/1000);

                    StringBuffer nextJob;
                    CCycleTimer waitTimer;
                    unsigned __int64 priority = disableQueuePriority ? 0 : getTimeStampNowValue();
                    do
                    {
                        StringBuffer wuid;
                        int ret = recvNextGraph(lingerRemaining, currentWuid.str(), currentWfId, wuid, currentGraphName, priority);
                        if (ret > 0)
                        {
                            currentWuid.set(wuid); // NB: will always be same if !multiJobLinger
                            break; // success
                        }
                        else if (ret < 0)
                            break; // timeout/abort
                        // else - reject/ignore duff message.
                    } while (!lingerTimer.timedout(&lingerRemaining));

                    __uint64 waitTimeNs = waitTimer.elapsedNs();
                    double expenseWait = calcCostNs(thorRate, waitTimeNs);
                    cost_type costWait = money2cost_type(expenseWait);

                    if (0 == currentGraphName.length())
                    {
                        recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorname } }, { StNumWaits, StTimeWaitFailure, StCostWait }, { 1, waitTimeNs, costWait });
                        if (!multiJobLinger)
                        {
                            // De-register the idle lingering entry.
                            Owned<IWorkUnitFactory> factory;
                            Owned<IConstWorkUnit> workunit;
                            factory.setown(getWorkUnitFactory());
                            workunit.setown(factory->openWorkUnit(currentWuid));
                            //Unlikely, but the workunit could have been deleted while we were lingering
                            //currentWuid can also be blank if the workunit this started for died before thor started
                            //processing the graph.  This test covers both (unlikely) situations.
                            if (workunit)
                            {
                                Owned<IWorkUnit> w = &workunit->lock();
                                w->setDebugValue(instance, "0", true);
                            }
                        }
                        break;
                    }

                    recordGlobalMetrics("Queue", { {"component", "thor" }, { "name", thorname } }, { StNumAccepts, StNumWaits, StTimeWaitSuccess, StCostWait }, { 1, 1, waitTimeNs, costWait });
                }
                thorQueue.clear();
            }
        }
        catch (IException *e)
        {
            IERRLOG(e);
            throw;
        }
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), e,"ThorMaster");
        e->Release();
    }
    if (multiThorMemoryThreshold)
        setMultiThorMemoryNotify(0,NULL);
}
