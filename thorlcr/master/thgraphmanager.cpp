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
#include "jfile.hpp"
#include "jmutex.hpp"
#include "jlog.hpp"
#include "jsecrets.hpp"
#include "rmtfile.hpp"

#include "portlist.h"
#include "wujobq.hpp"
#include "daclient.hpp"
#include "daqueue.hpp"

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
            threaded.start();
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

            if (strncmp(command,"print", 5) == 0)
            {
                const char *edgeId = queryXml->queryProp("@edgeId");
                if (!edgeId) throw MakeStringException(5300, "Debug command requires edgeId");

                ICommunicator &comm = job->queryNodeComm();
                CMessageBuffer mbuf;
                mbuf.append(DebugRequest);
                mbuf.append(job->queryKey());
                mptag_t replyTag = createReplyTag();
                serializeMPtag(mbuf, replyTag);
                mbuf.append(rawText);
                if (!comm.send(mbuf, RANK_ALL_OTHER, masterSlaveMpTag, MP_ASYNC_SEND))
                {
                    DBGLOG("Failed to send debug info to slave");
                    throwUnexpected();
                }
                unsigned nodes = job->queryNodes();
                response.appendf("<print graphId='%s' edgeId='%s'>", graphId, edgeId);
                while (nodes)
                {
                    rank_t sender;
                    mbuf.clear();
                    comm.recv(mbuf, RANK_ALL, replyTag, &sender, 10000);
                    while (mbuf.remaining())
                    {
                        StringAttr row;
                        mbuf.read(row);
                        response.append(row);
                    }
                    nodes--;
                }
                response.append("</print>");
            }
            else if (strncmp(command,"quit", 4) == 0)
            {
                LOG(MCwarning, thorJob, "ABORT detected from user during debug session");
                Owned<IException> e = MakeThorException(TE_WorkUnitAborting, "User signalled abort during debug session");
                job->fireException(e);
                response.appendf("<quit state='quit'/>");
            }
            else
                throw MakeStringException(5300, "Command not supported by Thor");

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
                    EXCLOG(E);
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
        LOG(MCdebugProgress, thorJob, "Stopping jobManager");
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
        EXCLOG(e, NULL);
        e->Release();
    }
    catch (...)
    {
        IERRLOG("Unknown exception in CJobManager::fatal");
    }
    LOG(MCauditInfo,",Progress,Thor,Terminate,%s,%s,%s,exception",
            queryServerStatus().queryProperties()->queryProp("@thorname"),
            queryServerStatus().queryProperties()->queryProp("@nodeGroup"),
            queryServerStatus().queryProperties()->queryProp("@queue"));

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



#define IDLE_RESTART_PERIOD (8*60) // 8 hours
class CIdleShutdown : public CSimpleInterface, implements IThreaded
{
    unsigned timeout;
    Semaphore sem;
    CThreaded threaded;
public:
    CIdleShutdown(unsigned _timeout) : timeout(_timeout*60000), threaded("CIdleShutdown") { threaded.init(this); }
    ~CIdleShutdown() { stop(); threaded.join(); }
    virtual void threadmain() override
    {
        if (!sem.wait(timeout)) // feeling neglected, restarting..
            abortThor(MakeThorException(TE_IdleRestart, "Thor has been idle for %d minutes, restarting", timeout/60000), TEC_Idle, false);
    }
    void stop() { sem.signal(); }
};

static int getRunningMaxPriority(const char *qname)
{
    int maxpriority = 0; // ignore neg
    try {
        Owned<IRemoteConnection> conn = querySDS().connect("/Status/Servers",myProcessSession(),RTM_LOCK_READ,30000);
        if (conn.get())
        {
            Owned<IPropertyTreeIterator> it(conn->queryRoot()->getElements("Server"));
            ForEach(*it) {
                StringBuffer instance;
                if(it->query().hasProp("@queue"))
                {
                    const char* queue=it->query().queryProp("@queue");
                    if(queue&&(strcmp(queue,qname)==0)) {
                        Owned<IPropertyTreeIterator> wuids = it->query().getElements("WorkUnit");
                        ForEach(*wuids) {
                            IPropertyTree &wu = wuids->query();
                            const char* wuid=wu.queryProp(NULL);
                            if (wuid&&*wuid) {
                                Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                                Owned<IConstWorkUnit> workunit = factory->openWorkUnit(wuid);
                                if (workunit) {
                                    int priority = workunit->getPriorityValue();
                                    if (priority>maxpriority)
                                        maxpriority = priority;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    catch (IException *e)
    {
        EXCLOG(e,"getRunningMaxPriority");
        e->Release();
    }
    return maxpriority;

}

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
    try
    {
        if (!workunit) // check workunit is available and ready to run.
            throw MakeStringException(0, "Could not locate workunit %s", wuid);
        if (workunit->getCodeVersion() == 0)
            throw makeStringException(0, "Attempting to execute a workunit that hasn't been compiled");
        if ((workunit->getCodeVersion() > ACTIVITY_INTERFACE_VERSION) || (workunit->getCodeVersion() < MIN_ACTIVITY_INTERFACE_VERSION))
            throw MakeStringException(0, "Workunit was compiled for eclagent interface version %d, this thor requires version %d..%d", workunit->getCodeVersion(), MIN_ACTIVITY_INTERFACE_VERSION, ACTIVITY_INTERFACE_VERSION);
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
                    throw MakeStringException(0, "Workunit was compiled by eclcc version %s which is not compatible with this runtime", buildVersion.str());
            }
        }

        if (debugListener)
        {
            WorkunitUpdate wu(&workunit->lock());
            StringBuffer sb;
            queryHostIP().getIpText(sb);
            wu->setDebugAgentListenerIP(sb); //tells debugger what IP to write commands to
            wu->setDebugAgentListenerPort(debugListener->getPort());
        }

        return doit(workunit, graphName, agentep);
    }
    catch (IException *e)
    {
        IThorException *te = QUERYINTERFACE(e, IThorException);
        if (te && tea_shutdown==te->queryAction())
            stopped = true;
        reply(workunit, wuid, e, agentep, false);
    }
    catch (CATCHALL)
    {
        reply(workunit, wuid, MakeStringException(0, "Unknown exception"), agentep, false);
    }
    return false;
}

void CJobManager::run()
{
    LOG(MCdebugProgress, thorJob, "Listening for graph");

    setWuid(NULL);
#ifndef _CONTAINERIZED
    StringBuffer soPath;
    globals->getProp("@query_so_dir", soPath);
    StringBuffer soPattern("*.");
#ifdef _WIN32
    soPattern.append("dll");
#else
    soPattern.append("so");
#endif
    querySoCache.init(soPath.str(), DEFAULT_QUERYSO_LIMIT, soPattern);

    SCMStringBuffer _queueNames;
    const char *thorName = globals->queryProp("@name");
    if (!thorName) thorName = "thor";
    getThorQueueNames(_queueNames, thorName);
    queueName.set(_queueNames.str());
#endif

    jobq.setown(createJobQueue(queueName.get()));
    struct cdynprio: public IDynamicPriority
    {
        const char *qn;
        int get()
        {
            int p = getRunningMaxPriority(qn);
            if (p)
                PROGLOG("Dynamic Min priority = %d",p);
            return p;
        }
    } *dp = NULL;

    if (globals->getPropBool("@multiThorPriorityLock")) {
        PROGLOG("multiThorPriorityLock enabled");
        dp = new cdynprio;
        dp->qn = queueName.get();
    }

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
            threaded.init(this);
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
                    PROGLOG("Unknown cmd = %s", cmd.get());
            }
        }
    } stopThorListener(MPTAG_THOR);
    StringBuffer exclusiveLockName;
    Owned<IDaliMutex> exclLockDaliMutex;
    if (globals->getProp("@multiThorExclusionLockName",exclusiveLockName))
    {
        if (exclusiveLockName.length())
        {
            if (globals->getPropBool("@multiThorPriorityLock"))
                FLLOG(MCoperatorWarning, thorJob, "multiThorPriorityLock cannot be used in conjunction with multiThorExclusionLockName");
            else
            {
                PROGLOG("Multi-Thor exclusive lock defined: %s", exclusiveLockName.str());
                exclLockDaliMutex.setown(createDaliMutex(exclusiveLockName.str()));
            }
        }
    }
    bool jobQConnected = false;
    while (!stopped)
    {
        handlingConversation = false;
        conversation.clear();
        SocketEndpoint masterEp(getMasterPortBase());
        StringBuffer url;
        PROGLOG("ThorLCR(%s) available, waiting on queue %s",masterEp.getUrlStr(url).str(),queueName.get());

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
                conversation.setown(jobq->acceptConversation(_item,30*1000,dp));    // 30s priority transition delay
                item.setown(_item);
            }
        }
        if (!conversation.get()||!item.get())
        {
            if (!stopped)
                setExitCode(0);
            PROGLOG("acceptConversation aborted - terminating");
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
            FLLOG(MCoperatorWarning, thorJob, e, "CJobManager::run");
            continue;
        }
        Owned<IWorkUnitFactory> factory;
        Owned<IConstWorkUnit> workunit;
        bool allDone = false;
        try
        {
            factory.setown(getWorkUnitFactory());
            workunit.setown(factory->openWorkUnit(wuid));

            {
                Owned<IWorkUnit> w = &workunit->lock();
                addTimeStamp(w, wfid, graphName, StWhenDequeued);
            }

            unsigned maxLogDetail = workunit->getDebugValueInt("maxlogdetail", DefaultDetail); 
            ILogMsgFilter *existingLogHandler = queryLogMsgManager()->queryMonitorFilter(logHandler);
            dbgassertex(existingLogHandler);
            verifyex(queryLogMsgManager()->changeMonitorFilterOwn(logHandler, getCategoryLogMsgFilter(existingLogHandler->queryAudienceMask(), existingLogHandler->queryClassMask(), maxLogDetail)));

            allDone = execute(workunit, wuid, graphName, agentep);
            daliLock.clear();
            reply(workunit, wuid, NULL, agentep, allDone);
        }
        catch (IException *e)
        {
            IThorException *te = QUERYINTERFACE(e, IThorException);
            if (te && tea_shutdown==te->queryAction())
                stopped = true;
            reply(workunit, wuid, e, agentep, false);
        }
        catch (CATCHALL)
        {
            reply(workunit, wuid, MakeStringException(0, "Unknown exception"), agentep, false);
        }

        // reset for next job
        setProcessAborted(false);
    }
    delete dp;
    jobq.clear();
}

bool CJobManager::doit(IConstWorkUnit *workunit, const char *graphName, const SocketEndpoint &agentep)
{
    StringBuffer s;
    StringAttr wuid(workunit->queryWuid());
    StringAttr user(workunit->queryUser());

    LOG(MCdebugInfo, thorJob, "Processing wuid=%s, graph=%s from agent: %s", wuid.str(), graphName, agentep.getUrlStr(s).str());
    LOG(MCauditInfo,",Progress,Thor,Start,%s,%s,%s,%s,%s,%s",
            queryServerStatus().queryProperties()->queryProp("@thorname"),
            wuid.str(),
            graphName,
            user.str(),
            queryServerStatus().queryProperties()->queryProp("@nodeGroup"),
            queryServerStatus().queryProperties()->queryProp("@queue"));
    Owned<IException> e;
    bool allDone = false;
    try
    {
        allDone = executeGraph(*workunit, graphName, agentep);
    }
    catch (IException *_e) { e.setown(_e); }
    LOG(MCauditInfo,",Progress,Thor,Stop,%s,%s,%s,%s,%s,%s",
            queryServerStatus().queryProperties()->queryProp("@thorname"),
            wuid.str(),
            graphName,
            user.str(),
            queryServerStatus().queryProperties()->queryProp("@nodeGroup"),
            queryServerStatus().queryProperties()->queryProp("@queue"));
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
        FLLOG(MCexception(e), thorJob, e, "WARNING: Failed to set wuid in SDS:");
        e->Release();
    }
    catch (CATCHALL)
    {
        FLLOG(MCerror, thorJob, "WARNING: Failed to set wuid in SDS: Unknown error");
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
            exitException.setown(e);
            Owned<IWorkUnit> w = &workunit->lock();
            Owned<IWUException> we = w->createException();
            we->setSeverity(SeverityInformation);
            StringBuffer errStr;
            e->errorMessage(errStr);
            we->setExceptionMessage(errStr);
            we->setExceptionSource("thormasterexception");
            we->setExceptionCode(e->errorCode());
            WUState newState = (WUStateRunning == w->getState()) ? WUStateWait : WUStateFailed;
            w->setState(newState);
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
    agentep.getUrlStr(s);
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
            EXCLOG(e, "Exception at time of pause");
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
        IERRLOG("Failed to reply to agent %s",agentep.getUrlStr(s).str());
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
        PROGLOG("Loading query name: %s", soName.str());
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
            PROGLOG("Using existing local dll: %s", compoundPath.str()); // It is assumed if present here then _still_ present on slaves from previous send.
        else
        {
            MemoryBuffer file;
            queryDllServer().getDll(soName.str(), file);
            PROGLOG("Saving dll: %s", compoundPath.str());
            OwnedIFile out = createIFile(compoundPath.str());
            try
            {
                out->setCreateFlags(S_IRWXU);
                OwnedIFileIO io = out->open(IFOcreate);
                io->write(0, file.length(), file.toByteArray());
                io.clear();
            }
            catch (IException *e)
            {
                FLLOG(MCexception(e), thorJob, e, "Failed to write query dll - ignoring!");
                e->Release();
            }
            sendSo = getExpertOptBool("dllsToSlaves", true);
        }
        querySo.setown(createDllEntry(compoundPath.str(), false, NULL, false));
        soPath.swapWith(compoundPath);
    }

    SCMStringBuffer eclstr;
    StringAttr user(workunit.queryUser());

    PROGLOG("Started wuid=%s, user=%s, graph=%s\n", wuid.str(), user.str(), graphName);

    PROGLOG("Query %s loaded", soPath.str());
    Owned<CJobMaster> job = createThorGraph(graphName, workunit, querySo, sendSo, agentEp);
    unsigned wfid = job->getWfid();
    StringBuffer graphScope;
    graphScope.append(WorkflowScopePrefix).append(wfid).append(":").append(graphName);
    PROGLOG("Graph %s created", graphName);
    PROGLOG("Running graph=%s", job->queryGraphName());
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

        {
#ifdef _CONTAINERIZED
            double stdDev = 0.0;
            unsigned __int64 min, max;
            unsigned minNode, maxNode;
            const StatisticsMapping podStatistics({StNumPods});
            CRuntimeSummaryStatisticCollection podStats(podStatistics);
            std::vector<std::string> nodeNames; // ordered list of the unique node names
            try
            {
                // collate pod distribution
                VStringBuffer selector("thorworker-job-%s-%s", wuid.get(), graphName);
                std::vector<std::vector<std::string>> pods = getPodNodes(selector.toLowerCase());
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
                EXCLOG(e);
                e->Release();
            }

            // calculate the above, before locking the workunit below to avoid holding lock whilst issuing getPodNodes call
#endif

            Owned<IWorkUnit> wu = &workunit.lock();
            wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTgraph, graphScope, StWhenStarted, NULL, startTs, 1, 0, StatsMergeAppend);
            //Could use addTimeStamp(wu, SSTgraph, graphName, StWhenStarted, wfid) if start time could be this point
            wu->setState(WUStateRunning);
            VStringBuffer version("%d.%d", THOR_VERSION_MAJOR, THOR_VERSION_MINOR);
            wu->setDebugValue("ThorVersion", version.str(), true);

#ifdef _CONTAINERIZED
            // issue warning and publish pod distribution stats, if any stddev
            if (stdDev)
            {
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
#endif
        }

        setWuid(workunit.queryWuid(), workunit.queryClusterName());

        allDone = job->go();

        Owned<IWorkUnit> wu = &workunit.lock();
        unsigned __int64 graphTimeNs = cycle_to_nanosec(get_cycles_now()-startCycles);
        StringBuffer graphTimeStr;
        formatGraphTimerLabel(graphTimeStr, graphName);

        updateWorkunitStat(wu, SSTgraph, graphName, StTimeElapsed, graphTimeStr, graphTimeNs, wfid);

        addTimeStamp(wu, SSTgraph, graphName, StWhenFinished, wfid);
        unsigned numberOfMachines = queryNodeClusterWidth() / globals->getPropInt("@numWorkersPerPod", 1); // Number of Pods or physical machines
        cost_type cost = money2cost_type(calculateThorCost(nanoToMilli(graphTimeNs), numberOfMachines));
        if (cost)
            wu->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTgraph, graphScope, StCostExecute, NULL, cost, 1, 0, StatsMergeReplace);
        updateSpillSize(wu, graphScope, SSTgraph);
        removeJob(*job);
    }
    catch (IException *e)
    {
        exception.setown(e);
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
    Owned<IException> _e;
    if (0 == aborting)
    {
        aborting = 1;
        if (errCode != TEC_Clean)
        {
            if (!e)
            {
                _e.setown(MakeThorException(TE_AbortException, "THOR ABORT"));
                e = _e;
            }
            EXCLOG(e,"abortThor");
        }
        LOG(MCdebugProgress, thorJob, "abortThor called");
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
        LOG(MCdebugProgress, thorJob, "aborting any current active job");
        if (jM)
        {
            if (!e)
            {
                _e.setown(MakeThorException(TE_AbortException, "THOR ABORT"));
                e = _e;
            }
            jM->fireException(e);
        }
        if (errCode == TEC_Clean)
        {
            LOG(MCdebugProgress, thorJob, "Removing sentinel upon normal shutdown");
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
    CDaliConnectionValidator(unsigned _pollDelay) : threaded("CDaliConnectionValidator") { pollDelay = _pollDelay*1000; stopped = false; threaded.init(this); }
    ~CDaliConnectionValidator() { stop(); threaded.join(); }
    virtual void threadmain() override
    {
        for (;;)
        {
            poll.wait(pollDelay);
            if (stopped) break;
            if (!verifyCovenConnection(pollDelay)) // use poll delay time for verify connection timeout
            {
                abortThor(MakeThorOperatorException(TE_AbortException, "Detected lost connectivity with dali server, aborting thor"), TEC_DaliDown);
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
static int recvNextGraph(unsigned timeoutMs, const char *wuid, StringBuffer &retWuid, StringBuffer &retGraphName)
{
    StringBuffer next;
    CMessageBuffer msg;
    if (thorQueue)
    {
        Owned<IJobQueueItem> item = thorQueue->dequeue(timeoutMs);
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
        WARNLOG("Unrecognised job format received: %s", next.str());
        return 0; // unrecognised format, ignore
    }
    retWuid.set(sArray.item(1));
    retGraphName.set(sArray.item(2));
    return 1; // success
}


static std::vector<std::string> connectedWorkerPods;
void addConnectedWorkerPod(const char *podName)
{
    connectedWorkerPods.push_back(podName);
}

static bool podInfoPublished = false;
void publishPodNames(IWorkUnit *workunit)
{
    // skip if Thor manager already published (implying worker pods already published too)
    if (workunit->setContainerizedProcessInfo("Thor", globals->queryProp("@name"), queryMyPodName(), nullptr))
    {
        for (unsigned workerNum=0; workerNum<connectedWorkerPods.size(); workerNum++)
        {
            const char *workerPodName = connectedWorkerPods[workerNum].c_str();
            workunit->setContainerizedProcessInfo("ThorWorker", globals->queryProp("@name"), workerPodName, std::to_string(workerNum+1).c_str());
        }
    }
    podInfoPublished = true;
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

        std::unordered_set<std::string> publishedPodWuids;
        Owned<CJobManager> jobManager = new CJobManager(logHandler);
        try
        {
            if (!isContainerized())
                jobManager->run();
            else
            {
                unsigned lingerPeriod = globals->getPropInt("@lingerPeriod", DEFAULT_LINGER_SECS)*1000;
                bool multiJobLinger = globals->getPropBool("@multiJobLinger", true);
                VStringBuffer multiJobLingerQueueName("%s_lingerqueue", globals->queryProp("@name"));
                StringBuffer instance("thorinstance_");

                if (multiJobLinger)
                {
                    StringBuffer thorQueueName;
                    getClusterThorQueueName(thorQueueName, globals->queryProp("@name"));
                    PROGLOG("multiJobLinger: on. Queue name: %s", thorQueueName.str());
                    thorQueue.setown(createJobQueue(thorQueueName));
                    thorQueue->connect(false);
                }

                queryMyNode()->endpoint().getUrlStr(instance);
                StringBuffer currentGraphName(graphName);
                StringBuffer currentWuid(wuid);

                while (true)
                {
                    PROGLOG("Executing: wuid=%s, graph=%s", currentWuid.str(), currentGraphName.str());

                    {
                        Owned<IWorkUnitFactory> factory;
                        Owned<IConstWorkUnit> workunit;
                        factory.setown(getWorkUnitFactory());
                        workunit.setown(factory->openWorkUnit(currentWuid));
                        if (!podInfoPublished)
                        {
                            Owned<IWorkUnit> wu = &workunit->lock();
                            publishPodNames(wu);
                        }
                        SocketEndpoint dummyAgentEp;
                        jobManager->execute(workunit, currentWuid, currentGraphName, dummyAgentEp);
                        IException *e = jobManager->queryExitException();
                        Owned<IWorkUnit> w = &workunit->lock();
                        WUState newState = (WUStateRunning == w->getState()) ? WUStateWait : WUStateFailed;
                        if (e)
                        {
                            if (WUStateWait != w->getState()) // if set already, CJobManager::reply may have already set to WUStateWait
                            {
                                Owned<IWUException> we = w->createException();
                                we->setSeverity(SeverityInformation);
                                StringBuffer errStr;
                                e->errorMessage(errStr);
                                we->setExceptionMessage(errStr);
                                we->setExceptionSource("thormasterexception");
                                we->setExceptionCode(e->errorCode());

                                w->setState(newState);
                            }
                            break;
                        }

                        if (!multiJobLinger && lingerPeriod)
                            w->setDebugValue(instance, "1", true);

                        w->setState(newState);
                    }
                    currentGraphName.clear();

                    if (lingerPeriod)
                    {
                        PROGLOG("Lingering time left: %.2f", ((float)lingerPeriod)/1000);
                        StringBuffer nextJob;
                        CTimeMon timer(lingerPeriod);
                        unsigned remaining;
                        while (!timer.timedout(&remaining))
                        {
                            StringBuffer wuid;
                            int ret = recvNextGraph(remaining, currentWuid.str(), wuid, currentGraphName);
                            if (ret > 0)
                            {
                                if (!streq(currentWuid, wuid))
                                {
                                    // perhaps slightly overkill, but avoid checking/locking wuid to add pod info.
                                    // if this instance has already done so.
                                    auto it = publishedPodWuids.find(wuid.str());
                                    if (it == publishedPodWuids.end())
                                    {                                        
                                        podInfoPublished = false;

                                        // trivial safe-guard against growing too big
                                        // but unlikely to ever grow this big
                                        if (publishedPodWuids.size() > 10000)
                                            publishedPodWuids.clear();

                                        publishedPodWuids.insert(wuid.str());
                                    }
                                    // NB: this set of pods could still already be published, if so, publishPodNames will not re-add.
                                }
                                currentWuid.set(wuid); // NB: will always be same if !multiJobLinger
                                break; // success
                            }
                            else if (ret < 0)
                                break; // timeout/abort
                            // else - reject/ignore duff message.
                        }
                        if (0 == currentGraphName.length()) // only ever true if !multiJobLinger
                        {
                            // De-register the idle lingering entry.
                            Owned<IWorkUnitFactory> factory;
                            Owned<IConstWorkUnit> workunit;
                            factory.setown(getWorkUnitFactory());
                            workunit.setown(factory->openWorkUnit(currentWuid));
                            Owned<IWorkUnit> w = &workunit->lock();
                            w->setDebugValue(instance, "0", true);
                            break;
                        }
                    }
                }
                thorQueue.clear();
            }
        }
        catch (IException *e)
        {
            EXCLOG(e, NULL);
            throw;
        }
    }
    catch (IException *e)
    {
        FLLOG(MCexception(e), thorJob, e,"ThorMaster");
        e->Release();
    }
    if (multiThorMemoryThreshold)
        setMultiThorMemoryNotify(0,NULL);
}
