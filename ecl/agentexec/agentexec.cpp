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

#include "jfile.hpp"
#include "jcontainerized.hpp"
#include "daclient.hpp"
#include "wujobq.hpp"
#include "jmisc.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jutil.hpp"
#include "daqueue.hpp"
#include "workunit.hpp"
#include "environment.hpp"
#include "dafdesc.hpp"

class CEclAgentExecutionServer : public CInterfaceOf<IThreadFactory>, implements IAbortHandler
{
public:
    IMPLEMENT_IINTERFACE_USING(CInterfaceOf<IThreadFactory>);

    CEclAgentExecutionServer(IPropertyTree *config);
    ~CEclAgentExecutionServer();

    int run();
    virtual IPooledThread *createNew() override;
    virtual bool onAbort() override;
private:
    bool executeWorkunit(IJobQueueItem *item);
    void maintainPrevPools();

    const char *agentName;
    const char *daliServers;
    const char *apptype;
    Owned<IJobQueue> queue;
    Owned<IJobQueue> lingerQueue; // used if thor agent for a thor configured with multiJobLinger=true
    Linked<IPropertyTree> config;
    std::atomic<bool> running = { false };
    bool isThorAgent = false;
    bool disableQueuePriority = false; // temporary JIC, while new client priority queuing beds in.

    // for containerized only
    Owned<IThreadPool> pool;
    IArrayOf<IThreadPool> prevPools;
    unsigned poolSize;
    unsigned updateConfigCBId;

friend class WaitThread;
};

//---------------------------------------------------------------------------------

CEclAgentExecutionServer::CEclAgentExecutionServer(IPropertyTree *_config) : config(_config)
{
    //Build logfile from component properties settings
#ifndef _CONTAINERIZED
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(config, "eclagent");
    lf->setCreateAliasFile(false);
    lf->beginLogging();
    PROGLOG("Logging to %s",lf->queryLogFileSpec());
#else
    setupContainerizedLogMsgHandler();
#endif

    agentName = config->queryProp("@name");
    assertex(agentName);

    daliServers = config->queryProp("@daliServers");
    assertex(daliServers);

    apptype = config->queryProp("@type");
    if (apptype)
        isThorAgent = streq("thor", apptype);
    else
        apptype = "hthor";
    disableQueuePriority = getComponentConfigSP()->getPropBool("expert/@disableQueuePriority");

    StatisticCreatorType ctype = SCThthor;
    if (strieq(apptype, "roxie"))
        ctype = SCTroxie;
    else if (strieq(apptype, "thor"))
        ctype = SCTthor;
    setStatisticsComponentName(ctype, agentName, true);
}


CEclAgentExecutionServer::~CEclAgentExecutionServer()
{
    if (updateConfigCBId)
        removeConfigUpdateHook(updateConfigCBId);

    if (pool)
        pool->joinAll(false, INFINITE);

    if (queue)
        queue->cancelAcceptConversation();
}


//---------------------------------------------------------------------------------

void CEclAgentExecutionServer::maintainPrevPools()
{
    DBGLOG("maintainPrevPools()");
    ForEachItemInRev(poolIdx, prevPools)
    {
        IThreadPool &prevPool = prevPools.item(poolIdx);
        DBGLOG("maintainPrevPools()\t%d: %d", poolIdx, prevPool.runningCount());
        if (prevPool.runningCount() == 0)
            prevPools.remove(poolIdx);
    }
    DBGLOG("maintainPrevPools()\t%d", prevPools.ordinality());
}

//---------------------------------------------------------------------------------

int CEclAgentExecutionServer::run()
{
    auto updateFunc = [this](const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
    {
        DBGLOG("updateFunc()");
        maintainPrevPools();

        // JCS - the pool approach would also work in bare-metal if it could be configured.
        Owned<IPropertyTree> config = getComponentConfigSP();
        unsigned newPoolSize = config->getPropInt("@maxActive", 100);
        DBGLOG("updateFunc()\tnewPoolSize: %d", newPoolSize);
        if (newPoolSize != poolSize)
        {
            poolSize = newPoolSize;

            if (pool->runningCount())
            {
                prevPools.append(*(pool.getClear()));
            }
            DBGLOG("updateFunc()\tcreateThreadPool agentPool: %d", newPoolSize);
            pool.setown(createThreadPool("agentPool", this, false, nullptr, poolSize, INFINITE));
        }
    };
    if (isContainerized())
        updateConfigCBId = installConfigUpdateHook(updateFunc, true);

    #ifdef _CONTAINERIZED
    StringBuffer queueNames;
#else
    SCMStringBuffer queueNames;
#endif
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
    try
    {
        Owned<IGroup> serverGroup = createIGroupRetry(daliServers, DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_AgentExec);
#ifdef _CONTAINERIZED
        if (isThorAgent)
        {
            getClusterThorQueueName(queueNames, agentName);
            if (disableQueuePriority)
            {
                // old mechanism used to test a separate queue that only Thor instances listened to (<name>.lingerthor)
                // by queuing to it for a short period, and if not pulled off, launching a new instance. (see queueJobIfQueueWaiting)
                // In new client priority implementation, the agent will not pull it off the queue in the 1st place.
                if (config->getPropBool("@multiJobLinger", defaultThorMultiJobLinger))
                {
                    StringBuffer lingerQueueName;
                    getClusterLingerThorQueueName(lingerQueueName, agentName);
                    lingerQueue.setown(createJobQueue(lingerQueueName));
                }
            }
        }
        else
        {
            getClusterEclAgentQueueName(queueNames, agentName);
            Owned<IPropertyTreeIterator> auxQueueIter = config->getElements("auxQueues");
            ForEach(*auxQueueIter)
            {
                queueNames.append(',');
                const char *auxQueueName = auxQueueIter->query().queryProp(nullptr);
                getClusterEclAgentQueueName(queueNames, auxQueueName);
            }
        }
#else
        getAgentQueueNames(queueNames, agentName);
#endif
        queue.setown(createJobQueue(queueNames.str()));
        queue->connect(false);
    }
    catch (IException *e)
    {
        EXCLOG(e, "Server queue create/connect: ");
        e->Release();
        return -1;
    }
    catch(...)
    {
        IERRLOG("Terminating unexpectedly");
    }

    CSDSServerStatus serverStatus("HThorServer");
    serverStatus.queryProperties()->setProp("@queue",queueNames.str());
    serverStatus.queryProperties()->setProp("@cluster", agentName);
    serverStatus.commitProperties();
    writeSentinelFile(sentinelFile);

    try
    {
        running = true;
        LocalIAbortHandler abortHandler(*this);
        unsigned __int64 priority = 0;
        while (running)
        {
            if (pool)
            {
                if (!pool->waitAvailable(10000))
                {
                    if (config->getPropInt("@traceLevel", 0) > 2)
                        DBGLOG("Blocked for 10 seconds waiting for an available agent slot");
                    continue;
                }
            }

            PROGLOG("AgentExec: Waiting on queue(s) '%s'", queueNames.str());
            Owned<IJobQueueItem> item = queue->dequeuePriority(priority);
            if (item.get())
            {
                PROGLOG("AgentExec: Dequeued workunit request '%s'", item->queryWUID());
                try
                {
                    executeWorkunit(item);
                }
                catch(IException *e)
                {
                    EXCLOG(e, "CEclAgentExecutionServer::run: ");
                    e->Release();
                }
                catch(...)
                {
                    IERRLOG("Unexpected exception in CEclAgentExecutionServer::run caught");
                }
            }
            else
            {
                removeSentinelFile(sentinelFile); // no reason to restart
                break;
            }

            // At the moment, we only prioritize if a thoragent, because it is the only component that fires instances that then also listen to the same queue
            // That will change, when eclccserver and hthor use the same model.
            //
            // In all other cases, there may be multiple agents (via replicas) listening to the same queue, but there's no particular advantage to prioritizing
            // one over the other, and theoretically could introduce some unexpected behaviour, like a skew of local disk usage.
            // That will requirement will also change, if we have automatic horizontal scaling in place. Then we would want to prioritize the agents that have
            // recently been in use.
            if (!disableQueuePriority && isThorAgent)
                priority = getTimeStampNowValue();
        }
        DBGLOG("Closing down");
    }

    catch (IException *e)
    {
        EXCLOG(e, "Server Exception: ");
        e->Release();
        PROGLOG("Exiting");
    }

    try
    {
        queue->disconnect();
    }
    catch (IException *e)
    {
        EXCLOG(e, "Server queue disconnect: ");
        e->Release();
    }
    PROGLOG("Exiting agentexec");
    return 1;
}

//---------------------------------------------------------------------------------

typedef std::tuple<IJobQueueItem *, unsigned, const char *, const char *> ThreadCtx;

// NB: WaitThread only used by if pool created (see CEclAgentExecutionServer ctor)
static std::atomic<unsigned> nextInstanceNumber{0};
class WaitThread : public CInterfaceOf<IPooledThread>
{
public:
    WaitThread(CEclAgentExecutionServer &_owner, const char *_dali, const char *_apptype, const char *_queue)
        : owner(_owner), dali(_dali), apptype(_apptype), queue(_queue)
    {
        isThorAgent = streq("thor", apptype);
        // The thread pool will never shrink, therefore the pool thread instance numbers will always range between 1 and max active.
        myInstanceNumber = ++nextInstanceNumber;
    }
    virtual void init(void *param) override
    {
        auto &context = *static_cast<ThreadCtx *>(param);
        IJobQueueItem *tmpItem;
        std::tie(tmpItem, wfid, wuid, graphName) = context;
        item.set(tmpItem);
    }
    virtual bool stop() override
    {
        return false;
    }
    virtual bool canReuse() const override
    {
        return false;
    }
    virtual void threadmain() override
    {
        Owned<IException> exception;
        bool sharedK8sJob = false;
        try
        {
            StringAttr jobSpecName(apptype);
            StringAttr processName(apptype);

            if (isThorAgent)
            {
                // JCSMORE - ideally apptype, image and executable name would all be same.
                jobSpecName.set("thormanager");
                processName.set("thormaster_lcr");
            }
            Owned<const IPropertyTree> compConfig = getComponentConfig();
            bool useChildProcesses = compConfig->getPropBool("@useChildProcesses");
            if (isContainerized() && !useChildProcesses)
            {
                sharedK8sJob = true;
                constexpr unsigned queueWaitingCheckPeriodMs = 1000;
                // NB: queueJobIfQueueWaiting is a legacy mechanism (should be deleted), for now only used if disableQueuePriority=true
                if (!owner.lingerQueue || !queueJobIfQueueWaiting(owner.lingerQueue, item, queueWaitingCheckPeriodMs, queueWaitingCheckPeriodMs))
                {
                    std::list<std::pair<std::string, std::string>> params = { };
                    if (compConfig->getPropBool("@useThorQueue", true))
                        params.push_back({ "queue", queue.get() });
                    StringBuffer jobName;
                    if (isThorAgent)
                    {
                        params.push_back({ "graphName", graphName.get() });
                        params.push_back({ "wfid", std::to_string(wfid) });

                        const char *targetName = compConfig->queryProp("@targetName");
                        if (targetName)
                        {
                            jobName.append(targetName).append('-');
                            jobName.append(myInstanceNumber);
                            params.push_back({ "instanceNum", std::to_string(myInstanceNumber) });
                        }
                        else
                            jobName.append(wuid).append('-').append(graphName);
                    }
                    else
                        jobName.append(wuid);

                    SCMStringBuffer optPlatformVersion;
                    {
                        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                        Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
                        if (!cw)
                        {
                            WARNLOG("Queued wuid does not exist: %s", wuid.str());
                            return; // exit pooled thread
                        }
                        if (isThorAgent)
                        {
                            SessionId agentSessionID = cw->getAgentSession();
                            if ((agentSessionID <= 0) || querySessionManager().sessionStopped(agentSessionID, 0))
                            {
                                WARNLOG("Discarding agentless queued Thor job: %s", wuid.str());
                                return; // exit pooled thread
                            }
                        }
                        cw->getDebugValue("platformVersion", optPlatformVersion);
                        if (optPlatformVersion.length())
                            params.push_back({ "_HPCC_JOB_VERSION_", optPlatformVersion.str() });

                        Owned<IWorkUnit> workunit = &cw->lock();
                        workunit->setContainerizedProcessInfo("AgentExec", compConfig->queryProp("@name"), k8s::queryMyPodName(), k8s::queryMyContainerName(), graphName, nullptr);
                        addTimeStamp(workunit, wfid, graphName, StWhenK8sLaunched);
                    }
                    k8s::runJob(jobSpecName, wuid, jobName, params);
                }
            }
            else
            {
                if (isContainerized())
                {
                    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                    Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid.str());
                    workunit->setContainerizedProcessInfo("AgentExec", compConfig->queryProp("@name"), k8s::queryMyPodName(), k8s::queryMyContainerName(), nullptr, nullptr);
                }
                bool useValgrind = compConfig->getPropBool("@valgrind", false);
                VStringBuffer exec("%s%s --workunit=%s --daliServers=%s", useValgrind ? "valgrind " : "", processName.get(), wuid.str(), dali.str());
                if (compConfig->hasProp("@config"))
                {
                    exec.append(" --config=");
                    compConfig->getProp("@config", exec);
                }
                if (compConfig->getPropBool("@useThorQueue", true))
                    exec.append(" --queue=").append(queue);
                if (isThorAgent)
                    exec.appendf(" --graphName=%s", graphName.get());
                Owned<IPipeProcess> pipe = createPipeProcess();
                pipe->setenv("SENTINEL", nullptr);
                if (!pipe->run(apptype.str(), exec.str(), ".", false, true, false, 0, false))
                    throw makeStringExceptionV(0, "Failed to run \"%s\"", exec.str());
                unsigned retCode = pipe->wait();
                if (retCode)
                    throw makeStringExceptionV(0, "Failed to run \"%s\": process exited with error: %u", exec.str(), retCode);
            }
        }
        catch (IException *e)
        {
            exception.setown(e);
        }
        if (exception)
        {
            EXCLOG(exception);
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IConstWorkUnit> cw = factory->openWorkUnit(wuid);
            if (cw)
            {
                if (!sharedK8sJob && ((cw->getState() == WUStateRunning) || (cw->getState() == WUStateBlocked) || (cw->getState() == WUStateWait)))
                {
                    Owned<IWorkUnit> workunit = &cw->lock();
                    // recheck now locked
                    if ((workunit->getState() == WUStateRunning) || (workunit->getState() == WUStateBlocked) || (workunit->getState() == WUStateWait))
                    {
                        workunit->setState(WUStateFailed);
                        StringBuffer eStr;
                        addExceptionToWorkunit(workunit, SeverityError, "agentexec", exception->errorCode(), exception->errorMessage(eStr).str(), nullptr, 0, 0, 0);
                        workunit->commit();
                    }
                }
            }
        }
    }
private:
    CEclAgentExecutionServer &owner;
    unsigned wfid = 0;
    StringAttr wuid;
    StringAttr graphName;
    StringAttr scopeName;
    StringAttr dali;
    StringAttr apptype;
    StringAttr queue;
    Linked<IJobQueueItem> item;
    bool isThorAgent{false};
    unsigned myInstanceNumber{0};
};

IPooledThread *CEclAgentExecutionServer::createNew()
{
    if (nullptr == pool)
        throwUnexpected();
    return new WaitThread(*this, daliServers, apptype, agentName);
}

bool CEclAgentExecutionServer::onAbort()
{
    DBGLOG("Close down requested");
    running = false;
    if (queue)
        queue->cancelAcceptConversation();
    return false;
}

bool CEclAgentExecutionServer::executeWorkunit(IJobQueueItem *item)
{
    unsigned wfid = 0;
    StringArray sArray;
    const char *graphName = nullptr;
    ThreadCtx threadCtx;
    const char *wuid = item->queryWUID();
    if (pool)
    {
        if (isThorAgent)
        {
            // NB: In the case of handling apptype='thor', the queued items is of the form <wfid>/<wuid>/<graphName>
            // wfid and graphName not needed in other contexts
            sArray.appendList(wuid, "/");
            assertex(3 == sArray.ordinality());
            wfid = atoi(sArray.item(0));
            wuid = sArray.item(1);
            graphName = sArray.item(2);
        }
        threadCtx = std::make_tuple(item, wfid, wuid, graphName);
    }

    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
        addTimeStamp(workunit, wfid, graphName, StWhenDequeued);
    }

    if (pool)
    {
        pool->start((void *) &threadCtx);
        return true;
    }

    //build eclagent command line
    StringBuffer command;

#ifdef _WIN32
    command.append(".\\hthor.exe");
#else
    command.append("start_eclagent");
#endif

    StringBuffer cmdLine(command);
    cmdLine.append(" --workunit=").append(wuid).append(" --daliServers=").append(daliServers);

    DWORD runcode;
    PROGLOG("AgentExec: Executing '%s'", cmdLine.str());
#ifdef _WIN32
    bool success = invoke_program(cmdLine.str(), runcode, false, NULL, NULL);
#else
    //specify "wait" to eliminate linux zombies. Waits for the startup script to
    //complete (not eclagent), because script starts eclagent in the background
    bool success = invoke_program(cmdLine.str(), runcode, true, NULL, NULL);
#endif
    if (success)
    {
        if (runcode != 0)
            PROGLOG("Process failed during execution: %s error(%" I64F "i)", cmdLine.str(), (unsigned __int64) runcode);
        else
            PROGLOG("Execution started");
    }
    else
    {
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
        if (workunit)
        {
            workunit->setState(WUStateFailed);
            workunit->commit();
        }
        PROGLOG("Process failed to start: %s", cmdLine.str());
    }

    return success && runcode == 0;
}

//---------------------------------------------------------------------------------

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
eclagent:
    name: myeclagent
    type: hthor
)!!";

int main(int argc, const char *argv[])
{
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

    InitModuleObjects();

    Owned<IPropertyTree> config;
    try
    {
        config.setown(loadConfiguration(defaultYaml, argv, "eclagent", "AGENTEXEC", "agentexec.xml", nullptr));
    }
    catch (IException *e)
    {
        EXCLOG(e, "Error processing config file\n");
        return 1;
    }

    int retcode = 0;
    try
    {
        CEclAgentExecutionServer server(config);
        server.run();
    }
    catch (...)
    {
        printf("Unexpected error running agentexec server\r\n");
        retcode = 1;
    }

    closedownClientProcess();
    releaseAtoms();
    ExitModuleObjects();

    return retcode;
}
