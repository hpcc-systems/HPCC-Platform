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
#include "daclient.hpp"
#include "wujobq.hpp"
#include "jmisc.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jutil.hpp"
#include "eclagent.hpp"
#include "environment.hpp"

class CEclAgentExecutionServer : public CInterfaceOf<IThreadFactory>
{
public:
    CEclAgentExecutionServer(IPropertyTree *config);
    ~CEclAgentExecutionServer();

    int run();
    virtual IPooledThread *createNew() override;
private:
    bool executeWorkunit(const char * wuid);

    const char *agentName;
    const char *daliServers;
    Owned<IJobQueue> queue;
    Linked<IPropertyTree> config;
#ifdef _CONTAINERIZED
    Owned<IThreadPool> pool;
#endif
};

//---------------------------------------------------------------------------------

CEclAgentExecutionServer::CEclAgentExecutionServer(IPropertyTree *_config) : config(_config)
{
    //Build logfile from component properties settings
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(config, "eclagent");
    lf->setCreateAliasFile(false);
    lf->beginLogging();
    PROGLOG("Logging to %s",lf->queryLogFileSpec());

    agentName = config->queryProp("@name");
    assertex(agentName);
    setStatisticsComponentName(SCThthor, agentName, true);

    daliServers = config->queryProp("@daliServers");
    assertex(daliServers);
#ifdef _CONTAINERIZED
    unsigned poolSize = config->getPropInt("@maxActive", 100);
    pool.setown(createThreadPool("agentPool", this, NULL, poolSize, INFINITE));
#endif
}


CEclAgentExecutionServer::~CEclAgentExecutionServer()
{
#ifdef _CONTAINERIZED
    pool->joinAll(false, INFINITE);
#endif
    if (queue)
        queue->cancelAcceptConversation();
}


//---------------------------------------------------------------------------------

int CEclAgentExecutionServer::run()
{
    SCMStringBuffer queueNames;
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
    try
    {
        Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_AgentExec);
        getAgentQueueNames(queueNames, agentName);
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
        while (true)
        {
#ifdef _CONTAINERIZED
            if (!pool->waitAvailable(10000))
            {
                if (config->getPropInt("@traceLevel", 0) > 2)
                    DBGLOG("Blocked for 10 seconds waiting for an available agent slot");
                continue;
            }
#endif
            PROGLOG("AgentExec: Waiting on queue(s) '%s'", queueNames.str());
            Owned<IJobQueueItem> item = queue->dequeue();
            if (item.get())
            {
                StringAttr wuid;
                wuid.set(item->queryWUID());
                PROGLOG("AgentExec: Dequeued workunit request '%s'", wuid.get());
                try
                {
                    executeWorkunit(wuid);
                }
                catch(IException *e)
                {
                    EXCLOG(e, "CEclAgentExecutionServer::run: ");
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
        }
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

#ifdef _CONTAINERIZED
class WaitThread : public CInterfaceOf<IPooledThread>
{
public:
    WaitThread(const char *_dali) : dali(_dali)
    {
    }
    virtual void init(void *param) override
    {
        wuid.set((const char *) param);
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
        try
        {
            if (queryComponentConfig().getPropBool("@containerPerAgent", false))  // MORE - make this a per-workunit setting?
            {
                runK8sJob("eclagent", wuid, wuid);
            }
            else
            {
                VStringBuffer exec("eclagent --workunit=%s --daliServers=%s", wuid.str(), dali.str());
                Owned<IPipeProcess> pipe = createPipeProcess();
                if (!pipe->run("eclagent", exec.str(), ".", false, true, false, 0, false))
                    throw makeStringExceptionV(0, "Failed to run %s", exec.str());
            }
        }
        catch (IException *E)
        {
            EXCLOG(E);
            E->Release();
            Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
            Owned<IWorkUnit> workunit = factory->updateWorkUnit(wuid);
            if (workunit)
            {
                workunit->setState(WUStateFailed);
                workunit->commit();
            }
        }
    }
private:
    StringAttr wuid;
    StringAttr dali;
};
#endif

IPooledThread *CEclAgentExecutionServer::createNew()
{
#ifdef _CONTAINERIZED
    return new WaitThread(daliServers);
#else
    throwUnexpected();
#endif
}

bool CEclAgentExecutionServer::executeWorkunit(const char * wuid)
{
#ifdef _CONTAINERIZED
    pool->start((void *) wuid);
    return true;
#else
    //build eclagent command line
    StringBuffer command;

#ifdef _WIN32
    command.append(".\\eclagent.exe");
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
#endif
}

//---------------------------------------------------------------------------------

int main(int argc, const char *argv[]) 
{ 
#ifndef _CONTAINERIZED
    for (unsigned i=0;i<(unsigned)argc;i++) {
        if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
            if (daemon(1,0) || write_pidfile(argv[++i])) {
                perror("Failed to daemonize");
                return EXIT_FAILURE;
            }
            break;
        }
    }
#endif
    InitModuleObjects();

    Owned<IPropertyTree> config;
    try
    {
        config.setown(loadConfiguration(eclagentDefaultYaml, argv, "EclAgent", "ECLAGENT", "agentexec.xml", nullptr));
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
