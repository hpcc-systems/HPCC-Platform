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
#include "agentexec.hpp"


Owned<CEclAgentExecutionServer> execSvr = NULL;


//---------------------------------------------------------------------------------

CEclAgentExecutionServer::CEclAgentExecutionServer() : Thread("Workunit Execution Server")
{
    started = false;
}

CEclAgentExecutionServer::~CEclAgentExecutionServer()
{
    if (started)
        stop();
    if (queue)
        queue.getClear();
}


void CEclAgentExecutionServer::start()
{
    if (started)
    {
        WARNLOG("START called when already started\n");
        assert(false);
    }

    Owned<IPropertyTree> properties;
    try
    {
        DBGLOG("AgentExec: Loading properties file 'agentexec.xml'");
        properties.setown(createPTreeFromXMLFile("agentexec.xml"));
    }
    catch (IException *e) 
    {
        EXCLOG(e, "Error processing properties file\n");
        throwUnexpected();
    }

    {
        //Build logfile from component properties settings
        Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(properties, "eclagent");
        lf->setCreateAliasFile(false);
        lf->beginLogging();
        PROGLOG("Logging to %s",lf->queryLogFileSpec());
    }

    //get name of workunit job queue
    StringBuffer sb;
    properties->getProp("@name", sb.clear());
    agentName.set(sb);
    if (!agentName.length())
    {
        ERRLOG("'name' not specified in properties file\n");
        throwUnexpected();
    }
    setStatisticsComponentName(SCThthor, agentName, true);

    //get dali server(s)
    properties->getProp("@daliServers", daliServers);
    if (!daliServers.length())
    {
        ERRLOG("'daliServers' not specified in properties file\n");
        throwUnexpected();
    }

    started = true;
    Thread::start();
    Thread::join();
}

//---------------------------------------------------------------------------------

void CEclAgentExecutionServer::stop()
{
    if (started)
    {
        started = false;
        if (queue)
            queue->cancelAcceptConversation();
    }
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
        queue->connect();
    }
    catch (IException *e) 
    {
        EXCLOG(e, "Server queue create/connect: ");
        e->Release();
        return -1;
    }
    catch(...)
    {
        ERRLOG("Terminating unexpectedly");
    }

    CSDSServerStatus serverStatus("HThorServer");
    serverStatus.queryProperties()->setProp("@queue",queueNames.str());
    serverStatus.queryProperties()->setProp("@cluster", agentName);
    serverStatus.commitProperties();
    writeSentinelFile(sentinelFile);

    try 
    {
        while (started)
        {
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
                    ERRLOG("Unexpected exception in CEclAgentExecutionServer::run caught");
                }
            }
            else
            {
                if (started)
                    ERRLOG("Unexpected dequeue of bogus job queue item, exiting agentexec");
                removeSentinelFile(sentinelFile);//no reason to restart
                assert(!started);
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

int CEclAgentExecutionServer::executeWorkunit(const char * wuid)
{
    //build eclagent command line
    StringBuffer command;

#ifdef _WIN32
    command.append(".\\eclagent.exe");
#else
    command.append("start_eclagent");
#endif

    StringBuffer cmdLine = command;
    cmdLine.append(" WUID=").append(wuid).append(" DALISERVERS=").append(daliServers);

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

bool ControlHandler()
{
    if (execSvr)
    {
        execSvr->stop();
    }
    return false;
}

//---------------------------------------------------------------------------------

int main(int argc, char **argv) 
{ 
    InitModuleObjects();

    addAbortHandler(ControlHandler);

    try
    { 
        execSvr.setown(new CEclAgentExecutionServer());
        execSvr->start();
    } 
    catch (...)
    {
        printf("Unexpected error running agentexec server\r\n");
    }
    if (execSvr)
    {
        execSvr->stop();
    }

    closedownClientProcess();
    releaseAtoms();
    ExitModuleObjects();

    return 0;
}
