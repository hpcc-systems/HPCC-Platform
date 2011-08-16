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


void CEclAgentExecutionServer::start(StringBuffer & codeDir)
{
    if (started)
    {
        WARNLOG("START called when already started\n");
        assert(false);
    }

    codeDirectory = codeDir;
    StringBuffer propertyFile = codeDirectory;
    addPathSepChar(propertyFile);
    propertyFile.append("agentexec.xml");
    
    Owned<IPropertyTree> properties;
    try
    {
        DBGLOG("AgentExec: Loading properties file '%s'\n", propertyFile.str());
        properties.setown(createPTreeFromXMLFile(propertyFile.str()));
    }
    catch (IException *e) 
    {
        EXCLOG(e, "Error processing properties file\n");
        throwUnexpected();
    }

    // get the logfile specification
    properties->getProp("@logDir", logDir.clear());
    if (!logDir.length())
    {
        WARNLOG("logDir not specified in properties file - assuming code dir\n");
        logDir.append(codeDir); //default to code dir
        addPathSepChar(logDir);
        logDir.append("logs");      //default folder name
    }
    recursiveCreateDirectory(logDir.str());

    addPathSepChar(logDir);
    rebuildLogfileName();

    //get name of workunit job queue
    StringBuffer sb;
    properties->getProp("@name", sb.clear());
    agentName.set(sb);
    if (!agentName.length())
    {
        ERRLOG("'name' not specified in properties file\n");
        throwUnexpected();
    }

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
    Owned<IFile> sentinelFile = createSentinelTarget();
    removeSentinelFile(sentinelFile);
    try
    {
        Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
        initClientProcess(serverGroup, DCR_EclServer);
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

    writeSentinelFile(sentinelFile);

    try 
    {
        while (started)
        {
            PROGLOG("AgentExec: Waiting on queue(s) '%s'", queueNames.str());
            Owned<IJobQueueItem> item = queue->dequeue(WAIT_FOREVER);
            if (item.get())
            {
                rebuildLogfileName();//rebuild in case date rollover
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
    PROGLOG("Exiting agentexec\n");
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
            PROGLOG("Process failed during execution: %s error(%"I64F"i)", cmdLine.str(), (unsigned __int64) runcode);
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

void CEclAgentExecutionServer::rebuildLogfileName()
{
    logfilespec.clear().append(logDir).append("eclagent");
    addFileTimestamp(logfilespec, true);
    logfilespec.append(".log");
    appendLogFile(logfilespec.str(), 0,0);
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
        StringBuffer codeDirectory;
        splitFilename(argv[0], &codeDirectory, &codeDirectory, NULL, NULL);
        if (!codeDirectory.length())
            codeDirectory.append(".");
        
        execSvr.setown(new CEclAgentExecutionServer());
        execSvr->start(codeDirectory);
    } 
    catch (...)
    {
        printf("Unexpected error running agentexec server\r\n");
    }
    if (execSvr)
    {
        execSvr->stop();
    }

    releaseAtoms();
    ExitModuleObjects();

    return 0;
} 
