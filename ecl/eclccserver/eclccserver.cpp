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

#include <jlib.hpp>
#include <jmisc.hpp>
#include <jfile.hpp>
#include <jencrypt.hpp>
#include <jregexp.hpp>
#include <mpbase.hpp>
#include <daclient.hpp>
#include <dasess.hpp>
#include <danqs.hpp>
#include <dalienv.hpp>
#include <workunit.hpp>
#include <wujobq.hpp>
#include <dllserver.hpp>
#include <thorplugin.hpp>

static unsigned traceLevel;
static StringAttr dllPath;
Owned<IPropertyTree> globals;

//------------------------------------------------------------------------------------------------------------------
// Class EclccCompileThread does the work of compiling workunits (using eclcc), and optionally then enqueueing them for execution by agentexec.
// A threadpool is used to allow multiple compiles to be submitted at once. Threads are reused when compilation completes.
//------------------------------------------------------------------------------------------------------------------

class EclccCompileThread : public CInterface, implements IPooledThread
{
    StringAttr wuid;
    Owned<IWorkUnit> workunit;

    void reportError(IException *e)
    {
        StringBuffer s;
        reportError(e->errorMessage(s).str(), 2);
    }

    void reportError(const char *errStr, unsigned retcode)
    {
        // MORE - if copyWorkUnit copied errors this would not be needed... should it?
        // A typical error looks like this: stdin:(385,29): warning C1041: Record doesn't have an explicit maximum record size
        // we will also see (and want to skip) nn error(s), nn warning(s)
        RegExpr errCount, errParse;
        errCount.init("[0-9]+ errors?, [0-9]+ warnings?.*");
        errParse.init("^{.+}\\({[0-9]+},{[0-9]+}\\): {[a-z]+} [A-Za-z]*{[0-9]+}:{.*$}");
        if (!errCount.find(errStr))
        {
            Owned<IWUException> err = workunit->createException();
            err->setExceptionSource("eclcc");
            if (errParse.find(errStr))
            {
                StringBuffer file, line, col, errClass, errCode, errText;
                errParse.findstr(file, 1);
                errParse.findstr(line, 2);
                errParse.findstr(col, 3);
                errParse.findstr(errClass, 4);
                errParse.findstr(errCode, 5);
                errParse.findstr(errText, 6);
                err->setExceptionFileName(file);
                err->setExceptionLineNo(atoi(line));
                err->setExceptionColumn(atoi(col));
                if (stricmp(errClass, "warning")==0)
                    err->setSeverity(ExceptionSeverityWarning);
                else
                    err->setSeverity(ExceptionSeverityError);
                err->setExceptionCode(atoi(errCode));
                err->setExceptionMessage(errText);
                err->setExceptionFileName(file); // any point if it just says stdin?
            }
            else
            {
                err->setSeverity(retcode ? ExceptionSeverityError : ExceptionSeverityWarning);
                err->setExceptionMessage(errStr);
                DBGLOG("%s", errStr);
            }
        }
    }

    bool compile(const char *wuid, const char *target, const char *targetCluster)
    {
        StringBuffer eclccCmd("eclcc - -shared");
        Owned<IPropertyTreeIterator> options = globals->getElements("./Option");
        ForEach(*options)
        {
            IPropertyTree &option = options->query();
            const char *name = option.queryProp("@name");
            const char *value = option.queryProp("@value");
            const char *cluster = option.queryProp("@cluster");
            if (name && (cluster==NULL || cluster[0]==0 || strcmp(cluster, targetCluster)==0))
            {
                // options starting '-' are simply passed through to eclcc as name=value
                // others are passed as -foption=value
                // if cluster is set it's specific to a particular target
                eclccCmd.append(" ");
                if (name[0]!='-')
                    eclccCmd.append("-f");
                eclccCmd.append(name);
                if (value)
                    eclccCmd.append('=').append(value);
            }
        }
        eclccCmd.appendf(" -o%s", wuid);
        eclccCmd.appendf(" -target=%s", target);
        Owned<IConstWUQuery> query = workunit->getQuery();
        SCMStringBuffer eclQuery;
        if (!query)
        {
            reportError("Workunit does not contain a query", 2);
            return false;
        }
        query->getQueryText(eclQuery);
        Owned<IStringIterator> debugValues = &workunit->getDebugValues();
        ForEach (*debugValues)
        {
            SCMStringBuffer debugStr, valueStr;
            debugValues->str(debugStr);
            workunit->getDebugValue(debugStr.str(), valueStr);
            if (memicmp(debugStr.str(), "eclcc:", 6) == 0)
            {
                //Allow eclcc:xx:1 so that multiple values can be passed through
                const char * start = debugStr.str() + 6;
                const char * colon = strchr(start, ':');
                StringAttr optName;
                if (colon)
                    optName.set(start, colon-start);
                else
                    optName.set(start);

                if (stricmp(optName, "compileOption") == 0)
                    eclccCmd.appendf(" -Wc,%s", valueStr.str());
                else if (stricmp(optName, "includeLibraryPath") == 0)
                    eclccCmd.appendf(" -I%s", valueStr.str());
                else if (stricmp(optName, "libraryPath") == 0)
                    eclccCmd.appendf(" -L%s", valueStr.str());
                else
                    eclccCmd.appendf(" -%s=%s", start, valueStr.str());
            }
            else
                eclccCmd.appendf(" -f%s=%s", debugStr.str(), valueStr.str());
        }
        if (workunit->getResultLimit())
        {
            eclccCmd.appendf(" -fapplyInstantEclTransformations=1 -fapplyInstantEclTransformationsLimit=%u", workunit->getResultLimit());
        }
        try
        {
            Owned<IPipeProcess> pipe = createPipeProcess();
            Owned<ErrorReader> errorReader = new ErrorReader(pipe, this);
            pipe->run("eclcc", eclccCmd, ".", true, false, true, 0);
            errorReader->start();
            try
            {
                pipe->write(eclQuery.s.length(), eclQuery.s.str());
                pipe->closeInput();
            }
            catch (IException *e)
            {
                reportError(e);
                e->Release();
            }
            unsigned retcode = pipe->wait();
            errorReader->join();
            if (retcode == 0)
            {
                StringBuffer realdllname, dllurl;
                realdllname.append(SharedObjectPrefix).append(wuid).append(SharedObjectExtension);

                StringBuffer realdllfilename(dllPath);
                realdllfilename.append(SharedObjectPrefix).append(wuid).append(SharedObjectExtension);

                StringBuffer wuXML;
                if (getWorkunitXMLFromFile(realdllfilename, wuXML))
                {
                    Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit();
                    embeddedWU->loadXML(wuXML);
                    queryExtendedWU(workunit)->copyWorkUnit(embeddedWU);
                    Owned<IWUQuery> query = workunit->updateQuery();
                    query->setQueryText(eclQuery.s.str());
                }

                createUNCFilename(realdllfilename.str(), dllurl);
                unsigned crc = crc_file(realdllfilename.str());

                Owned<IWUQuery> query = workunit->updateQuery();
                associateLocalFile(query, FileTypeDll, realdllfilename, "Workunit DLL", crc);
                queryDllServer().registerDll(realdllname.str(), "Workunit DLL", dllurl.str());

                workunit->commit();
                return true;
            }
        }
        catch (IException * e)
        {
            reportError(e);
            e->Release();
        }
        return false;
    }

public:
    IMPLEMENT_IINTERFACE;
    virtual void init(void *param)
    {
        wuid.set((const char *) param);
    }
    virtual void main()
    {
        if (traceLevel)
            DBGLOG("Compile request processing for workunit %s", wuid.get());
        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
        workunit.setown(factory->updateWorkUnit(wuid.get()));
        if (!workunit)
        {
            DBGLOG("Workunit %s no longer exists", wuid.get());
            return;
        }
        if (workunit->aborting() || workunit->getState()==WUStateAborted)
        {
            workunit->setState(WUStateAborted);
            DBGLOG("Workunit %s aborted", wuid.get());
            workunit->commit();
            workunit.clear();
            return;
        }
        workunit->setAgentSession(myProcessSession());
        SCMStringBuffer clusterName;
        workunit->getClusterName(clusterName);
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
        if (!clusterInfo)
        {
            StringBuffer errStr;
            errStr.appendf("Cluster %s not recognized", clusterName.str());
            reportError(errStr, 2);
            workunit->setState(WUStateFailed);
            workunit->commit();
            workunit.clear();
            return;
        }
        SCMStringBuffer agentQueue, platform;
        clusterInfo->getAgentQueue(agentQueue);
        clusterInfo->getPlatform(platform);
        clusterInfo.clear();
        workunit->setState(WUStateCompiling);
        workunit->commit();
        bool ok = compile(wuid, platform.str(), clusterName.str());
        if (ok)
        {
            if (isLibrary(workunit))
            {
                workunit->setState(WUStateCompleted);
            }
            else if (agentQueue.length() && workunit->getAction()==WUActionRun || workunit->getAction()==WUActionUnknown)  // Assume they meant run....
            {
                workunit->schedule();
                SCMStringBuffer dllBuff;
                Owned<IConstWUQuery> wuQuery = workunit->getQuery();
                wuQuery->getQueryDllName(dllBuff);
                wuQuery.clear();
                if (dllBuff.length() > 0)
                {
                    workunit.clear();
                    if (!runWorkUnit(wuid, clusterName.str()))
                    {
                        Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                        workunit.setown(factory->updateWorkUnit(wuid));
                        reportError("Failed to execute workunit", 2);
                        if (workunit->getState() != WUStateAborted)
                            workunit->setState(WUStateFailed);
                    }
                }
                else
                    workunit->setState(WUStateFailed);
            }
            else if (workunit->getAction()==WUActionCompile)
                workunit->setState(WUStateCompiled);
        }
        else if (workunit->getState() != WUStateAborted)
            workunit->setState(WUStateFailed);
        if (workunit)
            workunit->commit();
        workunit.clear();
    }

    virtual bool stop()
    {
        return false; // should I try to abort?
    }
    virtual bool canReuse()
    {
        return true;
    }

private:
    // We use a separate thread for reading eclcc's stderr output. This prevents the thread that is
    // writing to its stdin from being blocked because eclcc is trying to write to stderr...
    friend class ErrorReader;
    class ErrorReader : public Thread
    {
    public:
        ErrorReader(IPipeProcess *_pipe, EclccCompileThread *_owner) 
            : Thread("EclccCompileThread::ErrorReader"), pipe(_pipe), owner(_owner)
        {
        }

        virtual int run()
        {
            owner->readErrors(pipe);
            return 0;
        }
    private:
        IPipeProcess *pipe;
        EclccCompileThread *owner;
    };

    void readErrors(IPipeProcess *pipe)
    {
        MemoryAttr buf;
        const size32_t incSize = 512;
        size32_t bufferSize = 0;
        char * buffer = NULL;
        size_t remaining = 0;
        bool eof = false;
        while (!eof)
        {
            if (remaining == bufferSize)
            {
                bufferSize += incSize;
                buffer = (char *)buf.reallocate(bufferSize);
            }
            size32_t read = pipe->readError(bufferSize-remaining, buffer+remaining);

            if ((read == 0) || (read == (size32_t)-1))
                eof = true;
            else
                remaining += read;

            char *finger = buffer;
            while (remaining)
            {
                char *eolpos = (char *) memchr(finger, '\n', remaining);
                if (eolpos)
                {
                    *eolpos = '\0';
                    if (eolpos > finger && eolpos[-1]=='\r')
                        eolpos[-1] = '\0';
                    reportError(finger, 0);
                    remaining -= (eolpos-finger) + 1;
                    finger = eolpos + 1;
                }
                else if (eof)
                {
                    StringBuffer e(remaining, finger);
                    reportError(e, 0);
                    break;
                }
                else
                    break;
            }
            if (!eof && (finger != buffer))
                memmove(buffer, finger, remaining);
        }
    };

};

//------------------------------------------------------------------------------------------------------------------
// Class EclccServer manages a pool of compile threads
//------------------------------------------------------------------------------------------------------------------

class EclccServer : public CInterface, implements IThreadFactory, implements IAbortHandler
{
    StringAttr queueName;
    unsigned poolSize;
    Owned<IThreadPool> pool;

    unsigned threadsActive;
    unsigned maxThreadsActive;
    bool running;
    CSDSServerStatus serverstatus;

public:
    IMPLEMENT_IINTERFACE;
    EclccServer(const char *_queueName, unsigned _poolSize)
        : queueName(_queueName), poolSize(_poolSize), serverstatus("ECLCCserver")
    {
        threadsActive = 0;
        maxThreadsActive = 0;
        running = false;
        pool.setown(createThreadPool("eclccServerPool", this, NULL, poolSize, INFINITE));
        serverstatus.queryProperties()->setProp("@queue",queueName.get());
        serverstatus.commitProperties();
    }

    ~EclccServer()
    {
        pool->joinAll(false, INFINITE);
    }

    void run()
    {
        DBGLOG("eclccServer (%d threads) waiting for requests on queue(s) %s", poolSize, queueName.get());
        Owned<IJobQueue> queue = createJobQueue(queueName.get());
        queue->connect();
        running = true;
        LocalIAbortHandler abortHandler(*this);
        while (running)
        {
            try
            {
                Owned<IJobQueueItem> item = queue->dequeue(1000);
                if (item.get())
                {
                    try
                    {
                        pool->start((void *) item->queryWUID());
                    }
                    catch(IException *e)
                    {
                        StringBuffer m;
                        EXCLOG(e, "eclccServer::run exception");
                        e->Release();
                    }
                    catch(...)
                    {
                        ERRLOG("Unexpected exception in eclccServer::run caught");
                    }
                }
            }
            catch (IException *E)
            {
                EXCLOG(E);
                releaseAtoms();
                ExitModuleObjects();
                _exit(2);
            }
            catch (...)
            {
                DBGLOG("Unknown exception caught in eclccServer::run - restarting");
                releaseAtoms();
                ExitModuleObjects();
                _exit(2);
            }
        }
        DBGLOG("eclccServer closing");
    }

    virtual IPooledThread *createNew()
    {
        return new EclccCompileThread();
    }

    virtual bool onAbort() 
    {
        running = false;
        return false;
    }
};

void openLogFile()
{
    StringBuffer logname;
    if (!envGetConfigurationDirectory("log","eclccserver",globals->queryProp("@name"),logname))
    {
        if (!globals->getProp("@logDir", logname))
        {
            WARNLOG("No logfile directory specified - logs will be written locally");
            logname.clear().append(".");
        }
    }
    addPathSepChar(logname).append("eclccserver");
    StringBuffer aliasLogName(logname);
    aliasLogName.append(".log");
    queryLogMsgManager()->addMonitorOwn( getRollingFileLogMsgHandler(logname.str(), ".log", MSGFIELD_STANDARD, false, true, NULL, aliasLogName.str()), getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, DefaultDetail));
    if (traceLevel)
        DBGLOG("Logging to %s", aliasLogName.str());
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

void initSignals()
{
#ifndef _WIN32
//  signal(SIGTERM, caughtSIGTERM);
    signal(SIGPIPE, caughtSIGPIPE);
    signal(SIGHUP, caughtSIGHUP);
    signal(SIGALRM, caughtSIGALRM);

#endif
}   

int main(int argc, const char *argv[])
{
    InitModuleObjects();
    initSignals();
    NoQuickEditSection x;

    Owned<IFile> sentinelFile = createSentinelTarget();
    // We remove any existing sentinel until we have validated that we can successfully start (i.e. all options are valid...)
    removeSentinelFile(sentinelFile);

    try
    {
        globals.setown(createPTreeFromXMLFile("eclccserver.xml", ipt_caseInsensitive));
    }
    catch (IException * e)
    {
        EXCLOG(e, "Failed to load eclccserver.xml");
        e->Release();
        return 1;
    }
    catch(...)
    {
        ERRLOG("Failed to load eclccserver.xml");
        return 1;
    }

    if (globals->getPropBool("@enableSysLog",true))
        UseSysLogForOperatorMessages();
    const char *daliServers = globals->queryProp("@daliServers");
    if (!daliServers)
    {
        WARNLOG("No Dali server list specified - assuming local");
        daliServers = ".";
    }
    Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
    try
    {
        initClientProcess(serverGroup, DCR_EclServer);
        openLogFile();
        SCMStringBuffer queueNames;
        getEclCCServerQueueNames(queueNames, globals->queryProp("@name"));
        if (!queueNames.length())
            throw MakeStringException(0, "No clusters found to listen on");
        EclccServer server(queueNames.str(), globals->getPropInt("@maxCompileThreads", 1));
        // if we got here, eclserver is successfully started and all options are good, so create the "sentinel file" for re-runs from the script
        // put in its own "scope" to force the flush
        writeSentinelFile(sentinelFile);
        server.run();
    }
    catch (IException * e)
    {
        EXCLOG(e, "Terminating unexpectedly");
        e->Release();
    }
    catch(...)
    {
        ERRLOG("Terminating unexpectedly");
    }
    globals.clear();
    UseSysLogForOperatorMessages(false);
    ::closedownClientProcess(); // dali client closedown
    releaseAtoms();
    ExitModuleObjects();
    return 0;
}
