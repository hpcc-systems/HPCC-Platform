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

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jisem.hpp"
#include "jfile.hpp"
#include "jencrypt.hpp"
#include "jregexp.hpp"
#include "mpbase.hpp"
#include "daclient.hpp"
#include "dasess.hpp"
#include "danqs.hpp"
#include "dalienv.hpp"
#include "workunit.hpp"
#include "wujobq.hpp"
#include "dllserver.hpp"
#include "thorplugin.hpp"

static StringAttr dllPath;
Owned<IPropertyTree> globals;

//------------------------------------------------------------------------------------------------------------------
// We use a separate thread for reading eclcc's stderr output. This prevents the thread that is
// writing to its stdin from being blocked because eclcc is trying to write to stderr...
//------------------------------------------------------------------------------------------------------------------

interface IErrorReporter
{
    virtual void reportError(IException *e) = 0;
    virtual void reportError(const char *errStr, unsigned retcode) = 0;
};

class ErrorReader : public Thread
{
public:
    ErrorReader(IPipeProcess *_pipe, IErrorReporter *_errorReporter)
        : Thread("EclccCompileThread::ErrorReader"), pipe(_pipe), errorReporter(_errorReporter), errors(0)
    {
    }

    virtual int run()
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
                buffer = (char *) buf.reallocate(bufferSize);
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
                    if (errorReporter)
                        errorReporter->reportError(finger, 0);
                    else
                        DBGLOG("%s", finger);
                    errors++;
                    remaining -= (eolpos-finger) + 1;
                    finger = eolpos + 1;
                }
                else if (eof)
                {
                    StringBuffer e(remaining, finger);
                    if (errorReporter)
                        errorReporter->reportError(e, 0);
                    else
                        DBGLOG("%s", e.str());
                    errors++;
                    break;
                }
                else
                    break;
            }
            if (!eof && (finger != buffer))
                memmove(buffer, finger, remaining);
        }
        return 0;
    }

    unsigned errCount() const
    {
        return errors;
    }
private:
    IPipeProcess *pipe;
    IErrorReporter *errorReporter;
    unsigned errors;
};

//------------------------------------------------------------------------------------------------------------------
// Check for aborts of the workunit as it is compiling
//------------------------------------------------------------------------------------------------------------------

class AbortWaiter : public Thread
{
public:
    AbortWaiter(IPipeProcess *_pipe, IConstWorkUnit *_wu)
        : Thread("EclccCompileThread::AbortWaiter"), pipe(_pipe), wu(_wu)
    {
    }

    virtual int run()
    {
        wu->subscribe(SubscribeOptionAbort);
        try
        {
            for (;;)
            {
                if (sem.wait(2000))
                    break;
                if (wu->aborting())
                {
                    pipe->abort();
                    break;
                }
            }
        }
        catch (IException *E)
        {
            ::Release(E);
        }
        return 0;
    }

    void stop()
    {
        sem.interrupt(NULL);
        join();
    }
private:
    IPipeProcess *pipe;
    IConstWorkUnit *wu;
    InterruptableSemaphore sem;
};

//------------------------------------------------------------------------------------------------------------------
// Class EclccCompileThread does the work of compiling workunits (using eclcc), and optionally then enqueueing them for execution by agentexec.
// A threadpool is used to allow multiple compiles to be submitted at once. Threads are reused when compilation completes.
//------------------------------------------------------------------------------------------------------------------

class EclccCompileThread : implements IPooledThread, implements IErrorReporter, public CInterface
{
    StringAttr wuid;
    Owned<IWorkUnit> workunit;
    StringAttr clusterName;
    StringBuffer idxStr;
    StringArray filesSeen;

    virtual void reportError(IException *e)
    {
        StringBuffer s;
        reportError(e->errorMessage(s).str(), 2);
    }

    virtual void reportError(const char *errStr, unsigned retcode)
    {
        RegExpr errCount, errParse, timings;
        timings.init("^<stat");
        errParse.init("^<exception");
        try
        {
            if (timings.find(errStr))
            {
                OwnedPTree timing = createPTreeFromXMLString(errStr, ipt_fast);
                assertex (timing);
                unsigned __int64 nval = timing->getPropInt64("@value");
                unsigned __int64 nmax = timing->getPropInt64("@max");
                unsigned __int64 cnt = timing->getPropInt64("@count");
                const char * scope = timing->queryProp("@scope");
                StatisticScopeType scopeType = (StatisticScopeType)timing->getPropInt("@scopeType");
                StatisticKind kind = queryStatisticKind(timing->queryProp("@kind"), StKindNone);
                workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), scopeType, scope, kind, NULL, nval, cnt, nmax, StatsMergeReplace);
            }
            else if (errParse.find(errStr))
            {
                OwnedPTree exception = createPTreeFromXMLString(errStr, ipt_fast);
                assertex(exception);
                Owned<IError> error = createError(exception);
                addWorkunitException(workunit, error, false);
                const char *filename = exception->queryProp("@filename");
                StringArray filesSeen;
                if (filename && filesSeen.appendUniq(filename) && endsWithIgnoreCase(filename, ".cpp") && checkFileExists(filename))
                {
                    Owned<IWUQuery> query = workunit->updateQuery();
                    associateLocalFile(query, FileTypeCpp, filename, pathTail(filename), 0, 0, 0);
                }
            }
            else
                DBGLOG("Unrecognised error: %s", errStr);
        }
        catch (IException *E)
        {
            EXCLOG(E, "Error parsing compiler output");
        }
    }

    void processOption(const char *option, const char *value, StringBuffer &eclccCmd, StringBuffer &eclccProgName, IPipeProcess &pipe, bool isLocal)
    {
        if (memicmp(option, "eclcc-", 6) == 0 || *option=='-')
        {
            //Allow eclcc-xx-<n> so that multiple values can be passed through for the same named debug symbol
            const char * start = option + (*option=='-' ? 1 : 6);
            const char * finger = (*start=='-') ? start+1 : start; //support leading double dash
            const char * dash = strrchr(finger, '-');     // position of trailing dash, if present
            StringAttr optName;
            if (dash && (dash != start))
                optName.set(start, dash-start);
            else
                optName.set(start);

            if (!optName)
                return;

            if (stricmp(optName, "hook") == 0)
            {
                if (isLocal)
                    throw MakeStringException(0, "eclcc-hook option can not be set per-workunit");  // for security reasons
                eclccProgName.set(value);
            }
            else if (stricmp(optName, "compileOption") == 0)
                eclccCmd.appendf(" -Wc,%s", value);
            else if (stricmp(optName, "linkOption") == 0)
                eclccCmd.appendf(" -Wl,%s", value);
            else if (stricmp(optName, "includeLibraryPath") == 0)
                eclccCmd.appendf(" -I%s", value);
            else if (stricmp(optName, "libraryPath") == 0)
                eclccCmd.appendf(" -L%s", value);
            else if (strnicmp(optName, "-allow", 6)==0)
            {
                if (isLocal)
                    throw MakeStringException(0, "eclcc-allow option can not be set per-workunit");  // for security reasons
                eclccCmd.appendf(" -%s=%s", optName.get(), value);
            }
            else if (*optName == 'd')
            {
                //Short term work around for the problem that all debug names get lower-cased
                eclccCmd.appendf(" -D%s=%s", optName.get()+1, value);
            }
            else
                eclccCmd.appendf(" -%s=%s", optName.get(), value);
        }
        else if (strchr(option, '-'))
        {
            StringBuffer envVar;
            if (isLocal)
                envVar.append("WU_");
            envVar.append(option);
            envVar.toUpperCase();
            envVar.replace('-','_');
            pipe.setenv(envVar, value);
        }
        else
            eclccCmd.appendf(" -f%s=%s", option, value);
    }

    bool compile(const char *wuid, const char *target, const char *targetCluster)
    {
        Owned<IConstWUQuery> query = workunit->getQuery();
        if (!query)
        {
            reportError("Workunit does not contain a query", 2);
            return false;
        }

        addTimeStamp(workunit, SSTglobal, NULL, StWhenCompiled);

        SCMStringBuffer mainDefinition;
        SCMStringBuffer eclQuery;
        query->getQueryText(eclQuery);
        query->getQueryMainDefinition(mainDefinition);

        StringBuffer eclccProgName;
        splitDirTail(queryCurrentProcessPath(), eclccProgName);
        eclccProgName.append("eclcc");
        StringBuffer eclccCmd(" -shared");
        if (eclQuery.length())
            eclccCmd.append(" -");
        if (mainDefinition.length())
            eclccCmd.append(" -main ").append(mainDefinition);
        eclccCmd.append(" --timings --xml");
        eclccCmd.append(" --nostdinc");
        eclccCmd.append(" --metacache=");
        VStringBuffer logfile("%s.eclcc.log", workunit->queryWuid());
        eclccCmd.appendf(" --logfile=%s", logfile.str());
        if (globals->getPropBool("@enableEclccDali", true))
        {
            const char *daliServers = globals->queryProp("@daliServers");
            if (!daliServers)
                daliServers = ".";
            eclccCmd.appendf(" -dfs=%s", daliServers);
            const char *wuScope = workunit->queryWuScope();
            if (!isEmptyString(wuScope))
                eclccCmd.appendf(" -scope=%s", wuScope);
            eclccCmd.appendf(" -cluster=%s", targetCluster);
            SCMStringBuffer token;
            workunit->getWorkunitDistributedAccessToken(token);
            if (token.length())
                eclccCmd.appendf(" -wuid=%s -token=%s", workunit->queryWuid(), token.str());
        }
        Owned<IPipeProcess> pipe = createPipeProcess();
        pipe->setenv("ECLCCSERVER_THREAD_INDEX", idxStr.str());
        Owned<IPropertyTreeIterator> options = globals->getElements("./Option");
        ForEach(*options)
        {
            IPropertyTree &option = options->query();
            const char *name = option.queryProp("@name");
            const char *value = option.queryProp("@value");
            const char *cluster = option.queryProp("@cluster");                // if cluster is set it's specific to a particular target
            if (name && (cluster==NULL || cluster[0]==0 || strcmp(cluster, targetCluster)==0))
                processOption(name, value, eclccCmd, eclccProgName, *pipe, false);
        }
        eclccCmd.appendf(" -o%s", wuid);
        eclccCmd.appendf(" -platform=%s", target);
        eclccCmd.appendf(" --component=%s", queryStatisticsComponentName());

        Owned<IStringIterator> debugValues = &workunit->getDebugValues();
        ForEach (*debugValues)
        {
            SCMStringBuffer debugStr, valueStr;
            debugValues->str(debugStr);
            workunit->getDebugValue(debugStr.str(), valueStr);
            processOption(debugStr.str(), valueStr.str(), eclccCmd, eclccProgName, *pipe, true);
        }
        if (workunit->getResultLimit())
        {
            eclccCmd.appendf(" -fapplyInstantEclTransformations=1 -fapplyInstantEclTransformationsLimit=%u", workunit->getResultLimit());
        }
        try
        {
            Owned<ErrorReader> errorReader = new ErrorReader(pipe, this);
            Owned<AbortWaiter> abortWaiter = new AbortWaiter(pipe, workunit);
            eclccCmd.insert(0, eclccProgName);
            if (!pipe->run(eclccProgName, eclccCmd, ".", true, false, true, 0, true))
                throw makeStringExceptionV(999, "Failed to run eclcc command %s", eclccCmd.str());
            errorReader->start();
            abortWaiter->start();
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
            abortWaiter->stop();
            if (retcode == 0)
            {
                StringBuffer realdllname, dllurl;
                realdllname.append(SharedObjectPrefix).append(wuid).append(SharedObjectExtension);

                StringBuffer realdllfilename(dllPath);
                realdllfilename.append(SharedObjectPrefix).append(wuid).append(SharedObjectExtension);

                StringBuffer wuXML;
                if (!getWorkunitXMLFromFile(realdllfilename, wuXML))
                    throw makeStringException(999, "Failed to extract workunit from query dll");

                Owned<ILocalWorkUnit> embeddedWU = createLocalWorkUnit(wuXML);
                queryExtendedWU(workunit)->copyWorkUnit(embeddedWU, false, true);
                workunit->setIsClone(false);
                const char *jobname = embeddedWU->queryJobName();
                if (jobname && *jobname) //let ECL win naming job during initial compile
                    workunit->setJobName(jobname);
                if (!workunit->getDebugValueBool("obfuscateOutput", false))
                {
                    Owned<IWUQuery> query = workunit->updateQuery();
                    query->setQueryText(eclQuery.s.str());
                }

                createUNCFilename(realdllfilename.str(), dllurl);
                unsigned crc = crc_file(realdllfilename.str());
                Owned<IWUQuery> query = workunit->updateQuery();
                associateLocalFile(query, FileTypeLog, logfile, "Compiler log", 0);
                associateLocalFile(query, FileTypeDll, realdllfilename, "Workunit DLL", crc);
                queryDllServer().registerDll(realdllname.str(), "Workunit DLL", dllurl.str());
                workunit->commit();
                return true;
            }
            else
            {
                Owned<IWUQuery> query = workunit->updateQuery();
                associateLocalFile(query, FileTypeLog, logfile, "Compiler log", 0);
                workunit->commit();
                return false;
            }
        }
        catch (IException * e)
        {
            reportError(e);
            e->Release();
        }
        workunit->commit();
        return false;
    }

    void failCompilation(const char *error)
    {
        reportError(error, 2);
        workunit->setState(WUStateFailed);
        workunit->commit();
        workunit.clear();
    }

public:
    IMPLEMENT_IINTERFACE;
    EclccCompileThread(const char *_clusterName, unsigned _idx) : clusterName(_clusterName)
    {
        idxStr.append(_idx);
    }

    virtual void init(void *param) override
    {
        wuid.set((const char *) param);
    }
    virtual void threadmain() override
    {
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
        CSDSServerStatus serverstatus("ECLCCserverThread");
        serverstatus.queryProperties()->setProp("@cluster",clusterName.get());
        serverstatus.queryProperties()->setProp("@thread", idxStr.str());
        serverstatus.queryProperties()->setProp("WorkUnit",wuid.get());
        serverstatus.commitProperties();
        workunit->setAgentSession(myProcessSession());
        StringAttr clusterName(workunit->queryClusterName());
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
        if (!clusterInfo)
        {
            VStringBuffer errStr("Cluster %s not recognized", clusterName.str());
            failCompilation(errStr);
            return;
        }
        ClusterType platform = clusterInfo->getPlatform();
        clusterInfo.clear();
        workunit->setState(WUStateCompiling);
        workunit->commit();
        bool ok = false;
        try
        {
            ok = compile(wuid, clusterTypeString(platform, true), clusterName.str());
        }
        catch (IException * e)
        {
            StringBuffer msg;
            e->errorMessage(msg);
            addExceptionToWorkunit(workunit, SeverityError, "eclccserver", e->errorCode(), msg.str(), NULL, 0, 0, 0);
            e->Release();
        }
        if (ok)
        {
            workunit->setState(WUStateCompiled);
            const char *newClusterName = workunit->queryClusterName();   // Workunit can change the cluster name via #workunit, so reload it
            if (strcmp(newClusterName, clusterName.str()) != 0)
            {
                clusterInfo.setown(getTargetClusterInfo(newClusterName));
                if (!clusterInfo)
                {
                    VStringBuffer errStr("Cluster %s by #workunit not recognized", newClusterName);
                    failCompilation(errStr);
                    return;
                }
                if (platform != clusterInfo->getPlatform())
                {
                    VStringBuffer errStr("Cluster %s specified by #workunit is wrong type for this queue", newClusterName);
                    failCompilation(errStr);
                    return;
                }
                clusterInfo.clear();
            }
            if (workunit->getAction()==WUActionRun || workunit->getAction()==WUActionUnknown)  // Assume they meant run....
            {
                if (isLibrary(workunit))
                {
                    workunit->setState(WUStateCompleted);
                }
                else
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
                    {
                        reportError("Failed to execute workunit (unknown DLL name)", 2);
                        workunit->setState(WUStateFailed);
                    }
                }
            }
        }
        else if (workunit->getState() != WUStateAborted)
            workunit->setState(WUStateFailed);
        if (workunit)
            workunit->commit();
        workunit.clear();
    }

    virtual bool stop() override
    {
        return false; // should I try to abort?
    }
    virtual bool canReuse() const override
    {
        return true;
    }
};

#ifndef _WIN32
static void generatePrecompiledHeader()
{
    try
    {
        Owned<IPipeProcess> pipe = createPipeProcess();
        Owned<ErrorReader> errorReader = new ErrorReader(pipe, NULL);
        StringBuffer cmd;
        splitDirTail(queryCurrentProcessPath(), cmd);
        cmd.append("eclcc -pch");
        if (pipe->run("eclcc", cmd, ".", false, false, true, 0))
        {
            errorReader->start();
            unsigned retcode = pipe->wait();
            errorReader->join();
            if (retcode != 0 || errorReader->errCount() != 0)
                throw MakeStringException(0, "eclcc -pch failed");
            DBGLOG("Created precompiled header");
        }
    }
    catch (IException * e)
    {
        EXCLOG(e, "Creating precompiled header");
        e->Release();
    }
}

static void removePrecompiledHeader()
{
    removeFileTraceIfFail("eclinclude4.hpp.gch");
}
#endif

//------------------------------------------------------------------------------------------------------------------
// Class EclccServer manages a pool of compile threads
//------------------------------------------------------------------------------------------------------------------

class EclccServer : public CInterface, implements IThreadFactory, implements IAbortHandler
{
    StringAttr queueName;
    StringAttr clusterName;
    unsigned poolSize;
    Owned<IThreadPool> pool;

    unsigned threadsActive;
    CriticalSection threadActiveCrit;
    bool running;
    CSDSServerStatus serverstatus;
    Owned<IJobQueue> queue;

public:
    IMPLEMENT_IINTERFACE;
    EclccServer(const char *_queueName, const char *_clusterName, unsigned _poolSize)
        : queueName(_queueName), clusterName(_clusterName), poolSize(_poolSize), serverstatus("ECLCCserver")
    {
        threadsActive = 0;
        running = false;
        pool.setown(createThreadPool("eclccServerPool", this, NULL, poolSize, INFINITE));
        serverstatus.queryProperties()->setProp("@cluster",clusterName.get());
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
        queue.setown(createJobQueue(queueName.get()));
        queue->connect(false);
        running = true;
        LocalIAbortHandler abortHandler(*this);
        while (running)
        {
            try
            {
                Owned<IJobQueueItem> item = queue->dequeue();
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
        CriticalBlock b(threadActiveCrit);
        return new EclccCompileThread(clusterName, threadsActive++);
    }

    virtual bool onAbort() 
    {
        running = false;
        if (queue)
            queue->cancelAcceptConversation();
        return false;
    }
};

void openLogFile()
{
    StringBuffer logname;
    envGetConfigurationDirectory("log","eclccserver",globals->queryProp("@name"),logname);
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logname.str(), "eclccserver");
    lf->beginLogging();
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
    for (unsigned i=0;i<(unsigned)argc;i++) {
        if (streq(argv[i],"--daemon") || streq(argv[i],"-d")) {
            if (daemon(1,0) || write_pidfile(argv[++i])) {
                perror("Failed to daemonize");
                return EXIT_FAILURE;
            }
            break;
        }
    }

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

    const char * processName = globals->queryProp("@name");
    setStatisticsComponentName(SCTeclcc, processName, true);
    if (globals->getPropBool("@enableSysLog",true))
        UseSysLogForOperatorMessages();
#ifndef _WIN32
    if (globals->getPropBool("@generatePrecompiledHeader",true))
        generatePrecompiledHeader();
    else
        removePrecompiledHeader();
#endif

    const char *daliServers = globals->queryProp("@daliServers");
    if (!daliServers)
    {
        WARNLOG("No Dali server list specified - assuming local");
        daliServers = ".";
    }
    Owned<IGroup> serverGroup = createIGroup(daliServers, DALI_SERVER_PORT);
    try
    {
        initClientProcess(serverGroup, DCR_EclCCServer);
        openLogFile();
        unsigned optMonitorInterval = globals->getPropInt("@monitorInterval", 60);
        if (optMonitorInterval)
            startPerformanceMonitor(optMonitorInterval*1000, PerfMonStandard, nullptr);
        SCMStringBuffer queueNames;
        getEclCCServerQueueNames(queueNames, processName);
        if (!queueNames.length())
            throw MakeStringException(0, "No clusters found to listen on");
        // The option has been renamed to avoid confusion with the similarly-named eclcc option, but
        // still accept the old name if the new one is not present.
        unsigned maxThreads = globals->getPropInt("@maxEclccProcesses", globals->getPropInt("@maxCompileThreads", 4));
        EclccServer server(queueNames.str(), processName, maxThreads);
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
    stopPerformanceMonitor();
    globals.clear();
    UseSysLogForOperatorMessages(false);
    ::closedownClientProcess(); // dali client closedown
    releaseAtoms();
    ExitModuleObjects();
    return 0;
}
