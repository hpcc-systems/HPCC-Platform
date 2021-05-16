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
#include "jcomp.hpp"
#include "mpbase.hpp"
#include "daclient.hpp"
#include "dasess.hpp"
#include "danqs.hpp"
#include "workunit.hpp"
#include "wujobq.hpp"
#include "dllserver.hpp"
#include "thorplugin.hpp"
#include "daqueue.hpp"
#ifndef _CONTAINERIZED
#include "dalienv.hpp"
#endif
#include <unordered_map>
#include <string>
#include "codesigner.hpp"

static const char * * globalArgv = nullptr;

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
    StringBuffer idxStr;
    StringArray filesSeen;
    unsigned defaultMaxCompileThreads = 1;
    bool saveTemps = false;

    virtual void reportError(IException *e)
    {
        StringBuffer s;
        reportError(e->errorMessage(s).str(), 2);
    }

    virtual void reportError(const char *errStr, unsigned retcode)
    {
        RegExpr errParse, timings, summaryParse;
        timings.init("^<stat");
        errParse.init("^<exception");
        summaryParse.init("^<summary");
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
            else if (!summaryParse.find(errStr))
                IERRLOG("%s", errStr);
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
        {
            if (strieq(option, "maxCompileThreads"))
                defaultMaxCompileThreads = atoi(value);
            else if (strieq(option, "saveEclTempFiles"))
                saveTemps = strToBool(value);
            eclccCmd.appendf(" -f%s=%s", option, value);
        }
    }

    unsigned executeLine(const char *line, StringBuffer &output, bool alreadyFailed)
    {
        if (isEmptyString(line))
            return 0;
        if (line[0] == '#')
        {
            return 0;
        }
        else if (startsWith(line, "rmdir "))
        {
            DBGLOG("Removing directory %s", line+6);
            Owned<IFile> tempDir = createIFile(line+6);
            if (tempDir)
                recursiveRemoveDirectory(tempDir);
            return 0;
        }
        else if (startsWith(line , "rm "))
        {
            DBGLOG("Removing file %s", line+3);
            remove(line + 3);
            return 0;
        }
        else if (!alreadyFailed)
        {
            DBGLOG("Executing %s", line);
            unsigned retcode = runExternalCommand(nullptr, output, output, line, nullptr);
            if (retcode)
                DBGLOG("Error: retcode=%u executing %s", retcode, line);
            return retcode;
        }
        return 0;
    }
    unsigned doCompileCpp(const char *wuid, unsigned maxThreads)
    {
        RelaxedAtomic<unsigned> numFailed = { 0 };
        if (!maxThreads)
            maxThreads = 1;
        VStringBuffer ccfileName("%s.cc", wuid);
        VStringBuffer cclogfileName("%s.cc.log", wuid);
        char dir[_MAX_PATH];
        if (!GetCurrentDirectory(sizeof(dir), dir))
            strcpy(dir, "unknown");
        DBGLOG("Compiling generated file list from %s, current directory %s", ccfileName.str(), dir);
        StringBuffer ccfileContents;
        ccfileContents.loadFile(ccfileName, false);
        StringArray lines;
        lines.appendList(ccfileContents, "\n", false);
        // We expect #compile, then a bunch of compiles that we may do in parallel, followed by #link and/or #cleanup, after which we go back to sequential again
        if (!lines.length())
        {
            DBGLOG("Invalid compiler batch file %s - 0 lines found", ccfileName.str());
            return 1;
        }
        unsigned lineIdx = 0;
        StringBuffer output;
        if (streq(lines.item(lineIdx), "#compile"))
        {
            lineIdx++;
            unsigned firstCompile = lineIdx;
            while (lines.isItem(lineIdx) && lines.item(lineIdx)[0] != '#')
                lineIdx++;
            CriticalSection crit;
            DBGLOG("Compiling %u files, %u at once", lineIdx-firstCompile, maxThreads);
            asyncFor(lineIdx-firstCompile, maxThreads, [this, firstCompile, &lines, &numFailed, &crit, &output](unsigned i)
            {
                try
                {
                    StringBuffer loutput;
                    if (executeLine(lines.item(i+firstCompile), loutput, (numFailed != 0)))
                        numFailed++;
                    CriticalBlock b(crit);
                    output.append(loutput);
                }
                catch (...)
                {
                    numFailed++;
                    throw;
                }
            });
        }
        while (lines.isItem(lineIdx))
        {
            const char *line = lines.item(lineIdx);
            unsigned retcode = executeLine(line, output, (numFailed != 0));
            if (retcode)
                numFailed++;
            lineIdx++;
        }
        if (!saveTemps)
            removeFileTraceIfFail(ccfileName);

        Owned <IFile> dstfile = createIFile(cclogfileName);
        dstfile->remove();
        if (output.length())
        {
            Owned<IFileIO> dstIO = dstfile->open(IFOwrite);
            dstIO->write(0, output.length(), output.str());

            IArrayOf<IError> errors;
            extractErrorsFromCppLog(errors, output.str(), numFailed != 0);
            ForEachItemIn(i, errors)
                addWorkunitException(workunit, &errors.item(i), false);

            VStringBuffer failText("Compile/Link failed for %s (see %s for details)",wuid,cclogfileName.str());
            Owned<IWUException> msg = workunit->createException();
            msg->setExceptionSource("eclccserver");
            msg->setExceptionCode(3000);
            msg->setExceptionMessage(failText);
            msg->setSeverity(SeverityError);
        }
        return numFailed;
    }

#ifdef _CONTAINERIZED
    void removeGeneratedFiles(const char *wuid)
    {
        // Remove the files we generated into /tmp - any we want to retain will have been moved to dllserver dir when registered with workunit
        VStringBuffer temp("*%s.*", wuid);
        Owned<IDirectoryIterator> tempfiles = createDirectoryIterator(".", temp.str());
        ForEach(*tempfiles)
        {
            removeFileTraceIfFail(tempfiles->getName(temp.clear()).str());
        }

    }
#endif

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

        bool syntaxCheck = (workunit->getAction()==WUActionCheck);
        if (syntaxCheck && (mainDefinition.length() == 0))
        {
            SCMStringBuffer syntaxCheckAttr;
            workunit->getApplicationValue("SyntaxCheck", "AttributeName", syntaxCheckAttr);
            syntaxCheckAttr.s.trim();
            if (syntaxCheckAttr.length())
            {
                workunit->getApplicationValue("SyntaxCheck", "ModuleName", mainDefinition);
                mainDefinition.s.trim();
                if (mainDefinition.length())
                    mainDefinition.s.append('.');
                mainDefinition.s.append(syntaxCheckAttr.str());
            }
        }

        StringBuffer eclccProgName;
        splitDirTail(queryCurrentProcessPath(), eclccProgName);
        eclccProgName.append("eclcc");
        StringBuffer eclccCmd(" -shared");
        //Clone all the options that were passed to eclccserver (but not the filename) and also pass them to eclcc
        for (const char * * pArg = globalArgv+1; *pArg; pArg++)
            eclccCmd.append(' ').append(*pArg);

        if (eclQuery.length())
            eclccCmd.append(" -");
        if (mainDefinition.length())
            eclccCmd.append(" -main \"").append(mainDefinition).append("\"");
        eclccCmd.append(" --timings --xml");
        eclccCmd.append(" --nostdinc");
        eclccCmd.append(" --metacache=");

#ifdef _CONTAINERIZED
        /* stderr is reserved for actual errors, and is consumed by this (parent) process
         * stdout is unused and will be captured by container logging mechanism */
        eclccCmd.append(" --logtostdout");
#else
        VStringBuffer logfile("%s.eclcc.log", workunit->queryWuid());
        eclccCmd.appendf(" --logfile=%s", logfile.str());
#endif

        if (syntaxCheck)
            eclccCmd.appendf(" -syntax");

        Owned<IPropertyTree> config = getComponentConfig();
        if (config->getPropBool("@enableEclccDali", true))
        {
            const char *daliServers = config->queryProp("@daliServers");
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
        Owned<IPropertyTreeIterator> options = config->getElements("./Option");
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
        bool compileCppSeparately = config->getPropBool("@compileCppSeparately", true);
        if (compileCppSeparately)
        {
            workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTcompilestage, "compile", StWhenStarted, NULL, getTimeStampNowValue(), 1, 0, StatsMergeAppend);
            eclccCmd.appendf(" -Sx %s.cc", wuid);
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
            cycle_t startCompile = get_cycles_now();
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
            if (retcode == 0 && compileCppSeparately)
            {
                cycle_t startCompileCpp = get_cycles_now();
                workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTcompilestage, "compile:compile c++", StWhenStarted, NULL, getTimeStampNowValue(), 1, 0, StatsMergeAppend);
                retcode = doCompileCpp(wuid, workunit->getDebugValueInt("maxCompileThreads", defaultMaxCompileThreads));
                unsigned __int64 elapsed_compilecpp = cycle_to_nanosec(get_cycles_now() - startCompileCpp);
                workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTcompilestage, "compile:compile c++", StTimeElapsed, NULL, elapsed_compilecpp, 1, 0, StatsMergeReplace);
            }
            if (compileCppSeparately)
            {
                unsigned __int64 elapsed_compile = cycle_to_nanosec(get_cycles_now() - startCompile);
                workunit->setStatistic(queryStatisticsComponentType(), queryStatisticsComponentName(), SSTcompilestage, "compile", StTimeElapsed, NULL, elapsed_compile, 1, 0, StatsMergeReplace);
            }
            if (retcode == 0)
            {
                StringBuffer realdllname, dllurl;
                realdllname.append(SharedObjectPrefix).append(wuid).append(SharedObjectExtension);

                StringBuffer realdllfilename;
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
                    if (getArchiveXMLFromFile(realdllfilename, wuXML.clear()))  // MORE - if what was submitted was an archive, this is probably pointless?
                        query->setQueryText(wuXML.str());
                    else
                        query->setQueryText(eclQuery.s.str());
                }

                createUNCFilename(realdllfilename.str(), dllurl);
                unsigned crc = crc_file(realdllfilename.str());
                Owned<IWUQuery> query = workunit->updateQuery();
#ifndef _CONTAINERIZED
                associateLocalFile(query, FileTypeLog, logfile, "Compiler log", 0);
#endif
                associateLocalFile(query, FileTypeDll, realdllfilename, "Workunit DLL", crc);
#ifdef _CONTAINERIZED
                removeGeneratedFiles(wuid);
#endif
                queryDllServer().registerDll(realdllname.str(), "Workunit DLL", dllurl.str());
            }
            else
            {
#ifndef _CONTAINERIZED
                Owned<IWUQuery> query = workunit->updateQuery();
                associateLocalFile(query, FileTypeLog, logfile, "Compiler log", 0);
#endif
            }

            if (compileCppSeparately)
            {
                //Files need to be added after the workunit has been cloned - otherwise they are overwritten
                Owned<IWUQuery> query = workunit->updateQuery();
                VStringBuffer ccfileName("%s.cc", wuid);
                VStringBuffer cclogfileName("%s.cc.log", wuid);
                if (checkFileExists(cclogfileName))
                    associateLocalFile(query, FileTypeLog, cclogfileName, "CPP log", 0);
                if (saveTemps && checkFileExists(ccfileName))
                    associateLocalFile(query, FileTypeLog, ccfileName, "compile actions log", 0);
            }

            workunit->commit();
            return (retcode == 0);
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
    EclccCompileThread(unsigned _idx)
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
        Owned<IPropertyTree> config = getComponentConfig();
#ifdef _CONTAINERIZED
        if (!config->getPropBool("@useChildProcesses", false) && !config->hasProp("@workunit"))
        {
            Owned<IException> error;
            try
            {
                runK8sJob("compile", wuid, wuid);
            }
            catch (IException *E)
            {
                error.setown(E);
            }
            if (error)
            {
                Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
                workunit.setown(factory->updateWorkUnit(wuid.get()));
                if (workunit)
                {
                    if (workunit->aborting())
                        workunit->setState(WUStateAborted);
                    else if (workunit->getState()!=WUStateAborted)
                    {
                        StringBuffer msg;
                        error->errorMessage(msg);
                        addExceptionToWorkunit(workunit, SeverityError, "eclccserver", error->errorCode(), msg.str(), NULL, 0, 0, 0);
                        workunit->setState(WUStateFailed);
                    }
                }
                workunit->commit();
                workunit.clear();
            }
            return;
        }
#endif
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
        serverstatus.queryProperties()->setProp("@cluster", config->queryProp("@name"));
        serverstatus.queryProperties()->setProp("@thread", idxStr.str());
        serverstatus.queryProperties()->setProp("WorkUnit", wuid.get());
        serverstatus.commitProperties();
        workunit->setAgentSession(myProcessSession());
        StringAttr clusterName(workunit->queryClusterName());
#ifdef _CONTAINERIZED
        VStringBuffer xpath("queues[@name='%s']", clusterName.str());
        Owned<IPropertyTree> queueInfo = config->getBranch(xpath);
        assertex(queueInfo);
        const char *platformName = queueInfo->queryProp("@type");
#else
        Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(clusterName.str());
        if (!clusterInfo)
        {
            VStringBuffer errStr("Cluster %s not recognized", clusterName.str());
            failCompilation(errStr);
            return;
        }
        ClusterType platform = clusterInfo->getPlatform();
        const char *platformName = clusterTypeString(platform, true);
        clusterInfo.clear();
#endif
        workunit->setState(WUStateCompiling);
        workunit->commit();
        bool ok = false;
        try
        {
            ok = compile(wuid, platformName, clusterName.str());
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
            // Workunit can change the cluster name via #workunit, so reload it
            // Note that we need a StringAttr here not a char * as the lifetime of the value returned from queryClusterName is
            // only until the workunit object is released.
            StringAttr newClusterName = workunit->queryClusterName();
            if (strcmp(newClusterName, clusterName.str()) != 0)
            {
#ifdef _CONTAINERIZED
                // MORE?
#else
                clusterInfo.setown(getTargetClusterInfo(newClusterName));
                if (!clusterInfo)
                {
                    VStringBuffer errStr("Cluster %s by #workunit not recognized", newClusterName.str());
                    failCompilation(errStr);
                    return;
                }
                if (platform != clusterInfo->getPlatform())
                {
                    VStringBuffer errStr("Cluster %s specified by #workunit is wrong type for this queue", newClusterName.str());
                    failCompilation(errStr);
                    return;
                }
                clusterInfo.clear();
#endif
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
                        if (!runWorkUnit(wuid, newClusterName))
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

static StringBuffer &getQueues(StringBuffer &queueNames)
{
    Owned<IPropertyTree> config = getComponentConfig();
#ifdef _CONTAINERIZED
    bool filtered = false;
    std::unordered_map<std::string, bool> listenQueues;
    Owned<IPTreeIterator> listening = config->getElements("listen");
    ForEach (*listening)
    {
        const char *lq = listening->query().queryProp(".");
        if (lq)
        {
            listenQueues[lq] = true;
            filtered = true;
        }
    }
    Owned<IPTreeIterator> queues = config->getElements("queues");
    ForEach(*queues)
    {
        IPTree &queue = queues->query();
        const char *qname = queue.queryProp("@name");
        if (!filtered || listenQueues.count(qname))
        {
            if (queueNames.length())
                queueNames.append(",");
            getClusterEclCCServerQueueName(queueNames, qname);
        }
    }
#else
    const char * processName = config->queryProp("@name");
    SCMStringBuffer scmQueueNames;
    getEclCCServerQueueNames(scmQueueNames, processName);
    queueNames.append(scmQueueNames.str());
#endif
    return queueNames;
}

class EclccServer : public CInterface, implements IThreadFactory, implements IAbortHandler
{
    StringAttr queueNames;
    unsigned poolSize;
    Owned<IThreadPool> pool;

    unsigned threadsActive;
    CriticalSection threadActiveCrit;
    bool running;
    CSDSServerStatus serverstatus;
    Owned<IJobQueue> queue;
    CriticalSection queueUpdateCS;
    StringAttr updatedQueueNames;
    CConfigUpdateHook reloadConfigHook;


    void configUpdate()
    {
        StringBuffer newQueueNames;
        getQueues(newQueueNames);
        if (!newQueueNames.length())
            ERRLOG("No queues found to listen on");
        Linked<IJobQueue> currentQueue;
        {
            CriticalBlock b(queueUpdateCS);
            if (strsame(queueNames, newQueueNames))
                return;
            updatedQueueNames.set(newQueueNames);
            currentQueue.set(queue);
            PROGLOG("Updating queue due to queue names change from '%s' to '%s'", queueNames.str(), newQueueNames.str());
        }
        if (currentQueue)
            currentQueue->cancelAcceptConversation();
    }
public:
    IMPLEMENT_IINTERFACE;
    EclccServer(const char *_queueName, unsigned _poolSize)
        : updatedQueueNames(_queueName), poolSize(_poolSize), serverstatus("ECLCCserver")
    {
        threadsActive = 0;
        running = false;
        pool.setown(createThreadPool("eclccServerPool", this, NULL, poolSize, INFINITE));
        serverstatus.queryProperties()->setProp("@cluster", getComponentConfigSP()->queryProp("@name"));
        serverstatus.commitProperties();
        reloadConfigHook.installOnce(std::bind(&EclccServer::configUpdate, this), false);
    }

    ~EclccServer()
    {
        reloadConfigHook.clear();
        pool->joinAll(false, INFINITE);
    }

    void run()
    {
        running = true;
        LocalIAbortHandler abortHandler(*this);
        while (running)
        {
            try
            {
                bool newQueues = false;
                {
                    CriticalBlock b(queueUpdateCS);
                    if (updatedQueueNames)
                    {
                        queueNames.set(updatedQueueNames);
                        updatedQueueNames.clear();
                        queue.clear();
                        queue.setown(createJobQueue(queueNames.get()));
                        newQueues = true;
                    }
                    // onAbort could have triggered before or during the above switch, if so, we do no want to connect/block on new queue
                    if (!running)
                        break;
                }
                if (newQueues)
                {
                    queue->connect(false);
                    serverstatus.queryProperties()->setProp("@queue", queueNames.get());
                    serverstatus.commitProperties();
                    DBGLOG("eclccServer (%d threads) waiting for requests on queue(s) %s", poolSize, queueNames.get());
                }
                if (!pool->waitAvailable(10000))
                {
                    if (getComponentConfigSP()->getPropInt("@traceLevel", 0) > 2)
                        DBGLOG("Blocked for 10 seconds waiting for an available compiler thread");
                    continue;
                }
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
                        IERRLOG("Unexpected exception in eclccServer::run caught");
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
                IERRLOG("Unknown exception caught in eclccServer::run - restarting");
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
        return new EclccCompileThread(threadsActive++);
    }

    virtual bool onAbort() 
    {
        running = false;
        Linked<IJobQueue> currentQueue;
        {
            CriticalBlock b(queueUpdateCS);
            if (queue)
                currentQueue.set(queue);
        }
        if (currentQueue)
            currentQueue->cancelAcceptConversation();
        return false;
    }
};

void openLogFile()
{
#ifndef _CONTAINERIZED
    StringBuffer logname;
    envGetConfigurationDirectory("log","eclccserver", getComponentConfigSP()->queryProp("@name"),logname);
    Owned<IComponentLogFileCreator> lf = createComponentLogFileCreator(logname.str(), "eclccserver");
    lf->beginLogging();
#else
    setupContainerizedLogMsgHandler();
#endif
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

void initSignals()
{
#ifndef _WIN32
    signal(SIGPIPE, caughtSIGPIPE);
    signal(SIGHUP, caughtSIGHUP);
    signal(SIGALRM, caughtSIGALRM);
#endif
}

static constexpr const char * defaultYaml = R"!!(
version: "1.0"
eclccserver:
  daliServers: dali
  enableEclccDali: true
  enableSysLog: true
  generatePrecompiledHeader: true
  useChildProcesses: false
  name: myeclccserver
  traceLevel: 1
)!!";



int main(int argc, const char *argv[])
{
    if (!checkCreateDaemon(argc, argv))
        return EXIT_FAILURE;

    InitModuleObjects();
    initSignals();
    NoQuickEditSection x;

    Owned<IFile> sentinelFile = createSentinelTarget();
    // We remove any existing sentinel until we have validated that we can successfully start (i.e. all options are valid...)
    removeSentinelFile(sentinelFile);

    Owned<IPropertyTree> globals;
    try
    {
        globalArgv = argv;
        globals.setown(loadConfiguration(defaultYaml, argv, "eclccserver", "ECLCCSERVER", "eclccserver.xml", nullptr));
    }
    catch (IException * e)
    {
        UERRLOG(e);
        e->Release();
        return 1;
    }
    catch(...)
    {
        OERRLOG("Failed to load configuration");
        return 1;
    }
    const char * processName = globals->queryProp("@name");
    setStatisticsComponentName(SCTeclcc, processName, true);
    if (globals->getPropBool("@enableSysLog",true))
        UseSysLogForOperatorMessages();
#ifndef _CONTAINERIZED
#ifndef _WIN32
    if (globals->getPropBool("@generatePrecompiledHeader", true))
        generatePrecompiledHeader();
    else
        removePrecompiledHeader();
#endif
#endif

    const char *daliServers = globals->queryProp("@daliServers");
    if (!daliServers)
    {
        UWARNLOG("No Dali server list specified - assuming local");
        daliServers = ".";
    }
    Owned<IGroup> serverGroup = createIGroupRetry(daliServers, DALI_SERVER_PORT);
    try
    {
        initClientProcess(serverGroup, DCR_EclCCServer);
        openLogFile();
        const char *wuid = globals->queryProp("@workunit");
        if (wuid)
        {
            // One shot mode
            EclccCompileThread compiler(0);
            compiler.init(const_cast<char *>(wuid));
            compiler.threadmain();
        }
        else
        {
#ifndef _CONTAINERIZED
            unsigned optMonitorInterval = globals->getPropInt("@monitorInterval", 60);
            if (optMonitorInterval)
                startPerformanceMonitor(optMonitorInterval*1000, PerfMonStandard, nullptr);
#endif

            StringBuffer queueNames;
            getQueues(queueNames);
            if (!queueNames.length())
                throw MakeStringException(0, "No queues found to listen on");

#ifdef _CONTAINERIZED
            queryCodeSigner().initForContainer();

            bool useChildProcesses = globals->getPropInt("@useChildProcesses", false);
            unsigned maxThreads = globals->getPropInt("@maxActive", 4);
#else
            // The option has been renamed to avoid confusion with the similarly-named eclcc option, but
            // still accept the old name if the new one is not present.
            unsigned maxThreads = globals->getPropInt("@maxEclccProcesses", globals->getPropInt("@maxCompileThreads", 4));
#endif
            EclccServer server(queueNames.str(), maxThreads);
            // if we got here, eclserver is successfully started and all options are good, so create the "sentinel file" for re-runs from the script
            // put in its own "scope" to force the flush
            writeSentinelFile(sentinelFile);
            server.run();
        }
    }
    catch (IException * e)
    {
        EXCLOG(e, "Terminating unexpectedly");
        e->Release();
    }
    catch(...)
    {
        IERRLOG("Terminating unexpectedly");
    }
#ifndef _CONTAINERIZED
    stopPerformanceMonitor();
#endif
    UseSysLogForOperatorMessages(false);
    ::closedownClientProcess(); // dali client closedown
    releaseAtoms();
    ExitModuleObjects();
    return 0;
}
