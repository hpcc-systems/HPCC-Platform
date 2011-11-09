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
#include <stdio.h>
#include "jcomp.hpp"
#include "jfile.hpp"
#include "jlzw.hpp"
#include "jqueue.tpp"
#include "jargv.hpp"
#include "junicode.hpp"

#include "build-config.h"
#include "workunit.hpp"

#include "hqlecl.hpp"
#include "hqlerrors.hpp"
#include "hqlwuerr.hpp"
#include "hqlfold.hpp"
#include "hqlplugins.hpp"
#include "hqlmanifest.hpp"
#include "hqlcollect.hpp"
#include "hqlrepository.hpp"
#include "hqlerror.hpp"

#include "hqlgram.hpp"
#include "hqltrans.ipp"

#include "build-config.h"

#define INIFILE "eclcc.ini"
#define SYSTEMCONFDIR CONFIG_DIR
#define DEFAULTINIFILE "eclcc.ini"
#define SYSTEMCONFFILE ENV_CONF_FILE
#define DEFAULT_OUTPUTNAME  "a.out"

//=========================================================================================

//The following flag could be used not free items to speed up closedown
static bool optDebugMemLeak = false;
            
#if defined(_WIN32) && defined(_DEBUG)
static HANDLE leakHandle;
static void appendLeaks(size32_t len, const void * data)
{
    SetFilePointer(leakHandle, 0, 0, FILE_END);
    DWORD written;
    WriteFile(leakHandle, data, len, &written, 0);
}

void initLeakCheck(const char * title)
{
    StringBuffer leakFilename("eclccleaks.log");
    leakHandle = CreateFile(leakFilename.str(), GENERIC_READ|GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, 0, 0);
    if (title)
        appendLeaks(strlen(title), title);

    _CrtSetReportMode( _CRT_WARN, _CRTDBG_MODE_FILE|_CRTDBG_MODE_DEBUG );
    _CrtSetReportFile( _CRT_WARN, leakHandle );
    _CrtSetReportMode( _CRT_ERROR, _CRTDBG_MODE_FILE|_CRTDBG_MODE_DEBUG );
    _CrtSetReportFile( _CRT_ERROR, leakHandle );
    _CrtSetReportMode( _CRT_ASSERT, _CRTDBG_MODE_FILE|_CRTDBG_MODE_DEBUG );
    _CrtSetReportFile( _CRT_ASSERT, leakHandle );

//
//  set the states we want to monitor
//
    int LeakTmpFlag = _CrtSetDbgFlag( _CRTDBG_REPORT_FLAG );
    LeakTmpFlag    &= ~_CRTDBG_CHECK_CRT_DF; 
    LeakTmpFlag    |= _CRTDBG_LEAK_CHECK_DF;
    _CrtSetDbgFlag(LeakTmpFlag);
}

/**
 * Error handler for ctrl-break:  Don't care about memory leaks.
 */
void __cdecl IntHandler(int)
{
    enableMemLeakChecking(false);
    exit(2);
}

#include <signal.h> // for signal()

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    signal(SIGINT, IntHandler);
    return true;
}

#else

void initLeakCheck(const char *)
{
}

#endif // _WIN32 && _DEBUG

static bool extractOption(StringBuffer & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix)
{
    if (option.length())        // check if already specified via a command line option
        return true;
    if (globals->getProp(propertyName, option))
        return true;
    const char * env = getenv(envName);
    if (env)
    {
        option.append(env);
        return true;
    }
    option.append(defaultPrefix).append(defaultSuffix);
    return false;
}

static bool extractOption(StringAttr & option, IProperties * globals, const char * envName, const char * propertyName, const char * defaultPrefix, const char * defaultSuffix)
{
    if (option)
        return true;
    StringBuffer temp;
    bool ret = extractOption(temp, globals, envName, propertyName, defaultPrefix, defaultSuffix);
    option.set(temp.str());
    return ret;
}

struct EclCompileInstance
{
public:
    EclCompileInstance(IFile * _inputFile, IErrorReceiver & _errs, FILE * _errout, const char * _outputFilename, bool _legacyMode) :
      inputFile(_inputFile), errs(&_errs), errout(_errout), outputFilename(_outputFilename)
    {
        legacyMode = _legacyMode;
        ignoreUnknownImport = false;
        fromArchive = false;
    }

    bool reportErrorSummary();

public:
    Linked<IFile> inputFile;
    Linked<IPropertyTree> archive;
    Linked<IErrorReceiver> errs;
    Linked<IWorkUnit> wu;
    StringAttr eclVersion;
    const char * outputFilename;
    FILE * errout;
    Owned<IPropertyTree> srcArchive;
    bool legacyMode;
    bool fromArchive;
    bool ignoreUnknownImport;
};

class EclCC
{
public:
    EclCC(const char * _programName)
        : programName(_programName)
    {
        logVerbose = false;
        optArchive = false;
        optLegacy = false;
        optShared = false;
        optWorkUnit = false;
        optNoCompile = false;
        optOnlyCompile = false;
        optBatchMode = false;
        optSaveQueryText = false;
        optTargetClusterType = HThorCluster;
        optTargetCompiler = DEFAULT_COMPILER;
        optThreads = 0;
        optLogDetail = 0;
        batchPart = 0;
        batchSplit = 1;
        batchLog = NULL;
        cclogFilename.append("cc.").append((unsigned)GetCurrentProcessId()).append(".log");
    }

    bool parseCommandLineOptions(int argc, const char* argv[]);
    void loadOptions();
    bool processFiles();
    void processBatchedFile(IFile & file, bool multiThreaded);

protected:
    void applyDebugOptions(IWorkUnit * wu);
    bool checkWithinRepository(StringBuffer & attributePath, const char * sourcePathname);
    ICppCompiler * createCompiler(const char * coreName);
    void generateOutput(EclCompileInstance & instance);
    void instantECL(EclCompileInstance & instance, IWorkUnit *wu, IHqlExpression * query, const char * queryFullName, IErrorReceiver *errs, const char * outputFile);
    void getComplexity(IWorkUnit *wu, IHqlExpression * query, IErrorReceiver *errs);
    void processSingleQuery(EclCompileInstance & instance, IEclRepository * dataServer,
                               IFileContents * queryContents,
                               const char * queryAttributePath);
    void processXmlFile(EclCompileInstance & instance, const char *archiveXML);
    void processFile(EclCompileInstance & info);
    void processReference(EclCompileInstance & instance, const char * queryAttributePath);
    void processBatchFiles();
    void reportCompileErrors(IErrorReceiver *errs, const char * processName);
    void setDebugOption(const char * name, bool value);
    void usage();

    inline const char * queryTemplateDir() { return templatePath.length() ? templatePath.str() : NULL; }


protected:
    Owned<IEclRepository> pluginsRepository;
    Owned<IEclRepository> libraryRepository;
    Owned<IEclRepository> includeRepository;
    const char * programName;

    StringBuffer pluginsPath;
    StringBuffer templatePath;
    StringBuffer eclLibraryPath;
    StringBuffer stdIncludeLibraryPath;
    StringBuffer includeLibraryPath;
    StringBuffer cclogFilename;
    StringAttr optLogfile;
    StringAttr optIniFilename;
    StringAttr optManifestFilename;
    StringAttr optOutputDirectory;
    StringAttr optOutputFilename;
    StringAttr optQueryRepositoryReference;
    FILE * batchLog;

    IFileArray inputFiles;
    StringArray debugOptions;
    StringArray compileOptions;
    StringArray libraryPaths;

    ClusterType optTargetClusterType;
    CompilerType optTargetCompiler;
    unsigned optThreads;
    unsigned batchPart;
    unsigned batchSplit;
    unsigned optLogDetail;
    bool logVerbose;
    bool optArchive;
    bool optWorkUnit;
    bool optNoCompile;
    bool optBatchMode;
    bool optShared;
    bool optOnlyCompile;
    bool optSaveQueryText;
    bool optLegacy;
};


//=========================================================================================

static int doMain(int argc, const char *argv[])
{
    EclCC processor(argv[0]);
    if (!processor.parseCommandLineOptions(argc, argv))
        return 1;

    try
    {
        if (!processor.processFiles())
            return 2;
    }
    catch (IException *E)
    {
        StringBuffer m("Error: ");
        E->errorMessage(m);
        fputs(m.newline().str(), stderr);
        E->Release();
        return 2;
    }
#ifndef _DEBUG
    catch (...)
    {
        ERRLOG("Unexpected exception\n");
        return 4;
    }
#endif
    return 0;
}

int main(int argc, const char *argv[])
{
    InitModuleObjects();
    queryStderrLogMsgHandler()->setMessageFields(0);

    unsigned exitCode = doMain(argc, argv);
    releaseAtoms();
    return exitCode;
}

//=========================================================================================

void EclCC::loadOptions()
{
    Owned<IProperties> globals;
    if (!optIniFilename)
    {
        if (checkFileExists(INIFILE))
            optIniFilename.set(INIFILE);
        else
        {
            StringBuffer fn(SYSTEMCONFDIR);
            fn.append(PATHSEPSTR).append(DEFAULTINIFILE);
            if (checkFileExists(fn))
                optIniFilename.set(fn);
        }
    }
    if (logVerbose && optIniFilename.length())
        fprintf(stdout, "Found ini file '%s'\n", optIniFilename.get());

    globals.setown(createProperties(optIniFilename, true));

    StringBuffer compilerPath, includePath, libraryPath;
    
    if (globals->hasProp("targetGcc"))
        optTargetCompiler = globals->getPropBool("targetGcc") ? GccCppCompiler : Vs6CppCompiler;

#if _WIN32
    StringBuffer syspath;
    HMODULE hModule = GetModuleHandle(NULL);
    char path[MAX_PATH];
    GetModuleFileName(hModule, path, MAX_PATH);
    splitFilename(path, &syspath, &syspath, NULL, NULL);
    syspath.append(PATHSEPCHAR);
    extractOption(compilerPath, globals, "CL_PATH", "compilerPath", syspath, ".\\cl");
    extractOption(libraryPath, globals, "ECLCC_LIBRARY_PATH", "libraryPath", syspath, ".\\cl\\lib");
    extractOption(includePath, globals, "ECLCC_INCLUDE_PATH", "includePath", syspath, ".\\cl\\include");
    extractOption(pluginsPath, globals, "ECLCC_PLUGIN_PATH", "plugins", syspath, ".\\plugins");
    extractOption(templatePath, globals, "ECLCC_TPL_PATH", "templatePath", syspath, ".");
    extractOption(eclLibraryPath, globals, "ECLCC_ECLLIBRARY_PATH", "eclLibrariesPath", syspath, ".\\ecllibrary");
#else
    StringBuffer fn(SYSTEMCONFDIR);
    fn.append(PATHSEPSTR).append(SYSTEMCONFFILE);
    Owned<IProperties> sysconf = createProperties(fn, true);
    
    StringBuffer syspath;
    sysconf->getProp("path", syspath);
    syspath.append(PATHSEPCHAR);

    extractOption(compilerPath, globals, "CL_PATH", "compilerPath", "/usr", NULL);
    extractOption(libraryPath, globals, "ECLCC_LIBRARY_PATH", "libraryPath", syspath, "lib");
    extractOption(includePath, globals, "ECLCC_INCLUDE_PATH", "includePath", syspath, "componentfiles/cl/include");
    extractOption(pluginsPath, globals, "ECLCC_PLUGIN_PATH", "plugins", syspath, "plugins");
    extractOption(templatePath, globals, "ECLCC_TPL_PATH", "templatePath", syspath, "componentfiles");
    extractOption(eclLibraryPath, globals, "ECLCC_ECLLIBRARY_PATH", "eclLibrariesPath", syspath, "share/ecllibrary/");

#endif

    extractOption(stdIncludeLibraryPath, globals, "ECLCC_ECLINCLUDE_PATH", "eclIncludePath", ".", NULL);

    if (!optLogfile.length() && !optBatchMode)
        extractOption(optLogfile, globals, "ECLCC_LOGFILE", "logfile", "eclcc.log", NULL);

    if (!logVerbose)
    {
        Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_user, MSGCLS_error);
        queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
    }

    if (logVerbose || optLogfile)
    {
        if (optLogfile.length())
        {
            openLogFile(optLogfile, optLogDetail, false);
            if (logVerbose)
                fprintf(stdout, "Logging to '%s'\n",optLogfile.get());
        }
    }

    if (!optNoCompile)
        setCompilerPath(compilerPath.str(), includePath.str(), libraryPath.str(), NULL, optTargetCompiler, logVerbose);
}

//=========================================================================================

void EclCC::applyDebugOptions(IWorkUnit * wu)
{
    ForEachItemIn(i, debugOptions)
    {
        const char * option = debugOptions.item(i);
        const char * eq = strchr(option, '=');
        if (eq)
        {
            StringAttr name;
            name.set(option, eq-option);
            wu->setDebugValue(name, eq+1, true);
        }
        else
        {
            size_t len = strlen(option);
            if (len)
            {
                char last = option[len-1];
                if (last == '-' || last == '+')
                {
                    StringAttr name;
                    name.set(option, len-1);
                    wu->setDebugValueInt(name, last == '+' ? 1 : 0, true);
                }
                else
                    wu->setDebugValue(option, "1", true);
            }
        }
    }
}

//=========================================================================================

ICppCompiler * EclCC::createCompiler(const char * coreName)
{
    const char * sourceDir = NULL;
    const char * targetDir = NULL;
    Owned<ICppCompiler> compiler = ::createCompiler(coreName, sourceDir, targetDir, optTargetCompiler, logVerbose);
    compiler->setOnlyCompile(optOnlyCompile);
    compiler->setCCLogPath(cclogFilename);

    ForEachItemIn(iComp, compileOptions)
        compiler->addCompileOption(compileOptions.item(iComp));

    ForEachItemIn(iLib, libraryPaths)
        compiler->addLibraryPath(libraryPaths.item(iLib));
    
    return compiler.getClear();
}

void EclCC::reportCompileErrors(IErrorReceiver *errs, const char * processName)
{
    StringBuffer failText;
    StringBuffer absCCLogName;
    if (optLogfile.get())
        createUNCFilename(optLogfile.get(), absCCLogName, false);
    else
        absCCLogName = "log file";

    failText.appendf("Compile/Link failed for %s (see '%s' for details)",processName,absCCLogName.str());
    errs->reportError(ERR_INTERNALEXCEPTION, failText.toCharArray(), processName, 0, 0, 0);
    try
    {
        StringBuffer s;
        Owned<IFile> log = createIFile(cclogFilename);
        Owned<IFileIO> io = log->open(IFOread);
        if (io)
        {
            offset_t len = io->size();
            if (len)
            {
                io->read(0, (size32_t)len, s.reserve((size32_t)len));
#ifdef _WIN32
                const char * noCompiler = "is not recognized as an internal";
#else
                const char * noCompiler = "could not locate compiler";
#endif
                if (strstr(s.str(), noCompiler))
                {
                    ERRLOG("Fatal Error: Unable to locate C++ compiler/linker");
                }
                ERRLOG("\n---------- compiler output --------------\n%s\n--------- end compiler output -----------", s.str());
            }
        }
    }
    catch (IException * e)
    {
        e->Release();
    }
}

//=========================================================================================

void EclCC::instantECL(EclCompileInstance & instance, IWorkUnit *wu, IHqlExpression * query, const char * queryFullName, IErrorReceiver *errs, const char * outputFile)
{
    OwnedHqlExpr qquery = LINK(query);
    StringBuffer processName(outputFile);
    if (qquery && containsAnyActions(qquery))
    {
        try
        {
            const char * templateDir = queryTemplateDir();
            bool optSaveTemps = wu->getDebugValueBool("saveEclTempFiles", false);
            bool optSaveCpp = optSaveTemps || optNoCompile || wu->getDebugValueBool("saveCppTempFiles", false);
            //New scope - testing things are linked correctly
            {
                Owned<IHqlExprDllGenerator> generator = createDllGenerator(errs, processName.toCharArray(), NULL, wu, templateDir, optTargetClusterType, NULL, false);

                setWorkunitHash(wu, qquery);
                if (!optShared)
                    wu->setDebugValueInt("standAloneExe", 1, true);
                EclGenerateTarget target = optWorkUnit ? EclGenerateNone : (optNoCompile ? EclGenerateCpp : optShared ? EclGenerateDll : EclGenerateExe);
                if (optManifestFilename)
                    generator->addManifest(optManifestFilename);
                if (instance.srcArchive)
                    generator->addManifestFromArchive(instance.srcArchive);
                generator->setSaveGeneratedFiles(optSaveCpp);

                bool generateOk = generator->processQuery(qquery, target);
                if (generateOk && !optNoCompile)
                {
                    Owned<ICppCompiler> compiler = createCompiler(processName.toCharArray());
                    compiler->setSaveTemps(optSaveTemps);

                    bool compileOk = true;
                    if (optShared)
                    {
                        compileOk = generator->generateDll(compiler);
                    }
                    else
                    {
                        if (optTargetClusterType==RoxieCluster)
                            generator->addLibrary("ccd");
                        else
                            generator->addLibrary("hthor");


                        compileOk = generator->generateExe(compiler);
                    }

                    if (!compileOk)
                        reportCompileErrors(errs, processName);
                }
                else
                    wu->setState(generateOk ? WUStateCompleted : WUStateFailed);
            }

            if (logVerbose)
            {
                switch (wu->getState())
                {
                case WUStateCompiled:
                    fprintf(stdout, "Output file '%s' created\n",outputFile);
                    break;
                case WUStateFailed:
                    ERRLOG("Failed to create output file '%s'\n",outputFile);
                    break;
                case WUStateUploadingFiles:
                    fprintf(stdout, "Output file '%s' created, local file upload required\n",outputFile);
                    break;
                case WUStateCompleted:
                    fprintf(stdout, "No DLL/SO required\n");
                    break;
                default:
                    ERRLOG("Unexpected Workunit state %d\n", (int) wu->getState());
                    break;
                }
            }
        }
        catch (IException * e)
        {
            StringBuffer exceptionText;
            e->errorMessage(exceptionText);
            errs->reportError(ERR_INTERNALEXCEPTION, exceptionText.toCharArray(), queryFullName, 1, 0, 0);
            e->Release();
        }

        try
        {
            Owned<IFile> log = createIFile(cclogFilename);
            log->remove();
        }
        catch (IException * e)
        {
            e->Release();
        }
    }
}

//=========================================================================================

void EclCC::getComplexity(IWorkUnit *wu, IHqlExpression * query, IErrorReceiver *errs)
{
    double complexity = getECLcomplexity(query, errs, wu, optTargetClusterType);
    LOG(MCstats, unknownJob, "Complexity = %g", complexity);
}

//=========================================================================================

static bool convertPathToModule(StringBuffer & out, const char * filename)
{
    const char * dot = strrchr(filename, '.');
    if (dot)
    {
        if (!strieq(dot, ".ecl") && !strieq(dot, ".hql") && !strieq(dot, ".eclmod") && !strieq(dot, ".eclattr"))
            return false;
    }
    else
        return false;

    const unsigned copyLen = dot-filename;
    if (copyLen == 0)
        return false;

    out.ensureCapacity(copyLen);
    for (unsigned i= 0; i < copyLen; i++)
    {
        char next = filename[i];
        if (isPathSepChar(next))
            next = '.';
        out.append(next);
    }
    return true;
}


static bool findFilenameInSearchPath(StringBuffer & attributePath, const char * searchPath, const char * expandedSourceName)
{
    const char * cur = searchPath;
    unsigned lenSource = strlen(expandedSourceName);
    loop
    {
        const char * sep = strchr(cur, ENVSEPCHAR);
        StringBuffer curExpanded;
        if (!sep)
        {
            if (*cur)
                makeAbsolutePath(cur, curExpanded);
        }
        else if (sep != cur)
        {
            StringAttr temp(cur, sep-cur);
            makeAbsolutePath(temp, curExpanded);
        }

        if (curExpanded.length() && (curExpanded.length() < lenSource))
        {
#ifdef _WIN32
            //windows paths are case insensitive
            bool same = memicmp(curExpanded.str(), expandedSourceName, curExpanded.length()) == 0;
#else
            bool same = memcmp(curExpanded.str(), expandedSourceName, curExpanded.length()) == 0;
#endif
            if (same)
            {
                const char * tail = expandedSourceName+curExpanded.length();
                if (isPathSepChar(*tail))
                    tail++;
                if (convertPathToModule(attributePath, tail))
                    return true;
            }
        }

        if (!sep)
            return false;
        cur = sep+1;
    }
}

bool EclCC::checkWithinRepository(StringBuffer & attributePath, const char * sourcePathname)
{
    if (!sourcePathname)
        return false;

    StringBuffer searchPath;
    searchPath.append(eclLibraryPath).append(ENVSEPCHAR).append(stdIncludeLibraryPath).append(ENVSEPCHAR).append(includeLibraryPath);

    StringBuffer expandedSourceName;
    makeAbsolutePath(sourcePathname, expandedSourceName);

    return findFilenameInSearchPath(attributePath, searchPath, expandedSourceName);
}


void EclCC::processSingleQuery(EclCompileInstance & instance, IEclRepository * dataServer,
                               IFileContents * queryContents,
                               const char * queryAttributePath)
{
    Owned<IErrorReceiver> wuErrs = new WorkUnitErrorReceiver(instance.wu, "eclcc");
    Owned<IErrorReceiver> errs = createCompoundErrorReceiver(instance.errs, wuErrs);
    Linked<IEclRepository> repository = dataServer;

    //All dlls/exes are essentially cloneable because you may be running multiple instances at once
    //The only exception would be a dll created for a one-time query.  (Currently handled by eclserver.)
    instance.wu->setCloneable(true);

    applyDebugOptions(instance.wu);

    if (optTargetCompiler != DEFAULT_COMPILER)
        instance.wu->setDebugValue("targetCompiler", compilerTypeText[optTargetCompiler], true);

    bool withinRepository = (queryAttributePath && *queryAttributePath);
    Owned<IHqlScope> scope;
    bool syntaxChecking = instance.wu->getDebugValueBool("syntaxCheck", false);
    size32_t prevErrs = errs->errCount();
    unsigned startTime = msTick();
    OwnedHqlExpr qquery;
    const char * sourcePathname = queryContents ? queryContents->querySourcePath()->str() : NULL;
    const char * defaultErrorPathname = sourcePathname ? sourcePathname : queryAttributePath;

    {
        //Minimize the scope of the parse context to reduce lifetime of cached items.
        HqlParseContext parseCtx(repository, instance.archive);
        setLegacyEclSemantics(instance.legacyMode);
        if (instance.archive)
            instance.archive->setPropBool("@legacyMode", instance.legacyMode);

        parseCtx.ignoreUnknownImport = instance.ignoreUnknownImport;
        scope.setown(createPrivateScope());

        try
        {
            HqlLookupContext ctx(parseCtx, errs);

            if (withinRepository)
            {
                if (instance.archive)
                {
                    instance.archive->setProp("Query", "");
                    instance.archive->setProp("Query/@attributePath", queryAttributePath);
                }

                qquery.setown(getResolveAttributeFullPath(queryAttributePath, LSFpublic, ctx));
                if (!qquery && !syntaxChecking && (errs->errCount() == prevErrs))
                {
                    StringBuffer msg;
                    msg.append("Could not resolve attribute ").append(queryAttributePath);
                    errs->reportError(3, msg.str(), defaultErrorPathname, 0, 0, 0);
                }
            }
            else
            {
                if (instance.legacyMode)
                    importRootModulesToScope(scope, ctx);

                qquery.setown(parseQuery(scope, queryContents, ctx, NULL, true));

                if (instance.archive)
                {
                    StringBuffer queryText;
                    queryText.append(queryContents->length(), queryContents->getText());
                    const char * p = queryText;
                    if (0 == strncmp(p, (const char *)UTF8_BOM,3))
                        p += 3;
                    instance.archive->setProp("Query", p );
                    instance.archive->setProp("Query/@originalFilename", sourcePathname);
                }
            }

            gatherWarnings(ctx.errs, qquery);

            if (qquery && !syntaxChecking)
                qquery.setown(convertAttributeToQuery(qquery, ctx));

            if (instance.wu->getDebugValueBool("addTimingToWorkunit", true))
                instance.wu->setTimerInfo("EclServer: parse query", NULL, msTick()-startTime, 1, 0);
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            errs->reportError(3, s.toCharArray(), defaultErrorPathname, 1, 0, 0);
            e->Release();
        }
    }

    if (!syntaxChecking && (errs->errCount() == prevErrs) && (!qquery || !containsAnyActions(qquery)))
    {
        errs->reportError(3, "Query is empty", defaultErrorPathname, 1, 0, 0);
        return;
    }

    if (instance.archive)
        return;

    if (syntaxChecking)
        return;

    StringBuffer targetFilename;
    const char * outputFilename = instance.outputFilename;
    if (!outputFilename)
    {
        addNonEmptyPathSepChar(targetFilename.append(optOutputDirectory));
        targetFilename.append(DEFAULT_OUTPUTNAME);
    }
    else if (strcmp(outputFilename, "-") == 0)
        targetFilename.append("stdout:");
    else
        addNonEmptyPathSepChar(targetFilename.append(optOutputDirectory)).append(outputFilename);

    //Check if it overlaps with the source file and add .eclout if so
    if (instance.inputFile)
    {
        const char * originalFilename = instance.inputFile->queryFilename();
        if (streq(targetFilename, originalFilename))
            targetFilename.append(".eclout");
    }

    if (errs->errCount() == prevErrs)
        instantECL(instance, instance.wu, qquery, scope->queryFullName(), errs, targetFilename);
    else 
    {
        if (stdIoHandle(targetFilename) == -1)
        {
        // MORE - what about intermediate files?
#ifdef _WIN32
            StringBuffer goer;
            remove(goer.append(targetFilename).append(".exe"));
            remove(goer.clear().append(targetFilename).append(".exe.manifest"));
#else
            remove(targetFilename);
#endif
        }
    }

    if (instance.wu->getDebugValueBool("addTimingToWorkunit", true))
        instance.wu->setTimerInfo("EclServer: totalTime", NULL, msTick()-startTime, 1, 0);
}

void EclCC::processXmlFile(EclCompileInstance & instance, const char *archiveXML)
{
    Owned<IPropertyTree> archiveTree = createPTreeFromXMLString(archiveXML, ipt_caseInsensitive);

    instance.srcArchive.set(archiveTree);
    Owned<IPropertyTreeIterator> iter = archiveTree->getElements("Option");
    ForEach(*iter) 
    {
        IPropertyTree &item = iter->query();
        instance.wu->setDebugValue(item.queryProp("@name"), item.queryProp("@value"), true);
    }

    const char * queryText = archiveTree->queryProp("Query");
    const char * queryAttributePath = archiveTree->queryProp("Query/@attributePath");
    //Takes precedence over an entry in the archive - so you can submit parts of an archive.
    if (optQueryRepositoryReference)
        queryAttributePath = optQueryRepositoryReference;

    //The legacy mode (if specified) in the archive takes precedence - it needs to match to compile.
    instance.legacyMode = archiveTree->getPropBool("@legacyMode", instance.legacyMode);

    //Some old archives contained imports, but no definitions of the module.  This option is to allow them to compile.
    //It shouldn't be needed for new archives in non-legacy mode. (But neither should it cause any harm.)
    instance.ignoreUnknownImport = archiveTree->getPropBool("@ignoreUnknownImport", true);

    instance.eclVersion.set(archiveTree->queryProp("@eclVersion"));
    checkEclVersionCompatible(instance.errs, instance.eclVersion);

    Owned<IEclSourceCollection> archiveCollection;
    if (archiveTree->getPropBool("@testRemoteInterface", false))
    {
        //This code is purely here for regression testing some of the classes used in the enterprise version.
        Owned<IXmlEclRepository> xmlRepository = createArchiveXmlEclRepository(archiveTree);
        archiveCollection.setown(createRemoteXmlEclCollection(NULL, *xmlRepository, NULL, false));
        archiveCollection->checkCacheValid();
    }
    else
        archiveCollection.setown(createArchiveEclCollection(archiveTree));

    Owned<IEclRepository> archiveServer = createRepository(archiveCollection);
    if (queryText || queryAttributePath)
    {
        const char * sourceFilename = archiveTree->queryProp("Query/@originalFilename");
        Owned<IEclRepository> dataServer = createCompoundRepositoryF(pluginsRepository.get(), archiveServer.get(), NULL);

        Owned<ISourcePath> sourcePath = createSourcePath(sourceFilename);
        Owned<IFileContents> contents = createFileContentsFromText(queryText, sourcePath);
        processSingleQuery(instance, dataServer, contents, queryAttributePath);
    }
    else
    {
        //This is really only useful for regression testing
        const char * queryText = archiveTree->queryProp("SyntaxCheck");
        const char * syntaxCheckModule = archiveTree->queryProp("SyntaxCheck/@module");
        const char * syntaxCheckAttribute = archiveTree->queryProp("SyntaxCheck/@attribute");
        if (!queryText || !syntaxCheckModule || !syntaxCheckAttribute)
            throw MakeStringException(1, "No query found in xml");

        instance.wu->setDebugValueInt("syntaxCheck", true, true);
        StringBuffer fullPath;
        fullPath.append(syntaxCheckModule).append('.').append(syntaxCheckAttribute);

        //Create a repository with just that attribute, and place it before the archive in the resolution order.
        Owned<IFileContents> contents = createFileContentsFromText(queryText, NULL);
        Owned<IEclRepository> syntaxCheckRepository = createSingleDefinitionEclRepository(syntaxCheckModule, syntaxCheckAttribute, contents);
        Owned<IEclRepository> dataServer = createCompoundRepositoryF(pluginsRepository.get(), syntaxCheckRepository.get(), archiveServer.get(), NULL);
        processSingleQuery(instance, dataServer, NULL, fullPath.str());
    }
}


//=========================================================================================

void EclCC::processFile(EclCompileInstance & instance)
{
    const char * curFilename = instance.inputFile->queryFilename();
    assertex(curFilename);

    Owned<ISourcePath> sourcePath = createSourcePath(curFilename);
    Owned<IFileContents> queryText = createFileContentsFromFile(curFilename, sourcePath);
    const char * queryTxt = queryText->getText();
    if (optArchive)
        instance.archive.setown(createAttributeArchive());
    
    instance.wu.setown(createLocalWorkUnit());
    if (optSaveQueryText)
    {
        Owned<IWUQuery> q = instance.wu->updateQuery();
        q->setQueryText(queryTxt);
    }

    if (isArchiveQuery(queryTxt))
    {
        instance.fromArchive = true;
        processXmlFile(instance, queryTxt);
    }
    else
    {
        StringBuffer attributePath;
        bool inputFromStdIn = streq(curFilename, "stdin:");
        bool withinRepository = !inputFromStdIn && checkWithinRepository(attributePath, curFilename);

        StringBuffer expandedSourceName;
        if (!inputFromStdIn)
            makeAbsolutePath(curFilename, expandedSourceName);
        else
            expandedSourceName.append(curFilename);

        EclRepositoryArray repositories;
        repositories.append(*LINK(pluginsRepository));
        repositories.append(*LINK(libraryRepository));

        //Ensure that this source file is used as the definition (in case there are potential clashes)
        //Note, this will not override standard library files.
        if (withinRepository)
        {
            Owned<IEclSourceCollection> inputFileCollection = createSingleDefinitionEclCollection(attributePath, queryText);
            repositories.append(*createRepository(inputFileCollection));
        }
        else
        {
            //Ensure that $ is valid for any file submitted - even if it isn't in the include direcotories
            //Disable this for the moment when running the regression suite.
            if (!optBatchMode && !withinRepository && !inputFromStdIn && !optLegacy)
            {
                //Associate the contents of the directory with an internal module called _local_directory_
                //(If it was root it might override existing root symbols).  $ is the only public way to get at the symbol
                const char * moduleName = "_local_directory_";
                _ATOM moduleNameAtom = createAtom(moduleName);

                StringBuffer thisDirectory;
                StringBuffer thisTail;
                splitFilename(expandedSourceName, &thisDirectory, &thisDirectory, &thisTail, NULL);
                attributePath.append(moduleName).append(".").append(thisTail);

                Owned<IEclSourceCollection> inputFileCollection = createSingleDefinitionEclCollection(attributePath, queryText);
                repositories.append(*createRepository(inputFileCollection));

                Owned<IEclSourceCollection> directory = createFileSystemEclCollection(instance.errs, thisDirectory, 0, 0);
                Owned<IEclRepository> directoryRepository = createRepository(directory, moduleName);
                Owned<IEclRepository> nested = createNestedRepository(moduleNameAtom, directoryRepository);
                repositories.append(*LINK(nested));
            }
        }

        repositories.append(*LINK(includeRepository));

        Owned<IEclRepository> searchRepository = createCompoundRepository(repositories);
        processSingleQuery(instance, searchRepository, queryText, attributePath.str());
    }

    if (instance.reportErrorSummary() && !instance.archive)
        return;

    generateOutput(instance);
}

void EclCC::generateOutput(EclCompileInstance & instance)
{
    const char * outputFilename = instance.outputFilename;
    if (instance.archive)
    {
        if (optManifestFilename)
            addManifestResourcesToArchive(instance.archive, optManifestFilename);

        //Work around windows problem writing 64K to stdout if not redirected/piped
        StringBuffer archiveName;
        if (instance.outputFilename && !streq(instance.outputFilename, "-"))
            addNonEmptyPathSepChar(archiveName.append(optOutputDirectory)).append(instance.outputFilename);
        else
            archiveName.append("stdout:");
            
        OwnedIFile ifile = createIFile(archiveName);
        OwnedIFileIO ifileio = ifile->open(IFOcreate);
        Owned<IIOStream> stream = createIOStream(ifileio.get());
        Owned<IIOStream> buffered = createBufferedIOStream(stream,0x8000);
        saveXML(*buffered, instance.archive);
    }

    if (optWorkUnit && instance.wu)
    {
        StringBuffer xmlFilename;
        addNonEmptyPathSepChar(xmlFilename.append(optOutputDirectory));
        if (outputFilename)
            xmlFilename.append(outputFilename);
        else
            xmlFilename.append(DEFAULT_OUTPUTNAME);
        xmlFilename.append(".xml");
        exportWorkUnitToXMLFile(instance.wu, xmlFilename, 0);
    }
}


void EclCC::processReference(EclCompileInstance & instance, const char * queryAttributePath)
{
    const char * outputFilename = instance.outputFilename;

    instance.wu.setown(createLocalWorkUnit());

    Owned<IEclRepository> searchRepository = createCompoundRepositoryF(pluginsRepository.get(), libraryRepository.get(), includeRepository.get(), NULL);
    processSingleQuery(instance, searchRepository, NULL, queryAttributePath);

    if (instance.reportErrorSummary())
        return;
    generateOutput(instance);
}


bool EclCC::processFiles()
{
    loadOptions();

    StringBuffer searchPath;
    searchPath.append(stdIncludeLibraryPath).append(ENVSEPCHAR).append(includeLibraryPath);

    Owned<IErrorReceiver> errs = createFileErrorReceiver(stderr);
    pluginsRepository.setown(createNewSourceFileEclRepository(errs, pluginsPath.str(), ESFallowplugins, logVerbose ? PLUGIN_DLL_MODULE : 0));
    libraryRepository.setown(createNewSourceFileEclRepository(errs, eclLibraryPath.str(), 0, 0));
    includeRepository.setown(createNewSourceFileEclRepository(errs, searchPath.str(), 0, 0));

    //Ensure symbols for plugins are initialised - see comment before CHqlMergedScope...
//    lookupAllRootDefinitions(pluginsRepository);

    bool ok = true;
    if (optBatchMode)
    {
        processBatchFiles();
    }
    else if (inputFiles.ordinality() == 0)
    {
        assertex(optQueryRepositoryReference);
        EclCompileInstance info(NULL, *errs, stderr, optOutputFilename, optLegacy);
        processReference(info, optQueryRepositoryReference);
        ok = (errs->errCount() == 0);
    }
    else
    {
        EclCompileInstance info(&inputFiles.item(0), *errs, stderr, optOutputFilename, optLegacy);
        processFile(info);
        ok = (errs->errCount() == 0);
    }

    if (logVerbose)
        defaultTimer->printTimings();
    return ok;
}

void EclCC::setDebugOption(const char * name, bool value)
{
    StringBuffer temp;
    temp.append(name).append("=").append(value ? "1" : "0");
    debugOptions.append(temp);
}


bool EclCompileInstance::reportErrorSummary()
{
    if (errs->errCount() || errs->warnCount())
    {
        fprintf(errout, "%d error%s, %d warning%s\n", errs->errCount(), errs->errCount()<=1 ? "" : "s",
                errs->warnCount(), errs->warnCount()<=1?"":"s");
    }
    return errs->errCount() != 0;
}


//=========================================================================================

bool EclCC::parseCommandLineOptions(int argc, const char* argv[])
{
    if (argc < 2)
    {
        usage();
        return false;
    }

    ArgvIterator iter(argc, argv);
    StringAttr tempArg;
    bool tempBool;
    for (; !iter.done(); iter.next())
    {
        const char * arg = iter.query();
        if (iter.matchFlag(optBatchMode, "-b"))
        {
        }
        else if (iter.matchOption(tempArg, "-brk"))
        {
#if defined(_WIN32) && defined(_DEBUG)
            unsigned id = atoi(tempArg);
            if (id == 0)
                DebugBreak();
            else
                _CrtSetBreakAlloc(id);
#endif
        }
        else if (iter.matchFlag(optOnlyCompile, "-c"))
        {
        }
        else if (iter.matchFlag(optArchive, "-E"))
        {
        }
        else if (memcmp(arg, "-f", 2)==0)
        {
            if (arg[2])
                debugOptions.append(arg+2);
        }
        else if (iter.matchFlag(tempBool, "-g"))
        {
            if (tempBool)
            {
                debugOptions.append("debugQuery");
                debugOptions.append("saveCppTempFiles");
            }
            else
                debugOptions.append("debugQuery=0");
        }
        else if (strcmp(arg, "-internal")==0)
        {
            testHqlInternals();
        }
        else if (iter.matchFlag(tempBool, "-save-temps"))
        {
            setDebugOption("saveEclTempFiles", tempBool);
        }
        else if (strcmp(arg, "-help")==0 || strcmp(arg, "--help")==0)
        {
            usage();
            return false;
        }
        else if (iter.matchPathFlag(includeLibraryPath, "-I"))
        {
        }
        else if (iter.matchFlag(tempArg, "-L"))
        {
            libraryPaths.append(tempArg);
        }
        else if (iter.matchFlag(optLegacy, "-legacy"))
        {
        }
        else if (iter.matchOption(optLogfile, "--logfile"))
        {
        }
        else if (iter.matchOption(optLogDetail, "--logdetail"))
        {
        }
        else if (iter.matchOption(optQueryRepositoryReference, "-main"))
        {
        }
        else if (iter.matchFlag(optDebugMemLeak, "-m"))
        {
        }
        else if (iter.matchFlag(optOutputFilename, "-o"))
        {
        }
        else if (iter.matchFlag(optOutputDirectory, "-P"))
        {
        }
        else if (iter.matchFlag(optSaveQueryText, "-q"))
        {
        }
        else if (iter.matchFlag(optNoCompile, "-S"))
        {
        }
        else if (iter.matchFlag(optShared, "-shared"))
        {
        }
        else if (iter.matchFlag(tempBool, "-syntax"))
        {
            setDebugOption("syntaxCheck", tempBool);
        }
        else if (iter.matchOption(optIniFilename, "-specs"))
        {
            if (!checkFileExists(optIniFilename))
            {
                ERRLOG("Error: INI file '%s' does not exist",optIniFilename.get());
                return false;
            }
        }
        else if (iter.matchOption(optManifestFilename, "-manifest"))
        {
            if (!isManifestFileValid(optManifestFilename))
                return false;
        }
        else if (iter.matchOption(tempArg, "-split"))
        {
            batchPart = atoi(tempArg)-1;
            const char * split = strchr(tempArg, ':');
            if (!split)
            {
                ERRLOG("Error: syntax is -split=part:splits\n");
                return false;
            }
            batchSplit = atoi(split+1);
            if (batchSplit == 0)
                batchSplit = 1;
            if (batchPart >= batchSplit)
                batchPart = 0;
        }
        else if (iter.matchOption(tempArg, "-target"))
        {
            const char * platform = tempArg.get();
            ClusterType clusterType = getClusterType(platform);
            if (clusterType != NoCluster)
                optTargetClusterType = clusterType;
            else
            {
                ERRLOG("Unknown ecl target platform %s\n", platform);
                return false;
            }
        }
        else if (iter.matchFlag(logVerbose, "-v") || iter.matchFlag(logVerbose, "--verbose"))
        {
        }
        else if (strcmp(arg, "--version")==0)
        {
            fprintf(stdout,"%s %s\n", LANGUAGE_VERSION, BUILD_TAG);
            return false;
        }
        else if (startsWith(arg, "-Wc,"))
        {
            expandCommaList(compileOptions, arg+4);
        }
        else if (startsWith(arg, "-Wl,") || startsWith(arg, "-Wp,") || startsWith(arg, "-Wa,"))
        {
            //Pass these straigh through to the gcc compiler
            compileOptions.append(arg);
        }
        else if (iter.matchFlag(optWorkUnit, "-wu"))
        {
        }
        else if (strcmp(arg, "-")==0) 
        {
            processArgvFilename(inputFiles, "stdin:");
        }
        else if (arg[0] == '-')
        {
            ERRLOG("Error: unrecognised option %s",arg);
            usage();
            return false;
        }
        else
            processArgvFilename(inputFiles, arg);
    }
    // Option post processing follows:
    if (optArchive || optWorkUnit)
        optNoCompile = true;

    if (inputFiles.ordinality() == 0)
    {
        if (!optBatchMode && optQueryRepositoryReference)
            return true;
        ERRLOG("No input files supplied");
        return false;
    }

    if (optDebugMemLeak)
    {
        StringBuffer title;
        title.append(inputFiles.item(0).queryFilename()).newline();
        initLeakCheck(title);
    }

    return true;
}

//=========================================================================================

void EclCC::usage()
{
    //Flags can take form -Fx or -F x
    //options can take the form -option=x or -option x

    fprintf(stdout,"\nUsage:\n"
           "    eclcc <options> queryfile.ecl\n"
           "\nGeneral options:\n"
           "    -g            Enable debug symbols in generated code\n"
           "    -Ipath        Add path to locations to search for ecl imports\n"
           "    -Lpath        Add path to locations to search for system libraries\n"
           "    -ofile        Specify name of output file (default a.out if linking to\n"
           "                  executable, or stdout)\n"
           "    -Ppath        Specify the path of the output files\n"
           "    -Wc,xx        Supply option for the c++ compiler\n"
           "    -save-temps   Do not delete intermediate files (implied if -g)\n"
           "    -manifest     Specify path to manifest file listing resources to add\n"
           "\nECL options:\n"
           "    -E            Output preprocessed ECL in xml archive form\n"
           "    -S            Generate c++ output, but don't compile\n"
           "    -foption[=value] Set an ecl option (#option)\n"
           "    -main <ref>   Compile definition <ref> from the source collection\n"
           "    -q            Save ECL query text as part of workunit\n"
           "    -shared       Generate workunit shared object instead of a stand alone exe\n"
           "    -syntax       Perform a syntax check of the ECL\n"
           "    -target=hthor Generate code for hthor executable (default)\n"
           "    -target=roxie Generate code for roxie cluster\n"
           "    -target=thor  Generate code for thor cluster\n"
           "    -wu           Only generate workunit informaton as xml file\n"
           "\nOther options:\n"
           "    -b            Batch mode.  Each source file is processed in turn.  Output\n"
           "                  name depends on the input filename\n"
           "    -c            compile only (don't link)\n"
           "    -help         Display this message\n"
           "    --help        Display this message\n"
           "    --logdetail=n Set the level of detail in the log file\n"
           "    --logfile file Write log to specified file\n"
           "    -specs file   Read eclcc configuration from specified file\n"
           "    -v --verbose  Output additional tracing information while compiling\n"
           "    --version     Output version information\n\n"
);

    //Options not included in usage text
    //-brk        set breakpoint on nth allocation (windows)
    //-m          enable leak checking (windows)
    //-split a:b  only compile 1/b of the input files, sample a.  Use for parallel regression testing.
    //-Wl,-Wp,-Wa passed through to gcc compiler
}

//=========================================================================================

// The following methods are concerned with running eclcc in batch mode (primarily to aid regression testing)
void EclCC::processBatchedFile(IFile & file, bool multiThreaded)
{
    StringBuffer basename, logFilename, xmlFilename, outFilename;

    splitFilename(file.queryFilename(), NULL, NULL, &basename, &basename);
    addNonEmptyPathSepChar(logFilename.append(optOutputDirectory)).append(basename).append(".log");
    addNonEmptyPathSepChar(xmlFilename.append(optOutputDirectory)).append(basename).append(".xml");

    splitFilename(file.queryFilename(), NULL, NULL, &outFilename, &outFilename);

    unsigned startTime = msTick();
    FILE * logFile = fopen(logFilename.str(), "w");
    if (!logFile)
        throw MakeStringException(99, "couldn't create log output %s", logFilename.str());

    Owned<ILogMsgHandler> handler;
    try
    {
        fprintf(logFile, "--- %s --- \n", basename.str());
        {
            if (!multiThreaded)
            {
                handler.setown(getHandleLogMsgHandler(logFile, 0, false));
                Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_all, MSGCLS_all, DefaultDetail);
                queryLogMsgManager()->addMonitor(handler, filter);

                resetUniqueId();
                resetLexerUniqueNames();
            }

            Owned<IErrorReceiver> localErrs = createFileErrorReceiver(logFile);
            EclCompileInstance info(&file, *localErrs, logFile, outFilename, optLegacy);
            processFile(info);
            //Following only produces output if the system has been compiled with TRANSFORM_STATS defined
            dbglogTransformStats(true);
            if (info.wu &&
                (info.wu->getDebugValueBool("generatePartialOutputOnError", false) || info.errs->errCount() == 0))
                exportWorkUnitToXMLFile(info.wu, xmlFilename, XML_NoBinaryEncode64);
        }
    }
    catch (IException * e)
    {
        StringBuffer s;
        e->errorMessage(s);
        e->Release();
        fprintf(logFile, "Unexpected exception: %s", s.str());
    }
    if (handler)
        queryLogMsgManager()->removeMonitor(handler);

    fflush(logFile);
    fclose(logFile);

    unsigned nowTime = msTick();
    StringBuffer s;
    s.append(basename).append(":");
    s.padTo(50);
    s.appendf("%8d ms\n", nowTime-startTime);
    fprintf(batchLog, "%s", s.str());
//  fflush(batchLog);
}


typedef SafeQueueOf<IFile, true> RegressQueue;
class BatchThread : public Thread
{
public:
    BatchThread(EclCC & _compiler, RegressQueue & _queue, Semaphore & _fileReady) 
        : compiler(_compiler), queue(_queue), fileReady(_fileReady)
    {
    }
    virtual int run()
    {
        loop
        {
            fileReady.wait();
            IFile * next = queue.dequeue();
            if (!next)
                return 0;
            compiler.processBatchedFile(*next, true);
            next->Release();
        }
    }
protected:
    EclCC & compiler;
    RegressQueue & queue;
    Semaphore & fileReady;
};

int compareFilenames(IInterface * * pleft, IInterface * * pright)
{
    IFile * left = static_cast<IFile *>(*pleft);
    IFile * right = static_cast<IFile *>(*pright);

    return stricmp(pathTail(left->queryFilename()), pathTail(right->queryFilename()));
}


void EclCC::processBatchFiles()
{
    Thread * * threads = NULL;
    RegressQueue queue;
    Semaphore fileReady;
    unsigned startAllTime = msTick();
    if (optThreads > 0)
    {
        threads = new Thread * [optThreads];
        for (unsigned i = 0; i < optThreads; i++)
        {
            threads[i] = new BatchThread(*this, queue, fileReady);
            threads[i]->start();
        }
    }

    StringBuffer batchLogName;
    addNonEmptyPathSepChar(batchLogName.append(optOutputDirectory)).append("_batch_.");
    batchLogName.append(batchPart+1);
    batchLogName.append(".log");

    batchLog = fopen(batchLogName.str(), "w");
    if (!batchLog)
        throw MakeStringException(99, "couldn't create log output %s", batchLogName.str());

    //Divide the files up based on file size, rather than name
    inputFiles.sort(compareFilenames);
    unsigned __int64 totalSize = 0;
    ForEachItemIn(iSize, inputFiles)
    {
        IFile & cur = inputFiles.item(iSize);
        totalSize += cur.size();
    }

    //Sort the filenames so you have a consistent order between windows and linux

    unsigned __int64 averageFileSize = totalSize / inputFiles.ordinality();
    unsigned splitter = 0;
    unsigned __int64 sizeSoFar = 0;
    ForEachItemIn(i, inputFiles)
    {
        IFile &file = inputFiles.item(i);
        if (splitter == batchPart)
        {
            if (optThreads > 0)
            {
                queue.enqueue(LINK(&file));
                fileReady.signal();
            }
            else
                processBatchedFile(file, false);
        }

        unsigned __int64 thisSize = file.size();
        sizeSoFar += thisSize;
        if (sizeSoFar > averageFileSize)
        {
            sizeSoFar = 0;
            splitter++;
        }
        if (splitter == batchSplit)
            splitter = 0;
    }

    if (optThreads > 0)
    {
        for (unsigned i = 0; i < optThreads; i++)
            fileReady.signal();
        for (unsigned j = 0; j < optThreads; j++)
            threads[j]->join();
        for (unsigned i2 = 0; i2 < optThreads; i2++)
            threads[i2]->Release();
        delete [] threads;
    }

    fprintf(batchLog, "@%5ds total time for part %d\n", (msTick()-startAllTime)/1000, batchPart);
    fclose(batchLog);
    batchLog = NULL;
}


