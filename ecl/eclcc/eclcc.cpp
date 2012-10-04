/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "rmtfile.hpp"

//#define TEST_LEGACY_DEPENDENCY_CODE

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

static bool getPackageFolder(StringBuffer & path)
{
    StringBuffer folder;
    splitDirTail(queryCurrentProcessPath(), folder);
    removeTrailingPathSepChar(folder);
    if (folder.length())
    {
        StringBuffer foldersFolder;
        splitDirTail(folder.str(), foldersFolder);
        if (foldersFolder.length())
        {
            path = foldersFolder;
            return true;
        }
    }
    return false;
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
    Owned<IEclRepository> dataServer;  // A member which can be cleared after parsing the query
    OwnedHqlExpr query;  // parsed query - cleared when generating to free memory
    StringAttr eclVersion;
    const char * outputFilename;
    FILE * errout;
    Owned<IPropertyTree> srcArchive;
    Owned<IPropertyTree> generatedMeta;
    bool legacyMode;
    bool fromArchive;
    bool ignoreUnknownImport;
};

class EclCC
{
public:
    EclCC(int _argc, const char **_argv)
        : programName(_argv[0])
    {
        argc = _argc;
        argv = _argv;
        logVerbose = false;
        optArchive = false;
        optGenerateMeta = false;
        optGenerateDepend = false;
        optIncludeMeta = false;
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
    void loadManifestOptions();
    bool processFiles();
    void processBatchedFile(IFile & file, bool multiThreaded);

protected:
    void addFilenameDependency(StringBuffer & target, EclCompileInstance & instance, const char * filename);
    void applyDebugOptions(IWorkUnit * wu);
    bool checkWithinRepository(StringBuffer & attributePath, const char * sourcePathname);
    IFileIO * createArchiveOutputFile(EclCompileInstance & instance);
    ICppCompiler * createCompiler(const char * coreName);
    void generateOutput(EclCompileInstance & instance);
    void instantECL(EclCompileInstance & instance, IWorkUnit *wu, const char * queryFullName, IErrorReceiver *errs, const char * outputFile);
    bool isWithinPath(const char * sourcePathname, const char * searchPath);
    void getComplexity(IWorkUnit *wu, IHqlExpression * query, IErrorReceiver *errs);
    void outputXmlToOutputFile(EclCompileInstance & instance, IPropertyTree * xml);
    void processSingleQuery(EclCompileInstance & instance,
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
    StringBuffer hooksPath;
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
    StringArray inputFileNames;
    StringArray debugOptions;
    StringArray compileOptions;
    StringArray linkOptions;
    StringArray libraryPaths;

    ClusterType optTargetClusterType;
    CompilerType optTargetCompiler;
    unsigned optThreads;
    unsigned batchPart;
    unsigned batchSplit;
    unsigned optLogDetail;
    bool logVerbose;
    bool optArchive;
    bool optGenerateMeta;
    bool optGenerateDepend;
    bool optIncludeMeta;
    bool optWorkUnit;
    bool optNoCompile;
    bool optBatchMode;
    bool optShared;
    bool optOnlyCompile;
    bool optSaveQueryText;
    bool optLegacy;
    int argc;
    const char **argv;
};


//=========================================================================================

static int doMain(int argc, const char *argv[])
{
    EclCC processor(argc, argv);
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
    // Turn logging down (we turn it back up if -v option seen)
    Owned<ILogMsgFilter> filter = getCategoryLogMsgFilter(MSGAUD_user, MSGCLS_error);
    queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);

    unsigned exitCode = doMain(argc, argv);
    releaseAtoms();
    removeFileHooks();
    return exitCode;
}

//=========================================================================================
bool setTargetPlatformOption(const char *platform, ClusterType &optTargetClusterType)
{
    if (!platform || !*platform)
        return false;
    ClusterType clusterType = getClusterType(platform);
    if (clusterType == NoCluster)
    {
        ERRLOG("Unknown ecl target platform %s\n", platform);
        return false;
    }
    optTargetClusterType = clusterType;
    return true;
}

void EclCC::loadManifestOptions()
{
    if (!optManifestFilename)
        return;
    Owned<IPropertyTree> mf = createPTreeFromXMLFile(optManifestFilename);
    IPropertyTree *ecl = mf->queryPropTree("ecl");
    if (ecl)
    {
        if (ecl->hasProp("@filename"))
        {
            StringBuffer dir, abspath;
            splitDirTail(optManifestFilename, dir);
            makeAbsolutePath(ecl->queryProp("@filename"), dir.str(), abspath);
            processArgvFilename(inputFiles, abspath.str());
        }
        if (!optLegacy)
            optLegacy = ecl->getPropBool("@legacy");
        if (!optQueryRepositoryReference && ecl->hasProp("@main"))
            optQueryRepositoryReference.set(ecl->queryProp("@main"));

        if (ecl->hasProp("@targetPlatform"))
            setTargetPlatformOption(ecl->queryProp("@targetPlatform"), optTargetClusterType);
        else if (ecl->hasProp("@targetClusterType")) //deprecated name
            setTargetPlatformOption(ecl->queryProp("@targetClusterType"), optTargetClusterType);

        Owned<IPropertyTreeIterator> paths = ecl->getElements("IncludePath");
        ForEach(*paths)
        {
            IPropertyTree &item = paths->query();
            if (item.hasProp("@path"))
                includeLibraryPath.append(ENVSEPCHAR).append(item.queryProp("@path"));
        }
        paths.setown(ecl->getElements("LibraryPath"));
        ForEach(*paths)
        {
            IPropertyTree &item = paths->query();
            if (item.hasProp("@path"))
                libraryPaths.append(item.queryProp("@path"));
        }
    }
}

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

    StringBuffer syspath;
    if (getPackageFolder(syspath))
    {
#if _WIN32
        extractOption(compilerPath, globals, "CL_PATH", "compilerPath", syspath, "componentfiles\\cl");
#else
        extractOption(compilerPath, globals, "CL_PATH", "compilerPath", "/usr", NULL);
#endif
        extractOption(libraryPath, globals, "ECLCC_LIBRARY_PATH", "libraryPath", syspath, "lib");
        extractOption(includePath, globals, "ECLCC_INCLUDE_PATH", "includePath", syspath, "componentfiles/cl/include");
        extractOption(pluginsPath, globals, "ECLCC_PLUGIN_PATH", "plugins", syspath, "plugins");
        extractOption(hooksPath, globals, "HPCC_FILEHOOKS_PATH", "filehooks", syspath, "filehooks");
        extractOption(templatePath, globals, "ECLCC_TPL_PATH", "templatePath", syspath, "componentfiles");
        extractOption(eclLibraryPath, globals, "ECLCC_ECLLIBRARY_PATH", "eclLibrariesPath", syspath, "share/ecllibrary/");
    }
    extractOption(stdIncludeLibraryPath, globals, "ECLCC_ECLINCLUDE_PATH", "eclIncludePath", ".", NULL);

    if (!optLogfile.length() && !optBatchMode)
        extractOption(optLogfile, globals, "ECLCC_LOGFILE", "logfile", "eclcc.log", NULL);

    if (logVerbose || optLogfile)
    {
        if (optLogfile.length())
        {
            StringBuffer lf;
            openLogFile(lf, optLogfile, optLogDetail, false);
            if (logVerbose)
                fprintf(stdout, "Logging to '%s'\n",lf.str());
        }
    }
    if (hooksPath.length())
        installFileHooks(hooksPath.str());

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

    ForEachItemIn(iLink, linkOptions)
        compiler->addLinkOption(linkOptions.item(iLink));

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

void EclCC::instantECL(EclCompileInstance & instance, IWorkUnit *wu, const char * queryFullName, IErrorReceiver *errs, const char * outputFile)
{
    StringBuffer processName(outputFile);
    if (instance.query && containsAnyActions(instance.query))
    {
        try
        {
            const char * templateDir = queryTemplateDir();
            bool optSaveTemps = wu->getDebugValueBool("saveEclTempFiles", false);
            bool optSaveCpp = optSaveTemps || optNoCompile || wu->getDebugValueBool("saveCppTempFiles", false);
            //New scope - testing things are linked correctly
            {
                Owned<IHqlExprDllGenerator> generator = createDllGenerator(errs, processName.toCharArray(), NULL, wu, templateDir, optTargetClusterType, NULL, false);

                setWorkunitHash(wu, instance.query);
                if (!optShared)
                    wu->setDebugValueInt("standAloneExe", 1, true);
                EclGenerateTarget target = optWorkUnit ? EclGenerateNone : (optNoCompile ? EclGenerateCpp : optShared ? EclGenerateDll : EclGenerateExe);
                if (optManifestFilename)
                    generator->addManifest(optManifestFilename);
                if (instance.srcArchive)
                {
                    generator->addManifestFromArchive(instance.srcArchive);
                    instance.srcArchive.clear();
                }
                generator->setSaveGeneratedFiles(optSaveCpp);

                bool generateOk = generator->processQuery(instance.query, target);  // NB: May clear instance.query
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

bool EclCC::isWithinPath(const char * sourcePathname, const char * searchPath)
{
    if (!sourcePathname)
        return false;

    StringBuffer expandedSourceName;
    makeAbsolutePath(sourcePathname, expandedSourceName);

    StringBuffer attributePath;
    return findFilenameInSearchPath(attributePath, searchPath, expandedSourceName);
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


void EclCC::processSingleQuery(EclCompileInstance & instance,
                               IFileContents * queryContents,
                               const char * queryAttributePath)
{
#ifdef TEST_LEGACY_DEPENDENCY_CODE
    setLegacyEclSemantics(instance.legacyMode);
    Owned<IPropertyTree> dependencies = gatherAttributeDependencies(instance.dataServer, "");
    if (dependencies)
        saveXML("depends.xml", dependencies);
#endif

    Owned<IErrorReceiver> wuErrs = new WorkUnitErrorReceiver(instance.wu, "eclcc");
    Owned<IErrorReceiver> errs = createCompoundErrorReceiver(instance.errs, wuErrs);

    //All dlls/exes are essentially cloneable because you may be running multiple instances at once
    //The only exception would be a dll created for a one-time query.  (Currently handled by eclserver.)
    instance.wu->setCloneable(true);

    applyDebugOptions(instance.wu);

    if (optTargetCompiler != DEFAULT_COMPILER)
        instance.wu->setDebugValue("targetCompiler", compilerTypeText[optTargetCompiler], true);

    bool withinRepository = (queryAttributePath && *queryAttributePath);
    bool syntaxChecking = instance.wu->getDebugValueBool("syntaxCheck", false);
    size32_t prevErrs = errs->errCount();
    unsigned startTime = msTick();
    const char * sourcePathname = queryContents ? queryContents->querySourcePath()->str() : NULL;
    const char * defaultErrorPathname = sourcePathname ? sourcePathname : queryAttributePath;

    {
        //Minimize the scope of the parse context to reduce lifetime of cached items.
        HqlParseContext parseCtx(instance.dataServer, instance.archive);
        if (optGenerateMeta || optIncludeMeta)
        {
            HqlParseContext::MetaOptions options;
            options.includePublicDefinitions = instance.wu->getDebugValueBool("metaIncludePublic", true);
            options.includePrivateDefinitions = instance.wu->getDebugValueBool("metaIncludePrivate", true);
            options.onlyGatherRoot = instance.wu->getDebugValueBool("metaIncludeMainOnly", false);
            options.includeImports = instance.wu->getDebugValueBool("metaIncludeImports", true);
            options.includeExternalUses = instance.wu->getDebugValueBool("metaIncludeExternalUse", true);
            options.includeExternalUses = instance.wu->getDebugValueBool("metaIncludeExternalUse", true);
            options.includeLocations = instance.wu->getDebugValueBool("metaIncludeLocations", true);
            options.includeJavadoc = instance.wu->getDebugValueBool("metaIncludeJavadoc", true);
            parseCtx.setGatherMeta(options);
        }

        setLegacyEclSemantics(instance.legacyMode);
        if (instance.archive)
            instance.archive->setPropBool("@legacyMode", instance.legacyMode);

        parseCtx.ignoreUnknownImport = instance.ignoreUnknownImport;

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

                instance.query.setown(getResolveAttributeFullPath(queryAttributePath, LSFpublic, ctx));
                if (!instance.query && !syntaxChecking && (errs->errCount() == prevErrs))
                {
                    StringBuffer msg;
                    msg.append("Could not resolve attribute ").append(queryAttributePath);
                    errs->reportError(3, msg.str(), defaultErrorPathname, 0, 0, 0);
                }
            }
            else
            {
                Owned<IHqlScope> scope = createPrivateScope();
                if (instance.legacyMode)
                    importRootModulesToScope(scope, ctx);

                instance.query.setown(parseQuery(scope, queryContents, ctx, NULL, NULL, true));

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

            gatherWarnings(ctx.errs, instance.query);

            if (instance.query && !syntaxChecking && !optGenerateMeta)
                instance.query.setown(convertAttributeToQuery(instance.query, ctx));

            if (instance.wu->getDebugValueBool("addTimingToWorkunit", true))
                instance.wu->setTimerInfo("EclServer: parse query", NULL, msTick()-startTime, 1, 0);

            if (optIncludeMeta || optGenerateMeta)
                instance.generatedMeta.setown(parseCtx.getMetaTree());
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            errs->reportError(3, s.toCharArray(), defaultErrorPathname, 1, 0, 0);
            e->Release();
        }
    }

    //Free up the repository (and any cached expressions) as soon as the expression has been parsed
    instance.dataServer.clear();

    if (!syntaxChecking && (errs->errCount() == prevErrs) && (!instance.query || !containsAnyActions(instance.query)))
    {
        errs->reportError(3, "Query is empty", defaultErrorPathname, 1, 0, 0);
        return;
    }

    if (instance.archive)
        return;

    if (syntaxChecking || optGenerateMeta)
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
    {
        const char * queryFullName = NULL;
        instantECL(instance, instance.wu, queryFullName, errs, targetFilename);
    }
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
    instance.srcArchive.setown(createPTreeFromXMLString(archiveXML, ipt_caseInsensitive));

    IPropertyTree * archiveTree = instance.srcArchive;
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

    EclRepositoryArray repositories;
    repositories.append(*LINK(pluginsRepository));

    Owned<IFileContents> contents;
    StringBuffer fullPath; // Here so it doesn't get freed when leaving the else block

    if (queryText || queryAttributePath)
    {
        const char * sourceFilename = archiveTree->queryProp("Query/@originalFilename");
        Owned<ISourcePath> sourcePath = createSourcePath(sourceFilename);
        contents.setown(createFileContentsFromText(queryText, sourcePath));
        if (queryAttributePath && queryText && *queryText)
        {
            Owned<IEclSourceCollection> inputFileCollection = createSingleDefinitionEclCollection(queryAttributePath, contents);
            repositories.append(*createRepository(inputFileCollection));
        }
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
        fullPath.append(syntaxCheckModule).append('.').append(syntaxCheckAttribute);
        queryAttributePath = fullPath.str();

        //Create a repository with just that attribute, and place it before the archive in the resolution order.
        Owned<IFileContents> contents = createFileContentsFromText(queryText, NULL);
        repositories.append(*createSingleDefinitionEclRepository(syntaxCheckModule, syntaxCheckAttribute, contents));
    }

    repositories.append(*createRepository(archiveCollection));
    instance.dataServer.setown(createCompoundRepository(repositories));

    //Ensure classes are not linked by anything else
    archiveCollection.clear();
    repositories.kill();

    processSingleQuery(instance, contents, queryAttributePath);
}


//=========================================================================================

void EclCC::processFile(EclCompileInstance & instance)
{
    const char * curFilename = instance.inputFile->queryFilename();
    assertex(curFilename);

    Owned<ISourcePath> sourcePath = createSourcePath(curFilename);
    Owned<IFileContents> queryText = createFileContentsFromFile(curFilename, sourcePath);
    const char * queryTxt = queryText->getText();
    if (optArchive || optGenerateDepend)
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
        bool withinRepository = false;
        bool inputFromStdIn = streq(curFilename, "stdin:");

        //Specifying --main indicates that the query text (if present) replaces that definition
        if (optQueryRepositoryReference)
        {
            withinRepository = true;
            attributePath.clear().append(optQueryRepositoryReference);
        }
        else
        {
            withinRepository = !inputFromStdIn && checkWithinRepository(attributePath, curFilename);
        }


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
            //-main only overrides the definition if the query is non-empty.  Otherwise use the existing text.
            if (!optQueryRepositoryReference || queryText->length())
            {
                Owned<IEclSourceCollection> inputFileCollection = createSingleDefinitionEclCollection(attributePath, queryText);
                repositories.append(*createRepository(inputFileCollection));
            }
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

        instance.dataServer.setown(createCompoundRepository(repositories));
        repositories.kill();
        processSingleQuery(instance, queryText, attributePath.str());
    }

    if (instance.reportErrorSummary() && !instance.archive)
        return;

    generateOutput(instance);
}


IFileIO * EclCC::createArchiveOutputFile(EclCompileInstance & instance)
{
    StringBuffer archiveName;
    if (instance.outputFilename && !streq(instance.outputFilename, "-"))
        addNonEmptyPathSepChar(archiveName.append(optOutputDirectory)).append(instance.outputFilename);
    else
        archiveName.append("stdout:");

    //Work around windows problem writing 64K to stdout if not redirected/piped
    OwnedIFile ifile = createIFile(archiveName);
    return ifile->open(IFOcreate);
}

void EclCC::outputXmlToOutputFile(EclCompileInstance & instance, IPropertyTree * xml)
{
    OwnedIFileIO ifileio = createArchiveOutputFile(instance);
    if (ifileio)
    {
        //Work around windows problem writing 64K to stdout if not redirected/piped
        Owned<IIOStream> stream = createIOStream(ifileio.get());
        Owned<IIOStream> buffered = createBufferedIOStream(stream,0x8000);
        saveXML(*buffered, xml);
    }
}


void EclCC::addFilenameDependency(StringBuffer & target, EclCompileInstance & instance, const char * filename)
{
    if (!filename)
        return;

    //Ignore plugins and standard library components
    if (isWithinPath(filename, pluginsPath) || isWithinPath(filename, eclLibraryPath))
        return;

    //Don't include the input file in the dependencies.
    if (instance.inputFile)
    {
        const char * sourceFilename = instance.inputFile->queryFilename();
        if (sourceFilename && streq(sourceFilename, filename))
            return;
    }
    target.append(filename).newline();
}


void EclCC::generateOutput(EclCompileInstance & instance)
{
    const char * outputFilename = instance.outputFilename;
    if (instance.archive)
    {
        if (optGenerateDepend)
        {
            //Walk the archive, and output all filenames that aren't
            //a)in a plugin b) in std.lib c) the original source file.
            StringBuffer filenames;
            Owned<IPropertyTreeIterator> modIter = instance.archive->getElements("Module");
            ForEach(*modIter)
            {
                IPropertyTree * module = &modIter->query();
                if (module->hasProp("@plugin"))
                    continue;
                addFilenameDependency(filenames, instance, module->queryProp("@sourcePath"));

                Owned<IPropertyTreeIterator> defIter = module->getElements("Attribute");
                ForEach(*defIter)
                {
                    IPropertyTree * definition = &defIter->query();
                    addFilenameDependency(filenames, instance, definition->queryProp("@sourcePath"));
                }
            }

            OwnedIFileIO ifileio = createArchiveOutputFile(instance);
            if (ifileio)
                ifileio->write(0, filenames.length(), filenames.str());
        }
        else
        {
            // Output option settings
            instance.wu->getDebugValues();
            Owned<IStringIterator> debugValues = &instance.wu->getDebugValues();
            ForEach (*debugValues)
            {
                SCMStringBuffer debugStr, valueStr;
                debugValues->str(debugStr);
                instance.wu->getDebugValue(debugStr.str(), valueStr);
                Owned<IPropertyTree> option = createPTree("Option");
                option->setProp("@name", debugStr.str());
                option->setProp("@value", valueStr.str());
                instance.archive->addPropTree("Option", option.getClear());
            }
            if (optManifestFilename)
                addManifestResourcesToArchive(instance.archive, optManifestFilename);

            outputXmlToOutputFile(instance, instance.archive);
        }
    }

    if (optGenerateMeta && instance.generatedMeta)
        outputXmlToOutputFile(instance, instance.generatedMeta);

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
    if (optArchive || optGenerateDepend)
        instance.archive.setown(createAttributeArchive());

    instance.dataServer.setown(createCompoundRepositoryF(pluginsRepository.get(), libraryRepository.get(), includeRepository.get(), NULL));
    processSingleQuery(instance, NULL, queryAttributePath);

    if (instance.reportErrorSummary())
        return;
    generateOutput(instance);
}


bool EclCC::processFiles()
{
    loadOptions();
    ForEachItemIn(idx, inputFileNames)
    {
        processArgvFilename(inputFiles, inputFileNames.item(idx));
    }
    if (inputFiles.ordinality() == 0)
    {
        if (optBatchMode || !optQueryRepositoryReference)
        {
            ERRLOG("No input files could be opened");
            return false;
        }
    }


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
    bool showHelp = false;
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
        else if (iter.matchFlag(tempBool, "-save-cpps"))
        {
            setDebugOption("saveCppTempFiles", tempBool);
        }
        else if (iter.matchFlag(tempBool, "-save-temps"))
        {
            setDebugOption("saveEclTempFiles", tempBool);
        }
        else if (iter.matchFlag(showHelp, "-help") || iter.matchFlag(showHelp, "--help"))
        {
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
        else if (iter.matchFlag(optIncludeMeta, "-meta"))
        {
        }
        else if (iter.matchFlag(optGenerateMeta, "-M"))
        {
        }
        else if (iter.matchFlag(optGenerateDepend, "-Md"))
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
        else if (iter.matchOption(tempArg, "-platform") || /*deprecated*/ iter.matchOption(tempArg, "-target"))
        {
            if (!setTargetPlatformOption(tempArg.get(), optTargetClusterType))
                return false;
        }
        else if (iter.matchFlag(logVerbose, "-v") || iter.matchFlag(logVerbose, "--verbose"))
        {
            Owned<ILogMsgFilter> filter = getDefaultLogMsgFilter();
            queryLogMsgManager()->changeMonitorFilter(queryStderrLogMsgHandler(), filter);
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
        else if (startsWith(arg, "-Wl,"))
        {
            //Pass these straight through to the linker - with -Wl, prefix removed
            linkOptions.append(arg+4);
        }
        else if (startsWith(arg, "-Wp,") || startsWith(arg, "-Wa,"))
        {
            //Pass these straight through to the gcc compiler
            compileOptions.append(arg);
        }
        else if (iter.matchFlag(optWorkUnit, "-wu"))
        {
        }
        else if (strcmp(arg, "-")==0) 
        {
            inputFileNames.append("stdin:");
        }
        else if (arg[0] == '-')
        {
            ERRLOG("Error: unrecognised option %s",arg);
            usage();
            return false;
        }
        else
            inputFileNames.append(arg);
    }
    if (showHelp)
    {
        usage();
        return false;
    }

    // Option post processing follows:
    if (optArchive || optWorkUnit || optGenerateMeta || optGenerateDepend)
        optNoCompile = true;

    loadManifestOptions();

    if (inputFileNames.ordinality() == 0)
    {
        if (!optBatchMode && optQueryRepositoryReference)
            return true;
        ERRLOG("No input filenames supplied");
        return false;
    }

    if (optDebugMemLeak)
    {
        StringBuffer title;
        title.append(inputFileNames.item(0)).newline();
        initLeakCheck(title);
    }

    return true;
}

//=========================================================================================

// Exclamation in the first column indicates it is only part of the verbose output
const char * const helpText[] = {
    "",
    "Usage:",
    "    eclcc <options> queryfile.ecl",
    "",
    "General options:",
    "    -I <path>     Add path to locations to search for ecl imports",
    "    -L <path>     Add path to locations to search for system libraries",
    "    -o <file>     Specify name of output file (default a.out if linking to",
    "                  executable, or stdout)",
    "    -manifest     Specify path to manifest file listing resources to add",
    "    -foption[=value] Set an ecl option (#option)",
    "    -main <ref>   Compile definition <ref> from the source collection",
    "    -syntax       Perform a syntax check of the ECL",
    "    -platform=hthor Generate code for hthor executable (default)",
    "    -platform=roxie Generate code for roxie cluster",
    "    -platform=thor  Generate code for thor cluster",
    "",
    "Output control options",
    "    -E            Output preprocessed ECL in xml archive form",
    "!   -M            Output meta information for the ecl files",
    "!   -Md           Output dependency information",
    "    -q            Save ECL query text as part of workunit",
    "    -wu           Only generate workunit information as xml file",
    "",
    "c++ options",
    "    -S            Generate c++ output, but don't compile",
    "!   -c            compile only (don't link)",
    "    -g            Enable debug symbols in generated code",
    "    -Wc,xx        Pass option xx to the c++ compiler",
    "!   -Wl,xx        Pass option xx to the linker",
    "!   -Wa,xx        Passed straight through to c++ compiler",
    "!   -Wp,xx        Passed straight through to c++ compiler",
    "!   -save-cpps    Do not delete generated c++ files (implied if -g)",
    "!   -save-temps   Do not delete intermediate files",
    "    -shared       Generate workunit shared object instead of a stand-alone exe",
    "",
    "Other options:",
    "!   -b            Batch mode.  Each source file is processed in turn.  Output",
    "!                 name depends on the input filename",
#ifdef _WIN32
    "!   -brk <n>      Trigger a break point in eclcc after nth allocation",
#endif
    "    -help, --help Display this message",
    "    -help -v      Display verbose help message",
    "!   -internal     Run internal tests",
    "!   -legacy       Use legacy import semantics (deprecated)",
    "    --logfile <file> Write log to specified file",
    "!   --logdetail=n Set the level of detail in the log file",
#ifdef _WIN32
    "!   -m            Enable leak checking",
#endif
    "!   -P <path>     Specify the path of the output files (only with -b option)",
    "    -specs file   Read eclcc configuration from specified file",
    "!   -split m:n    Process a subset m of n input files (only with -b option)",
    "    -v --verbose  Output additional tracing information while compiling",
    "    --version     Output version information",
    "!",
    "!#options",
    "! -factivitiesPerCpp      Number of activities in each c++ file",
    "!                         (requires -fspanMultipleCpp)",
    "! -fapplyInstantEclTransformations Limit non file outputs with a CHOOSEN",
    "! -fapplyInstantEclTransformationsLimit Number of records to limit to",
    "! -fcheckAsserts          Check ASSERT() statements",
    "! -fmaxCompileThreads     Number of compiler instances to compile the c++",
    "! -fnoteRecordSizeInGraph Add estimates of record sizes to the graph",
    "! -fpickBestEngine        Allow simple thor queries to be passed to thor",
    "! -fshowActivitySizeInGraph Show estimates of generated c++ size in the graph",
    "! -fshowMetaInGraph       Add distribution/sort orders to the graph",
    "! -fshowRecordCountInGraph Show estimates of record counts in the graph",
    "! -fspanMultipleCpp       Generate a work unit in multiple c++ files",
    "",
};

void EclCC::usage()
{
    for (unsigned line=0; line < _elements_in(helpText); line++)
    {
        const char * text = helpText[line];
        if (*text == '!')
        {
            if (logVerbose)
            {
                //Allow conditional headers
                if (text[1] == ' ')
                    fprintf(stdout, " %s\n", text+1);
                else
                    fprintf(stdout, "%s\n", text+1);
            }
        }
        else
            fprintf(stdout, "%s\n", text);
    }
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
        // Print compiler and arguments to help reproduce problems
        for (int i=0; i<argc; i++)
            fprintf(logFile, "%s ", argv[i]);
        fprintf(logFile, "\n");
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
    {
        queryLogMsgManager()->removeMonitor(handler);
        handler.clear();
    }

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


