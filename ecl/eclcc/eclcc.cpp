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
#include "hqlrepository.hpp"
#include "hqlerror.hpp"

#include "hqlgram.hpp"
#include "hqlremote.hpp"
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

//=========================================================================================

class IndirectErrorReceiver : public CInterface, implements IErrorReceiver
{
public:
    IndirectErrorReceiver(IErrorReceiver * _prev) : prev(_prev) {}
    IMPLEMENT_IINTERFACE

    virtual void reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        prev->reportError(errNo, msg, filename, lineno, column, pos);
    }
    virtual void report(IECLError* err)
    {
        prev->report(err);
    }
    virtual void reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        prev->reportWarning(warnNo, msg, filename, lineno, column, pos);
    }
    virtual size32_t errCount()
    {
        return prev->errCount();
    }
    virtual size32_t warnCount()
    {
        return prev->warnCount();
    }

protected:
    Linked<IErrorReceiver> prev;
};

class ErrorInserter : public IndirectErrorReceiver
{
public:
    ErrorInserter(IErrorReceiver * _prev, IECLError * _error) : IndirectErrorReceiver(_prev), error(_error) {}

    virtual void reportError(int errNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        flush();
        IndirectErrorReceiver::reportError(errNo, msg, filename, lineno, column, pos);
    }
    virtual void report(IECLError* err)
    {
        flush();
        IndirectErrorReceiver::report(err);
    }
    virtual void reportWarning(int warnNo, const char *msg, const char *filename, int lineno, int column, int pos)
    {
        flush();
        IndirectErrorReceiver::reportWarning(warnNo, msg, filename, lineno, column, pos);
    }

protected:
    void flush()
    {
        if (error)
        {
            IndirectErrorReceiver::report(error);
            error.clear();
        }
    }

protected:
    Linked<IECLError> error;
};




struct EclCompileInstance
{
public:
    EclCompileInstance(IFile * _inputFile, IErrorReceiver & _errs, FILE * _errout, const char * _outputFilename) :
      inputFile(_inputFile), errs(&_errs), errout(_errout), outputFilename(_outputFilename)
    {
        importAllModules = queryLegacyEclSemantics();
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
    bool importAllModules;
    Owned<IPropertyTree> srcArchive;
    bool fromArchive;
};

class EclCC
{
public:
    EclCC(const char * _programName)
        : programName(_programName)
    {
        logVerbose = false;
        optArchive = false;
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
    void processSingleQuery(EclCompileInstance & instance, IEclRepository * dataServer, const char * sourcePathname,
                               const char * queryText,
                               const char * queryAttributePath,
                               const char * syntaxCheckModule, const char * syntaxCheckAttribute
                               );
    void processXmlFile(EclCompileInstance & instance, const char *archiveXML);
    void processFile(EclCompileInstance & info);
    void processReference(EclCompileInstance & instance, const char * queryAttributePath);
    void processBatchFiles();
    void reportCompileErrors(IErrorReceiver *errs, const char * processName);
    void setDebugOption(const char * name, bool value);
    void usage();

    inline const char * queryTemplateDir() { return templatePath.length() ? templatePath.str() : NULL; }


protected:
    Owned<IEclRepository> libraryRepository;
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

static IHqlExpression * transformAttributeToQuery(EclCompileInstance & instance, IHqlExpression * expr, HqlLookupContext & ctx)
{
    if (expr->isMacro())
    {
        if (!queryLegacyEclSemantics())
            return NULL;
        //Only expand macros if legacy semantics enabled
        IHqlExpression * macroBodyExpr;
        if (expr->getOperator() == no_funcdef)
        {
            if (expr->queryChild(1)->numChildren() != 0)
                return NULL;
            macroBodyExpr = expr->queryChild(0);
        }
        else
            macroBodyExpr = expr;

        IFileContents * macroContents = static_cast<IFileContents *>(macroBodyExpr->queryUnknownExtra());
        Owned<IHqlRemoteScope> scope = createRemoteScope(NULL, NULL, ctx.queryRepository(), NULL, NULL, false);
        return parseQuery(scope->queryScope(), macroContents, ctx, NULL, true);
    }

    if (expr->isFunction())
    {
        //If a scope with parameters then assume we are building a library.
        if (expr->isScope())
            return LINK(expr);

        HqlExprArray actuals;
        if (!allParametersHaveDefaults(expr))
        {
            if (!expandMissingDefaultsAsStoreds(actuals, expr))
            {
                //For each parameter that doesn't have a default, create a stored variable of the appropriate type
                //with a null value as the default value, and use that.
                const char * name = expr->queryName()->str();
                StringBuffer msg;
                msg.appendf("Definition %s() does not supply default values for all parameters", name ? name : "");
                ctx.errs->reportError(HQLERR_CannotSubmitFunction, msg.str(), NULL, 1, 0, 0);
                return NULL;
            }
        }

        return createBoundFunction(instance.errs, expr, actuals, ctx.functionCache, ctx.queryExpandCallsWhenBound());
    }

    if (expr->isScope())
    {
        IHqlScope * scope = expr->queryScope();
        OwnedHqlExpr main = scope->lookupSymbol(createAtom("main"), LSFpublic, ctx);
        if (main)
            return main.getClear();

        StringBuffer msg;
        const char * name = scope->queryFullName();
        msg.appendf("Module %s does not EXPORT an attribute main()", name ? name : "");
        ctx.errs->reportError(HQLERR_CannotSubmitModule, msg.str(), NULL, 1, 0, 0);
        return NULL;
    }

    return LINK(expr);
}

static IHqlExpression * convertAttributeToQuery(EclCompileInstance & instance, IHqlExpression * expr, HqlLookupContext & ctx)
{
    OwnedHqlExpr query = LINK(expr);
    loop
    {
        OwnedHqlExpr transformed = transformAttributeToQuery(instance, query, ctx);
        if (!transformed || transformed == query)
            return transformed.getClear();
        query.set(transformed);
    }
}

inline bool endsWith(unsigned lenSrc, const char * src, unsigned lenSuffix, const char * suffix)
{
    return (lenSrc >= lenSuffix) && (memcmp(src+lenSrc-lenSuffix, suffix, lenSuffix) == 0);
}

static bool convertPathToModule(StringBuffer & out, const char * filename)
{
    const unsigned len = strlen(filename);
    unsigned trail;
    if (endsWith(len, filename, 4, ".ecl"))
        trail = 4;
    else if (endsWith(len, filename, 7, ".eclmod"))
        trail = 7;
    else
        return false;

    const unsigned copyLen = len-trail;
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


void EclCC::processSingleQuery(EclCompileInstance & instance, IEclRepository * dataServer, const char * sourcePathname,
                               const char * queryText,
                               const char * queryAttributePath,
                               const char * syntaxCheckModule, const char * syntaxCheckAttribute
                               )
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

    StringBuffer attributePath;
    bool withinRepository = false;
    if (queryAttributePath)
    {
        withinRepository = true;
        attributePath.append(queryAttributePath);
    }
    else if (!instance.fromArchive)
    {
        withinRepository = checkWithinRepository(attributePath, sourcePathname);

        //Disable this for the moment when running the regression suite.
        //It also doesn't work in batch mode for files in nested directories since filename is reduced to basename
        if (!optBatchMode && !withinRepository)
        {
            StringBuffer expandedSourceName;
            makeAbsolutePath(sourcePathname, expandedSourceName);

            StringBuffer thisDirectory;
            StringBuffer thisTail;
            splitFilename(expandedSourceName, &thisDirectory, &thisDirectory, &thisTail, &thisTail);

            Owned<IEclRepository> localRepository =  createSourceFileEclRepository(errs, NULL, thisDirectory, NULL, 0);
            Owned<IEclRepository> compound = createCompoundRepositoryF(repository, localRepository.get(), NULL);
            repository.set(compound);

            if (convertPathToModule(attributePath, thisTail))
                withinRepository = true;
        }
    }

    Owned<IHqlRemoteScope> rScope;
    Owned<IHqlScope> scope;
    Owned<ISourcePath> sourcePath = createSourcePath(sourcePathname);
    bool syntaxChecking = instance.wu->getDebugValueBool("syntaxCheck", false) || (syntaxCheckAttribute != NULL);
    size32_t prevErrs = errs->errCount();
    unsigned startTime = msTick();
    OwnedHqlExpr qquery;
    const char * defaultErrorPathname = sourcePathname ? sourcePathname : attributePath.str();

    {
        //Minimize the scope of the parse context to reduce lifetime of cached items.
        HqlParseContext parseCtx(repository, instance.archive);
        if (!withinRepository && syntaxCheckModule)
        {
            if (!sourcePath)
            {
                StringBuffer temp;
                temp.append(syntaxCheckModule).append('.').append(syntaxCheckAttribute);
                sourcePath.setown(createSourcePath(temp));
            }

            scope.setown(createSyntaxCheckScope(parseCtx, syntaxCheckModule, syntaxCheckAttribute));
            if (!scope)
            {
                StringBuffer msg;
                msg.appendf("Module %s does not exist",syntaxCheckModule);
                errs->reportError(0, msg.str(), sourcePath->str(), 1, 0, 0);
                return;
            }
        }
        else
        {
            rScope.setown(createRemoteScope(NULL, NULL, repository, NULL, NULL, false));
            scope.set(rScope->queryScope());
        }

        try
        {
            HqlLookupContext ctx(parseCtx, errs);

            if (withinRepository)
            {
                if (instance.archive)
                {
                    instance.archive->setProp("Query", "");
                    instance.archive->setProp("Query/@attributePath", attributePath);
                }
                qquery.setown(getResolveAttributeFullPath(attributePath, LSFpublic, ctx));
                if (!qquery && !syntaxChecking && (errs->errCount() == prevErrs))
                {
                    StringBuffer msg;
                    msg.append("Could not resolve attribute ").append(attributePath.str());
                    errs->reportError(3, msg.str(), defaultErrorPathname, 0, 0, 0);
                }
            }
            else
            {
                Owned<IFileContents> contents = createFileContentsFromText(queryText, sourcePath);

                if (instance.importAllModules)
                {
                    HqlScopeArray rootScopes;
                    repository->getRootScopes(rootScopes, ctx);
                    ForEachItemIn(i, rootScopes)
                    {
                        IHqlScope & cur = rootScopes.item(i);
                        scope->defineSymbol(cur.queryName(), NULL, LINK(queryExpression(&cur)), false, true, ob_module);
                    }
                }

                qquery.setown(parseQuery(scope, contents, ctx, NULL, true));
            }

            gatherWarnings(ctx.errs, qquery);

            if (instance.wu->getDebugValueBool("addTimingToWorkunit", true))
                instance.wu->setTimerInfo("EclServer: parse query", NULL, msTick()-startTime, 1, 0);

            if (qquery && !syntaxChecking)
                qquery.setown(convertAttributeToQuery(instance, qquery, ctx));
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
    instance.srcArchive.setown(createPTreeFromXMLString(archiveXML, ipt_caseInsensitive));
    Owned<IEclRepository> dataServer = createXmlDataServer(instance.srcArchive, libraryRepository);
    Owned<IPropertyTreeIterator> iter = instance.srcArchive->getElements("Option");
    ForEach(*iter) 
    {
        IPropertyTree &item = iter->query();
        instance.wu->setDebugValue(item.queryProp("@name"), item.queryProp("@value"), true);
    }

    const char * queryText = instance.srcArchive->queryProp("Query");
    const char * queryAttributePath = instance.srcArchive->queryProp("Query/@attributePath");
    //Takes precedence over an entry in the archive - so you can submit parts of an archive.
    if (optQueryRepositoryReference)
        queryAttributePath = optQueryRepositoryReference;

    const char * sourceFilename = instance.srcArchive->queryProp("Query/@originalFilename");
    instance.eclVersion.set(instance.srcArchive->queryProp("@eclVersion"));
    if (instance.eclVersion)
    {
        unsigned major, minor, subminor;
        if (extractVersion(major, minor, subminor, instance.eclVersion))
        {
            if (major != LANGUAGE_VERSION_MAJOR)
                throwError2(HQLERR_VersionMismatch, instance.eclVersion.get(), LANGUAGE_VERSION);
            if (minor != LANGUAGE_VERSION_MINOR)
            {
                StringBuffer msg;
                msg.appendf("Mismatch in minor version number (%s v %s)", instance.eclVersion.get(), LANGUAGE_VERSION);
                instance.errs->reportWarning(HQLERR_VersionMismatch, msg.str(), NULL, 0, 0);
            }
            else if (subminor != LANGUAGE_VERSION_MINOR)
            {
                StringBuffer msg;
                msg.appendf("Mismatch in subminor version number (%s v %s)", instance.eclVersion.get(), LANGUAGE_VERSION);
                Owned<IECLError> warning = createECLWarning(HQLERR_VersionMismatch, msg.str(), NULL, 0, 0);
                instance.errs.setown(new ErrorInserter(instance.errs, warning));
            }
        }
    }

    if (queryText || queryAttributePath)
    {
        processSingleQuery(instance, dataServer, sourceFilename, queryText, queryAttributePath, NULL, NULL);
    }
    else
    {
        //This is really only useful for regression testing
        const char * queryText = instance.srcArchive->queryProp("SyntaxCheck");
        const char * syntaxCheckModule = instance.srcArchive->queryProp("SyntaxCheck/@module");
        const char * syntaxCheckAttribute = instance.srcArchive->queryProp("SyntaxCheck/@attribute");
        if (queryText && syntaxCheckModule && syntaxCheckAttribute)
            processSingleQuery(instance, dataServer, syntaxCheckModule, queryText, NULL, syntaxCheckModule, syntaxCheckAttribute);
        else
            throw MakeStringException(1, "No query found in xml");
    }
}


//=========================================================================================

void EclCC::processFile(EclCompileInstance & instance)
{
    const char * curFilename = instance.inputFile->queryFilename();
    assertex(curFilename);
    StringBuffer fname;
    if (optBatchMode)
        splitFilename(curFilename, NULL, NULL, &fname, &fname);
    else
        fname.append(curFilename);

    Owned<ISourcePath> sourcePath = createSourcePath(fname.str());
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
        processSingleQuery(instance, libraryRepository, fname.str(), queryTxt, NULL, NULL, NULL);

    if (instance.reportErrorSummary())
        return;

    if (instance.archive)
    {
        if (!instance.archive->hasProp("Query/@attributePath"))
        {
            const char * p = queryTxt;
            if (0 == strncmp(p, (const char *)UTF8_BOM,3))
                p += 3;
            instance.archive->setProp("Query", p );
            instance.archive->setProp("Query/@originalFilename", fname.str());
        }
    }

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

    processSingleQuery(instance, libraryRepository, NULL, NULL, queryAttributePath, NULL, NULL);

    if (instance.reportErrorSummary())
        return;
    generateOutput(instance);
}


bool EclCC::processFiles()
{
    loadOptions();

    StringBuffer searchPath;
    searchPath.append(eclLibraryPath).append(ENVSEPCHAR).append(stdIncludeLibraryPath).append(ENVSEPCHAR).append(includeLibraryPath);
    Owned<IErrorReceiver> errs = createFileErrorReceiver(stderr);
    libraryRepository.setown(createSourceFileEclRepository(errs, pluginsPath.str(), searchPath.str(), NULL, logVerbose ? PLUGIN_DLL_MODULE : 0));

    bool ok = true;
    if (optBatchMode)
    {
        processBatchFiles();
    }
    else if (inputFiles.ordinality() == 0)
    {
        assertex(optQueryRepositoryReference);
        EclCompileInstance info(NULL, *errs, stderr, optOutputFilename);
        processReference(info, optQueryRepositoryReference);
        ok = (errs->errCount() == 0);
    }
    else
    {
        EclCompileInstance info(&inputFiles.item(0), *errs, stderr, optOutputFilename);
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
        else if (iter.matchFlag(tempBool, "-legacy"))
        {
            setLegacyEclSemantics(tempBool);
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
            EclCompileInstance info(&file, *localErrs, logFile, outFilename);
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


