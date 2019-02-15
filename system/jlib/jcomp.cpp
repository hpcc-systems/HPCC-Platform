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


#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jcomp.hpp"
#include "jsem.hpp"
#include "jexcept.hpp"
#include "jregexp.hpp"
#include "jerror.hpp"

#ifdef _WIN32
#include <windows.h>
#include <winbase.h>
#include <io.h>
#else
#include <sys/wait.h>
#endif
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>

#include "jfile.hpp"
#include "jdebug.hpp"
#include "jcomp.ipp"

#define CC_EXTRA_OPTIONS        ""
#ifdef GENERATE_LISTING
#undef CC_EXTRA_OPTIONS
#define CC_EXTRA_OPTIONS        " /FAs"
#endif

#ifdef _WIN32
#define DEFAULT_COMPILER Vs6CppCompiler
#else
#define DEFAULT_COMPILER GccCppCompiler
#endif

//---------------------------------------------------------------------------

#define BASE_ADDRESS "0x00480000"
//#define BASE_ADDRESS        "0x10000000"

static const char * CC_NAME_CPP[] =   { "\"#" PATHSEPSTR "bin" PATHSEPSTR "cl.bat\"",   "\"#" PATHSEPSTR "bin" PATHSEPSTR "g++\"" };
static const char * CC_NAME_C[] =   { "\"#" PATHSEPSTR "bin" PATHSEPSTR "cl.bat\"",   "\"#" PATHSEPSTR "bin" PATHSEPSTR "gcc\"" };
static const char * LINK_NAME[] = { "\"#" PATHSEPSTR "bin" PATHSEPSTR "link.bat\"", "\"#" PATHSEPSTR "bin" PATHSEPSTR "g++\"" };
static const char * LIB_DIR[] = { "\"#\\lib\"", "\"#/lib\"" };
static const char * LIB_OPTION_PREFIX[] = { "", "-Wl," };
static const char * USE_LIBPATH_FLAG[] = { "/libpath:\"", "-L" };
static const char * USE_LIBPATH_TAIL[] = { "\"", "" };
static const char * USE_LIBRPATH_FLAG[] = { NULL, "-Wl,-rpath," };
static const char * USE_LIB_FLAG[] = { "", "-l" };
static const char * USE_LIB_TAIL[] = { ".lib", "" };
static const char * USE_INCLUDE_FLAG[] = { "/I\"", "\"-I" };
static const char * USE_DEFINE_FLAG[] = { "/D", "-D" };
static const char * USE_INCLUDE_TAIL[] = { "\"", "\"" };
static const char * INCLUDEPATH[] = { "\"#\\include\"", "\"#/include\"" };
static const char * LINK_SEPARATOR[] = { " /link ", " " };
static const char * OBJECT_FILE_EXT[] = { "obj", "o" };
static const char * PCH_FILE_EXT[] = { "", "gch" };

static const char * LIBFLAG_DEBUG[] = { "/MDd", "" };
static const char * LIBFLAG_RELEASE[] = { "/MD", "" };
static const char * COMPILE_ONLY[] = { "/c", "-c" };

static const char * CC_OPTION_CORE[] = { "", "-fvisibility=hidden -DUSE_VISIBILITY=1 -Werror -Wno-tautological-compare" };
static const char * LINK_OPTION_CORE[] = { "/DLL /libpath:." , "" };
static const char * CC_OPTION_DEBUG[] = { "/Zm500 /EHsc /GR /Zi /nologo /bigobj", "-g -fPIC  -O0" };
static const char * CC_OPTION_RELEASE[] = { "/Zm500 /EHsc /GR /Oi /Ob1 /GF /nologo /bigobj", "-fPIC  -O0" };
static const char * CC_OPTION_C[] = { "", "" };
static const char * CC_OPTION_CPP[] = { "", "-std=c++11" };

static const char * CC_OPTION_PRECOMPILEHEADER[] = { "", " -x c++-header" };

#ifdef __APPLE__
static const char * DLL_LINK_OPTION_DEBUG[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO /DEBUG /DEBUGTYPE:CV", "-g -shared -L. -fPIC -pipe -O0" };
static const char * EXE_LINK_OPTION_DEBUG[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO /DEBUG /DEBUGTYPE:CV", "-g -L. -fPIC -pipe -O0 -Wl,-export_dynamic -v" };
static const char * DLL_LINK_OPTION_RELEASE[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO", "-shared -L. -fPIC -pipe -O0" };
static const char * EXE_LINK_OPTION_RELEASE[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO", "-L. -fPIC -pipe -O0 -Wl,-export_dynamic -v" };
#else
static const char * DLL_LINK_OPTION_DEBUG[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO /DEBUG /DEBUGTYPE:CV", "-g -shared -L. -fPIC -pipe -O0" };
static const char * EXE_LINK_OPTION_DEBUG[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO /DEBUG /DEBUGTYPE:CV", "-g -L. -Wl,-E -fPIC -pipe -O0" };
static const char * DLL_LINK_OPTION_RELEASE[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO", "-shared -L. -fPIC -pipe -O0" };
static const char * EXE_LINK_OPTION_RELEASE[] = { "/BASE:" BASE_ADDRESS " /NOLOGO /LARGEADDRESSAWARE /INCREMENTAL:NO", "-L. -Wl,-E -fPIC -pipe -O0" };
#endif

static const char * LINK_TARGET[] = { " /out:", " -o " };
static const char * DEFAULT_CC_LOCATION[] = { ".", "." };

//===========================================================================

static StringAttr compilerRoot;
static StringAttr stdIncludes;
static StringBuffer stdLibs;

static StringBuffer &dequote(StringBuffer &in)
{
    if (in.length() >= 2 && in.charAt(0)=='"' && in.charAt(in.length()-1)=='"')
    {
        in.remove(in.length()-1, 1);
        in.remove(0, 1);
    }
    return in;
}

static void doSetCompilerPath(const char * path, const char * includes, const char * libs, const char * tmpdir, unsigned targetCompiler, bool verbose)
{
    if (!includes)
        includes = INCLUDEPATH[targetCompiler];
    if (!libs)
        libs = LIB_DIR[targetCompiler];
    if (verbose)
    {
        LOG(MCoperatorInfo, "Include directory set to %s", includes);
        LOG(MCoperatorInfo, "Library directory set to %s", libs);
    }
    compilerRoot.set(path ? path : targetCompiler==GccCppCompiler ? "/usr" : ".\\CL");
    stdIncludes.set(includes);
    stdLibs.clear();
    for (;;)
    {
        StringBuffer thislib;
        while (*libs && *libs != ENVSEPCHAR)
            thislib.append(*libs++);
        if (thislib.length())
        {
            stdLibs.append(" ").append(USE_LIBPATH_FLAG[targetCompiler]).append(thislib).append(USE_LIBPATH_TAIL[targetCompiler]);
            if (USE_LIBRPATH_FLAG[targetCompiler])
                stdLibs.append(" ").append(USE_LIBRPATH_FLAG[targetCompiler]).append(thislib);
        }
        if (!*libs)
            break;
        libs++;
    }
    StringBuffer fname;
    if (path)
    {
        const char *finger = CC_NAME_CPP[targetCompiler];
        while (*finger)
        {
            if (*finger == '#')
                fname.append(path);
            else
                fname.append(*finger);
            finger++;
        }

#if defined(__linux__)
        StringBuffer clbin_dir;
        const char* dir_end = strrchr(fname, '/');
        if(dir_end == NULL)
            clbin_dir.append(".");
        else
            clbin_dir.append((dir_end - fname.str()) + 1, fname.str());
        
        StringBuffer pathenv(clbin_dir.str());
        const char* oldpath = getenv("PATH");
        if(oldpath != NULL && *oldpath != '\0')
        pathenv.append(":").append(oldpath);
        setenv("PATH", pathenv.str(), 1);
#endif
    }
    else
    {
        fname.append(compilerRoot).append(CC_NAME_CPP[targetCompiler]);
        fname.replaceString("#",NULL);
    }
    if (verbose)
        LOG(MCoperatorInfo, "Compiler path set to %s", fname.str());

    dequote(fname);
#ifdef _WIN32
    if (_access(fname.str(), 4))
    {
#else
#if defined(__linux__) || defined(__FreeBSD__) || defined(__APPLE__)
    struct stat filestatus;
    int r = stat(fname.str(), &filestatus);
    if (    (r != 0)
        ||  (!S_ISREG(filestatus.st_mode))
        ||  ((filestatus.st_mode&(S_IXOTH|S_IXGRP|S_IXUSR))==0))
    {
        if (r == -1) errno = ENOENT;
#endif
#endif
        if (verbose)
            LOG(MCoperatorInfo, "SetCompilerPath - no compiler found");
        throw makeOsExceptionV(GetLastError(), "setCompilerPath could not locate compiler %s", fname.str());
    }

    if(tmpdir && *tmpdir)
    {
        //MORE: this should be done for the child process instead of the parent but invoke does not let me do it
#if defined(__linux__)
        setenv("TMPDIR", tmpdir, 1);
#endif

#ifdef _WIN32
        StringBuffer tmpbuf;
        tmpbuf.append("TMP=").append(tmpdir);
        _putenv(tmpbuf.str());
#endif
    }
}

//===========================================================================

class CCompilerThreadParam : public CInterface
{
public:
    CCompilerThreadParam(const StringBuffer & _cmdline, Semaphore & _finishedCompiling, const StringBuffer & _logfile) : cmdline(_cmdline), logfile(_logfile), finishedCompiling(_finishedCompiling) {};

    StringBuffer        cmdline;
    StringBuffer        logfile;
    Semaphore       &   finishedCompiling;
};

//===========================================================================

static void setDirectoryPrefix(StringAttr & target, const char * source)
{
    if (source && *source)
    {
        StringBuffer temp;
        target.set(addDirectoryPrefix(temp, source));
    }
}

CppCompiler::CppCompiler(const char * _coreName, const char * _sourceDir, const char * _targetDir, unsigned _targetCompiler, bool _verbose)
{
    coreName.set(_coreName);
    targetCompiler = _targetCompiler;
    createDLL = true;
#ifdef _DEBUG
    setDebug(true);
    setDebugLibrary(true);
#else
    setDebug(false);
    setDebugLibrary(false);
#endif
    
    setDirectoryPrefix(sourceDir, _sourceDir);
    setDirectoryPrefix(targetDir, _targetDir);
    maxCompileThreads = 1;
    onlyCompile = false;
    verbose = _verbose;
    saveTemps = false;
    abortChecker = NULL;
    precompileHeader = false;
    linkFailed = false;
}

void CppCompiler::addCompileOption(const char * option)
{
    compilerOptions.append(' ').append(option);
}

void CppCompiler::setPrecompileHeader(bool _pch)
{
    if (targetCompiler!=GccCppCompiler)
        throw MakeStringException(0, "precompiled header generation only supported for g++ and compatible compilers");
    precompileHeader = _pch;
}

bool CppCompiler::fireException(IException *e)
{
    CriticalBlock block(cs);
    exceptions.append(*LINK(e));
    return true;
}

void CppCompiler::addDefine(const char * symbolName, const char * value)
{
    compilerOptions.append(" ").append(USE_DEFINE_FLAG[targetCompiler]).append(symbolName);

    if (value)
        compilerOptions.append('=').append(value);
}

void CppCompiler::addLibrary(const char * libName)
{
    if (verbose)
        LOG(MCoperatorInfo, "addLibrary %s", libName);

    const char* lname = libName;
    const char * quote;
    StringBuffer path, tail; // NOTE - because of the (hacky) code below that sets lname to point within tail.str(), this must NOT be moved inside the if block
    if (targetCompiler == GccCppCompiler)
    {
        // It seems gcc compiler doesn't like things like -lmydir/libx.so
        splitFilename(libName, &path, &path, &tail, &tail);
        if(path.length())
        {
            addLibraryPath(path);
            lname = tail.str();
            // HACK - make it work with plugins. This should be handled at caller end!
            if (strncmp(lname, "lib", 3) == 0)
                lname += 3;
        }
        quote = NULL;   //quoting lib names with gcc causes link error (lib not found)
    }
    else
    {
        quote = "\"";
    }
    linkerLibraries.append(" ").append(USE_LIB_FLAG[targetCompiler]).append(quote).append(lname).append(USE_LIB_TAIL[targetCompiler]).append(quote);
}

void CppCompiler::addLibraryPath(const char * libPath)
{
    linkerOptions.append(" ").append(USE_LIBPATH_FLAG[targetCompiler]).append(libPath).append(USE_LIBPATH_TAIL[targetCompiler]);
    if (USE_LIBRPATH_FLAG[targetCompiler])
        linkerOptions.append(" ").append(USE_LIBRPATH_FLAG[targetCompiler]).append(libPath);
}

void CppCompiler::_addInclude(StringBuffer &s, const char * paths)
{
    if (!paths)
        return;
    StringBuffer includePath;
    for (;;)
    {
        while (*paths && *paths != ENVSEPCHAR)
            includePath.append(*paths++);
        if (includePath.length())
            s.append(" ").append(USE_INCLUDE_FLAG[targetCompiler]).append(includePath).append(USE_INCLUDE_TAIL[targetCompiler]);
        if (!*paths)
            break;
        paths++;
        includePath.clear();
    }
}

void CppCompiler::addInclude(const char * paths)
{
    _addInclude(compilerOptions, paths);
}

void CppCompiler::addLinkOption(const char * option)
{
    if (option && *option)	
        linkerOptions.append(' ').append(LIB_OPTION_PREFIX[targetCompiler]).append(option);
}

void CppCompiler::addSourceFile(const char * filename, const char *flags)
{
    DBGLOG("addSourceFile %s", filename);
    allSources.append(filename);
    allFlags.append(flags);
}

void CppCompiler::addObjectFile(const char * filename)
{
    linkerLibraries.append(" ").append(filename);
}

void CppCompiler::writeLogFile(const char* filepath, StringBuffer& log)
{
    if(!filepath || !*filepath || !log.length())
        return;

    Owned <IFile> f = createIFile(filepath);
    if(f->exists())
        f->remove();

    Owned <IFileIO> fio = f->open(IFOcreaterw);
    if(fio.get())
        fio->write(0, log.length(), log.str());
}

bool CppCompiler::compile()
{
    if (abortChecker && abortChecker->abortRequested())
        return false;

    TIME_SECTION(!verbose ? NULL : onlyCompile ? "compile" : "compile/link");

    Owned<IThreadPool> pool = createThreadPool("CCompilerWorker", this, this, maxCompileThreads?maxCompileThreads:1, INFINITE);
    addCompileOption(COMPILE_ONLY[targetCompiler]);

    bool ret = false;
    Semaphore finishedCompiling;
    int numSubmitted = 0;
    numFailed.store(0);

    ForEachItemIn(i0, allSources)
    {
        ret = compileFile(pool, allSources.item(i0), allFlags.item(i0), finishedCompiling);
        if (!ret)
            break;
        ++numSubmitted;
    }

    for (int idx = 0; idx < numSubmitted; idx++)
    {
        if (abortChecker)
        {
            while (!finishedCompiling.wait(5*1000))
            {
                if (abortChecker && abortChecker->abortRequested())
                {
                    UERRLOG("Aborting compilation");
                    pool->stopAll(true);
                    if (!pool->joinAll(true, 10*1000))
                        WARNLOG("CCompilerWorker; timed out waiting for threads in pool");
                    return false;
                }
            }
        }
        else
            finishedCompiling.wait();
    }

    if (numFailed > 0)
        ret = false;
    else if (!onlyCompile && !precompileHeader)
        ret = doLink();

    if (!saveTemps && !onlyCompile)
    {
        removeTemporaries();
        StringBuffer temp;
        ForEachItemIn(i2, allSources)
            remove(getObjectName(temp.clear(), allSources.item(i2)).str());
    }

    //Combine logfiles
    const char* cclog = ccLogPath.get();
    if(!cclog||!*cclog)
        cclog = queryCcLogName();
    Owned <IFile> dstfile = createIFile(cclog);
    dstfile->remove();

    Owned<IFileIO> dstIO = dstfile->open(IFOwrite);
    ForEachItemIn(i2, logFiles)
    {
        Owned <IFile> srcfile = createIFile(logFiles.item(i2));
        if (srcfile->exists())
        {
            dstIO->appendFile(srcfile);
            srcfile->remove();
        }
    }

    //Don't leave lots of blank log files around if the compile was successful
    bool logIsEmpty = (dstIO->size() == 0);
    dstIO.clear();
    if (ret && logIsEmpty)
        dstfile->remove();

    pool->joinAll(true, 1000);
    return ret;
}

bool CppCompiler::compileFile(IThreadPool * pool, const char * filename, const char *flags, Semaphore & finishedCompiling)
{
    if (!filename || *filename == 0)
        return false;

    StringBuffer cmdline;
    const char *ext = pathExtension(filename);
    bool isC = ext != nullptr && strieq(ext, ".c");
    cmdline.append(isC ? CC_NAME_C[targetCompiler] : CC_NAME_CPP[targetCompiler]);
    if (precompileHeader)
        cmdline.append(CC_OPTION_PRECOMPILEHEADER[targetCompiler]);
    cmdline.append(" \"");
    if (sourceDir.length())
    {
        cmdline.append(sourceDir);
        addPathSepChar(cmdline);
    }
    cmdline.append(filename);
    cmdline.append("\" ");
    expandCompileOptions(cmdline, isC);

    if (useDebugLibrary)
        cmdline.append(" ").append(LIBFLAG_DEBUG[targetCompiler]);
    else
        cmdline.append(" ").append(LIBFLAG_RELEASE[targetCompiler]);

    _addInclude(cmdline, stdIncludes);
    
    if (targetCompiler == Vs6CppCompiler)
    {
        if (targetDir.get())
            cmdline.append(" /Fo").append("\"").append(targetDir).append("\"");

        StringBuffer basename;
        splitFilename(filename, &basename, &basename, &basename, NULL);
        cmdline.append(" /Fd").append("\"").append(targetDir).append(createDLL ? SharedObjectPrefix : NULL).append(filename).append(".pdb").append("\"");//MORE: prefer create a single pdb file using coreName
    }
    else
    {
        cmdline.append(" -o ").append("\"");
        if (precompileHeader)
            cmdline.append(targetDir).append(filename).append('.').append(PCH_FILE_EXT[targetCompiler]);
        else
            getObjectName(cmdline, filename);
        cmdline.append("\"");
    }
    if (flags)
        cmdline.append(" ").append(flags);
    
    StringBuffer expanded;
    expandRootDirectory(expanded, cmdline);
    StringBuffer logFile;
    logFile.append(filename).append(".log.tmp");
    logFiles.append(logFile);

    Owned<CCompilerThreadParam> parm;
    if (verbose)
        UERRLOG("%s", expanded.str());
    parm.setown(new CCompilerThreadParam(expanded, finishedCompiling, logFile));
    pool->start(parm.get());

    return true;
}

void CppCompiler::extractErrors(IArrayOf<IError> & errors)
{
    ForEachItemIn(i, exceptions)
    {
        IException & cur = exceptions.item(i);
        StringBuffer msg;
        cur.errorMessage(msg);
        errors.append(*createError(JLIBERR_CppCompileError, msg, nullptr));
    }
    const char* cclog = ccLogPath.get();
    if(!cclog||!*cclog)
        cclog = queryCcLogName();
    Owned <IFile> logfile = createIFile(cclog);
    if (!logfile->exists())
        return;

    try
    {
        StringBuffer file;
        file.loadFile(logfile);

        RegExpr vsErrorPattern("^{.+}({[0-9]+}) : error {.*$}");
        RegExpr vsLinkErrorPattern("^{.+} : error {.*$}");

        //cpperr.ecl:7:10: error: ‘syntaxError’ was not declared in this scope
        RegExpr gccErrorPattern("^{.+}:{[0-9]+}:{[0-9]+}: {[a-z]+}: {.*$}");
        RegExpr gccErrorPattern2("^{.+}:{[0-9]+}: {[a-z]+}: {.*$}");
        RegExpr gccLinkErrorPattern("^{.+}:{[0-9]+}: {.*$}"); // undefined reference
        RegExpr gccLinkErrorPattern2("^.+ld: {.*$}"); // fail to find library etc.
        RegExpr gccExitStatusPattern("^.*exit status$"); // collect2: error: ld returned 1 exit status
        const char * cur = file.str();
        do
        {
            const char * newline = strchr(cur, '\n');
            StringAttr next;
            if (newline)
            {
                next.set(cur, newline-cur);
                cur = newline+1;
                if (*cur == '\r')
                    cur++;
            }
            else
            {
                next.set(cur);
                cur = NULL;
            }

            if (gccExitStatusPattern.find(next))
            {
                //ignore
            }
            else if (gccErrorPattern.find(next))
            {
                StringBuffer filename, line, column, kind, msg;
                gccErrorPattern.findstr(filename, 1);
                gccErrorPattern.findstr(line, 2);
                gccErrorPattern.findstr(column, 3);
                gccErrorPattern.findstr(kind, 4);
                gccErrorPattern.findstr(msg, 5);

                if (strieq(kind, "error"))
                    errors.append(*createError(CategoryError, SeverityError, JLIBERR_CppCompileError, msg.str(), filename.str(), atoi(line), atoi(column), 0));
                else
                    errors.append(*createError(CategoryCpp, SeverityWarning, JLIBERR_CppCompileError, msg.str(), filename.str(), atoi(line), atoi(column), 0));
            }
            else if (gccErrorPattern2.find(next))
            {
                StringBuffer filename, line, kind, msg;
                gccErrorPattern2.findstr(filename, 1);
                gccErrorPattern2.findstr(line, 2);
                gccErrorPattern2.findstr(kind, 3);
                gccErrorPattern2.findstr(msg, 4);

                if (strieq(kind, "error"))
                    errors.append(*createError(CategoryError, SeverityError, JLIBERR_CppCompileError, msg.str(), filename.str(), atoi(line), 0, 0));
                else
                    errors.append(*createError(CategoryCpp, SeverityWarning, JLIBERR_CppCompileError, msg.str(), filename.str(), atoi(line), 0, 0));
            }
            else if (gccLinkErrorPattern.find(next))
            {
                StringBuffer filename, line, msg;
                gccLinkErrorPattern.findstr(filename, 1);
                gccLinkErrorPattern.findstr(line, 2);
                gccLinkErrorPattern.findstr(msg, 3);

                ErrorSeverity severity = linkFailed ? SeverityError : SeverityWarning;
                errors.append(*createError(CategoryError, severity, JLIBERR_CppCompileError, msg.str(), filename.str(), atoi(line), 0, 0));
            }
            else if (gccLinkErrorPattern2.find(next))
            {
                StringBuffer msg("C++ link error: ");
                gccLinkErrorPattern2.findstr(msg, 1);
                ErrorSeverity severity = linkFailed ? SeverityError : SeverityWarning;
                errors.append(*createError(CategoryError, severity, JLIBERR_CppCompileError, msg.str(), NULL, 0, 0, 0));
            }
            else if (vsErrorPattern.find(next))
            {
                StringBuffer filename, line, msg("C++ compiler error: ");
                vsErrorPattern.findstr(filename, 1);
                vsErrorPattern.findstr(line, 2);
                vsErrorPattern.findstr(msg, 3);
                errors.append(*createError(CategoryError, SeverityError, JLIBERR_CppCompileError, msg.str(), filename.str(), atoi(line), 0, 0));
            }
            else if (vsLinkErrorPattern.find(next))
            {
                StringBuffer filename, msg("C++ link error: ");
                vsLinkErrorPattern.findstr(filename, 1);
                vsLinkErrorPattern.findstr(msg, 2);
                errors.append(*createError(CategoryError, SeverityError, JLIBERR_CppCompileError, msg.str(), filename.str(), 0, 0, 0));
            }
        } while (cur);
    }
    catch (IException * e)
    {
        e->Release();
    }
}

void CppCompiler::expandCompileOptions(StringBuffer & target, bool isC)
{
    target.append(" ").append(CC_OPTION_CORE[targetCompiler]).append(" ");
    if (targetDebug)
        target.append(CC_OPTION_DEBUG[targetCompiler]);
    else
        target.append(CC_OPTION_RELEASE[targetCompiler]);
    target.append(compilerOptions).append(CC_EXTRA_OPTIONS);
    if (isC)
        target.append(" ").append(CC_OPTION_C[targetCompiler]);
    else
        target.append(" ").append(CC_OPTION_CPP[targetCompiler]);
}

bool CppCompiler::doLink()
{
    StringBuffer cmdline;
    cmdline.append(LINK_NAME[targetCompiler]).append(LINK_SEPARATOR[targetCompiler]);

    cmdline.append(" ");
    if (targetDebug)
        cmdline.append(createDLL ? DLL_LINK_OPTION_DEBUG[targetCompiler] : EXE_LINK_OPTION_DEBUG[targetCompiler]);
    else
        cmdline.append(createDLL ? DLL_LINK_OPTION_RELEASE[targetCompiler] : EXE_LINK_OPTION_RELEASE[targetCompiler]);
    cmdline.append(" ");

    if (createDLL)
        cmdline.append(" ").append(LINK_OPTION_CORE[targetCompiler]);
    cmdline.append(stdLibs);

    ForEachItemIn(i0, allSources)
    {
        StringBuffer objFilename;
        getObjectName(objFilename, allSources.item(i0));
        cmdline.append(" ").append("\"").append(objFilename).append("\"");
    }

    cmdline.append(linkerOptions);
    cmdline.append(linkerLibraries);

    StringBuffer outName;
    outName.append(createDLL ? SharedObjectPrefix : NULL).append(coreName).append(createDLL ? SharedObjectExtension : ProcessExtension);
    cmdline.append(LINK_TARGET[targetCompiler]).append("\"").append(targetDir).append(outName).append("\"");

    StringBuffer temp;
    remove(temp.clear().append(targetDir).append(outName).str());

    StringBuffer expanded;
    expandRootDirectory(expanded, cmdline);

    DWORD runcode = 0;
    if (verbose)
        UERRLOG("%s", expanded.str());
    StringBuffer logFile = StringBuffer(coreName).append("_link.log.tmp");
    logFiles.append(logFile);

    bool ret;
    try
    {
        ret = invoke_program(expanded.str(), runcode, true, logFile, nullptr, true) && (runcode == 0);
    }
    catch (IException * e)
    {
        exceptions.append(*e);
        ret = false;
    }
    linkFailed = !ret;
    return ret;
}

void CppCompiler::expandRootDirectory(StringBuffer & expanded, StringBuffer & in)
{
    unsigned len = in.length();
    unsigned i;
    for (i = 0; i < len; i++)
    {
        char c = in.charAt(i);
        if (c == '#')
        {
            if (compilerRoot)
                expanded.append(compilerRoot);
            else
                expanded.append(DEFAULT_CC_LOCATION[targetCompiler]);
        }
        else
            expanded.append(c);
    }
}


StringBuffer & CppCompiler::getObjectName(StringBuffer & out, const char * filename)
{
    out.append(targetDir);
    if (targetCompiler == Vs6CppCompiler)
        splitFilename(filename, NULL, NULL, &out, NULL);
    else
        splitFilename(filename, NULL, NULL, &out, &out);
    return out.append(".").append(OBJECT_FILE_EXT[targetCompiler]);
}

void CppCompiler::removeTemporaries()
{
    DBGLOG("Remove temporaries");

    StringBuffer temp;
    switch (targetCompiler)
    {
    case Vs6CppCompiler:
        {
            removeFileTraceIfFail(temp.clear().append(targetDir).append(coreName).append(".exp").str());
            removeFileTraceIfFail(temp.clear().append(targetDir).append(coreName).append(".lib").str());
            removeFileTraceIfFail(temp.clear().append(targetDir).append(coreName).append(".res").str());
            break;
        }
    case GccCppCompiler:
        {
            temp.clear().append(coreName).append(".res.s*");
            DBGLOG("Remove %s%s",targetDir.str(), temp.str());
            Owned<IDirectoryIterator> resTemps = createDirectoryIterator(targetDir, temp.str());
            ForEach(*resTemps)
            {
                removeFileTraceIfFail(resTemps->getName(temp.clear().append(targetDir)).str());
            }
            break;
        }
    }
}

void CppCompiler::setDebug(bool _debug)
{
    targetDebug = _debug;
}

void CppCompiler::setDebugLibrary(bool debug)
{
    useDebugLibrary = debug;
}

void CppCompiler::setCreateExe(bool _createExe) 
{ 
    createDLL = !_createExe; 
}


void CppCompiler::setOptimizeLevel(unsigned level)
{
    const char * option = NULL;

    switch (targetCompiler)
    {
    case Vs6CppCompiler:
        switch (level)
        {
        case 0: option = "/Od"; break;
        case 1: option = "/O1"; break;
        case 2: option = "/O2"; break;
        default: // i.e. 3 or higher
            option = "/Ob1gty /G6"; break;
        }
        break;
    case GccCppCompiler:
        switch (level)
        {
        case 0: option = "-O0"; break;  // do not optimize
        case 1: option = "-O1"; break;
        case 2: option = "-O2"; break;
        default: // i.e. 3 or higher
            option = "-O3"; break;
        }
        break;
    }

    if (option)
        addCompileOption(option);
}

void CppCompiler::setTargetBitLength(unsigned bitlength)
{
    const char * option = NULL;
    switch (targetCompiler)
    {
    case Vs6CppCompiler:
        switch (bitlength)
        {
        case 32: break; // option is passed with --arch to VsDevCmd, cannot control from the command line
        case 64: break; // you will get a link error if it has not been set compatibility
        default:
            throwUnexpected();
        }
        break;
    case GccCppCompiler:
#if defined (_ARCH_X86_64_) || defined(_ARCH_X86_)
        // Note that gcc only seems to support these options on the x86 architecture
        switch (bitlength)
        {
        case 32: option = "-m32"; break;
        case 64: option = "-m64"; break;
        default:
            throwUnexpected();
        }
#endif
        break;
    }
    if (option)
        addCompileOption(option);
}

void CppCompiler::setCCLogPath(const char* path)
{
    if(path && *path)
        ccLogPath.set(path);
}


//===========================================================================

bool fileIsOlder(const char *dest, const char *src)
{
    int h1 = _open(dest, _O_RDONLY);
    if (h1 == -1)
        return true;
    int h2 = _open(src, _O_RDONLY);
    assertex(h2 != -1);
    struct _stat stat1;
    struct _stat stat2;
    _fstat(h1, &stat1);
    _fstat(h2, &stat2);
    _close(h1);
    _close(h2);
    return stat1.st_mtime < stat2.st_mtime;
}

//===========================================================================

void setCompilerPath(const char * path, const char * includes, const char * libs, const char * tmpdir, bool verbose)
{
    doSetCompilerPath(path, includes, libs, tmpdir, DEFAULT_COMPILER, verbose);
}

void setCompilerPath(const char * path, const char * includes, const char * libs, const char * tmpdir, CompilerType targetCompiler, bool verbose)
{
    doSetCompilerPath(path, includes, libs, tmpdir, targetCompiler, verbose);
}


ICppCompiler * createCompiler(const char * coreName, const char * sourceDir, const char * targetDir, bool verbose)
{
    return new CppCompiler(coreName, sourceDir, targetDir, DEFAULT_COMPILER, verbose);
}

ICppCompiler * createCompiler(const char * coreName, const char * sourceDir, const char * targetDir, CompilerType targetCompiler, bool verbose)
{
    return new CppCompiler(coreName, sourceDir, targetDir, targetCompiler, verbose);
}

//===========================================================================

class CCompilerWorker : implements IPooledThread, public CInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CCompilerWorker(CppCompiler * _compiler, bool _okToAbort) : compiler(_compiler), okToAbort(_okToAbort)
    {
        handle = 0;
        aborted = false;
    }
    virtual bool canReuse() const override { return true; }
    virtual bool stop() override
    {
        if (okToAbort)
            interrupt_program(handle, true);
        aborted = true;
        return true;
    }
    virtual void init(void *_params) override    { params.set((CCompilerThreadParam *)_params); }

    virtual void threadmain() override
    {
        DWORD runcode = 0;
        bool success;
        aborted = false;
        handle = 0;
        Owned<IException> error;
        try
        {
            success = invoke_program(params->cmdline, runcode, false, params->logfile, &handle, true, okToAbort);
            if (success)
                wait_program(handle, runcode, true);
        }
        catch(IException* e)
        {
            StringBuffer sb;
            e->errorMessage(sb);
            error.setown(e);
            if (sb.length())
                IERRLOG("%s", sb.str());
            success = false;
        }
        handle = 0;

        if (!success || aborted || runcode != 0)
            compiler->numFailed++;
        params->finishedCompiling.signal();
        if (error)
            throw error.getClear();
    }

private:
    CppCompiler * compiler;
    Owned<CCompilerThreadParam> params;
    HANDLE handle;
    bool aborted;
    bool okToAbort;
};

IPooledThread *CppCompiler::createNew()
{
    return new CCompilerWorker(this, (abortChecker != NULL));
}
