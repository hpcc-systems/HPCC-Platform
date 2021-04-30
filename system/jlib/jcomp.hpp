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



#ifndef JCOMP_HPP
#define JCOMP_HPP

#include "jlib.hpp"
#include "jmisc.hpp"

enum CompilerType
{
    Vs6CppCompiler      = 0,
    GccCppCompiler      = 1,
    MaxCompiler
};
const char * const compilerTypeText[MaxCompiler] = {"vs6", "gcc" };

#ifdef _WIN32
#define DEFAULT_COMPILER Vs6CppCompiler
#else
#define DEFAULT_COMPILER GccCppCompiler
#endif

extern jlib_decl void setCompilerPath(const char * path, const char *ipath, const char *lpath, const char * tmpdir, bool verbose);
extern jlib_decl void setCompilerPath(const char * path, const char *ipath, const char *lpath, const char * tmpdir, CompilerType compiler, bool verbose);
extern jlib_decl bool fileIsOlder(const char *dest, const char *src);
extern jlib_decl void extractErrorsFromCppLog(IArrayOf<IError> & errors, const char * cur, bool linkFailed);

interface ICppCompiler : public IInterface
{
public:
    virtual void addCompileOption(const char * option) = 0;
    virtual void addDefine(const char * symbolName, const char * value = NULL) = 0;
    virtual void addLibrary(const char * libName) = 0;
    virtual void addLibraryPath(const char * libPath) = 0;
    virtual void addInclude(const char * includePath) = 0;
    virtual void addLinkOption(const char * option) = 0;
    virtual void addSourceFile(const char * filename, const char *flags) = 0;
    virtual void addObjectFile(const char * filename) = 0;
    virtual bool compile() = 0;
    virtual void extractErrors(IArrayOf<IError> & errors) = 0;
    virtual void setDebug(bool _debug) = 0;
    virtual void setDebugLibrary(bool _debug) = 0;
    virtual void setOnlyCompile(bool _onlyCompile) = 0;
    virtual void setCreateExe(bool _createExe) = 0;
    virtual void setOptimizeLevel(unsigned level) = 0;
    virtual void setTargetBitLength(unsigned bitlength) = 0;
    virtual void setMaxCompileThreads(const unsigned max) = 0;
    virtual void setCCLogPath(const char* path) = 0;
    virtual void setSaveTemps(bool _save) = 0;
    virtual void setPrecompileHeader(bool _pch) = 0;
    virtual void setAbortChecker(IAbortRequestCallback * abortChecker) = 0;
    virtual void removeTemporary(const char *fname) = 0;
    virtual void removeTempDir(const char *fname) = 0;
    virtual bool reportOnly() const = 0;
    virtual void finish() = 0;

};

extern jlib_decl ICppCompiler * createCompiler(const char * coreName, const char * sourceDir, const char * targetDir, CompilerType compiler, bool verbose, const char *compileBatchOut);

#endif
