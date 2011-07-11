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

interface ICppCompiler : public IInterface
{
public:
    virtual void addCompileOption(const char * option) = 0;
    virtual void addDefine(const char * symbolName, const char * value = NULL) = 0;
    virtual void addLibrary(const char * libName) = 0;
    virtual void addLibraryPath(const char * libPath) = 0;
    virtual void addInclude(const char * includePath) = 0;
    virtual void addLinkOption(const char * option) = 0;
    virtual void addSourceFile(const char * filename) = 0;
    virtual bool compile() = 0;
    virtual void setDebug(bool _debug) = 0;
    virtual void setDebugLibrary(bool _debug) = 0;
    virtual void setLinkOptions(const char * option) = 0;
    virtual void setOnlyCompile(bool _onlyCompile) = 0;
    virtual void setCreateExe(bool _createExe) = 0;
    virtual void setOptimizeLevel(unsigned level) = 0;
    virtual void setTargetBitLength(unsigned bitlength) = 0;
    virtual void setMaxCompileThreads(const unsigned max) = 0;
    virtual void setCCLogPath(const char* path) = 0;
    virtual void setSaveTemps(bool _save) = 0;
    virtual void setAbortChecker(IAbortRequestCallback * abortChecker) = 0;
};

extern jlib_decl ICppCompiler * createCompiler(const char * coreName, const char * sourceDir = NULL, const char * targetDir = NULL, bool verbose = true);
extern jlib_decl ICppCompiler * createCompiler(const char * coreName, const char * sourceDir, const char * targetDir, CompilerType compiler, bool verbose);

#endif
