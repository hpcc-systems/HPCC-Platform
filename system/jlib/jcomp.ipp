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


#ifndef JCOMP_IPP
#define JCOMP_IPP


#include "jlib.hpp"

class CppCompiler : public CInterface, implements ICppCompiler, implements IThreadFactory
{
public:
    CppCompiler(const char * _coreName, const char * _sourceDir, const char * _targetDir, unsigned _targetCompiler, bool _verbose);
    IMPLEMENT_IINTERFACE

    virtual void addCompileOption(const char * option);
    virtual void addDefine(const char * symbolName, const char * value = NULL);
    virtual void addLibrary(const char * libName);
    virtual void addLibraryPath(const char * libPath);
    virtual void addLinkOption(const char * option);
    virtual void addInclude(const char * includePath);
    virtual void addSourceFile(const char * filename);
    virtual bool compile();
    virtual void setDebug(bool _debug);
    virtual void setDebugLibrary(bool _debug);
    virtual void setLinkOptions(const char * option);
    virtual void setOnlyCompile(bool _onlyCompile) { onlyCompile = _onlyCompile; }
    virtual void setCreateExe(bool _createExe); 
    virtual void setOptimizeLevel(unsigned level);
    virtual void setTargetBitLength(unsigned bitlength);
    virtual void setMaxCompileThreads(const unsigned max) { maxCompileThreads = max; }
    virtual IPooledThread *createNew();
    virtual void setCCLogPath(const char* path);
    virtual void setSaveTemps(bool _save) { saveTemps = _save; }
    virtual void setAbortChecker(IAbortRequestCallback * _abortChecker) {abortChecker = _abortChecker;}

protected:
    void expandCompileOptions(StringBuffer & target);
    void expandRootDirectory(StringBuffer & expanded, StringBuffer & in);
    StringBuffer & getObjectName(StringBuffer & out, const char * filename);
    void removeTemporaries();
    void resetLinkOptions();
    bool compileFile(IThreadPool * pool, const char * filename, Semaphore & finishedCompiling);
    bool doLink();
    void writeLogFile(const char* filepath, StringBuffer& log);

public:
    atomic_t        numFailed;

protected:
    StringBuffer    compilerOptions;
    StringAttr      libraryOptions;
    StringBuffer    linkerOptions;
    StringBuffer    linkerLibraries;
    StringAttr      sourceDir;
    StringAttr      targetDir;
    StringArray     allSources;
    StringArray     logFiles;
    StringAttr      ccLogPath;
    unsigned        targetCompiler;
    unsigned        maxCompileThreads;
    bool            onlyCompile;
    bool            createDLL;
    bool            targetDebug;
    bool            verbose;
    void _addInclude(StringBuffer &s, const char *paths);
    bool            saveTemps;
    IAbortRequestCallback * abortChecker;
};


#endif
