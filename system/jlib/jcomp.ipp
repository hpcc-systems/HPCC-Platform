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
    virtual void setOnlyCompile(bool _onlyCompile) { onlyCompile = _onlyCompile; }
    virtual void setCreateExe(bool _createExe); 
    virtual void setOptimizeLevel(unsigned level);
    virtual void setTargetBitLength(unsigned bitlength);
    virtual void setMaxCompileThreads(const unsigned max) { maxCompileThreads = max; }
    virtual IPooledThread *createNew();
    virtual void setCCLogPath(const char* path);
    virtual void setSaveTemps(bool _save) { saveTemps = _save; }
    virtual void setPrecompileHeader(bool _pch);
    virtual void setAbortChecker(IAbortRequestCallback * _abortChecker) {abortChecker = _abortChecker;}

protected:
    void expandCompileOptions(StringBuffer & target);
    void expandRootDirectory(StringBuffer & expanded, StringBuffer & in);
    StringBuffer & getObjectName(StringBuffer & out, const char * filename);
    void removeTemporaries();
    bool compileFile(IThreadPool * pool, const char * filename, Semaphore & finishedCompiling);
    bool doLink();
    void writeLogFile(const char* filepath, StringBuffer& log);

public:
    atomic_t        numFailed;

protected:
    StringBuffer    compilerOptions;
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
    bool            useDebugLibrary;
    bool            verbose;
    void _addInclude(StringBuffer &s, const char *paths);
    bool            saveTemps;
    bool            precompileHeader;
    IAbortRequestCallback * abortChecker;
};


#endif
