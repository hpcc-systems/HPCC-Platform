/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

// NLP-ENGINE requires LINUX to be set for linux compiles.
// The NLP-ENGINE needs to be updated to __linux__.
#if defined(__linux__) || defined(__APPLE__)
#define LINUX 1
#endif

#include "nlp_engine.h"
#include "nlp_eng.hpp"
#include "jfile.hpp"
#include "jlog.hpp"
#include "jutil.hpp"

#include <iostream>
#include <sys/stat.h>

static NLP_ENGINE *nlpEngine = NULL;

static CriticalSection csNLP;

NLPEng::NLPEng(const char *_manifestFolder) : manifestFolder(_manifestFolder) {}

NLPEng::~NLPEng() {}

#include <sstream>

bool folderExists(const std::string &folderPath)
{
    struct stat info;
    return (stat(folderPath.c_str(), &info) == 0 && S_ISDIR(info.st_mode));
}

void copyFolder(IFile *sourceDir, const char *destdir)
{
    Owned<IDirectoryIterator> files = sourceDir->directoryFiles(NULL, false, true);
    ForEach(*files)
    {
        IFile *thisFile = &files->query();
        StringBuffer tail;
        splitFilename(thisFile->queryFilename(), NULL, NULL, &tail, &tail);
        StringBuffer destname(destdir);
        destname.append(PATHSEPCHAR).append(tail);
        Owned<IFile> targetFile = createIFile(destname);
        if (thisFile->isDirectory() == fileBool::foundYes)
        {
            targetFile->createDirectory();
            copyFolder(thisFile, destname);
        }
        else
        {
            doCopyFile(targetFile, thisFile, 1024 * 1024, NULL, NULL, false);
        }
    }
}

int NLPEng::nlpEngAnalyze(const char *analyzerName, const char *inputText, std::ostringstream &sso)
{
    DBGLOG("analyzerName:  %s", analyzerName);

#ifdef NLP_DEBUG
    ofstream handle;
    handle.open(NLP_DEBUG_FILE, std::ofstream::out | std::ofstream::app);
    handle << "=====================================================================================" << endl;
    handle << "[queryCurrentProcessPath: " << w << "]" << endl;
#endif

    //  Backward compatibility  ---
    std::string w = queryCurrentProcessPath();
    size_t pos = w.find_last_of("/");
    size_t pos2 = w.find_last_of("/", pos - 1);
    std::string parent = w.substr(0, pos2);
    VStringBuffer nlpEngineFolder("%s/%s", parent.c_str(), "plugins/nlp/nlp-engine");

    VStringBuffer workingFolder("%s/analyzers/%s", manifestFolder.c_str(), analyzerName);
    if (folderExists(workingFolder.str()))
    {
        workingFolder.set(manifestFolder.c_str());

        //  TODO:  nlp-engine should allow "data" folder location to be specified.
        VStringBuffer sourceFolder("%s/%s", nlpEngineFolder.str(), "data");
        VStringBuffer destFolder("%s/%s", workingFolder.str(), "data");
        recursiveCreateDirectory(destFolder.str());
        copyFolder(createIFile(sourceFolder.str()), destFolder.str());
    }
    else
    {
        workingFolder = nlpEngineFolder;
    }

    DBGLOG("workingFolder:  %s", workingFolder.str());

#ifdef NLP_DEBUG
    handle << "[parent: " << parent << "]" << endl;
    handle << "[workingFolder nlp: " << workingFolder.str() << "]" << endl;
    handle.close();
#endif

    {
        CriticalBlock block(csNLP);
        if (nlpEngine == NULL)
        {
            nlpEngine = new NLP_ENGINE(workingFolder.str());
        }
    }

    std::istringstream ssi(inputText);

#ifdef NLP_DEBUG
    clock_t s_time, e_time;
    s_time = clock();
#endif

    nlpEngine->analyze((char *)analyzerName, &ssi, &sso, true);
    sso.seekp(0, std::ios_base::end);
    int len = sso.tellp();

#ifdef NLP_DEBUG
    e_time = clock();
    handle.open(NLP_DEBUG_FILE, std::ofstream::out | std::ofstream::app);
    handle << "===============================================" << endl;
    handle << "[Analyzer: " << analyzerName << "]" << endl;
    handle << "[Text: " << inputText << "]" << endl;
    handle << "[Exec analyzer time="
           << (double)(e_time - s_time) / CLOCKS_PER_SEC
           << " sec]" << endl;
    handle << sso.str() << endl;
    handle << "[len: " << len << "]" << endl;
    handle.close();
#endif

    return len;
}