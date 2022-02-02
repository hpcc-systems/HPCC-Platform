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

static NLP_ENGINE *nlpEngine = NULL;
static CriticalSection csNLP; 

NLPEng::NLPEng() {}

NLPEng::~NLPEng() {}

#include <sstream>

int NLPEng::nlpEngAnalyze(const char *analyzerName, const char *inputText, ostringstream &sso)
{
    string w = queryCurrentProcessPath();
    
#ifdef NLP_DEBUG
    ofstream handle;
    handle.open(NLP_DEBUG_FILE, std::ofstream::out | std::ofstream::app);
    handle << "[queryCurrentProcessPath: " << w << "]" << endl;
#endif

    // This is where the nlp analyzers reside that can be called by the user.
    // The path logic here is hacked for now for the first version given that
    // how these files will get to the server has yet to be determined.
    size_t pos = w.find_last_of("/");
    size_t pos2 = w.find_last_of("/",pos-1);
    string parent = w.substr(0,pos2);
    VStringBuffer workingFolder("%s/%s",parent.c_str(),"plugins/nlp/nlp-engine");

#ifdef NLP_DEBUG
    handle << "[parent: " << parent << "]" << endl;
    handle << "[workingFolder nlp: " << workingFolder.str() << "]" << endl;
    handle.close();
#endif

    {
        CriticalBlock block(csNLP);
        if (nlpEngine == NULL) {
            nlpEngine = new NLP_ENGINE(workingFolder.str());
        }
    }

    istrstream ssi(inputText);

#ifdef NLP_DEBUG
    clock_t s_time, e_time;
    s_time = clock();
#endif

    nlpEngine->analyze((char *)analyzerName,&ssi,&sso);

#ifdef NLP_DEBUG
    e_time = clock();
    handle.open(NLP_DEBUG_FILE, std::ofstream::out | std::ofstream::app);
    handle << "===============================================" << endl;
    handle << "[Analyzer: " << analyzerName << "]" << endl;
    handle << "[Text: " << inputText << "]" << endl;
    handle << "[Exec analyzer time="
           << (double) (e_time - s_time)/CLOCKS_PER_SEC
           << " sec]" << endl;
    handle << sso.str() << endl;
    handle.close();
#endif
    sso.seekp(0, ios_base::end);
    return sso.tellp();
}