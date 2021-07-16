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

#include "nlp_eng.hpp"
#include "nlp_engine.h"

static NLP_ENGINE *nlpEngine = NULL;

NLPEng::NLPEng(string workingFolderPath) {
    if (!workingFolderPath.empty()) {
        int len = strlen(workingFolderPath.c_str());
        workingFolder = (char *)malloc(len+1);
        strncpy(workingFolder,workingFolderPath.c_str(),len);
        workingFolder[len] = '\0';
    } else {
        workingFolder[0] = '\0';
    }

#ifdef NLP_DEBUG    
    ofstream handle;
    handle.open(NLP_DEBUG_FILE, std::ofstream::out | std::ofstream::app);
    handle << "[workingFolder: " << workingFolder << "]" << endl;
    handle.close();
#endif
}

NLPEng::~NLPEng() {
    if (nlpEngine != NULL) {
        delete nlpEngine;
        nlpEngine = NULL;
    }
    if (workingFolder[0] != '\0') {
        delete workingFolder;
        workingFolder = NULL;
    }
}

#include <sstream>

int NLPEng::nlpEngAnalyze(const char *analyzerName, const char *inputText, ostringstream &sso)
{
    if (nlpEngine == NULL) {
        nlpEngine = new NLP_ENGINE(workingFolder);
    }
    istrstream ssi(inputText);

#ifdef NLP_DEBUG
    clock_t s_time, e_time;
    s_time = clock();
#endif

    nlpEngine->analyze((char *)analyzerName,&ssi,&sso);

#ifdef NLP_DEBUG
    ofstream handle;
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