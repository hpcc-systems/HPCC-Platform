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

//#define NLP_DEBUG

#ifdef NLP_DEBUG
#include <iostream>
#define NLP_DEBUG_FILE "/tmp/nlp_debug.txt"
#endif

#include<sstream>
#include "jutil.hpp"
#include "jmutex.hpp"
using namespace std;

class NLPEng
{
private:
    string workingFolder;
public:
    NLPEng();
    ~NLPEng();
    int nlpEngAnalyze(const char *analyzerName, const char *inputText, ostringstream &sso);
};