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

#include "platform.h"

#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "eclrtl.hpp"
#include "nlp.hpp"

#define NLP_VERSION "nlp plugin 1.0.0"

ECL_NLP_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = NLP_VERSION;
    pb->moduleName = "lib_nlp";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for nlp\n";
    return true;
}

#include <fstream>

namespace nlp {

    IPluginContext * parentCtx = NULL;
    static CriticalSection cs; 
    static NLPEng *nlpEng = NULL;

    ECL_NLP_API void setPluginContext(IPluginContext * _ctx) { parentCtx = _ctx; }

    //--------------------------------------------------------------------------------
    //                           ECL SERVICE ENTRYPOINTS
    //--------------------------------------------------------------------------------

    ECL_NLP_API void ECL_NLP_CALL AnalyzeText(size32_t & tgtLen, char * & tgt, size32_t anaLen, const char * ana, size32_t txtLen, const char * txt)
    {
        {
            CriticalBlock block(cs);
            if (nlpEng == NULL) {
                nlpEng = new NLPEng();
            }
        }
        ostringstream sso;
        tgtLen = nlpEng->nlpEngAnalyze(ana,txt,sso);
        tgt = (char *) CTXMALLOC(parentCtx, tgtLen);
        memcpy_iflen(tgt, sso.str().c_str(), tgtLen);
    }
} // namespace nlp