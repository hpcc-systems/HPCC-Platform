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
#include "jexcept.hpp"
#include "nlp.hpp"
#include "manifest.hpp"

#include "unicode/usearch.h"
using namespace icu;

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

namespace nlp
{

    IPluginContext *parentCtx = NULL;
    static CriticalSection cs;
    static NLPEng *nlpEng = NULL;
    void ensureNLPEng(ICodeContext *ctx)
    {
        CriticalBlock block(cs);
        if (nlpEng == NULL)
        {
            std::shared_ptr<IManifest> manifest(createIManifest(ctx));
            StringBuffer sb;
            manifest->extractResources(sb);
            nlpEng = new NLPEng(sb.str());
        }
    }

    //--------------------------------------------------------------------------------
    //                           ECL SERVICE ENTRYPOINTS
    //--------------------------------------------------------------------------------

    ECL_NLP_API void ECL_NLP_CALL AnalyzeText(ICodeContext *ctx, size32_t &tgtLen, char *&tgt, size32_t anaLen, const char *ana, size32_t txtLen, const char *txt)
    {
        ensureNLPEng(ctx);
        StringBuffer anaBuff(anaLen, ana);
        StringBuffer txtBuff(txtLen, txt);
        std::ostringstream sso;
        tgtLen = nlpEng->nlpEngAnalyze(anaBuff, txtBuff, sso);
        tgt = (char *)CTXMALLOC(parentCtx, tgtLen);
        memcpy_iflen(tgt, sso.str().c_str(), tgtLen);
    }

    ECL_NLP_API void ECL_NLP_CALL AnalyzeTextU(ICodeContext *ctx, unsigned &tgtLen, UChar *&tgt, size32_t anaLen, const char *ana, unsigned txtLen, UChar const *txt)
    {
        ensureNLPEng(ctx);
        UnicodeString const in(false, txt, txtLen);
        std::string str;
        in.toUTF8String(str);
        StringBuffer anaBuff(anaLen, ana);
        std::ostringstream sso;
        nlpEng->nlpEngAnalyze(anaBuff, str.c_str(), sso);
        UnicodeString unicode = UnicodeString::fromUTF8(StringPiece(sso.str()));
        tgtLen = unicode.length();
        tgt = (UChar *)CTXMALLOC(parentCtx, tgtLen * 2);
        unicode.extract(0, tgtLen, tgt);
    }

    ECL_NLP_API void ECL_NLP_CALL TestExtractResources(ICodeContext *ctx, size32_t &tgtLen, char *&tgt)
    {
        std::shared_ptr<IManifest> manifest(createIManifest(ctx));

        StringBuffer sb;
        manifest->extractResources(sb);
        if (sb.length() == 0)
        {
            throw makeStringExceptionV(1, "No resources found, did you forget to add the manifest?");
        }

        tgtLen = sb.length();
        tgt = (char *)CTXMALLOC(parentCtx, tgtLen);
        memcpy_iflen(tgt, sb.str(), tgtLen);
    }

    ECL_NLP_API void ECL_NLP_CALL TestLoadResource(ICodeContext *ctx, size32_t &tgtLen, char *&tgt, size32_t anaLen, const char *ana, size32_t txtLen, const char *txt)
    {
        std::shared_ptr<IManifest> manifest(createIManifest(ctx));

        MemoryBuffer mb;
        if (!manifest->getResourceData(ana, mb))
        {
            throw makeStringExceptionV(1, "Resource `%s` not found", ana);
        }

        tgtLen = mb.length();
        tgt = (char *)CTXMALLOC(parentCtx, tgtLen);
        memcpy_iflen(tgt, mb.toByteArray(), tgtLen);
    }

} // namespace nlp

using namespace nlp;

ECL_NLP_API void setPluginContext(IPluginContext *_ctx) { parentCtx = _ctx; }