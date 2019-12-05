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

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "logging.hpp"
#include "jlog.hpp"

#define LOGGING_VERSION "LOGGING 1.0.1"
static const char * compatibleVersions[] = {
    "LOGGING 1.0.0 [66aec3fb4911ceda247c99d6a2a5944c]", // linux version
    LOGGING_VERSION,
    NULL };

static const char * EclDefinition =
"export Logging := SERVICE : time\n"
"  dbglog(const string src) : c,action,entrypoint='logDbgLog'; \n"
"  addWorkunitInformation(const varstring txt, unsigned code=0, unsigned severity=0, const varstring source='user') : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitWarning(const varstring txt, unsigned code=0, unsigned severity=1, const varstring source='user') : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitError(const varstring txt, unsigned code=0, unsigned severity=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitInformationEx(const varstring txt, unsigned code=0, unsigned severity=0, unsigned audience=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuExceptionEx'; \n"
"  addWorkunitWarningEx(const varstring txt, unsigned code=0, unsigned severity=1, unsigned audience=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuExceptionEx'; \n"
"  addWorkunitErrorEx(const varstring txt, unsigned code=0, unsigned severity=2, unsigned audience=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuExceptionEx'; \n"
"  varstring getGlobalId() : c,context,entrypoint='logGetGlobalId'; \n"
"  varstring getLocalId() : c,context,entrypoint='logGetLocalId'; \n"
"  varstring getCallerId() : c,context,entrypoint='logGetCallerId'; \n"
"END;";

LOGGING_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb) 
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
        pbx->compatibleVersions = compatibleVersions;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;
    pb->magicVersion = PLUGIN_VERSION;
    pb->version = LOGGING_VERSION;
    pb->moduleName = "lib_logging";
    pb->ECL = EclDefinition;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "Logging library";
    return true;
}

//-------------------------------------------------------------------------------------------------------------------------------------------

LOGGING_API void LOGGING_CALL logDbgLog(unsigned srcLen, const char * src)
{
    StringBuffer log(srcLen, src);
    StringArray loglines;
    log.replace('\r', ' ');
    loglines.appendList(log, "\n", false);
    ForEachItemIn(idx, loglines)
    {
        DBGLOG("%s", loglines.item(idx));
    }
}

LOGGING_API char *  LOGGING_CALL logGetGlobalId(ICodeContext *ctx)
{
    StringBuffer ret(ctx->queryContextLogger().queryGlobalId());
    return ret.detach();
}

LOGGING_API char *  LOGGING_CALL logGetLocalId(ICodeContext *ctx)
{
    StringBuffer ret(ctx->queryContextLogger().queryLocalId());
    return ret.detach();
}

LOGGING_API char *  LOGGING_CALL logGetCallerId(ICodeContext *ctx)
{
    StringBuffer ret(ctx->queryContextLogger().queryCallerId());
    return ret.detach();
}
