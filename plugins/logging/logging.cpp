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

#define LOGGING_VERSION "LOGGING 1.0.2"
static const char * compatibleVersions[] = {
    "LOGGING 1.0.0 [66aec3fb4911ceda247c99d6a2a5944c]", // linux version
    LOGGING_VERSION,
    NULL };

static const char * EclDefinition =
"export Logging := SERVICE : time\n"
"  dbglog(const string src) : c,context,action,entrypoint='logDbgLogV2'; \n"
"  addWorkunitInformation(const varstring txt, unsigned code=0, unsigned severity=0, const varstring source='user') : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitWarning(const varstring txt, unsigned code=0, unsigned severity=1, const varstring source='user') : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitError(const varstring txt, unsigned code=0, unsigned severity=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuException'; \n"
"  addWorkunitInformationEx(const varstring txt, unsigned code=0, unsigned severity=0, unsigned audience=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuExceptionEx'; \n"
"  addWorkunitWarningEx(const varstring txt, unsigned code=0, unsigned severity=1, unsigned audience=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuExceptionEx'; \n"
"  addWorkunitErrorEx(const varstring txt, unsigned code=0, unsigned severity=2, unsigned audience=2, const varstring source='user') : ctxmethod,action,entrypoint='addWuExceptionEx'; \n"
"  varstring getGlobalId() : c,context,entrypoint='logGetGlobalId'; \n"
"  varstring getLocalId() : c,context,entrypoint='logGetLocalId'; \n"
"  varstring getCallerId() : c,context,entrypoint='logGetCallerId'; \n"
"  varstring generateGloballyUniqueId() : c,entrypoint='logGenerateGloballyUniqueId'; \n"
"  unsigned4 getElapsedMs() : c,context,entrypoint='logGetElapsedMs'; \n"
"  varstring getTraceSpanHeader() : c,context,entrypoint='getTraceSpanHeader'; \n"
"  varstring getTraceStateHeader() : c,context,entrypoint='getTraceStateHeader'; \n"
"  varstring getTraceID() : c,context,entrypoint='getTraceID'; \n"
"  varstring getSpanID() : c,context,entrypoint='getSpanID'; \n"
"  setSpanAttribute(const string name, const string value) : c,context,action,entrypoint='setSpanAttribute'; \n"
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

LOGGING_API void LOGGING_CALL logDbgLogV2(ICodeContext *ctx, unsigned srcLen, const char * src)
{
    StringBuffer log(srcLen, src);
    StringArray loglines;
    log.replace('\r', ' ');
    loglines.appendList(log, "\n", false);
    ForEachItemIn(idx, loglines)
    {
        ctx->queryContextLogger().CTXLOG("%s", loglines.item(idx));
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

LOGGING_API char * LOGGING_CALL logGenerateGloballyUniqueId()
{
    StringBuffer ret;
    appendGloballyUniqueId(ret);
    return ret.detach();
}

LOGGING_API unsigned int LOGGING_CALL logGetElapsedMs(ICodeContext *ctx)
{
    return ctx->getElapsedMs();
}

LOGGING_API char * LOGGING_CALL getTraceID(ICodeContext *ctx)
{
    Owned<IProperties> httpHeaders = ctx->queryContextLogger().getSpanContext();
    StringBuffer ret(httpHeaders->queryProp("traceID"));

    return ret.detach();
}

LOGGING_API char * LOGGING_CALL getSpanID(ICodeContext *ctx)
{
    Owned<IProperties> httpHeaders = ctx->queryContextLogger().getSpanContext();
    StringBuffer ret(httpHeaders->queryProp("spanID"));

    return ret.detach();
}

LOGGING_API char * LOGGING_CALL getTraceSpanHeader(ICodeContext *ctx)
{
    Owned<IProperties> clientHeaders = ctx->queryContextLogger().getClientHeaders();
    StringBuffer ret(clientHeaders->queryProp("traceparent"));

    return ret.detach();
}

LOGGING_API char * LOGGING_CALL getTraceStateHeader(ICodeContext *ctx)
{
    Owned<IProperties> clientHeaders = ctx->queryContextLogger().getClientHeaders();
    StringBuffer ret(clientHeaders->queryProp("tracestate"));

    return ret.detach();
}

LOGGING_API void LOGGING_CALL setSpanAttribute(ICodeContext *ctx, unsigned nameLen, const char * name, unsigned valueLen, const char * value)
{
    StringBuffer attName(nameLen, name);
    StringBuffer attVal(valueLen, value);

    ctx->queryContextLogger().setSpanAttribute(attName.str(), attVal.str());
}
