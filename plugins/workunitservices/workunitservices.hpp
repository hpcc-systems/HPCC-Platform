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

#ifndef _WORKUNITSERVICES_INCL
#define _WORKUNITSERVICES_INCL

#ifdef _WIN32
#define WORKUNITSERVICES_CALL _cdecl
#ifdef WORKUNITSERVICES_EXPORTS
#define WORKUNITSERVICES_API __declspec(dllexport)
#else
#define WORKUNITSERVICES_API __declspec(dllimport)
#endif
#else
#define WORKUNITSERVICES_CALL
#define WORKUNITSERVICES_API
#endif

#include "hqlplugins.hpp"
#include "workunit.hpp"
#include "eclhelper.hpp"

extern "C"
{
  WORKUNITSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
  WORKUNITSERVICES_API void setPluginContext(IPluginContext * _ctx);
}

WORKUNITSERVICES_API char * WORKUNITSERVICES_CALL wsGetBuildInfo(void);

WORKUNITSERVICES_API bool WORKUNITSERVICES_CALL wsWorkunitExists(ICodeContext *ctx, const char *wuid, bool online, bool archived);

WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitList( ICodeContext *ctx,
                                                                size32_t & __lenResult,
                                                                void * & __result, 
                                                                const char *lowwuid,
                                                                const char *highwuid,
                                                                const char *username,
                                                                const char *cluster,
                                                                const char *jobname,
                                                                const char *state,
                                                                const char *priority,
                                                                const char *fileread,
                                                                const char *filewritten,
                                                                const char *roxiecluster,
                                                                const char *eclcontains,
                                                                bool online,
                                                                bool archived,
                                                                const char *appvalues);


WORKUNITSERVICES_API char * wsWUIDonDate(unsigned year,unsigned month,unsigned day,unsigned hour,unsigned minute);
WORKUNITSERVICES_API char * wsWUIDdaysAgo(unsigned daysago);
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitTimeStamps( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitMessages( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitFilesRead( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitFilesWritten( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitTimings( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API IRowStream * WORKUNITSERVICES_CALL wsWorkunitStatistics( ICodeContext *ctx, IEngineRowAllocator * allocator, const char *wuid, bool includeActivities, const char * filterText);

WORKUNITSERVICES_API bool WORKUNITSERVICES_CALL wsWorkunitTimings( ICodeContext *ctx, const char *wuid, const char * appname, const char *key, const char *value, bool overwrrite);

#endif

