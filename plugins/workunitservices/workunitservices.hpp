/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

extern "C" {
WORKUNITSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
WORKUNITSERVICES_API void setPluginContext(IPluginContext * _ctx);
WORKUNITSERVICES_API char * WORKUNITSERVICES_CALL wsGetBuildInfo(void);

WORKUNITSERVICES_API bool WORKUNITSERVICES_CALL wsWorkunitExists(const char *wuid, bool online, bool archived);

WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitList(
                                                                ICodeContext *ctx,
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
                                                                bool archived );


WORKUNITSERVICES_API char * wsWUIDonDate(unsigned year,unsigned month,unsigned day,unsigned hour,unsigned minute);
WORKUNITSERVICES_API char * wsWUIDdaysAgo(unsigned daysago);
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitTimeStamps( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitMessages( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitFilesRead( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitFilesWritten( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );
WORKUNITSERVICES_API void WORKUNITSERVICES_CALL wsWorkunitTimings( ICodeContext *ctx, size32_t & __lenResult, void * & __result, const char *wuid );


}

#endif

