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

#ifndef _DEBUGSERVICES_INCL
#define _DEBUGSERVICES_INCL

#ifdef _WIN32
#define DEBUGSERVICES_CALL _cdecl
#else
#define DEBUGSERVICES_CALL
#endif

#ifdef DEBUGSERVICES_EXPORTS
#define DEBUGSERVICES_API DECL_EXPORT
#else
#define DEBUGSERVICES_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"

extern "C" {
DEBUGSERVICES_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
DEBUGSERVICES_API char * DEBUGSERVICES_CALL dsGetBuildInfo(void);
DEBUGSERVICES_API void DEBUGSERVICES_CALL dsSleep(unsigned millis);

void DEBUGSERVICES_API __stdcall dsInitDebugServices(const char *_wuid);
}

#endif

