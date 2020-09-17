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

#ifndef LOGGING_INCL
#define LOGGING_INCL

#ifdef _WIN32
#define LOGGING_CALL _cdecl
#else
#define LOGGING_CALL
#endif

#ifdef LOGGING_EXPORTS
#define LOGGING_API DECL_EXPORT
#else
#define LOGGING_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"
#include "eclhelper.hpp"

extern "C" {
LOGGING_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
LOGGING_API void LOGGING_CALL logDbgLog(unsigned srcLen, const char * src);
LOGGING_API char * LOGGING_CALL logGetGlobalId(ICodeContext *ctx);
LOGGING_API char * LOGGING_CALL logGetCallerId(ICodeContext *ctx);
LOGGING_API char * LOGGING_CALL logGetLocalId(ICodeContext *ctx);
LOGGING_API char * LOGGING_CALL logGenerateGloballyUniqueId();

}

#endif
