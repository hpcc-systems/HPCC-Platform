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

#ifndef AUDITLIB_INCL
#define AUDITLIB_INCL

#ifdef _WIN32
#define AUDITLIB_CALL _cdecl
#else
#define AUDITLIB_CALL
#endif

#ifdef AUDITLIB_EXPORTS
#define AUDITLIB_API DECL_EXPORT
#else
#define AUDITLIB_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"

extern "C" {
AUDITLIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
AUDITLIB_API bool alAudit(unsigned typeLen, char const * type, unsigned msgLen, char const * msg);
AUDITLIB_API bool alAuditData(unsigned typeLen, char const * type, unsigned msgLen, char const * msg, unsigned dataLen, void const * dataBlock);
}

#endif
