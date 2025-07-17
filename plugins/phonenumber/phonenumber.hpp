/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef ECL_PHONENUMBER_INCL
#define ECL_PHONENUMBER_INCL

#ifdef _WIN32
#define ECL_PHONENUMBER_CALL _cdecl
#else
#define ECL_PHONENUMBER_CALL
#endif

#ifdef ECL_PHONENUMBER_EXPORTS
#define ECL_PHONENUMBER_API DECL_EXPORT
#else
#define ECL_PHONENUMBER_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"
#include "eclhelper.hpp"

extern "C"
{
    ECL_PHONENUMBER_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
    ECL_PHONENUMBER_API void setPluginContext(IPluginContext * _ctx);
}

#endif