/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef H3_INCL
#define H3_INCL

#ifdef _WIN32
#define ECL_H3_CALL _cdecl
#else
#define ECL_H3_CALL
#endif

#ifdef ECL_H3_EXPORTS
#define ECL_H3_API DECL_EXPORT
#else
#define ECL_H3_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"
#include "eclhelper.hpp"

extern "C"
{
    ECL_H3_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
    ECL_H3_API void setPluginContext(IPluginContext *_ctx);
}

extern "C++"
{
    namespace h3
    {
    } // namespace h3
}
#endif
