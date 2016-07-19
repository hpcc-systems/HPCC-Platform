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

#ifndef EXAMPLELIB_INCL
#define EXAMPLELIB_INCL

#ifdef _WIN32
#define EXAMPLELIB_CALL _cdecl
#else
#define EXAMPLELIB_CALL
#endif

#ifdef EXAMPLELIB_EXPORTS
#define EXAMPLELIB_API DECL_EXPORT
#else
#define EXAMPLELIB_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"

extern "C" {
EXAMPLELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
EXAMPLELIB_API void setPluginContext(IPluginContext * _ctx);
EXAMPLELIB_API void EXAMPLELIB_CALL elEchoString(unsigned & tgtLen, char * & tgt, unsigned srcLen, const char * src);
}

#endif
