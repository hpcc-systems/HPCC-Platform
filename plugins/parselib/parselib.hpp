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

#ifndef PARSELIB_INCL
#define PARSELIB_INCL

#ifdef _WIN32
#define PARSELIB_CALL _cdecl
#else
#define PARSELIB_CALL
#endif

#ifdef PARSELIB_EXPORTS
#define PARSELIB_API DECL_EXPORT
#else
#define PARSELIB_API DECL_IMPORT
#endif

#include "hqlplugins.hpp"

extern "C" {
PARSELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
PARSELIB_API void setPluginContext(IPluginContext * _ctx);
PARSELIB_API void plGetDefaultParseTree(IMatchWalker * walker, unsigned & len, char * & text);
PARSELIB_API void plGetXmlParseTree(IMatchWalker * walker, unsigned & len, char * & text);
}

#endif
