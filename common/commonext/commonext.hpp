/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef _COMMONEXT_
#define _COMMONEXT_

#ifdef _WIN32
    #ifdef COMMONEXT_EXPORTS
        #define thcommonext_decl __declspec(dllexport)
    #else
        #define thcommonext_decl __declspec(dllimport)
    #endif
#else
    #define thcommonext_decl
#endif

#include "eclhelper.hpp"

extern thcommonext_decl const char *activityKindStr(ThorActivityKind kind);

#endif

