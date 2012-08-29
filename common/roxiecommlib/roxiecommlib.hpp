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

#ifndef ROXIECOMMLIB_INCL
#define ROXIECOMMLIB_INCL

#ifdef _WIN32
    #ifdef ROXIECOMMLIB_EXPORTS
        #define ROXIECOMMLIB_API __declspec(dllexport)
    #else
        #define ROXIECOMMLIB_API __declspec(dllimport)
    #endif
#else
    #define ROXIECOMMLIB_API
#endif


#include "roxiecommlibscm.hpp"


#define ROXIECOMM_SOCKET_ERROR 1450



#endif
