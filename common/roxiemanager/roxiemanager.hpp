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

#ifndef ROXIEQUERYMANAGER_INCL
#define ROXIEQUERYMANAGER_INCL

#ifdef _WIN32
    #ifdef ROXIEMANAGER_EXPORTS
        #define ROXIEMANAGER_API __declspec(dllexport)
    #else
        #define ROXIEMANAGER_API __declspec(dllimport)
    #endif
#else
    #define ROXIEMANAGER_API
#endif

#include "roxiemanagerscm.hpp"

#include "errorlist.h"

#define ROXIEMANAGER_IGNORE_EXCEPTION   0   // not a real exception

#define ROXIEMANAGER_UNRESOLVED_FILE    ROXIE_MGR_START
#define ROXIEMANAGER_MISSING_FILE_PARTS ROXIE_MGR_START+1
#define ROXIEMANAGER_MISSING_ID         ROXIE_MGR_START+2
#define ROXIEMANAGER_SOCKET_ERROR       ROXIE_MGR_START+3
#define ROXIEMANAGER_UNEXPECTION_WU_ERROR ROXIE_MGR_START+4
#define ROXIEMANAGER_FILE_MISMATCH      ROXIE_MGR_START+5
#define ROXIEMANAGER_DEPLOY_FAILED      ROXIE_MGR_START+6
#define ROXIEMANAGER_DALI_LOOKUP_ERROR  ROXIE_MGR_START+7
#define ROXIEMANAGER_ROD_NOT_SUPPORTED  ROXIE_MGR_START+8
#define ROXIEMANAGER_FILE_PERMISSION_ERR ROXIE_MGR_START+9
#define ROXIEMANAGER_FILE_SIZE_ERROR    ROXIE_MGR_START+10

#endif

