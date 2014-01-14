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

#ifndef PACKAGEPROCESS_ERRORS_H
#define PACKAGEPROCESS_ERRORS_H

#include "errorlist.h"

#define PKG_NAME_EXISTS     PKG_PROCESS_ERROR_START
#define PKG_MISSING_PARAM   PKG_PROCESS_ERROR_START+1
#define PKG_DALI_LOOKUP_ERROR    PKG_PROCESS_ERROR_START+2
#define PKG_MISSING_DALI_LOOKUP_IP  PKG_PROCESS_ERROR_START+3
#define PKG_TARGET_NOT_DEFINED   PKG_PROCESS_ERROR_START+4
#define PKG_ACTIVATE_NOT_FOUND   PKG_PROCESS_ERROR_START+5
#define PKG_DEACTIVATE_NOT_FOUND   PKG_PROCESS_ERROR_START+6
#define PKG_DELETE_NOT_FOUND   PKG_PROCESS_ERROR_START+7
#define PKG_NONE_DEFINED   PKG_PROCESS_ERROR_START+8
#define PKG_CREATE_PACKAGESET_FAILED   PKG_PROCESS_ERROR_START+9
#define PKG_PACKAGEMAP_NOT_FOUND   PKG_PROCESS_ERROR_START+10
#define PKG_LOAD_PACKAGEMAP_FAILED   PKG_PROCESS_ERROR_START+11
#define PKG_INVALID_CLUSTER_TYPE   PKG_PROCESS_ERROR_START+12
#define PKG_INVALID_QUERY_NAME   PKG_PROCESS_ERROR_START+13
#define PKG_INFO_NOT_DEFINED   PKG_PROCESS_ERROR_START+14
#define PKG_INVALID_PARAMETER   PKG_PROCESS_ERROR_START+15

#endif
