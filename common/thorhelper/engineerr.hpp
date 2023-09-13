/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.
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

#pragma once

#include "errorlist.h"

#define ENGINEERR_EXTEND_CLUSTER_WRITE     ENGINE_ERROR_START
#define ENGINEERR_MIXED_COMPRESSED_WRITE   ENGINE_ERROR_START+1
#define ENGINEERR_FILE_TYPE_MISMATCH       ENGINE_ERROR_START+2
#define ENGINEERR_MISSING_OPTIONAL_FILE    ENGINE_ERROR_START+3
#define ENGINEERR_FILE_UPTODATE            ENGINE_ERROR_START+4
