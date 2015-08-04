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

#ifndef _THPRSLAVE_IPP
#define _THPRSLAVE_IPP

#include "slave.ipp"

#include "jsocket.hpp"
#include "jio.hpp"
#include "jfile.hpp"
#include "thactivityutil.ipp"

#include "eclhelper.hpp" // tmp for IHThor..Arg interfaces.

#define RECORD_BUFFER_SIZE  64 * 1024       // 64k


activityslaves_decl CActivityBase *createPipeReadSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createPipeThroughSlave(CGraphElementBase *container);

#endif
