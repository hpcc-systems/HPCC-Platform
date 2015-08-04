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

// Handling Ungrouped Rollup and Dedup
// (Grouped code in groupdedup)

#ifndef _throllupslave_ipp
#define _throllupslave_ipp

#include "platform.h"
#include "eclhelper.hpp"
#include "slave.ipp"


activityslaves_decl CActivityBase *createDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createGroupDedupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRollupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalRollupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createGroupRollupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRollupGroupSlave(CGraphElementBase *container);

#endif
