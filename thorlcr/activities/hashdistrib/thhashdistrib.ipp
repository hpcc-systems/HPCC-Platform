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

#ifndef _THHASHDISTRIB_IPP
#define _THHASHDISTRIB_IPP

#include "thactivitymaster.ipp"

CActivityBase *createNWayDistributeActivityMaster(CMasterGraphElement *info);
CActivityBase *createHashDistributeActivityMaster(CMasterGraphElement *info);
CActivityBase *createDistributeMergeActivityMaster(CMasterGraphElement *info);
CActivityBase *createHashDedupMergeActivityMaster(CMasterGraphElement *info);
CActivityBase *createHashJoinActivityMaster(CMasterGraphElement *info);
CActivityBase *createHashAggregateActivityMaster(CMasterGraphElement *info);
CActivityBase *createKeyedDistributeActivityMaster(CMasterGraphElement *info);

#endif
