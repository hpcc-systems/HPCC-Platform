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

#ifndef _THDISKREAD_IPP
#define _THDISKREAD_IPP

#include "thdiskbase.ipp"

CActivityBase *createDiskReadActivityMaster(CMasterGraphElement *info, IHThorArg *_helper=NULL);
CActivityBase *createDiskAggregateActivityMaster(CMasterGraphElement *info);
CActivityBase *createDiskCountActivityMaster(CMasterGraphElement *info);
CActivityBase *createDiskGroupAggregateActivityMaster(CMasterGraphElement *info);

#endif
