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

#ifndef _THFUNNELSLAVE_IPP
#define _THFUNNELSLAVE_IPP

#include "platform.h"

#include "jmutex.hpp"
#include "jthread.hpp"

#include "slave.ipp"
#include "thactivityutil.ipp"




activityslaves_decl CActivityBase *createFunnelSlave(CGraphElementBase *container);
activityslaves_decl IRowStream *createParallelFunnel(CActivityBase &activity, IRowStream **instreams, unsigned numstreams, const bool &aborted);
activityslaves_decl IRowStream *createParallelFunnel(CActivityBase &activity, IThorDataLink **instreams, unsigned numstreams, bool startInputs, const bool &aborted);
activityslaves_decl CActivityBase *createCombineSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createRegroupSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNonEmptySlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWaySelectSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWayInputSlave(CGraphElementBase *container);



#endif
