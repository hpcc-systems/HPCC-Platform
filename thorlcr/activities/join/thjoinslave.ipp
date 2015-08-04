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

#ifndef _thjoinslave_ipp
#define _thjoinslave_ipp

#include "platform.h"
#include "jiface.hpp"       // IInterface defined in jlib
#include "eclhelper.hpp"        // for IHThorJoinArg
#include "slave.ipp"



activityslaves_decl CActivityBase *createJoinSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalJoinSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createDenormalizeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createLocalDenormalizeSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNWayMergeJoinActivity(CGraphElementBase *container);


#endif
