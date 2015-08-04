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

#ifndef _thnullslave_ipp
#define _thnullslave_ipp

#include "platform.h"
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"

activityslaves_decl CActivityBase *createNullSinkSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createNullSlave(CGraphElementBase *container);
activityslaves_decl CActivityBase *createThroughSlave(CGraphElementBase *container);

#endif
