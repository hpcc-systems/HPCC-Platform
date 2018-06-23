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

#ifndef _THACTIVITYMASTER_IPP
#define _THACTIVITYMASTER_IPP

#include "jthread.hpp"
#include "jset.hpp"

#include "dasess.hpp"
#include "deftype.hpp"


#include "thgraphmaster.ipp"
#include "eclhelper.hpp"        // for IHThorArg etc.


#include "thactivitymaster.hpp"

WUFileKind getDiskOutputKind(unsigned flags);

void updateActivityResult(IConstWorkUnit &workunit, unsigned helperFlags, unsigned sequence, const char *logicalFilename, unsigned __int64 recordCount);
void checkFormatCrc(CActivityBase *activity, IDistributedFile *file, unsigned expectedFormatCrc, IOutputMetaData *expected, unsigned projectedFormatCrc, IOutputMetaData *projected, bool index);

#endif
