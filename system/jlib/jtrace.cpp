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


#include "platform.h"

#include "jmisc.hpp"
#include "jtrace.hpp"
#include "lnuid.h"

using namespace ln_uid;

/*
* Sets global id if provided, and assign a localId
*/
void LogTrace::setGlobalId(const char* id)
{
    if (!isEmptyString(id))
    {
        globalId.set(id);
        assignLocalId();
    }
}

/*
* Sets global id if provided, assigns new localID
*/
LogTrace::LogTrace(const char * globalId)
{
    setGlobalId(globalId);
}

const char* LogTrace::assignLocalId()
{
    localId.set(createUniqueIdString().c_str());
    return localId.get();
}

const char* LogTrace::queryGlobalId() const
{
    return globalId.get();
}

void LogTrace::setCallerId(const char* id)
{
    callerId.set(id);
}

const char* LogTrace::queryCallerId() const
{
    return callerId.get();
}

const char* LogTrace::queryLocalId() const
{
    return localId.get();
}
