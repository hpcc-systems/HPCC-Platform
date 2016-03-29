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

#ifndef _ECLWATCH_EXCEPTIONS_HPP__
#define _ECLWATCH_EXCEPTIONS_HPP__

#include "errorlist.h"
#include "jlog.hpp"
#include "eclwatch_errorlist.hpp"

inline void FORWARDEXCEPTION(IEspContext &context, IException *e, unsigned codeNew)
{
    if (!e)
        return;

    time_t tNow;
    struct tm timeStruct;
    char timeString[32];
    StringBuffer eMsg;

    int err = e->errorCode();
    e->errorMessage(eMsg);
    e->Release();

    context.setException(err);

    //set time stamp in the result for this machine
    time(&tNow);
    gmtime_r(&tNow, &timeStruct);
    strftime(timeString, 32, "%Y-%m-%d %H:%M:%S GMT", &timeStruct);
    throw MakeStringException(err, "%s: %s", timeString, eMsg.str());
        
    return;
}
#endif //_ECLWATCH_EXCEPTIONS_HPP__

