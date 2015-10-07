/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#include "jstring.hpp"
#include "ExceptionStrings.hpp"

const int nDefaultCode = 99;

IException *MakeExceptionFromMap(int code, enum eExceptionCodes eCode, const char* pMsg)
{
    static StringBuffer strExceptionMessage;
    strExceptionMessage.setf("Exception: %s\nPossible Action(s): %s\n",  pExceptionStringArray[eCode-1], pExceptionStringActionArray[eCode-1]);

    if (pMsg != NULL)
        strExceptionMessage.append("\nAdditonal Infomation: ").append(pMsg);

    return MakeStringException(code, "%s", strExceptionMessage.str());
}

IException *MakeExceptionFromMap(enum eExceptionCodes eCode, const char* pMsg)
{
    return MakeExceptionFromMap(nDefaultCode, eCode, pMsg);
}
