/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#if !defined __LOGGINGCOMMONDEF_HPP__
#define __LOGGINGCOMMONDEF_HPP__

#pragma warning (disable : 4786)

#ifdef LOGGINGCOMMON_EXPORTS
    #define LOGGINGCOMMON_API DECL_EXPORT
#else
    #define LOGGINGCOMMON_API DECL_IMPORT
#endif

#define MAXLOGSERVICES 32

enum LOGServiceType
{
    LGSTterm = 0,
    LGSTGetTransactionSeed = 1,
    LGSTUpdateLOG = 2,
    LGSTGetTransactionID = 3
};

#endif // !defined __LOGGINGCOMMONDEF_HPP__
