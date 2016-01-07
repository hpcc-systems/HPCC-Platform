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

#ifndef __LOGGING_ERRORS_HPP__
#define __LOGGING_ERRORS_HPP__

#include "EspErrors.hpp"

namespace MySQLErrors
{
    const unsigned int Base = EspCoreErrors::Base+1800;       //2800 -- pls don't change this....the web is coded to expect the following codes
    const unsigned int PrepedStmtAllocHandleFailed  = Base+0; //"Can't allocate a handle for the prepared statement."
    const unsigned int PrepedStmtMissing            = Base+1; //"The prepared statement is NULL."
    const unsigned int PrepedStmtBindParamsFailed   = Base+2; //"Binding of the parameters to the prepared statement failed."
    const unsigned int PrepedStmtBindColumnsFailed  = Base+3; //"Binding of the columns to the prepared statement failed."
    const unsigned int PrepedStmtExecuteFailed      = Base+4; //"Executing the prepared statement failed."
    const unsigned int PrepedStmtStoreResultFailed  = Base+5; //"Storing fetched records on the client for the prepared statement failed."
    const unsigned int MismatchingNumOfParams       = Base+6; //"The number of params to be set is different from the number of params in the prepared statement."
    const unsigned int ConnectionTimedOut           = Base+7; //The connection to mysql timed out
    const unsigned int ExecuteStatementFailed       = Base+8; //Failed to Get Cumulative Timer
    const unsigned int EmptySQLQuery                = Base+9; //Empty SQL query cannot be executed
}

namespace EspLoggingErrors
{
    const unsigned int Base = EspCoreErrors::Base+2500;       //3500 -- pls don't change this....the web is coded to expect the following codes
    const unsigned int ConfigurationFileEntryError  = Base+1;  // Required entry in esp.xml missing or invalid
    const unsigned int LoadLoggingLibraryError      = Base+2;
    const unsigned int AddToLoggingQueueFailed      = Base+3;
    const unsigned int GetTransactionSeedFailed     = Base+4;
    const unsigned int UpdateLogFailed              = Base+5;
    const unsigned int WSLoggingAccessDenied        = Base+6;
}

#endif //__LOGGING_ERRORS_HPP__
