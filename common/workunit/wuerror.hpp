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

#ifndef WUERROR_HPP
#define WUERROR_HPP

#include "jexcept.hpp"

/* Errors can occupy range 5000..5099 */

#define WUERR_AccessError                       5000
#define WUERR_SecurityNotAvailable              5001
#define WUERR_WorkunitProtected                 5002
#define WUERR_WorkunitActive                    5003
#define WUERR_WorkunitScheduled                 5004
#define WUERR_ConnectFailed                     5005
#define WUERR_LockFailed                        5006
#define WUERR_WorkunitAccessDenied              5007
#define WUERR_MismatchClusterSize               5008
#define WUERR_MismatchThorType                  5009
#define WUERR_InternalUnknownImplementation     5010
#define WUERR_CannotCloneWorkunit               5011
#define WUERR_CorruptResult                     5012
#define WUERR_ResultFormatMismatch              5013
#define WUERR_InvalidResultFormat               5014
#define WUERR_MissingFormatTranslator           5015
#define WUERR_InvalidCluster                    5016
#define WUERR_InvalidQueue                      5017
#define WUERR_CannotSchedule                    5018
#define WUERR_InvalidUploadFormat               5019
#define WUERR_InvalidSecurityToken              5020
#define WUERR_ScheduleLockFailed                5021
#define WUERR_PackageAlreadyExists              5022
#define WUERR_MismatchClusterType               5023
#define WUERR_InvalidDll                        5024
#define WUERR_WorkunitPublished                 5025
#define WUERR_GraphProgressWriteUnsupported     5026
#define WUERR_WorkunitPluginError               5027
#define WUERR_WorkunitVersionMismatch           5028
#define WUERR_InvalidFieldUsage                 5029
#define WUERR_InvalidUserInput                  5030
#define WUERR_CannotImportWorkunit              5031
#endif
