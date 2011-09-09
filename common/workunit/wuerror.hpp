/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

#endif
