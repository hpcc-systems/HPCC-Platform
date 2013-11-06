################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################
HPCC_ADD_SUBDIRECTORY (ecldirect "PLATFORM")
IF (USE_OPENLDAP)
    HPCC_ADD_SUBDIRECTORY (ws_access "PLATFORM")
ENDIF(USE_OPENLDAP)
HPCC_ADD_SUBDIRECTORY (ws_account "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_config "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_dfu "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_ecl "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_fileio "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_fs "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_machine "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_smc "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_topology "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_workunits "PLATFORM")
HPCC_ADD_SUBDIRECTORY (WsDeploy "PLATFORM")
HPCC_ADD_SUBDIRECTORY (ws_packageprocess "PLATFORM")
IF (USE_ANTLRc)
    HPCC_ADD_SUBDIRECTORY (ws_sql "PLATFORM")
ENDIF (USE_ANTLRc)
