################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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


# Component: daliadminlib
#####################################################
# Description:
# ------------
#    Cmake Input File for daliadminlib
#####################################################

project( daliadminlib )

set (    SRCS
         daadmin.cpp
    )

include_directories (
         .
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/dali/server
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/dali/ft
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/esp/clients/wsdfuaccess
         ${HPCC_SOURCE_DIR}/esp/clients/ws_dfsclient
    )

ADD_DEFINITIONS ( -D_USRDLL -DDALIADMIN_API_EXPORTS )

HPCC_ADD_LIBRARY( daliadminlib SHARED ${SRCS} )
install ( TARGETS daliadminlib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( daliadminlib
         jlib
         mp
         dafsclient
         dalibase
         workunit
         dllserver
         wsdfuaccess
         ws_dfsclient
    )
