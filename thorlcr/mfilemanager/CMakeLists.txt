################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
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

# Component: mfilemanager_lcr 

#####################################################
# Description:
# ------------
#    Cmake Input File for mfilemanager_lcr
#####################################################

project( mfilemanager_lcr ) 

set (    SRCS 
         ../mfilemanager/thmfilemanager.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/dali/ft
         ${HPCC_SOURCE_DIR}/esp/clients/ws_dfsclient
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/thorlcr/graph
         ${HPCC_SOURCE_DIR}/thorlcr/master
         ${HPCC_SOURCE_DIR}/thorlcr/mfilemanager
         ${HPCC_SOURCE_DIR}/thorlcr/thorutil
         ${HPCC_SOURCE_DIR}/thorlcr/shared
         ${HPCC_SOURCE_DIR}/thorlcr/thorcodectx
    )

ADD_DEFINITIONS( -D_USRDLL -DMFILEMANAGER_EXPORTS )

HPCC_ADD_LIBRARY( mfilemanager_lcr SHARED ${SRCS} )
install ( TARGETS mfilemanager_lcr RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( mfilemanager_lcr
         dalibase
         deftype
         dllserver
         eclrtl
         graph_lcr
         jhtree
         jlib
         nbcd
         remote
         workunit
         ws_dfsclient
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( mfilemanager_lcr environment )
endif()

