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


# Component: ws_workunits
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_workunits
#####################################################

project( ws_workunits )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ../../../dali/sasha/sacmd.cpp
         ../../../dali/dfu/dfuutil.cpp
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp
         ${HPCC_SOURCE_DIR}/esp/scm/ws_workunits.ecm
         ${HPCC_SOURCE_DIR}/esp/clients/roxiecontrol.cpp
         ws_workunitsPlugin.cpp
         ws_workunitsService.cpp
         ws_workunitsService.hpp
         ws_workunitsHelpers.cpp
         ws_workunitsHelpers.hpp
         ws_workunitsAuditLogs.cpp
         ws_workunitsQuerySets.cpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}/oss
         ./../../esplib
         ./../../../system/mp
         ./../../platform
         ./../../../dali/dfu
         ./../../../dali/sasha
         ./../../../common/remote
         ./../../../system/jlib
         ./../../../common/environment
         ./../../../roxie/roxie
         ./../../services
         ./../common
         ./../../../system/xmllib
         ./../../../common/deftype
         ./../../../ecl/hql
         ./../../../system/security/securesocket
         ./../../../system/security/shared
         ./../../../system/include
         ./../../../common/workunit
         ./../../../common/wuwebview
         ./../../../ecl/schedulectrl
         ./../../clients
         ./../../../common/fileview2
         ./../../../dali/base
         ./../../../system/security/zcrypt
         ./../../../rtl/include
         ./../../../common/dllserver
         ./../../bindings
         ./../../smc/SMCLib
         ./../../bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/dali/dfu
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_WORKUNITS_EXPORTS )

HPCC_ADD_LIBRARY( ws_workunits SHARED ${SRCS} )
add_dependencies ( ws_workunits espscm )
install ( TARGETS ws_workunits RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_workunits
         jlib
         mp
         hrpc
         remote
         xmllib
         esphttp
         dalibase
         environment
         dalift
         dllserver
         nbcd
         eclrtl
         deftype
         workunit
         wuwebview
         SMCLib
         schedulectrl
         roxiecommlib
         hql
         jhtree
         fileview2
         securesocket
    )

IF (USE_ZLIB)
  target_link_libraries ( ws_workunits
      ${ZLIB_LIBRARIES}
      zcrypt
      )
ENDIF()
