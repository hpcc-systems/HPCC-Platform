################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems.
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


# Component: ws_decoupledlogging
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_decoupledlogging
#####################################################

project( ws_decoupledlogging )

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/ws_decoupledlogging_esp.cpp
         ws_decoupledloggingplugin.cpp
         ws_decoupledloggingservice.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/logging/logginglib
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL -DLOGGINGCOMMON_EXPORTS)

HPCC_ADD_LIBRARY( ws_decoupledlogging SHARED ${SRCS} )
add_dependencies( ws_decoupledlogging espscm )

install ( TARGETS ws_decoupledlogging RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_decoupledlogging
         jlib
         xmllib
         logginglib
         esphttp
    )
    
IF (USE_OPENSSL)
    target_link_libraries ( ws_decoupledlogging 
        securesocket
    )
ENDIF()
