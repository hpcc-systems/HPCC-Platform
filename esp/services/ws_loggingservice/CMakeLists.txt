################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.
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

# Component: ws_loggingservice

project( ws_loggingservice )

# include (${HPCC_SOURCE_DIR}/cmake_modules/shares.cmake)
include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

include_directories (
     ${HPCC_SOURCE_DIR}/system/include
     ${HPCC_SOURCE_DIR}/system/jlib
     ${HPCC_SOURCE_DIR}/system/xmllib
     ${HPCC_SOURCE_DIR}/system/mp
     ${HPCC_SOURCE_DIR}/system/security/shared
     ${HPCC_SOURCE_DIR}/system/security/securesocket
     ${HPCC_SOURCE_DIR}/common/environment
     ${HPCC_SOURCE_DIR}/dali/base
     ${HPCC_SOURCE_DIR}/esp/platform
     ${HPCC_SOURCE_DIR}/esp/bindings
     ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
     ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/Platform
     ${HPCC_SOURCE_DIR}/esp/esplib
     ${HPCC_SOURCE_DIR}/esp/clients
     ${HPCC_SOURCE_DIR}/esp/clients/LoggingClient
     ${HPCC_SOURCE_DIR}/esp/logging/logginglib
     ${HPCC_SOURCE_DIR}/common/thorhelper
     ./..
)

ADD_DEFINITIONS( -D_USRDLL -DLOGGINGCOMMON_EXPORTS )

set ( SRCS
    ${ESPSCM_GENERATED_DIR}/ws_loggingservice_esp.cpp
    loggingservice.cpp
    loggingserviceplugin.cpp
)

HPCC_ADD_LIBRARY( ws_loggingservice SHARED ${SRCS} )

install ( TARGETS ws_loggingservice RUNTIME DESTINATION bin LIBRARY DESTINATION lib )
add_dependencies (ws_loggingservice espscm)
target_link_libraries ( ws_loggingservice
    esphttp
    jlib
    logginglib
)
