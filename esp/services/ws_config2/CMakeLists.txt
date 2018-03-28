##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.
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
#
###############################################################################

project(ws_config2)

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set(SRCS
    ${ESPSCM_GENERATED_DIR}/ws_config2_esp.cpp
    ws_config2Service.cpp
    ws_config2_plugin.cpp
    )

include_directories(
    ${HPCC_SOURCE_DIR}/esp/esplib
    ${HPCC_SOURCE_DIR}/esp/platform
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/common/environment
    ${HPCC_SOURCE_DIR}/esp/services
    ${HPCC_SOURCE_DIR}/common
    ${HPCC_SOURCE_DIR}/system/security/securesocket
    ${HPCC_SOURCE_DIR}/system/security/shared
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/common/remote
    ${HPCC_SOURCE_DIR}/clients
    ${HPCC_SOURCE_DIR}/dali/base
    ${HPCC_SOURCE_DIR}/common/dllserver
    ${HPCC_SOURCE_DIR}/esp/bindings
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
    ${HPCC_SOURCE_DIR}/esp/bindings/http/client
    ${HPCC_SOURCE_DIR}/esp/http/platform
    ${HPCC_SOURCE_DIR}/system/mp
    ${HPCC_SOURCE_DIR}/system/xmllib
    ${HPCC_SOURCE_DIR}/configuration/config2
    ${CMAKE_BINARY_DIR}
    ${CMAKE_BINARY_DIR}/oss
    ${CMAKE_BINARY_DIR}/esp/services/ws_config2
    )

add_definitions(-D_USRDLL)

HPCC_ADD_LIBRARY(ws_config2 SHARED ${SRCS})
install(
    TARGETS ws_config2
    RUNTIME DESTINATION ${EXEC_DIR}
    LIBRARY DESTINATION ${LIB_DIR}
    COMPONENT Runtime
    )
add_dependencies(ws_config2 espscm config2)
target_link_libraries(ws_config2
    jlib
    xmllib
    esphttp
    environment
    dllserver
    securesocket
    config2
    )
