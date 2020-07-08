################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems.
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
# Component: ws_store
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_store
#####################################################

project(ws_store)

HPCC_ADD_SUBDIRECTORY (espstorelib)

    include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)
    include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

    set(SRCS
        ${CMAKE_CURRENT_SOURCE_DIR}/ws_storePlugin.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/ws_storeService.cpp
        ${ESPSCM_GENERATED_DIR}/common_esp.cpp
        ${ESPSCM_GENERATED_DIR}/ws_store_esp.cpp
        )

    include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/espstorelib
        ##added for build-config.h
        ${CMAKE_BINARY_DIR}
        ##added for build-config.h
        ${CMAKE_BINARY_DIR}/oss
        ##necessary for windows build
        ${HPCC_SOURCE_DIR}/esp/clients
        ${HPCC_SOURCE_DIR}/esp/bindings
        ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
        ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
        ${HPCC_SOURCE_DIR}/esp/platform
        ${HPCC_SOURCE_DIR}/esp/services
        ${HPCC_SOURCE_DIR}/system/xmllib
        ${HPCC_SOURCE_DIR}/system/include
        ${HPCC_SOURCE_DIR}/system/jlib
        ${HPCC_SOURCE_DIR}/system/security/shared
        ${HPCC_SOURCE_DIR}/system/security/securesocket
        ${HPCC_SOURCE_DIR}/system/security/zcrypt
        ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
        ${HPCC_SOURCE_DIR}/system/mp
        ${HPCC_SOURCE_DIR}/dali/base/
        ${HPCC_SOURCE_DIR}/common/remote
        ${HPCC_SOURCE_DIR}/common/environment
        ${HPCC_SOURCE_DIR}/common/deftype
        ${ESPSCM_GENERATED_DIR}
        ${HPCC_SOURCE_DIR}/common/thorhelper
        )

    add_definitions(-D_USRDLL)
    HPCC_ADD_LIBRARY(ws_store SHARED ${SRCS})

    target_link_libraries(
        ws_store
        jlib
        xmllib
        esphttp
        dalibase
        wuwebview
        )

IF (USE_OPENSSL)
    target_link_libraries ( ws_store
        securesocket
    )
ENDIF()

    install(TARGETS ws_store
        RUNTIME DESTINATION ${EXEC_DIR}
        LIBRARY DESTINATION ${LIB_DIR}
        COMPONENT Runtime
        )
