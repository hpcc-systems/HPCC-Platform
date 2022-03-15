################################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems.
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
# Component: ldapenvironment
#####################################################
# Description:
# ------------
#    Cmake Input File for ldapenvironment
#####################################################

project(ldapenvironment)

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set(SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/ldapenvironmentPlugin.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ldapenvironmentService.cpp
    ${ESPSCM_GENERATED_DIR}/ldapenvironment_esp.cpp
    )

include_directories(
    ${HPCC_SOURCE_DIR}/esp/bindings
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
    ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
    ${HPCC_SOURCE_DIR}/esp/platform
    ${HPCC_SOURCE_DIR}/system/xmllib
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/system/security/shared
    ${HPCC_SOURCE_DIR}/system/security/securesocket
    ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
    ${HPCC_SOURCE_DIR}/common/thorhelper
    ${HPCC_SOURCE_DIR}/rtl/include
    ${HPCC_SOURCE_DIR}/rtl/eclrtl
    )

add_definitions(-D_USRDLL)
HPCC_ADD_LIBRARY(ldapenvironment SHARED ${SRCS})
add_dependencies(ldapenvironment espscm)
target_link_libraries(
    ldapenvironment
    jlib
    xmllib
    esphttp
    LdapSecurity
    )
         
install(
    TARGETS ldapenvironment
    RUNTIME DESTINATION ${EXEC_DIR}
    LIBRARY DESTINATION ${LIB_DIR}
    COMPONENT Runtime
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( ldapenvironment environment )
endif()
