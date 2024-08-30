################################################################################
#    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.
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

# Component: espapi

#####################################################
# Description:
# ------------
#    Cmake Input File for espapi
#####################################################
project(esp-api)

include_directories (
    ${CMAKE_BINARY_DIR}
    ${HPCC_SOURCE_DIR}/esp/esdllib
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/esp/platform
    ${HPCC_SOURCE_DIR}/system/xmllib
    ${HPCC_SOURCE_DIR}/esp/bindings
    ${HPCC_SOURCE_DIR}/esp/bindings/http/client
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/esp/services/common
    ${HPCC_SOURCE_DIR}/system/security/shared
    ${HPCC_SOURCE_DIR}/common/dllserver
    ${CMAKE_BINARY_DIR}/oss
    ${HPCC_SOURCE_DIR}/system/mp
    ${HPCC_SOURCE_DIR}/rtl/eclrtl
    ${HPCC_SOURCE_DIR}/rtl/include #IXMLWriter
    ${HPCC_SOURCE_DIR}/system/masking/include
)

set (  SRCS
       espapi.cpp
       espapi.hpp
       espshell.cpp
       espshell.hpp
       espservice.hpp
       espservice.cpp
       espmain.cpp
    )

ADD_DEFINITIONS(-D_CONSOLE)

HPCC_ADD_EXECUTABLE ( esp-api ${SRCS} )

install ( TARGETS esp-api RUNTIME DESTINATION ${EXEC_DIR} )

add_dependencies ( esp-api esdllib jlib espscm)

target_link_libraries( esp-api
    esdllib
    jlib
    xmllib
    dllserver
    esphttp)
