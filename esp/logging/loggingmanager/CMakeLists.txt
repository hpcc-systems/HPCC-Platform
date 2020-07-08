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


# Component: loggingmanager
#####################################################
# Description:
# ------------
#   Cmake Input File for loggingmanager
#####################################################

project( loggingmanager )

# include(${HPCC_SOURCE_DIR}/cmake_modules/shares.cmake)
include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set ( SRCS
    ${ESPSCM_GENERATED_DIR}/ws_loggingservice_esp.cpp
    loggingmanager.cpp
)

include_directories (
    ${HPCC_SOURCE_DIR}/esp/platform
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/system/security/shared
    ${HPCC_SOURCE_DIR}/system/xmllib
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/client
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/Platform
    ${HPCC_SOURCE_DIR}/esp/clients
    ${HPCC_SOURCE_DIR}/esp/clients/espcommonclient
    ${HPCC_SOURCE_DIR}/esp/clients/LoggingClient
    ${HPCC_SOURCE_DIR}/esp/bindings
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/LoggingService
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
#    ./../../protocols/sidex
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib
    ${HPCC_SOURCE_DIR}/common/thorhelper
    ./..
)

ADD_DEFINITIONS( -D_USRDLL -DLOGGINGCOMMON_EXPORTS -DLOGGINGMANAGER_EXPORTS )

if (RELEASE_BUILD EQUAL 1)
    ADD_DEFINITIONS( -DISC_NO_MAIN)
endif (RELEASE_BUILD EQUAL 1)

HPCC_ADD_LIBRARY( loggingmanager SHARED ${SRCS} )

add_dependencies( loggingmanager espscm )

install ( TARGETS loggingmanager RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( loggingmanager
    ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
    jlib
    xmllib
    logginglib
    esphttp
)
