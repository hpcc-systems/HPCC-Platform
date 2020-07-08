################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
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

# Component: tankfile 

#####################################################
# Description:
# ------------
#    Cmake Input File for tankfile
#####################################################


project( tankfile )

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS
         archive.cpp
         main.cpp
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_decoupledlogging_esp.cpp 
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( tankfile ${SRCS} )
add_dependencies ( tankfile espscm )
install ( TARGETS tankfile RUNTIME DESTINATION ${EXEC_DIR} COMPONENT Runtime)

target_link_libraries ( tankfile
         jlib
         esphttp
    )
IF (USE_OPENSSL)
    target_link_libraries ( tankfile
         securesocket
    )
ENDIF()

if ( PLATFORM )
    install ( PROGRAMS tankfile.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS tankfile.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif()
