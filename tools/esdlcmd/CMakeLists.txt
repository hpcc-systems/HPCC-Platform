################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
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

# Component: esdlcmd

#####################################################
# Description:
# ------------
#    Cmake Input File for esdlcmd
#####################################################


project( esdlcmd )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set ( SRCS
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdlcmd.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdlcmd_shell.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdlcmd_core.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdlcmd_common.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdl2ecl.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdlcmd_monitor.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdl-publish.cpp
         ${CMAKE_CURRENT_SOURCE_DIR}/esdlcmdutils.cpp
         ${ESPSCM_GENERATED_DIR}/ws_esdlconfig_esp.cpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/fileview2
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/scm
         ${HPCC_SOURCE_DIR}/system/security/scm
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/tools/esdlcomp
         ${HPCC_SOURCE_DIR}/tools/esdlcmd-xml
         ${HPCC_SOURCE_DIR}/esp/esdllib
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_CONSOLE )
if (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_CLANGXX)
    ADD_DEFINITIONS( -O0 )
endif ()

HPCC_ADD_EXECUTABLE ( esdl ${SRCS} )

add_dependencies ( esdl
                   espscm
                 )

install ( TARGETS esdl RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( esdl
        jlib
        esphttp
        xmllib
        esdllib
        esdlcomp
    )

if ( UNIX )
    install ( PROGRAMS esdl.install DESTINATION etc/init.d/install COMPONENT Runtime )
    install ( PROGRAMS esdl.uninstall DESTINATION etc/init.d/uninstall COMPONENT Runtime )
endif ( UNIX )
