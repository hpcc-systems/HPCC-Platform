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


# Component: ecl
#####################################################
# Description:
# ------------
#    Cmake Input File for ecl
#####################################################

add_subdirectory (queries)
add_subdirectory (roxie)

project( ecl )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp
         eclcmd.hpp
         ecl.cpp
         eclcmd_common.hpp
         eclcmd_shell.cpp
         eclcmd_common.cpp
         eclcmd_core.hpp
         eclcmd_core.cpp
    )

include_directories (
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../system/include
         ./../../system/jlib
         ./../../esp/clients
         ./../../system/security/shared
         ./../../system/mp
         ./../../common/dllserver
         ./../../common/fileview2
         ./../../common/workunit
         ./../../esp/bindings/http/client
         ./../../common/environment
         ./../../esp/bindings/SOAP/xpp
         ./../../system/include
         ./../../esp/bindings
         ./../../ecl/hql
         ./../../dali/base
         ./../../esp/platform
         ./../../system/jlib
         ./../../system/xmllib
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( ecl ${SRCS} )
add_dependencies ( ecl espscm workunit ws_workunits )
install ( TARGETS ecl RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( ecl
        jlib
        esphttp
        workunit
    )
