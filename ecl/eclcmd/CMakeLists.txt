################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################


# Component: ecl
#####################################################
# Description:
# ------------
#    Cmake Input File for ecl
#####################################################


project( ecl )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
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
         ./../../esp/clients
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

add_executable ( ecl ${SRCS} )
add_dependencies ( ecl espscm workunit ws_workunits )
install ( TARGETS ecl DESTINATION ${OSSDIR}/bin )
target_link_libraries ( ecl
        jlib
        esphttp
        workunit
    )
