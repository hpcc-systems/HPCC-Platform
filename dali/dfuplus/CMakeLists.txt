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


# Component: dfuplus
#####################################################
# Description:
# ------------
#    Cmake Input File for dfuplus
#####################################################

project( dfuplus )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ../../esp/bindings/bindutil.cpp
         ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_fs_esp.cpp
         dfuplus.cpp
         main.cpp
    )

include_directories (
         ./../../system/security/shared
         ./../../esp/bindings/SOAP/xpp
         ./../dfu
         ./../ft
         ./../../system/xmllib
         ./../../esp/platform
         ./../../esp/clients
         ./../../rtl/include
         ./../../system/include
         ./../../system/jlib
         ./../../esp/bindings
         ./../../common/workunit
         ./../../common/remote
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

set_source_files_properties (dfuplus.cpp PROPERTIES COMPILE_FLAGS -DDAFILESRV_LOCAL)

ADD_DEFINITIONS( -D_CONSOLE )

add_executable ( dfuplus ${SRCS} )
add_dependencies ( dfuplus espscm )
install ( TARGETS dfuplus DESTINATION ${OSSDIR}/bin )
target_link_libraries ( dfuplus
     jlib
     remote
     xmllib
     esphttp
     ws_fs
     ws_dfu
)

