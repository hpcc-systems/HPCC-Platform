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

# Component: dafilesrv 
#####################################################
# Description:
# ------------
#    Cmake Input File for dafilesrv
#####################################################

project( dafilesrv ) 

set (    SRCS 
         dafilesrv.cpp 
    )

include_directories ( 
         ./../../system/hrpc 
         ./../../common/remote 
         ./../../system/include 
         ./../../system/jlib 
    )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

add_executable ( dafilesrv ${SRCS} )
set_target_properties (dafilesrv PROPERTIES COMPILE_FLAGS -D_CONSOLE)
install ( TARGETS dafilesrv DESTINATION ${OSSDIR}/bin )
target_link_libraries ( dafilesrv
         jlib
         remote
    )

