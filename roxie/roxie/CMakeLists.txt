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


# Component: roxie

#####################################################
# Description:
# ------------
#    Cmake Input File for roxie
#####################################################


project( roxie ) 

set (   SRCS 
        roxie.cpp 
        roxie.hpp        
    )

include_directories ( 
         .
         ./../../roxie/ccd
         ./../../system/include 
         ./../../system/jlib
         ./../../rtl/include 
         ./../../rtl/eclrtl

         ./../../roxie/udplib 
         ./../../roxie/roxie 
         ./../../roxie/roxiemem 
         ./../../common/thorhelper 
         ./../../common/roxiehelper 
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

add_executable ( roxie ${SRCS} )
install ( TARGETS roxie DESTINATION ${OSSDIR}/bin )

target_link_libraries ( roxie
         ccd
         jlib 
    )



