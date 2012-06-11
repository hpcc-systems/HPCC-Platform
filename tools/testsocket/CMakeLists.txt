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

# Component: testsocket
#####################################################
# Description:
# ------------
#    Cmake Input File for testsocket
#####################################################

project( testsocket ) 

include_directories (            
         ./../../system/jlib
         ./../../system/include 
    )

ADD_DEFINITIONS( -D_CONSOLE )

set ( SRCS testsocket.cpp )
add_executable ( testsocket ${SRCS} )
target_link_libraries ( testsocket
         jlib
         )
install ( TARGETS testsocket DESTINATION ${OSSDIR}/bin )

if ( PLATFORM )
    install ( PROGRAMS testsocket.install DESTINATION ${DIR_NAME}/etc/init.d/install COMPONENT Runtime )
endif()
