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

# Component: myssl 

#####################################################
# Description:
# ------------
#    Cmake Input File for myssl
#####################################################


project( myssl ) 

set (    SRCS 
         main.cpp 
    )

include_directories ( 
         ./../../../jlib 
         ./../../securesocket 
         ./../../../include 
    )

ADD_DEFINITIONS( -D_CONSOLE )

if ( NOT CLIENTTOOLS_ONLY )
    add_executable ( myssl ${SRCS} )
    install ( TARGETS myssl DESTINATION ${OSSDIR}/bin )
    target_link_libraries ( myssl 
         jlib
         securesocket 
    )
endif()

