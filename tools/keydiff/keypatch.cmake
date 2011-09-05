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

# Component: keypatch 

#####################################################
# Description:
# ------------
#    Cmake Input File for keypatch
#####################################################

project( keypatch ) 

set (    SRCS 
         patchmain.cpp 
    )

include_directories ( 
         ./../../common/deftype 
         ./../../system/mp 
         ./../../system/jhtree 
         ./../../rtl/eclrtl 
         ./../../rtl/include 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS ( -DNO_SYBASE -D_CONSOLE )

add_executable ( keypatch ${SRCS} )
install ( TARGETS keypatch DESTINATION ${OSSDIR}/bin )
target_link_libraries ( keypatch 
         jlib
         jhtree 
         mp 
    )


