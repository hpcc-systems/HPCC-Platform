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


# Component: daliadmin 
#####################################################
# Description:
# ------------
#    Cmake Input File for daliadmin
#####################################################

project( daliadmin ) 

set (    SRCS 
         daliadmin.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../server 
         ./../base 
         ./../../system/mp 
         . 
         ./../../system/include 
         ./../../system/jlib 
         ./../ft 
    )

ADD_DEFINITIONS( -D_CONSOLE )

if ( NOT CLIENTTOOLS_ONLY )
    add_executable ( daliadmin ${SRCS} )
    install ( TARGETS daliadmin DESTINATION ${OSSDIR}/bin )
    target_link_libraries ( daliadmin  
         jlib
         mp 
         remote 
         dalibase 
    )
endif()

