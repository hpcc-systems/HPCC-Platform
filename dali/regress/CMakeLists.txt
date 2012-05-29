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


# Component: daregress 
#####################################################
# Description:
# ------------
#    Cmake Input File for daregress
#####################################################

project( daregress ) 

set (    SRCS 
         daregdfs.cpp 
         daregress.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../server 
         ./../base 
         ./../../system/mp 
         . 
         ./../../system/include 
         ./../../system/jlib 
    )

ADD_DEFINITIONS( -D_CONSOLE )

if ( NOT CLIENTTOOLS_ONLY )
    add_executable ( daregress ${SRCS} )
    install ( TARGETS daregress DESTINATION ${OSSDIR}/bin )
    target_link_libraries ( daregress  
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
    )
endif()

