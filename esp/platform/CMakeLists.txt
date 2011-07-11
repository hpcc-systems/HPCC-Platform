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


# Component: esp 
#####################################################
# Description:
# ------------
#    Cmake Input File for esp
#####################################################

project( esp ) 

set (    SRCS 
         espcfg.cpp 
         espp.cpp 
    )

include_directories ( 
         ./../bindings/SOAP/xpp 
         ./../../system/xmllib 
         ./../../system/security/shared 
         ./../../system/security/securesocket 
         ./../bindings 
         ./../platform 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../bindings/http/platform 
         ./../../common/environment 
         ./../../system/mp 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

add_executable ( esp ${SRCS} )
install ( TARGETS esp DESTINATION ${OSSDIR}/bin )
target_link_libraries ( esp 
         jlib
         mp 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         xmllib 
         esphttp 
         hrpc 
         remote 
         dalibase 
         environment 
    )


