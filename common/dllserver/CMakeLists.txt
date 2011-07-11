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


# Component: dllserver 
#####################################################
# Description:
# ------------
#    Cmake Input File for dllserver
#####################################################

project( dllserver ) 

set (    SRCS 
         dllserver.cpp 
         thorplugin.cpp 

         dllserver.hpp 
         thorplugin.hpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../../rtl/eclrtl 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../../common/environment 
    )

IF (NOT WIN32)
  include_directories(${BINUTILS_INCLUDE_DIR})
ENDIF()

ADD_DEFINITIONS( -D_USRDLL -DDLLSERVER_EXPORTS )

HPCC_ADD_LIBRARY( dllserver SHARED ${SRCS} )
install ( TARGETS dllserver DESTINATION ${OSSDIR}/lib )
target_link_libraries ( dllserver
         ${ZLIB_LIBRARIES}
         jlib 
         mp 
         hrpc 
         eclrtl 
         remote 
         dalibase 
         environment 
    )
if (NOT WIN32)
  target_link_libraries ( dllserver ${BINUTILS_LIBRARIES})
endif()

