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


# Component: daserver 

#####################################################
# Description:
# ------------
#    Cmake Input File for daserver
#####################################################


project( daserver ) 

set (    SRCS 
         daldap.cpp 
         daserver.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../base 
         ./../../system/include 
         ./../../system/security/shared 
         ./../../system/security/LdapSecurity 
         ./../../system/jlib 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
        set (CMAKE_EXE_LINKER_FLAGS "/STACK:262144 ${CMAKE_EXE_LINKER_FLAGS}")  
    else ()
        set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
    endif ()
endif()

add_executable ( daserver ${SRCS} )
install ( TARGETS daserver DESTINATION ${OSSDIR}/bin)
target_link_libraries ( daserver 
         jlib
         LdapSecurity 
         mp 
         hrpc 
         remote 
         dalibase 
    )

