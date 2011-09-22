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

# Sasha and saserver
#######################################################
# Description:
# ------------
#           Cmake file for sasha and saserver executables
#
########################################################

project (sasha)

INCLUDE(CheckLibraryExists) 

if (NOT WIN32 AND NOT WIN64)
CHECK_LIBRARY_EXISTS(crypt crypt "" HAVE_LIBCRYPT) 
endif ()

set (    SRCS 
         saarch.cpp 
         sacmd.cpp 
         sacoalescer.cpp 
         salds.cpp 
         saqmon.cpp 
         saserver.cpp 
         sautil.cpp 
         saverify.cpp 
         saxref.cpp 
    )

include_directories ( 
         . 
         ${HPCC_SOURCE_DIR}/common/environment 
         ${HPCC_SOURCE_DIR}/common/workunit 
         ${HPCC_SOURCE_DIR}/common/dllserver 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/common/remote 
         ${HPCC_SOURCE_DIR}/plugins/workunitservices 
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

add_executable ( saserver ${SRCS} )
install ( TARGETS saserver DESTINATION ${OSSDIR}/bin)


if ( ${HAVE_LIBCRYPT} )
target_link_libraries ( saserver crypt)
endif ( ${HAVE_LIBCRYPT} )

target_link_libraries ( saserver 
         jlib 
         remote 
         dalibase 
         environment 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
    )

set (    SRCS 
         sacmd.cpp 
         sasha.cpp 
    )

include_directories ( 
         . 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/common/remote 
    )

ADD_DEFINITIONS ( -D_CONSOLE )

add_executable ( sasha ${SRCS} )
install ( TARGETS sasha DESTINATION ${OSSDIR}/bin)

target_link_libraries ( sasha 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         mp 
    )




