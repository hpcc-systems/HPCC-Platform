################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

# Sasha and saserver
#######################################################
# Description:
# ------------
#           Cmake file for sasha and saserver executables
#
########################################################

project (sasha)

include (saruncmd.cmake)

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
         sadbghk.cpp
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
         ${HPCC_SOURCE_DIR}/fs/dafsclient 
         ${HPCC_SOURCE_DIR}/plugins/workunitservices 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/security/shared
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
        set (CMAKE_EXE_LINKER_FLAGS "/STACK:262144 ${CMAKE_EXE_LINKER_FLAGS}")  
    else ()
        set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
    endif ()
endif()

HPCC_ADD_EXECUTABLE ( saserver ${SRCS} )
install ( TARGETS saserver RUNTIME DESTINATION ${EXEC_DIR})


if ( ${HAVE_LIBCRYPT} )
target_link_libraries ( saserver crypt)
endif ( ${HAVE_LIBCRYPT} )

target_link_libraries ( saserver 
         jlib 
         dafsclient 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( saserver environment )
endif()

set (    SRCS 
         sacmd.cpp 
         sasha.cpp 
    )

include_directories ( 
         . 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/fs/dafsclient 
    )

ADD_DEFINITIONS ( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( sasha ${SRCS} )
install ( TARGETS sasha RUNTIME DESTINATION ${EXEC_DIR})

target_link_libraries ( sasha 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         mp
         saruncmdlib
    )




