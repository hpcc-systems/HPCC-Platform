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


# Component: eclcc 
#####################################################
# Description:
# ------------
#    Cmake Input File for eclcc
#####################################################


project( eclcc ) 

set (    SRCS
         eclcc.hpp
         eclcc.cpp
    )

include_directories ( 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/ecl/hqlcpp 
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/common/workunit 
         ${HPCC_SOURCE_DIR}/common/dllserver 
         ${HPCC_SOURCE_DIR}/common/deftype 
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/esp/clients/ws_dfsclient
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/rtl/include 
         ${HPCC_SOURCE_DIR}/rtl/eclrtl 
         ${HPCC_SOURCE_DIR}/system/codesigner
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/zcrypt
    )

ADD_DEFINITIONS( -D_CONSOLE )

#The transformations can use a lot of stack space processing very deep graphs
if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:10000000 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( eclcc ${SRCS} )
install ( TARGETS eclcc RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( eclcc 
         eclrtl 
         deftype 
         dafsclient 
         dalibase 
         dllserver
         hql
         hqlcpp
         jlib
         nbcd 
         thorhelper 
         workunit 
         ws_dfsclient
    )

if (NOT CONTAINERIZED)
  target_link_libraries ( eclcc environment )
endif()

IF (USE_ZLIB)
    target_link_libraries ( eclcc
        ${ZLIB_LIBRARIES}
        zcrypt
    )
ENDIF()
