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
         ./../../ecl/hqlcpp 
         ./../../common/environment
         ./../../common/workunit 
         ./../../common/dllserver 
         ./../../common/deftype 
         ./../../system/include 
         ./../../ecl/hql
         ./../../dali/base
         ./../../system/mp
         ./../../rtl/include 
         ./../../rtl/eclrtl 
         ./../../system/jlib 
         ./../../fs/dafsclient
         ./../../system/security/shared
         ./../../system/security/zcrypt
    )

ADD_DEFINITIONS( -D_CONSOLE )

#The transformations can use a lot of stack space processing very deep graphs
if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:10000000 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( eclcc ${SRCS} )
install ( TARGETS eclcc RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries ( eclcc 
         jlib
         nbcd 
         eclrtl 
         deftype 
         dafsclient 
         dalibase 
         workunit 
         thorhelper 
         hql
         hqlcpp
         dllserver
    )

if (CONTAINERIZED)
  target_link_libraries ( eclcc environment )
endif()

IF (USE_ZLIB)
    target_link_libraries ( eclcc
        ${ZLIB_LIBRARIES}
        zcrypt
    )
ENDIF()
