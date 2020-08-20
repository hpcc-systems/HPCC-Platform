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

# Component: thorslave_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for thorslave_lcr
#####################################################

project( thorslave_lcr ) 

set (    SRCS 
         backup.cpp 
         slavmain.cpp 
         slwatchdog.cpp 
         thslavemain.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/common/deftype 
         ${HPCC_SOURCE_DIR}/common/dllserver 
         ${HPCC_SOURCE_DIR}/common/environment 
         ${HPCC_SOURCE_DIR}/common/thorhelper 
         ${HPCC_SOURCE_DIR}/common/workunit 
         ${HPCC_SOURCE_DIR}/dali/base 
         ${HPCC_SOURCE_DIR}/fs/dafsclient 
         ${HPCC_SOURCE_DIR}/fs/dafsserver
         ${HPCC_SOURCE_DIR}/rtl/eclrtl 
         ${HPCC_SOURCE_DIR}/rtl/include 
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jhtree 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/thorlcr/graph 
         ${HPCC_SOURCE_DIR}/thorlcr/master 
         ${HPCC_SOURCE_DIR}/thorlcr/msort 
         ${HPCC_SOURCE_DIR}/thorlcr/shared 
         ${HPCC_SOURCE_DIR}/thorlcr/slave 
         ${HPCC_SOURCE_DIR}/thorlcr/thorutil 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( thorslave_lcr ${SRCS} )
install ( TARGETS thorslave_lcr RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries (  thorslave_lcr 
         jlib
         dafsclient 
         dalibase 
         environment 
         dllserver 
         nbcd 
         hql
         eclrtl 
         deftype 
         workunit 
         thorhelper 
         thorcodectx_lcr 
         jhtree 
         graph_lcr 
         graphslave_lcr 
         thorsort_lcr 
         activityslaves_lcr 
    )

if (CONTAINERIZED) # only used when dev. option _dafsStorage is enabled
target_link_libraries (  thorslave_lcr 
         dafsserver
    )
endif()
