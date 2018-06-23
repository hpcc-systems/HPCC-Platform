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
         ./../thorutil 
         ./../../common/remote 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../common/workunit 
         ./../shared 
         .
         ./../../common/environment 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../msort 
         ./../../system/jlib 
         ./../../rtl/eclrtl 
         ./../master 
         ./../graph 
         ./../../common/thorhelper 
         ./../../roxie/roxiemem
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../system/security/shared
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( thorslave_lcr ${SRCS} )
install ( TARGETS thorslave_lcr RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries (  thorslave_lcr 
         jlib
         remote 
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


