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

# Component: thormaster_lcr 
#####################################################
# Description:
# ------------
#    Cmake Input File for thormaster_lcr
#####################################################

project( thormaster_lcr ) 

set (    SRCS 
         mawatchdog.cpp 
         thdemonserver.cpp 
         thgraphmanager.cpp 
         thmastermain.cpp 
    )

include_directories ( 
         ./../thorutil 
         ./../../fs/dafsclient 
         ./../../system/jhtree 
         ./../../system/mp 
         ./../../common/workunit 
         ./../shared 
         ./../graph 
         ./../../common/environment 
         ./../../common/deftype 
         ./../../system/include 
         ./../../dali/base 
         ./../../ecl/hql
         ./../../rtl/include 
         ./../../common/dllserver 
         ./../../system/jlib 
         ./../thorcodectx 
         ./../mfilemanager 
         ./../activities 
         ./../../rtl/eclrtl 
         ./../master 
         ./../../common/thorhelper 
         ./../../tools/swapnode
         ./../../roxie/roxiemem
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ./../../system/security/shared
         ./../../system/security/securesocket
    )

ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
endif()

HPCC_ADD_EXECUTABLE ( thormaster_lcr ${SRCS} )
install ( TARGETS thormaster_lcr RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries (  thormaster_lcr 
         jlib
         jhtree 
         dafsclient 
         dalibase 
         dllserver 
         nbcd 
         eclrtl 
         hql
         deftype 
         workunit 
         graph_lcr 
         thorsort_lcr 
         thorhelper 
         thorcodectx_lcr 
         dalift 
         mfilemanager_lcr 
         graphmaster_lcr 
         activitymasters_lcr 
         swapnodelib
    )

if (USE_OPENSSL)
    target_link_libraries(thormaster_lcr securesocket)
endif()

if (NOT CONTAINERIZED)
    target_link_libraries ( thormaster_lcr environment )
endif()
