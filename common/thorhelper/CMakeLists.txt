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


# Component: thorhelper 

#####################################################
# Description:
# ------------
#    Cmake Input File for thorhelper
#####################################################

project( thorhelper ) 

find_package(TBB CONFIG REQUIRED)

set (    SRCS 

         commonext.cpp
         csvsplitter.cpp 
         thorcommon.cpp 
         thorfile.cpp 
         thormeta.cpp
         thorparse.cpp 
         thorpipe.cpp 
         thorread.cpp
         thorrparse.cpp 
         thorsoapcall.cpp 
         thorstats.cpp
         thorstep.cpp 
         thorstep2.cpp 
         thorstrand.cpp
         thortalgo.cpp 
         thortlex.cpp 
         thortparse.cpp 
         thorxmlread.cpp 
         thorxmlwrite.cpp
         roxierow.cpp
         roxiehelper.cpp
         roxiedebug.cpp
         
         commonext.hpp
         csvsplitter.hpp 
         thorcommon.hpp 
         thorfile.hpp 
         thormeta.hpp
         thorparse.hpp 
         thorpipe.hpp 
         thorread.hpp
         thorrparse.hpp 
         thorsoapcall.hpp 
         thorstats.hpp
         thorstep.hpp 
         thorstrand.hpp
         thorxmlread.hpp 
         thorxmlwrite.hpp
         roxierow.hpp
         thorralgo.ipp
         thorrparse.ipp
         thorsort.cpp
         thorsort.hpp
         persistent.cpp
         persistent.hpp
         thorstore.cpp
         thorstore.hpp
         
         roxiedebug.hpp
         roxiedebug.ipp
         roxiehelper.hpp
         roxiehelper.ipp
         roxielmj.hpp
                 
         hpccconfig.cpp
         hpccconfig.hpp
    )

include_directories ( 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/nbcd
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/security/zcrypt
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/testing/unittests
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/fs/dafsclient
    )

ADD_DEFINITIONS( -DTHORHELPER_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( thorhelper SHARED ${SRCS} )
install ( TARGETS thorhelper RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( thorhelper 
         jlib 
         nbcd 
         hql
         eclrtl 
         roxiemem
         deftype 
         dalibase
    )

if (USE_NUMA)
 target_link_libraries ( thorhelper ${NUMA_LIBRARIES} )
endif ()

if (USE_TBB)
   target_link_libraries ( thorhelper TBB::tbb )
endif()


IF (USE_OPENSSL)
    target_link_libraries ( thorhelper 
    	securesocket
    )
ENDIF()

IF (USE_ZLIB)
    target_link_libraries ( thorhelper
        ${ZLIB_LIBRARIES}
        zcrypt
    )
ENDIF()
