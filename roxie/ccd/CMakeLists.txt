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


# Component: ccd 

#####################################################
# Description:
# ------------
#    Cmake Input File for ccd
#####################################################


project( ccd ) 

set (   SRCS 
        ccdactivities.cpp 
        ccddali.cpp
        ccdcontext.cpp
        ccddebug.cpp
        ccdserver.cpp 
        ccdfile.cpp 
        ccdkey.cpp 
        ccdlistener.cpp
        ccdmain.cpp
        ccdprotocol.cpp
        ccdquery.cpp
        ccdqueue.cpp
        ccdsnmp.cpp 
        ccdstate.cpp 
         
        ccd.hpp
        ccdactivities.hpp
        ccdcontext.hpp
        ccddebug.hpp
        ccddali.hpp
        ccdserver.hpp
        ccdfile.hpp
        ccdkey.hpp
        ccdlistener.hpp
        ccdprotocol.hpp
        ccdquery.hpp
        ccdqueue.ipp
        ccdsnmp.hpp
        ccdstate.hpp 
        hpccprotocol.hpp
        
                sourcedoc.xml
    )

include_directories ( 
         .
         ${HPCC_SOURCE_DIR}/common/remote
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/roxie/udplib
         ${HPCC_SOURCE_DIR}/roxie/roxie
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/ecl/hthor
         ${HPCC_SOURCE_DIR}/ecl/schedulectrl
         ${HPCC_SOURCE_DIR}/rtl/nbcd
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/dali/dfu
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/testing/unittests
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/dali/ft
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/securesocket
    )

ADD_DEFINITIONS( -D_USRDLL -DCCD_EXPORTS -DSTARTQUERY_EXPORTS )

    if (CMAKE_COMPILER_IS_CLANGXX)
      SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-switch-enum -Wno-format-security")
    endif()

HPCC_ADD_LIBRARY( ccd SHARED ${SRCS} )
install ( TARGETS ccd RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} ARCHIVE DESTINATION componentfiles/cl/lib )

target_link_libraries ( ccd 
         jlib
         nbcd 
         roxiemem 
         udplib 
         remote 
         eclrtl 
         dalibase 
         deftype 
         thorhelper 
         jhtree 
         environment 
         schedulectrl
         dllserver 
         workunit 
    )

IF (USE_OPENSSL)
    target_link_libraries ( ccd
    	securesocket
    )
ENDIF()

IF (USE_TBBMALLOC)
   add_dependencies ( ccd tbb )
   target_link_libraries ( ccd libtbbmalloc_proxy libtbbmalloc)
ENDIF()


