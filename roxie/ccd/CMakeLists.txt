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
SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STRICT_CXX_FLAGS}")

IF (USE_TBBMALLOC AND USE_TBBMALLOC_ROXIE)
  find_package(TBB CONFIG REQUIRED)
endif()

set (   SRCS 
        ccdactivities.cpp
        ../ccdcache/ccdcache.cpp
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
         ${HPCC_SOURCE_DIR}/common/deftype
         ${HPCC_SOURCE_DIR}/common/dllserver
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/common/pkgfiles
         ${HPCC_SOURCE_DIR}/common/workunit
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/dali/dfu
         ${HPCC_SOURCE_DIR}/dali/ft
         ${HPCC_SOURCE_DIR}/ecl/hthor
         ${HPCC_SOURCE_DIR}/ecl/schedulectrl
         ${HPCC_SOURCE_DIR}/esp/clients/ws_dfsclient
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/roxie/ccdcache
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/nbcd
         ${HPCC_SOURCE_DIR}/roxie/roxie
         ${HPCC_SOURCE_DIR}/roxie/roxiemem
         ${HPCC_SOURCE_DIR}/roxie/udplib
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/libbase58
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/testing/unittests
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-switch -Wno-unused-parameter -Werror -Wno-delete-non-virtual-dtor -Wno-overloaded-virtual")
  if (CMAKE_COMPILER_IS_GNUCC AND NOT CMAKE_CXX_COMPILER_VERSION VERSION_LESS "8.0.0")
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-class-memaccess")
  endif()
endif()

ADD_DEFINITIONS( -D_USRDLL -DCCD_EXPORTS -DSTARTQUERY_EXPORTS )

if (CMAKE_COMPILER_IS_CLANGXX)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-switch-enum -Wno-format-security -Werror=reorder")
endif()


HPCC_ADD_LIBRARY( ccd SHARED ${SRCS} )
if (EMSCRIPTEN)
    set_target_properties(ccd PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS} -Wno-error=nontrivial-memcall")
endif ()
install ( TARGETS ccd RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} ARCHIVE DESTINATION componentfiles/cl/lib )

target_link_libraries ( ccd 
         dafsclient 
         dalibase 
         deftype 
         dllserver 
         eclrtl 
         jhtree 
         jlib
         libbase58
         nbcd
         roxiemem 
         schedulectrl
         thorhelper 
         udplib 
         workunit 
         ws_dfsclient
         pkgfiles
    )

if (NOT CONTAINERIZED)
    target_link_libraries ( ccd environment )
endif()

IF (USE_OPENSSL)
    target_link_libraries ( ccd
    	securesocket
    )
ENDIF()

IF (USE_TBBMALLOC AND USE_TBBMALLOC_ROXIE)
   target_link_libraries ( ccd TBB::tbb )
ENDIF()