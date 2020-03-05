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


# Component: remote 
#####################################################
# Description:
# ------------
#    Cmake Input File for remote
#####################################################

project( remote ) 

HPCC_ADD_SUBDIRECTORY(hooks)

set (    SRCS 
         rmtspawn.cpp 
         rmtssh.cpp 
         rmtsmtp.cpp 
    )
if (USE_URIPARSER)
  set ( SRCS
        ${SRCS}
        uri.cpp
        uri.hpp
      )
endif(USE_URIPARSER)

include_directories (
         ${HPCC_SOURCE_DIR}/system/hrpc 
         ${HPCC_SOURCE_DIR}/system/mp 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/jlib 
         ${HPCC_SOURCE_DIR}/system/jhtree
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/cryptohelper
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/testing/unittests
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/ecl/hql
         ${HPCC_SOURCE_DIR}/common/deftype
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_USRDLL -DREMOTE_EXPORTS )

HPCC_ADD_LIBRARY( remote SHARED ${SRCS}  )
install ( TARGETS remote RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( remote 
    eclrtl
    jlib
    jhtree 
    hql   
    mp
    ${URIPARSER_LIBRARIES}
    ${CPPUNIT_LIBRARIES}
    )

IF (USE_OPENSSL)
    target_link_libraries ( remote 
    	securesocket
    )
ENDIF()
