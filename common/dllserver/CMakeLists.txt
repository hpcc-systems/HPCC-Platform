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


# Component: dllserver 
#####################################################
# Description:
# ------------
#    Cmake Input File for dllserver
#####################################################

project( dllserver ) 

set (    SRCS 
         dllserver.cpp 
         thorplugin.cpp 

         dllserver.hpp 
         thorplugin.hpp 
    )

include_directories ( 
         ./../../fs/dafsclient
         ./../../system/mp 
         ./../../rtl/eclrtl 
         ./../../system/include 
         ./../../dali/base 
         ./../../system/jlib 
         ./../../system/security/shared
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

if (NOT CONTAINERIZED)
  include_directories( ./../../common/environment )
endif()

IF (NOT WIN32)
  include_directories(${BINUTILS_INCLUDE_DIR})
ENDIF()

ADD_DEFINITIONS( -D_USRDLL -DDLLSERVER_EXPORTS )

HPCC_ADD_LIBRARY( dllserver SHARED ${SRCS} )
install ( TARGETS dllserver RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( dllserver
         ${ZLIB_LIBRARIES}
         jlib 
         mp 
         hrpc 
         eclrtl 
         dafsclient
         dalibase 
    )

if (NOT CONTAINERIZED)
  target_link_libraries ( dllserver environment )
endif()

if (NOT WIN32)
  target_link_libraries ( dllserver ${BINUTILS_LIBRARIES})
endif()

