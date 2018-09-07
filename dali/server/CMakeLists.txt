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


# Component: daserver 

#####################################################
# Description:
# ------------
#    Cmake Input File for daserver
#####################################################


project( daserver ) 

set (    SRCS 
         daldap.cpp 
         daserver.cpp 
    )

include_directories ( 
         ./../../common/remote 
         ./../../system/mp 
         ./../base 
         ./../../system/include 
         ./../../system/security/shared 
         ./../../system/security/LdapSecurity 
         ./../../system/security/cryptohelper
         ./../../system/jlib 
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )
ADD_DEFINITIONS( -D_CONSOLE )

if (WIN32)
    if ("${CMAKE_BUILD_TYPE}" MATCHES "Debug")
        set (CMAKE_EXE_LINKER_FLAGS "/STACK:262144 ${CMAKE_EXE_LINKER_FLAGS}")  
    else ()
        set (CMAKE_EXE_LINKER_FLAGS "/STACK:65536 ${CMAKE_EXE_LINKER_FLAGS}")
    endif ()
endif()

HPCC_ADD_EXECUTABLE ( daserver ${SRCS} )
install ( TARGETS daserver RUNTIME DESTINATION ${EXEC_DIR})
target_link_libraries ( daserver 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
    )
IF (USE_OPENLDAP)
target_link_libraries ( daserver LdapSecurity )
ENDIF(USE_OPENLDAP)
