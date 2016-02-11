################################################################################
#    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems®.
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

# Component: initldap

#####################################################
# Description:
# ------------
#    Cmake Input File for initldap
#####################################################


project( initldap )

if (APPLE AND CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif()

set (    SRCS
         initldap.cpp
    )

include_directories (
         ./../../system/security/LdapSecurity
         ./../../system/security/shared
         ./../../system/jlib
         ./../../system/include
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
    )

ADD_DEFINITIONS( -D_CONSOLE )


HPCC_ADD_EXECUTABLE ( initldap ${SRCS} )
install ( TARGETS initldap RUNTIME DESTINATION ${EXEC_DIR} )



target_link_libraries ( initldap
         jlib
         LdapSecurity
    )


