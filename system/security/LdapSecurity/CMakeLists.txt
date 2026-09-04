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


# Component: LdapSecurity 
#####################################################
# Description:
# ------------
#    Cmake Input File for LdapSecurity
#####################################################

project( LdapSecurity ) 

if (APPLE AND CMAKE_COMPILER_IS_CLANG)
  SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")
endif()

set (    SRCS 
         ../shared/authmap.cpp 
         ../shared/caching.cpp 
         aci.cpp 
         ldapsanitization.cpp
         ldaptimeutils.cpp
         ldapconnection.cpp 
         ldapsecurity.cpp 
         permissions.cpp 
         ldaputils.cpp
    )

# Unit tests for this component are compiled directly into LdapSecurity
# itself (rather than a separate test library) so that CppUnit fixtures
# can white-box test private/internal classes not exposed via any header
# (e.g. CLdapClient, CAci). Test-only source files are kept in the
# unittests/ subdirectory for separation from production code, but built
# into this same library, following the jhtree/thorhelper/eclrtl pattern.
# Only added when USE_CPPUNIT is on (CppUnit_LIBRARIES is empty otherwise).
IF (USE_CPPUNIT)
    set ( SRCS ${SRCS}
         unittests/ldapsecuritytests.cpp
    )
ENDIF (USE_CPPUNIT)

include_directories ( 
         ./../../include 
         ./../shared 
         ./../cryptohelper
         ./../../jlib 
         ./../../../esp/platform 
	     ./../../../dali/base
	     ./../../../system/mp
	     ./../../../common/workunit
	     ./../../../testing/unittests
	     ./ 
    )

ADD_DEFINITIONS( -DLDAPSECURITY_EXPORTS -D_USRDLL )

HPCC_ADD_LIBRARY( LdapSecurity SHARED ${SRCS} )
install ( TARGETS LdapSecurity RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} CALC_DEPS)
target_link_libraries ( LdapSecurity PRIVATE
         jlib
         dalibase
         workunit
         PkgConfig::OPENLDAP
    )

IF (USE_CPPUNIT)
    target_link_libraries ( LdapSecurity PRIVATE ${CppUnit_LIBRARIES} )
ENDIF (USE_CPPUNIT)

IF (USE_OPENSSL)
    target_link_libraries ( LdapSecurity PRIVATE ${OPENSSL_LIBRARIES} )
ENDIF (USE_OPENSSL)


