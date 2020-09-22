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


# Component: ws_access 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_access
#####################################################

project( ws_access ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)
include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_access_esp.cpp
         ws_accessPlugin.cpp 
         ws_accessService.cpp 
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/services
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

# NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly 
include_directories ( ${OPENLDAP_INCLUDE_DIR} )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_access SHARED ${SRCS} )
install ( TARGETS ws_access RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} COMPONENT Runtime)
add_dependencies (ws_access espscm)
target_link_libraries ( ws_access 
         jlib
         xmllib 
         esphttp 
         LdapSecurity 
    )

IF (USE_OPENSSL)
    target_link_libraries ( ws_access
    	securesocket
    )
ENDIF()
