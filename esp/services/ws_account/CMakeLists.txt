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


# Component: ws_account 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_account
#####################################################

project( ws_account ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_account_esp.cpp
         ws_accountPlugin.cpp 
         ws_accountService.cpp 
    )

include_directories ( 
         ./../.. 
         ./../../platform 
         ./../../../system/jlib 
         ./../../services 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/include 
         ./../../clients 
         ./../../../system/security/LdapSecurity 
         ./../../../system/security/shared 
         ./../../bindings 
         ./../../bindings/SOAP/xpp
         ./../../smc/SMCLib  
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

if (USE_OPENLDAP)
    # NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly
    include_directories ( ${OPENLDAP_INCLUDE_DIR} )
endif ()

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_account SHARED ${SRCS} )
install ( TARGETS ws_account RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} COMPONENT Runtime )
add_dependencies (ws_account espscm)
target_link_libraries ( ws_account 
         jlib
         xmllib 
         esphttp 
    )
IF (USE_OPENSSL)
    target_link_libraries ( ws_account
    	securesocket
    )
ENDIF()
IF (USE_OPENLDAP)
target_link_libraries ( ws_account LdapSecurity )
ENDIF(USE_OPENLDAP)
