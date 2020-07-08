################################################################################
#    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.
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


# Component: ws_espcontrol
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_espcontrol
#####################################################

project( ws_espcontrol )

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/ws_espcontrol_esp.cpp
         ws_espcontrolplugin.cpp
         ws_espcontrolservice.cpp
    )

include_directories (
         ./../..
         ./../../platform
         ./../../../system/include
         ./../../../system/jlib
         ./../../../system/xmllib
         ./../../../system/security/securesocket
         ./../../../system/security/LdapSecurity
         ./../../../system/security/shared
         ./../../../system/mp 
         ./../../../dali/base
         ./../../clients
         ./../../bindings
         ./../../bindings/SOAP/xpp
         ./../../smc/SMCLib
         ./../../../common/environment
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_espcontrol SHARED ${SRCS} )
install ( TARGETS ws_espcontrol RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_espcontrol
         jlib
         xmllib
         esphttp
    )
    
IF (USE_OPENSSL)
    target_link_libraries ( ws_espcontrol 
        securesocket
    )
ENDIF()

    
