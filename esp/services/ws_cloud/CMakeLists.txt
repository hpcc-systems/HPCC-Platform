################################################################################
#    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
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


# Component: ws_cloud
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_cloud
#####################################################

project( ws_cloud )

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_cloud_esp.cpp
         ws_cloudPlugin.cpp
         ws_cloudService.cpp
    )

include_directories (
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/include
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/dali/sasha
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
    )

if (USE_OPENLDAP)
    # NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly
    include_directories ( ${OPENLDAP_INCLUDE_DIR} )
endif ()

ADD_DEFINITIONS( -D_USRDLL -DWS_CLOUD_EXPORTS )

HPCC_ADD_LIBRARY( ws_cloud SHARED ${SRCS}  )
add_dependencies ( ws_cloud espscm )
install ( TARGETS ws_cloud RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_cloud
         jlib
         xmllib
         esphttp
         SMCLib
    )

IF (USE_OPENSSL)
    target_link_libraries ( ws_cloud
         securesocket
    )
ENDIF()

IF (USE_OPENLDAP)
target_link_libraries ( ws_cloud LdapSecurity )
ENDIF(USE_OPENLDAP)

