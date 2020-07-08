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


# Component: ws_smc 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_smc
#####################################################

project( ws_smc ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${HPCC_SOURCE_DIR}/esp/scm/ws_smc.ecm
         ${HPCC_SOURCE_DIR}/esp/scm/common.ecm
         ${ESPSCM_GENERATED_DIR}/ws_smc_esp.cpp
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${HPCC_SOURCE_DIR}/esp/clients/roxiecontrol.cpp
         ws_smcPlugin.cpp 
         ws_smcService.cpp 
    )

include_directories ( 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../common 
         ./../../../system/xmllib 
         ./../../../system/include 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../tools/swapnode 
         ./../../../dali/base
         ./../../../dali/sasha
         ./../ws_workunits 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
         ./../../../system/security/LdapSecurity
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

if (USE_OPENLDAP)
    # NOTE - this should not be needed, it's the result of poor encapsulation and using CLdapSecManager directly
    include_directories ( ${OPENLDAP_INCLUDE_DIR} )
endif ()

ADD_DEFINITIONS( -D_USRDLL -DWS_SMC_EXPORTS )

HPCC_ADD_LIBRARY( ws_smc SHARED ${SRCS}  )
add_dependencies ( ws_smc espscm )
install ( TARGETS ws_smc RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_smc 
         jlib
         xmllib 
         esphttp 
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         ws_machine 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         SMCLib 
         jhtree 
         hql 
         fileview2 
         dfuXRefLib 
         dfuwu 
         roxiecommlib 
         ws_dfu 
         ws_fs 
         ws_topology 
         schedulectrl 
         ws_workunits 
    )
IF (USE_OPENSSL)
    target_link_libraries ( ws_smc
    	securesocket
    )
ENDIF()
IF (USE_OPENLDAP)
target_link_libraries ( ws_smc LdapSecurity )
ENDIF(USE_OPENLDAP)

