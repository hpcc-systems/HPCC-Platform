################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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


# Component: ws_dfu 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_dfu
#####################################################

project( ws_dfu ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ../../../dali/dfu/dfuutil.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_dfuXref_esp.cpp 
         ${HPCC_SOURCE_DIR}/esp/scm/ws_dfu.ecm
         ${HPCC_SOURCE_DIR}/esp/scm/ws_dfuXref.ecm
         ${HPCC_SOURCE_DIR}/esp/clients/roxiecontrol.cpp
         ws_dfuPlugin.cpp 
         ws_dfuService.cpp 
         ws_dfuXRefService.cpp 
    )

include_directories ( 
         ${CMAKE_BINARY_DIR}/oss
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../ws_topology 
         ./../../../system/jlib 
         ./../../../rtl/eclrtl 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../common 
         ./../../../dali/dfuXRefLib 
         ./../../../system/xmllib 
         ./../../../common/deftype 
         ./../../../ecl/hql 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../dali/base 
         ./../ws_workunits 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../../common/fileview2 
         ./../../bindings/SOAP/xpp 
         ${HPCC_SOURCE_DIR}/esp/clients
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_DFU_EXPORTS )

HPCC_ADD_LIBRARY( ws_dfu SHARED ${SRCS} )
add_dependencies ( ws_dfu espscm )
install ( TARGETS ws_dfu RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_dfu
         jlib
         mp 
         hrpc 
         remote 
         nbcd 
         eclrtl 
         jhtree 
         dalibase 
         environment 
         dllserver 
         deftype 
         workunit 
         hql   
         fileview2 
         xmllib 
         esphttp 
         dalift 
         SMCLib 
         dfuXRefLib 
         dfuwu 
         securesocket 
         roxiecommlib 
    )


