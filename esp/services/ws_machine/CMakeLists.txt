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


# Component: ws_machine 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_machine
#####################################################

project( ws_machine ) 

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_machine_esp.cpp 
         ws_machineBinding.cpp 
         ws_machinePlugin.cpp 
         ws_machineService.cpp 
         ws_machineServiceMetrics.cpp 
         ws_machineServiceRexec.cpp 
         componentstatus.cpp
    )

include_directories ( 
         ./../.. 
         ./../../../system/mp 
         ./../../../system/jlib 
         ./../../services 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared   
         ./../../../system/include 
         ./../../../common/remote 
         ./../../../common/environment
         ./../../../common/workunit
         ./../../clients 
         ./../../../dali/base 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../../esp/platform 
         ./../../smc/SMCLib 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_machine SHARED ${SRCS} )
add_dependencies( ws_machine espscm )
install ( TARGETS ws_machine RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_machine
         jlib
         roxiecommlib
         xmllib 
         esphttp 
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         securesocket 
         workunit
    )

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/DhcpMethods.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/StartStop.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/StartStopBegin.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/StartStopDone.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/machines.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/metrics.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/rexec.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/preflightControls.xslt
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/smc_xslt/ws_machine COMPONENT Runtime )
ENDFOREACH ( iFILES )

