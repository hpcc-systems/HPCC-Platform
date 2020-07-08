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


# Component: ws_topology 

#####################################################
# Description:
# ------------
#    Cmake Input File for ws_topology
#####################################################


project( ws_topology ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_topology_esp.cpp 
         ws_topologyPlugin.cpp 
         ws_topologyService.cpp 
    )

include_directories ( 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket         
         ./../../../system/security/shared 
         ./../../../system/security/zcrypt 
         ./../../../system/include 
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
         ./../..//esplib
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_TOPOLOGY_EXPORTS )

HPCC_ADD_LIBRARY( ws_topology SHARED ${SRCS} )
add_dependencies ( ws_topology espscm )
install ( TARGETS ws_topology RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_topology
         jlib
         remote 
         xmllib 
         esphttp 
         dalibase 
         environment 
         dalift 
         dllserver 
         nbcd 
         eclrtl 
         deftype 
         workunit 
         SMCLib 
         swapnodelib
         )

IF (USE_OPENSSL)
    target_link_libraries ( ws_topology
    	securesocket
    )
ENDIF()
         
IF (USE_ZLIB)
  target_link_libraries ( ws_topology
      ${ZLIB_LIBRARIES}
      zcrypt
      )
ENDIF()


