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


# Component: WsDeploy 
#####################################################
# Description:
# ------------
#    Cmake Input File for WsDeploy
#####################################################

project( WsDeploy ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/WsDeploy_esp.cpp 
         WsDeployPlugin.cpp 
         WsDeployEngine.cpp 
         WsDeployBinding.cpp 
         WsDeployService.cpp 
    )

include_directories ( 
         ./../../esplib 
         ./../.. 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../common 
         ./../../../system/xmllib 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/remote 
         ./../../clients 
         ./../../../dali/base 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../bindings/http/client
         ./../../../system/mp 
         ../../../deployment/deploy 
         ../../../deployment/deployutils 
         ${HPCC_SOURCE_DIR}/deployment/configutils
         ${CMAKE_BINARY_DIR}
         ${CMAKE_BINARY_DIR}/oss
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( WsDeploy SHARED ${SRCS}  )
add_dependencies (WsDeploy espscm)
install ( TARGETS WsDeploy RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( WsDeploy  
         jlib
         xmllib 
         esphttp 
         dalibase 
         environment 
         dllserver 
         eclrtl 
         hql 
         deploy
         deployutils
         configutils
    )

IF (USE_OPENSSL)
    target_link_libraries ( WsDeploy
    	securesocket
    )
ENDIF()
