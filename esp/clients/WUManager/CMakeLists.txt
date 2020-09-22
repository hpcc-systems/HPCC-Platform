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


# Component: wumanager 
#####################################################
# Description:
# ------------
#    Cmake Input File for wumanager
#####################################################

project( wumanager ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_struct_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_req_resp_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_queryset_req_resp_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp
         WUManager.cpp 
    )

include_directories ( 
         ./../../../system/security/shared
         ./../../../system/include 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ./../../../system/xmllib 
         ./../../../system/jlib 
         ./../../platform 
         ./../../clients 
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( wumanager ${SRCS} )
add_dependencies ( wumanager espscm )
#install ( TARGETS wumanager RUNTIME DESTINATION ${EXEC_DIR} ) # do not install - this is an example program

target_link_libraries ( wumanager 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         xmllib 
         esphttp
    )

IF (USE_OPENSSL)
    target_link_libraries ( wumanager
    	securesocket
    )
ENDIF()

