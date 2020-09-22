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


# Component: ws_config 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_config
#####################################################

project( ws_config ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_config_esp.cpp 
         ws_configPlugin.cpp 
         ws_configService.cpp 
    )

include_directories ( 
         ./../../esplib 
         ./../.. 
         ./../../platform 
         ./../../../system/jlib 
         ./../../services 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../system/xmllib
         ./../../../system/mp 
         ./../../../common/environment
         ./../../clients 
         ./../../../dali/base 
         ./../../bindings 
         ./../../bindings/SOAP/xpp 
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_config SHARED ${SRCS} )
install ( TARGETS ws_config RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} COMPONENT Runtime )
add_dependencies (ws_config espscm)
target_link_libraries ( ws_config 
         jlib
         mp 
         hrpc 
         remote 
         dalibase 
         environment 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         xmllib 
         esphttp 
    )


