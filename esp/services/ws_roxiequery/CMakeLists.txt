################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################


# Component: ws_roxiequery 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_roxiequery
#####################################################

project( ws_roxiequery ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_roxiequery_esp.cpp 
         ws_roxiequeryplugin.cpp 
         ws_roxiequeryservice.cpp 
         
         ws_roxiequeryservice.cpp
         
         ${HPCC_SOURCE_DIR}/esp/scm/ws_roxiequery.ecm
    )

include_directories ( 
         ./../../../dali/dfu 
         ./../../../system/mp 
         ./../../platform 
         ./../../../system/jlib 
         ./../../../common/environment 
         ./../../services 
         ./../../../dali/ft 
         ./../common 
         ./../../../system/xmllib 
         ./../../../esp/bindings/http/platform 
         ./../../../system/security/securesocket 
         ./../../../system/security/shared 
         ./../../../system/include 
         ./../../../common/workunit 
         ./../../../common/remote 
         ./../../clients 
         ./../../../esp/esplib 
         ./../../../dali/base 
         ./../../bindings/SOAP/scrubbed 
         ./../ws_workunits 
         ./../../../rtl/include 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
         ./../../../common/fileview2 
         ./../../../rtl/eclrtl 
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_roxiequery SHARED ${SRCS} )
add_dependencies ( ws_roxiequery espscm )
install ( TARGETS ws_roxiequery RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( ws_roxiequery 
         jlib
         fileview2
         remote 
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
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
         roxiecommlib 
         roxiemanager
    )

