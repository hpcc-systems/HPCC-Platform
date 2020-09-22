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


# Component: eclplus 
#####################################################
# Description:
# ------------
#    Cmake Input File for eclplus
#####################################################

project( eclplus ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         ../../esp/bindings/bindutil.cpp 
         ${ESPSCM_GENERATED_DIR}/common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_struct_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_req_resp_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_queryset_req_resp_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_workunits_esp.cpp 
         DeleteHelper.cpp 
         DumpHelper.cpp 
         eclplus.cpp 
         formattypes.cpp 
         ListHelper.cpp 
         main.cpp 
         QueryHelper.cpp 
         ViewHelper.cpp 
    )

include_directories ( 
         ./../../system/security/shared
         ./../../system/mp 
         ./../../common/dllserver 
         ./../../common/fileview2 
         ./../../common/workunit 
         ./../../esp/clients 
         ./../../common/environment 
         ./../../esp/bindings/SOAP/xpp 
         ./../../system/include 
         ./../../esp/bindings 
         ./../../ecl/hql 
         ./../../dali/base 
         ./../../esp/platform 
         ./../../system/jlib 
         ./../../system/xmllib 
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -DECLPLUS_EXPORTS -D_CONSOLE )

HPCC_ADD_EXECUTABLE ( eclplus ${SRCS} )
add_dependencies ( eclplus espscm )
install ( TARGETS eclplus RUNTIME DESTINATION ${EXEC_DIR} )
target_link_libraries (  eclplus 
         jlib
         esphttp 
    )

