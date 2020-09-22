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


# Component: ws_fileio 
#####################################################
# Description:
# ------------
#    Cmake Input File for ws_fileio
#####################################################

project( ws_fileio ) 

include(${HPCC_SOURCE_DIR}/esp/scm/additional.cmake)

set (    SRCS 
         ${ESPSCM_GENERATED_DIR}/ws_fileio_esp.cpp 
         ws_fileioplugin.cpp 
         ws_fileioservice.cpp 
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
         ./../../../rtl/include 
         ./../../../common/dllserver 
         ./../../bindings 
         ./../../smc/SMCLib 
         ./../../bindings/SOAP/xpp 
         ${HPCC_SOURCE_DIR}/common/thorhelper
    )

ADD_DEFINITIONS( -D_USRDLL )

HPCC_ADD_LIBRARY( ws_fileio SHARED ${SRCS} )
install ( TARGETS ws_fileio RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
add_dependencies (ws_fileio espscm)
target_link_libraries ( ws_fileio 
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
