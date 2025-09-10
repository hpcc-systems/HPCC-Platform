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


# Component: ws_ecl_client

#####################################################
# Description:
# ------------
#    Cmake Input File for ws_ecl_client
#####################################################


project( ws_ecl_client )

set (    SRCS
         ../edwin.cpp
         ws_ecl_client.cpp
    )

include_directories (
         ./../../../system/include
         ./../../../system/xmllib
         ./../../../system/security/shared
         ./../../../system/security/securesocket
         ./../../bindings
         ./..
         ./../../bindings/SOAP/client
         ./../../bindings/SOAP/xpp
         ./../../../system/jlib
         ./../../platform
         ./../../bindings/SOAP/Platform
         ./../../bindings/SOAP/ws_ecl_client
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ./
    )

ADD_DEFINITIONS( -D_USRDLL -DWS_ECL_CLIENT_EXPORTS )

HPCC_ADD_LIBRARY( ws_ecl_client SHARED ${SRCS} )
install ( TARGETS ws_ecl_client RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries (  ws_ecl_client
         ${XALAN_LIBRARIES} ${XERCES_LIBRARIES}
         jlib
         xmllib
         esphttp
    )
