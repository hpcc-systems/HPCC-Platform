################################################################################
#    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
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


# Component: wsdfuaccess

#####################################################
# Description:
# ------------
#    Cmake Input File for wsdfuaccess
#####################################################


project( wsdfuaccess ) 

include(${HPCC_SOURCE_DIR}/esp/scm/smcscm.cmake)

set (    SRCS 
         wsdfuaccess.cpp 
         ${ESPSCM_GENERATED_DIR}/ws_dfu_common_esp.cpp
         ${ESPSCM_GENERATED_DIR}/ws_dfu_esp.cpp
    )

include_directories ( 
         ${HPCC_SOURCE_DIR}/system/include 
         ${HPCC_SOURCE_DIR}/system/xmllib
         ${HPCC_SOURCE_DIR}/system/security/shared
         ${HPCC_SOURCE_DIR}/system/security/securesocket
         ${HPCC_SOURCE_DIR}/system/security/cryptohelper
         ${HPCC_SOURCE_DIR}/fs/dafsclient
         ${HPCC_SOURCE_DIR}/fs/dafsstream
         ${HPCC_SOURCE_DIR}/fs/dafsserver
         ${HPCC_SOURCE_DIR}/common/environment
         ${HPCC_SOURCE_DIR}/common/thorhelper
         ${HPCC_SOURCE_DIR}/rtl/include
         ${HPCC_SOURCE_DIR}/rtl/eclrtl
         ${HPCC_SOURCE_DIR}/dali/base
         ${HPCC_SOURCE_DIR}/system/mp
         ${HPCC_SOURCE_DIR}/esp/bindings
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/client
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
         ${HPCC_SOURCE_DIR}/system/jlib
         ${HPCC_SOURCE_DIR}/esp/platform
         ${HPCC_SOURCE_DIR}/esp/clients
         ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/Platform
         ${HPCC_SOURCE_DIR}/esp/smc/SMCLib
         ${HPCC_SOURCE_DIR}/testing/unittests
    )

ADD_DEFINITIONS( -D_USRDLL -DWSDFUACCESS_EXPORTS )

HPCC_ADD_LIBRARY( wsdfuaccess SHARED ${SRCS} )
install ( TARGETS wsdfuaccess RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )
target_link_libraries ( wsdfuaccess 
         jlib
         xmllib 
         esphttp
         dafsclient
         dafsserver
         dafsstream
         thorhelper
         dalibase
         environment
         ${CPPUNIT_LIBRARIES}
    )


