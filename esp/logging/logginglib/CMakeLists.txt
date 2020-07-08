################################################################################
#    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.
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
# Component: logginglib

project( logginglib )

include(${HPCC_SOURCE_DIR}/esp/scm/espscm.cmake)

include_directories (
    ${HPCC_SOURCE_DIR}/system/include
    ${HPCC_SOURCE_DIR}/system/jlib
    ${HPCC_SOURCE_DIR}/system/xmllib
    ${HPCC_SOURCE_DIR}/system/security/shared       #seclib.hpp in generated/ws_loggingservice_esp.ipp
    ${HPCC_SOURCE_DIR}/esp/platform                 #EspCoreErrors
    ${HPCC_SOURCE_DIR}/esp/bindings                 #mime.hpp in generated/ws_loggingservice_esp.ipp
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp        #XmlPullParser.h in generated/ws_loggingservice_esp.ipp
    ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/Platform   #CSoapRequestBinding
    ${HPCC_SOURCE_DIR}/esp/clients                  #edwin.h in generated/ws_loggingservice.esp
    ${HPCC_SOURCE_DIR}/common/thorhelper
)

ADD_DEFINITIONS ( -D_USRDLL -DLOGGINGCOMMON_EXPORTS )

set ( SRCS
    ${ESPSCM_GENERATED_DIR}/ws_loggingservice_esp.cpp
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib/datafieldmap.cpp
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib/compressutil.cpp
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib/loggingagentbase.cpp
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib/LogSerializer.cpp
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib/LogFailSafe.cpp
    ${HPCC_SOURCE_DIR}/esp/logging/logginglib/logthread.cpp
)

HPCC_ADD_LIBRARY( logginglib SHARED ${SRCS} )

install ( TARGETS logginglib RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

add_dependencies ( logginglib jlib espscm)

target_link_libraries ( logginglib
    jlib
    xmllib
    esphttp
)
