################################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
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

# Component: PrometheusReporterService 

#####################################################
# Description:
# ------------
#    Cmake Input File for PrometheusReporterService
#####################################################


project( PrometheusReporterService ) 

set (    SRCS 
         PrometheusReporterService.cpp 
    )

include_directories (
          ${HPCC_SOURCE_DIR}/system/jlib 
          ${HPCC_SOURCE_DIR}/system/include 
          ${HPCC_SOURCE_DIR}/system/security/LdapSecurity
          ${HPCC_SOURCE_DIR}/system/security/shared
          ${HPCC_SOURCE_DIR}/system/security/securesocket/
          ${HPCC_SOURCE_DIR}/dali/base
          ${HPCC_SOURCE_DIR}/system/mp

          ${CMAKE_BINARY_DIR}/generated
          ${HPCC_SOURCE_DIR}/esp/bindings/http/platform # http processing
          ${HPCC_SOURCE_DIR}/esp/platform 
          ${HPCC_SOURCE_DIR}/esp/bindings 
          ${HPCC_SOURCE_DIR}/esp/clients 
          ${HPCC_SOURCE_DIR}/esp/bindings/SOAP/xpp
          ${HPCC_SOURCE_DIR}/system/xmllib
    )

ADD_DEFINITIONS( -D_USRDLL )
HPCC_ADD_EXECUTABLE ( PrometheusReporterService ${SRCS} )
add_dependencies( PrometheusReporterService espscm )
#HPCC_ADD_LIBRARY ( PrometheusReporterService ${SRCS} )
#install ( TARGETS PrometheusReporterService RUNTIME DESTINATION ${EXEC_DIR} LIBRARY DESTINATION ${LIB_DIR} )

target_link_libraries ( PrometheusReporterService 
         jlib
         LdapSecurity 
         esphttp
    )

        install(TARGETS PrometheusReporterService
        RUNTIME DESTINATION ${EXEC_DIR}
        LIBRARY DESTINATION ${LIB_DIR}
        COMPONENT Runtime
        )