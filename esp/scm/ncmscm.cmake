################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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

#####################################################
# Description:
# ------------
#    Cmake include file ncm scm file generation
#####################################################

set ( ESPSCM_SOURCE_DIR ${HPCC_SOURCE_DIR}/esp/scm )
set ( ESPSCM_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated )
GET_TARGET_PROPERTY(HIDL_EXE hidl LOCATION)
GET_TARGET_PROPERTY(ESDL-XML_EXE esdl-xml LOCATION)

set ( ECMCSM_SRCS 
      ws_riskwise.ncm
      ws_riskwise_wsidentity.ncm
      wsm_riskwise1.ncm
      wsm_riskwise2.ncm
      wsm_riskwise3.ncm
    )

foreach ( loop_var ${ESPSCM_SRCS} )
    string(  REGEX REPLACE "[.]ncm" "" result ${loop_var} )
    add_custom_command ( DEPENDS hidl ${ESPSCM_SOURCE_DIR}/${loop_var}
                         OUTPUT ${ESPSCM_GENERATED_DIR}/${result}.esp ${ESPSCM_GENERATED_DIR}/${result}.hpp ${ESPSCM_GENERATED_DIR}/${result}.int ${ESPSCM_GENERATED_DIR}/${result}.ipp ${ESPSCM_GENERATED_DIR}/${result}_esp.cpp ${ESPSCM_GENERATED_DIR}/${result}_esp.ipp ${ESPSCM_GENERATED_DIR}/${result}_esp_ng.cpp ${ESPSCM_GENERATED_DIR}/${result}_esp_ng.ipp
                         COMMAND ${HIDL_EXE} ${ESPSCM_SOURCE_DIR}/${result}.ncm ${ESPSCM_GENERATED_DIR}
                       )
    add_custom_command ( DEPENDS esdl-xml ${ESPSCM_SOURCE_DIR}/${loop_var}
                         OUTPUT ${ESPSCM_GENERATED_DIR}/${result}.xml 
                         COMMAND ${ESDL-XML_EXE} ${ESPSCM_SOURCE_DIR}/${result}.ecm ${ESPSCM_GENERATED_DIR}
                       )
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.esp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.hpp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.int PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.ipp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}_esp.cpp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}_esp.ipp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}_esp_ng.cpp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}_esp_ng.ipp PROPERTIES ESPSCM_GENERATED_DIR TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.xml PROPERTIES ESPSCM_GENERATED_DIR TRUE)
endforeach ( loop_var ${ESPECM_SRCS} )

include_directories ( ${ESPSCM_GENERATED_DIR} )
