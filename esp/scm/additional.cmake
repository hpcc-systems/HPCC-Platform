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

#####################################################
# Description:
# ------------
#    Cmake include file additional scm file generation
#####################################################

set ( ESPSCM_SOURCE_DIR ${HPCC_SOURCE_DIR}/esp/scm )
set ( ESPSCM_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated )

set ( ESPSCM_SRCS
      ws_config.ecm
      ws_fileio.ecm
      ws_account.ecm
      ws_sql.ecm
      ws_store.ecm
      ldapenvironment.ecm
      ##### LIST FOR ESPECL
      WsDeploy.ecm
    )

foreach ( loop_var ${ESPSCM_SRCS} )
    string(  REGEX REPLACE "[.]ecm" "" result ${loop_var} )
    if (SCM_BUILD)
      add_custom_command ( DEPENDS hidl ${ESPSCM_SOURCE_DIR}/${loop_var}
                           OUTPUT ${ESPSCM_GENERATED_DIR}/${result}.esp ${ESPSCM_GENERATED_DIR}/${result}.hpp ${ESPSCM_GENERATED_DIR}/${result}.int ${ESPSCM_GENERATED_DIR}/${result}.ipp ${ESPSCM_GENERATED_DIR}/${result}_esp.cpp ${ESPSCM_GENERATED_DIR}/${result}_esp.ipp
                           COMMAND $<TARGET_FILE:hidl> ${ESPSCM_SOURCE_DIR}/${result}.ecm ${ESPSCM_GENERATED_DIR}
                         )
      add_custom_command ( DEPENDS esdl-xml ${ESPSCM_SOURCE_DIR}/${loop_var}
                           OUTPUT ${ESPSCM_GENERATED_DIR}/${result}.xml
                           COMMAND $<TARGET_FILE:esdl-xml> ${ESPSCM_SOURCE_DIR}/${result}.ecm ${ESPSCM_GENERATED_DIR}
                         )
    endif ()
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.esp PROPERTIES GENERATED TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.hpp PROPERTIES GENERATED TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.int PROPERTIES GENERATED TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.ipp PROPERTIES GENERATED TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}_esp.cpp PROPERTIES GENERATED TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}_esp.ipp PROPERTIES GENERATED TRUE)
    set_source_files_properties(${ESPSCM_GENERATED_DIR}/${result}.xml PROPERTIES GENERATED TRUE)
    set ( ESP_GENERATED_INCLUDES ${ESP_GENERATED_INCLUDES} ${ESPSCM_GENERATED_DIR}/${result}.esp ${ESPSCM_GENERATED_DIR}/${result}.hpp ${ESPSCM_GENERATED_DIR}/${result}.int ${ESPSCM_GENERATED_DIR}/${result}.ipp ${ESPSCM_GENERATED_DIR}/${result}_esp.ipp ${ESPSCM_GENERATED_DIR}/${result}.xml )
endforeach ( loop_var ${ESPSCM_SRCS} )

include_directories ( ${ESPSCM_GENERATED_DIR} )
