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

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/dropzone.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/dropzone.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/eclagent_config.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/eclagent_config.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/esp.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/esp.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/SiteCertificate.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/SiteCertificate.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/espsmcservice.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/espsmcservice.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/ftslave_linux.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/ftslave_linux.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/roxie.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/roxie.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/RoxieTopology.xsl ${CMAKE_CURRENT_BINARY_DIR}/RoxieTopology.xsl)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/thor.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/thor.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/thor.xsl ${CMAKE_CURRENT_BINARY_DIR}/thor.xsl)

configure_file(environment.xsd.in environment.xsd @ONLY)
configure_file(cgencomplist_linux.xml.in cgencomplist_linux.xml @ONLY)
configure_file(cgencomplist_win.xml.in cgencomplist_win.xml @ONLY)
configure_file(toposerver.xsd.in toposerver.xsd @ONLY)

if("${HPCC_PROJECT_ABBR}" STREQUAL "CC")
    configure_file("buildset${HPCC_PROJECT_ABBR}.xml.in" "buildset.xml")
    install(FILES ${CMAKE_CURRENT_BINARY_DIR}/buildset.xml DESTINATION componentfiles/configxml COMPONENT Runtime)
endif()

FOREACH( iFILES
    ${CMAKE_CURRENT_BINARY_DIR}/dropzone.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/eclagent_config.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/esp.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/espsmcservice.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/ftslave_linux.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/roxie.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/RoxieTopology.xsl
    ${CMAKE_CURRENT_BINARY_DIR}/thor.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/thor.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/setvars_linux.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/custom_plugin.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/dafilesrv.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/dali.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuplus.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuserver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/DhcpServer.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/directories.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/eclplus.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/eclcc.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/eclccserver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/eclscheduler.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/environment.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_account.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl2.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wssql.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsstore.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/GABConfig.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/generic.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/installset.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/ldapserver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/ldapserver.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/mysqlserver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/plugin.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/regress.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/sasha.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/SiteCertificate.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/soapplus.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/topology.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/WsDeploy.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/cgencomplist_linux.xml
    ${CMAKE_CURRENT_BINARY_DIR}/cgencomplist_win.xml
    ${CMAKE_CURRENT_SOURCE_DIR}/esp.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dali.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuplus.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuserver.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/agentexec.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclscheduler.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclccserver.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclplus.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/slaves.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/roxievars_linux.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/sasha.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclcc.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/validateAll.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esdlsvcengine.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/loggingmanager.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_decoupledlogging.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esploggingagent.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/wslogging.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/cassandraloggingagent.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/modularlogagent.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/daliplugin.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/backupnode.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/backupnode_vars.xsl
    ${CMAKE_CURRENT_BINARY_DIR}/toposerver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/toposerver.xsl
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/configxml COMPONENT Runtime)
ENDFOREACH ( iFILES )

add_subdirectory (@temp)
