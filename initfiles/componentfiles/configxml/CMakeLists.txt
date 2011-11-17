################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################

CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/dropzone.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/dropzone.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/eclagent_config.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/eclagent_config.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/eclagent_config.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/eclagent_config.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/espsmcservice.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/espsmcservice.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/ftslave_linux.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/ftslave_linux.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/roxie.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/roxie.xsd)
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/thor.xsd.in ${CMAKE_CURRENT_BINARY_DIR}/thor.xsd)

if ( ${HPCC_PROJECT} STREQUAL "community" )
    configure_file("buildsetCC.xml.in" "buildset.xml")
endif()

FOREACH( iFILES
    ${CMAKE_CURRENT_BINARY_DIR}/dropzone.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/eclagent_config.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/eclagent_config.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/espsmcservice.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/ftslave_linux.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/roxie.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/thor.xsd
    ${CMAKE_CURRENT_BINARY_DIR}/buildset.xml
    ${CMAKE_CURRENT_SOURCE_DIR}/thor.xsl
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
    ${CMAKE_CURRENT_SOURCE_DIR}/Environment.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_account.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_ecldirect.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl2.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/ftslave.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/GABConfig.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/generic.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/installset.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/ldapserver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/mysqlserver.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/plugin.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/regress.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/sasha.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/SiteCertificate.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/soapplus.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/topology.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/WsDeploy.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/cgencomplist_linux.xml
    ${CMAKE_CURRENT_SOURCE_DIR}/cgencomplist_win.xml
    ${CMAKE_CURRENT_SOURCE_DIR}/esp.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dali.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuplus.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/dfuserver.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/agentexec.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclscheduler.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclccserver.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclplus.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/RoxieTopology.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/slaves.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/roxievars_linux.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/sasha.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/eclcc.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/validateAll.xsl
)
    Install ( FILES ${iFILES} DESTINATION ${OSSDIR}/componentfiles/configxml COMPONENT Runtime)
ENDFOREACH ( iFILES )

add_subdirectory (@temp)
