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

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wsecl_onchange.xslt
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/plugins.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_WsSMC.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/roxiePlugins.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_DynamicESDL.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_logging_transid.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_logging_agent_basic.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wslogging.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/decoupled_logging.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/logging_agent.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/wslogserviceespagent.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wssql.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsstore.xsl
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_decoupledlogging.xsl
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/configxml/@temp COMPONENT Runtime )
ENDFOREACH ( iFILES )
