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

# Only register ws_sql2 when its plugin is built, otherwise the ESP fails to load it
if (WSSQL2_SERVICE)
    set ( WSSQL2_APPLICATION_SERVICE "   - ws_sql2" )
    set ( WSSQL2_PLUGIN_ENTRY "  ws_sql2: ws_sql2" )
else ()
    set ( WSSQL2_APPLICATION_SERVICE "" )
    set ( WSSQL2_PLUGIN_ENTRY "" )
endif ()

configure_file ( ${CMAKE_CURRENT_SOURCE_DIR}/application.yaml.in ${CMAKE_CURRENT_BINARY_DIR}/application.yaml @ONLY )
configure_file ( ${CMAKE_CURRENT_SOURCE_DIR}/plugins.yaml.in ${CMAKE_CURRENT_BINARY_DIR}/plugins.yaml @ONLY )

set ( ESP_APPLICATION_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/esp.yaml
    ${CMAKE_CURRENT_BINARY_DIR}/application.yaml
    ${CMAKE_CURRENT_SOURCE_DIR}/ldap_authorization_map.yaml
    ${CMAKE_CURRENT_BINARY_DIR}/plugins.yaml
)

FOREACH( iFile ${ESP_APPLICATION_FILES} )
    Install( FILES ${iFile} DESTINATION componentfiles/applications/sql2ecl COMPONENT Runtime )
ENDFOREACH ( iFile )
