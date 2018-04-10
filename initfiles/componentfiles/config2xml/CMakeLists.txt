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


FOREACH( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/esp.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_smc.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/esp_service_wsecl2.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/hardware.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/types.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/dafilesrv.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/dali.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/environment.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/eclagent.xsd
    ${CMAKE_CURRENT_SOURCE_DIR}/buildset.xml
)
    Install ( FILES ${iFILES} DESTINATION componentfiles/config2xml COMPONENT Runtime)
ENDFOREACH ( iFILES )
