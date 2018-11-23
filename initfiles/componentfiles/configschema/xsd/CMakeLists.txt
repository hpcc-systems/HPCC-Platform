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

FOREACH( xsdFile
    esp.xsd
    esp_service_smc.xsd
    hardware.xsd
    types.xsd
    dafilesrv.xsd
    dali.xsd
    environment.xsd
    secmgr_singleuser.xsd
    buildset.xml
    roxie.xsd
    )
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/${xsdFile} ${CMAKE_CURRENT_BINARY_DIR}/${xsdFile})
    Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/${xsdFile} DESTINATION componentfiles/configschema/xsd COMPONENT Runtime)
ENDFOREACH ( xsdFile )
