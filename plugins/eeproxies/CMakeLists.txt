###############################################################################
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

set (proxies_out_dir "plugins")

FOREACH ( iFILES
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_addrclean.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_bridgerscore.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_canadaclean.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/_cmslib.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_datalib.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_date.eclmod
    ${CMAKE_CURRENT_SOURCE_DIR}/dms.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_entityscore.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_fingerprintlib.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_hktlib.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_namelib.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_ofac.ecllib
    ${CMAKE_CURRENT_SOURCE_DIR}/lib_ziplib.ecllib
)
    install ( FILES ${iFILES} DESTINATION ${proxies_out_dir} COMPONENT Runtime )
ENDFOREACH ( iFILES )
