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

SET(outFiles)
GENERATE_BASH(processor ${bash-vars} "init_dafilesrv.in" outFiles)
GENERATE_BASH(processor ${bash-vars} "init_eclagent.in" outFiles)
ADD_CUSTOM_TARGET(ProcessFiles-initfiles-bin ALL DEPENDS ${outFiles})

FOREACH( iFILES
    ${outFiles}
    ${CMAKE_CURRENT_SOURCE_DIR}/init_configesp
    ${CMAKE_CURRENT_SOURCE_DIR}/init_dali
    ${CMAKE_CURRENT_SOURCE_DIR}/init_dfuserver
    ${CMAKE_CURRENT_SOURCE_DIR}/init_eclccserver
    ${CMAKE_CURRENT_SOURCE_DIR}/init_eclscheduler
    ${CMAKE_CURRENT_SOURCE_DIR}/init_esp
    ${CMAKE_CURRENT_SOURCE_DIR}/init_ftslave
    ${CMAKE_CURRENT_SOURCE_DIR}/init_roxie
    ${CMAKE_CURRENT_SOURCE_DIR}/init_roxie_cluster
    ${CMAKE_CURRENT_SOURCE_DIR}/init_sasha
    ${CMAKE_CURRENT_SOURCE_DIR}/init_thor
    ${CMAKE_CURRENT_SOURCE_DIR}/init_thorslave
)
    install ( PROGRAMS ${iFILES} DESTINATION ${EXEC_DIR} COMPONENT Runtime )
ENDFOREACH ( iFILES )
