################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.
#
#    All rights reserved. This program is free software: you can redistribute it
#    and/or modify
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
configure_file("version.in" "version")
configure_file("environment.conf.in" "environment.conf" IMMEDIATE)
configure_file("environment.xml.in" "environment.xml" IMMEDIATE)
FOREACH( iFILES
    ${CMAKE_CURRENT_BINARY_DIR}/environment.conf
    ${CMAKE_CURRENT_BINARY_DIR}/version
)
    Install ( FILES ${iFILES}  DESTINATION etc/${DIR_NAME} COMPONENT Runtime )
ENDFOREACH ( iFILES)

Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.xml DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime )
Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.conf DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime )
Install ( FILES ${CMAKE_CURRENT_SOURCE_DIR}/genenvrules.conf DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime )
Install ( FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.xml DESTINATION ${CONFIG_PREFIX}/${DIR_NAME}/${CONFIG_SOURCE_DIR} )

ADD_SUBDIRECTORY(configmgr)

if(NOT "${DESTDIR}" STREQUAL "")
    message(STATUS "DESTDIR is set to ${DESTDIR}. Enabling configure target.")
    add_custom_command(OUTPUT 
        ${HOME_DIR}/${RUNTIME_USER}/.ssh/authorized_keys
        ${HOME_DIR}/${RUNTIME_USER}/.ssh/id_rsa.pub
        ${HOME_DIR}/${RUNTIME_USER}/.ssh/id_rsa
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/configure_keys.sh
        COMMENT "Attempting to generate ssh keys if not present"
        WORKING_DIRECTORY ${HOME_DIR}/${RUNTIME_USER}/.ssh VERBATIM)
    add_custom_target(configure
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CONFIG_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/${ENV_XML_FILE} ${CONFIG_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/${ENV_CONF_FILE} ${CONFIG_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/genenvrules.conf ${CONFIG_DIR}
        DEPENDS ${HOME_DIR}/${RUNTIME_USER}/.ssh/authorized_keys
        ${HOME_DIR}/${RUNTIME_USER}/.ssh/id_rsa.pub
        ${HOME_DIR}/${RUNTIME_USER}/.ssh/id_rsa)
endif()
