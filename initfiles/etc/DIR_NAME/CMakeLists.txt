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
foreach(iFILES
    ${CMAKE_CURRENT_BINARY_DIR}/environment.conf
    ${CMAKE_CURRENT_BINARY_DIR}/version)
    install(FILES ${iFILES}  DESTINATION etc/${DIR_NAME} COMPONENT Runtime)
endforeach(iFILES)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.xml DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.conf DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/genenvrules.conf DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime)

add_subdirectory(configmgr)
add_subdirectory(config2mgr)

# Developer build
# target: configure
if(NOT "${DESTDIR}" STREQUAL "")
    message(STATUS "DESTDIR is set to ${DESTDIR}. Enabling configure target.")

    set(dirlist ${RUNTIME_PATH} ${LOG_PATH} ${LOCK_PATH} ${PID_PATH} ${INIT_PATH} ${CONFIG_DIR})
    foreach(dir ${dirlist})
        add_custom_command(OUTPUT ${dir}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${dir}
            COMMENT "creating ${dir}")
    endforeach()
    add_custom_target(configure_directories
        DEPENDS ${dirlist})

    add_custom_command(OUTPUT ${CONFIG_DIR}/${ENV_XML_FILE}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/${ENV_XML_FILE} ${CONFIG_DIR}
        DEPENDS configure_directories ${CMAKE_CURRENT_BINARY_DIR}/environment.xml)
    add_custom_target(configure_environment_xml
        DEPENDS ${CONFIG_DIR}/${ENV_XML_FILE})

    add_custom_command(OUTPUT ${CONFIG_DIR}/${ENV_CONF_FILE}
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/${ENV_CONF_FILE} ${CONFIG_DIR}
        DEPENDS configure_directories ${CMAKE_CURRENT_BINARY_DIR}/environment.conf)
    add_custom_target(configure_environment_conf
        DEPENDS ${CONFIG_DIR}/${ENV_CONF_FILE})
    
    add_custom_command(OUTPUT ${CONFIG_DIR}/genenvrules.conf
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/genenvrules.conf ${CONFIG_DIR}
        DEPENDS configure_directories ${CMAKE_CURRENT_SOURCE_DIR}/genenvrules.conf)
    add_custom_target(configure_genenvrules_conf
        DEPENDS ${CONFIG_DIR}/genenvrules.conf)
   
    add_custom_command(OUTPUT ${INIT_PATH}/dafilesrv
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/initfiles/bash/etc/init.d/dafilesrv ${INIT_PATH}/dafilesrv
        DEPENDS configure_directories
                ProcessFiles-initfiles-bash-etc-init.d
                ${CMAKE_BINARY_DIR}/initfiles/bash/etc/init.d/dafilesrv)
    add_custom_command(OUTPUT ${INIT_PATH}/hpcc-init
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/initfiles/bash/etc/init.d/hpcc-init ${INIT_PATH}/hpcc-init
        DEPENDS configure_directories
                ProcessFiles-initfiles-bash-etc-init.d
                ${CMAKE_BINARY_DIR}/initfiles/bash/etc/init.d/hpcc-init)
    add_custom_target(configure_service_files
        DEPENDS ${INIT_PATH}/hpcc-init
                ${INIT_PATH}/dafilesrv)

    add_custom_target(configure ALL
        DEPENDS configure_directories
                configure_environment_xml
                configure_environment_conf
                configure_genenvrules_conf
                configure_service_files
        COMMENT "Configuring ${DESTDIR} for deployment")
endif()
