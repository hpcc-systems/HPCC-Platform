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

if(NOT WSSQL_SERVICE)
    set(WSSQL_SERVICE_BLOCK_S "<!-- Built with -DWSSQL_SERVICE=OFF ")
    set(WSSQL_SERVICE_BLOCK_E "-->")
endif(NOT WSSQL_SERVICE)

if(NOT "${CUSTOM_PACKAGE_SUFFIX}" STREQUAL "")
    set(version_file_prefix "${CUSTOM_PACKAGE_SUFFIX}_")
endif()

configure_file("version.in" "version")
configure_file("environment.conf.in" "environment.conf" IMMEDIATE)
configure_file("environment.xml.in" "environment.xml" IMMEDIATE)
configure_file("genenvrules.conf.in" "genenvrules.conf" @ONLY)
foreach(iFILES
        ${CMAKE_CURRENT_BINARY_DIR}/environment.conf
        ${CMAKE_CURRENT_BINARY_DIR}/version)
    install(FILES ${iFILES}  DESTINATION etc/${DIR_NAME} COMPONENT Runtime)
endforeach(iFILES)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.xml DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/environment.conf DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/genenvrules.conf DESTINATION etc/${DIR_NAME}/rpmnew COMPONENT Runtime)

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

    add_custom_command(OUTPUT ${CONFIG_DIR}/genenvrules.conf
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/genenvrules.conf ${CONFIG_DIR}
        DEPENDS configure_directories ${CMAKE_CURRENT_BINARY_DIR}/genenvrules.conf)
    add_custom_target(configure_genenvrules_conf
        DEPENDS ${CONFIG_DIR}/genenvrules.conf)

    add_custom_command(OUTPUT ${INIT_PATH}/dafilesrv
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/../../bash/etc/init.d/dafilesrv ${INIT_PATH}/dafilesrv
        DEPENDS configure_directories
        ProcessFiles-initfiles-bash-etc-init.d
        ${CMAKE_CURRENT_BINARY_DIR}/../../bash/etc/init.d/dafilesrv)
    add_custom_command(OUTPUT ${INIT_PATH}/hpcc-init
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/../../bash/etc/init.d/hpcc-init ${INIT_PATH}/hpcc-init
        DEPENDS configure_directories
        ProcessFiles-initfiles-bash-etc-init.d
        ${CMAKE_CURRENT_BINARY_DIR}/../../bash/etc/init.d/hpcc-init)
    add_custom_target(configure_service_files
        DEPENDS ${INIT_PATH}/hpcc-init
        ${INIT_PATH}/dafilesrv)


    install(CODE "
    if(NOT EXISTS ${CONFIG_DIR}/${ENV_XML_FILE})
        execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/${ENV_XML_FILE} ${CONFIG_DIR})
        message(STATUS \"Copying ${ENV_XML_FILE} to ${CONFIG_DIR}\")
    else()
        message(STATUS \"Copy of ${ENV_XML_FILE} already exists at ${CONFIG_DIR}\")
    endif()
    if(NOT EXISTS ${CONFIG_DIR}/${ENV_CONF_FILE})
        execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/${ENV_CONF_FILE} ${CONFIG_DIR})
        message(STATUS \"Copying ${ENV_CONF_FILE} to ${CONFIG_DIR}\")
    else()
        message(STATUS \"Copy of ${ENV_CONF_FILE} already exists at ${CONFIG_DIR}\")
    endif()
    " COMPONENT Runtime)

    add_custom_target(configure ALL
        DEPENDS configure_directories
        configure_genenvrules_conf
        configure_service_files
        COMMENT "Configuring ${DESTDIR} for deployment")

endif()
