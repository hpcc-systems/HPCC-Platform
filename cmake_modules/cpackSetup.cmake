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

###
## CPack install and packaging setup.
###
INCLUDE(InstallRequiredSystemLibraries)
if ( PLATFORM )
    set(CPACK_PACKAGE_NAME "hpccsystems-platform")
elseif( CLIENTTOOLS )
    set(CPACK_PACKAGE_NAME "hpccsystems-clienttools")
else()
    set(CPACK_PACKAGE_NAME "${CMAKE_PROJECT_NAME}")
endif()
SET(CPACK_PACKAGE_VERSION_MAJOR ${majorver})
SET(CPACK_PACKAGE_VERSION_MINOR ${minorver})
SET(CPACK_PACKAGE_VERSION_PATCH ${point}${stagever})
set ( CPACK_PACKAGE_CONTACT "HPCCSystems <ossdevelopment@lexisnexis.com>" )
set( CPACK_SOURCE_GENERATOR TGZ )
set ( CPACK_RPM_PACKAGE_VERSION "${projname}")
SET(CPACK_RPM_PACKAGE_RELEASE "${version}${stagever}")
if ( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "x86_64")
else( ${ARCH64BIT} EQUAL 1 )
    set ( CPACK_RPM_PACKAGE_ARCHITECTURE "i386")
endif ( ${ARCH64BIT} EQUAL 1 )
set(CPACK_SYSTEM_NAME "${CMAKE_SYSTEM_NAME}-${CPACK_RPM_PACKAGE_ARCHITECTURE}")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Release")
    set(CPACK_STRIP_FILES TRUE)
endif()
if ( CMAKE_SYSTEM MATCHES Linux )
    EXECUTE_PROCESS (
                COMMAND ${CMAKE_MODULE_PATH}/distrocheck.sh
                    OUTPUT_VARIABLE packageManagement
                        ERROR_VARIABLE  packageManagement
                )
    EXECUTE_PROCESS (
                COMMAND ${CMAKE_MODULE_PATH}/getpackagerevisionarch.sh
                    OUTPUT_VARIABLE packageRevisionArch
                        ERROR_VARIABLE  packageRevisionArch
                )
    EXECUTE_PROCESS (
                COMMAND ${CMAKE_MODULE_PATH}/getpackagerevisionarch.sh --noarch
                    OUTPUT_VARIABLE packageRevision
                        ERROR_VARIABLE  packageRevision
                )

    message ( "-- Auto Detecting Packaging type")
    message ( "-- distro uses ${packageManagement}, revision is ${packageRevisionArch}" )

    if ( ${packageManagement} STREQUAL "DEB" )
        if ( DEVEL )
            set(CPACK_PACKAGE_NAME "hpccsystems-platform-dev")
        endif()
        set(CPACK_PACKAGE_FILE_NAME	"${CPACK_PACKAGE_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}${packageRevisionArch}")
    elseif ( ${packageManagement} STREQUAL "RPM" )
        if ( DEVEL )
            set(CPACK_PACKAGE_NAME "hpccsystems-platform-devel")
        endif()
        set(CPACK_PACKAGE_FILE_NAME	"${CPACK_PACKAGE_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}.${packageRevisionArch}")
        else()
        set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}_${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}${CPACK_SYSTEM_NAME}")
    endif ()
endif ( CMAKE_SYSTEM MATCHES Linux )
MESSAGE ("-- Current release version is ${CPACK_PACKAGE_FILE_NAME}")
set ( CPACK_SOURCE_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}" )
set( CPACK_SOURCE_GENERATOR TGZ )
set(CPACK_SOURCE_IGNORE_FILES
        "~$"
        "\\\\.cvsignore$"
        "^${PROJECT_SOURCE_DIR}.*/CVS/"
        "^${PROJECT_SOURCE_DIR}.*/.svn/"
        "^${PROJECT_SOURCE_DIR}.*/.git/"
        "^${PROJECT_SOURCE_DIR}/ln/"
        "^${PROJECT_SOURCE_DIR}/externals/"
        "^${PROJECT_SOURCE_DIR}.*/*.mk$"
        "^${PROJECT_SOURCE_DIR}/makefile$"
        "^${PROJECT_SOURCE_DIR}/make.common$"
        "^${PROJECT_SOURCE_DIR}/make.post$"
        "^${PROJECT_SOURCE_DIR}/build$"
        "^${PROJECT_SOURCE_DIR}/buildall$"
        "^${PROJECT_SOURCE_DIR}/lastbuilds$"
        "^${PROJECT_SOURCE_DIR}/imerge$"
        "^${PROJECT_SOURCE_DIR}/tmerge$"
        "^${PROJECT_SOURCE_DIR}/tmerge.bat$"
        "^${PROJECT_SOURCE_DIR}/tag$"
        "^${PROJECT_SOURCE_DIR}/tag_build$"
        "^${PROJECT_SOURCE_DIR}/old_tag$"
        "^${PROJECT_SOURCE_DIR}/ecl/regress/"
    "^${PROJECT_SOURCE_DIR}/testing/"
        )

###
## Run file configuration to set build tag along with install lines for generated
## config files.
###
set( BUILD_TAG "${CPACK_RPM_PACKAGE_VERSION}_${version}-${stagever}")
if (USE_GIT_DESCRIBE OR CHECK_GIT_TAG)
    FETCH_GIT_TAG (${CMAKE_SOURCE_DIR} ${CPACK_RPM_PACKAGE_VERSION} GIT_BUILD_TAG)
    message ("-- Git tag is '${GIT_BUILD_TAG}'")
    if (NOT "${GIT_BUILD_TAG}" STREQUAL "${BUILD_TAG}")
        if (CHECK_GIT_TAG)
            message(FATAL_ERROR "Git tag '${GIT_BUILD_TAG}' does not match source version '${BUILD_TAG}'" )
        else()
            if(NOT "${GIT_BUILD_TAG}" STREQUAL "") # probably means being built from a tarball...
                set( BUILD_TAG "${BUILD_TAG}[${GIT_BUILD_TAG}]")
            endif()
        endif()
    endif()
endif()
message ("-- Build tag is '${BUILD_TAG}'")
if (NOT "${BASE_BUILD_TAG}" STREQUAL "")
    set(BASE_BUILD_TAG "${BUILD_TAG}")
endif()
message ("-- Base build tag is '${BASE_BUILD_TAG}'")
if ( PLATFORM OR CLIENTTOOLS )
    configure_file(${HPCC_SOURCE_DIR}/build-config.h.cmake "build-config.h" )
endif()

#set( CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON )
#set( CPACK_DEB_PACKAGE_COMPONENT ON )


###
## CPack commands in this section require cpack 2.8.1 to function.
## When using cpack 2.8.1, the command "make package" will create
## an RPM.
###

if (NOT "${CMAKE_VERSION}" VERSION_LESS "2.8.7")
    if ( CMAKE_SYSTEM MATCHES Linux )
        if ( ${packageManagement} STREQUAL "DEB" )
                set ( CPACK_GENERATOR "${packageManagement}" )
                message("-- Will build DEB package")
                if ( PLATFORM OR CLIENTTOOLS )
                    message ("-- Packing BASH installation files")
                    set ( CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst;${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm;${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
                endif()
        elseif ( ${packageManagement} STREQUAL "RPM" )
            set ( CPACK_GENERATOR "${packageManagement}" )
            message("-- Will build RPM package")
            if ( PLATFORM OR CLIENTTOOLS )
                message ("-- Packing BASH installation files")
                set ( CPACK_RPM_POST_INSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postinst" )
                set ( CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/sbin/prerm" )
                set ( CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/initfiles/bash/sbin/deb/postrm" )
            endif()
        else()
            message("WARNING: Unsupported package ${packageManagement}.")
        endif ()
        if ( PLATFORM OR CLIENTTOOLS )
            if ( EXISTS ${CMAKE_MODULE_PATH}/dependencies/${packageRevision}.cmake )
                include( ${CMAKE_MODULE_PATH}/dependencies/${packageRevision}.cmake )
            else()
                message("-- WARNING: DEPENDENCY FILE FOR ${packageRevision} NOT FOUND, Using deps template.")
                include( ${CMAKE_MODULE_PATH}/dependencies/template.cmake )
            endif()
        else()
            set ( DEPVER ${CPACK_RPM_PACKAGE_RELEASE} )
            if ( Hpcc_VERSION )
                set ( DEPVER ${Hpcc_VERSION} )
            endif()
            set ( CPACK_DEBIAN_PACKAGE_DEPENDS "hpccsystems-platform (= ${DEPVER})")
            set ( CPACK_RPM_PACKAGE_REQUIRES "hpccsystems-platform = ${DEPVER}")
        endif()
    endif ( CMAKE_SYSTEM MATCHES Linux )
else()
    message("WARNING: CMAKE 2.8.7 or later required to create RPMs from this project")
endif()

if (APPLE)
    set ( CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}_${CPACK_RPM_PACKAGE_VERSION}-${version}-${stagever}-${CPACK_SYSTEM_NAME}")
    set ( CPACK_MONOLITHIC_INSTALL TRUE )
    set ( CPACK_PACKAGE_VENDOR "HPCC Systems" )
    file(WRITE "${PROJECT_BINARY_DIR}/welcome.txt"
        "HPCC Systems - Client Tools\r"
        "===========================\r\r"
        "This installer will install the HPCC Systems Client Tools.")
    set ( CPACK_RESOURCE_FILE_README "${PROJECT_BINARY_DIR}/welcome.txt" )
    set ( CPACK_RESOURCE_FILE_LICENSE "${HPCC_SOURCE_DIR}/${LICENSE_FILE}" )
    set ( CPACK_PACKAGE_DESCRIPTION_SUMMARY "HPCC Systems Client Tools." )
endif()
include (CPack)


if ( PLATFORM )

    configure_file (
        ${CMAKE_MODULE_PATH}/HpccConfig.cmake_installed.in
        ${CMAKE_BINARY_DIR}/HpccConfig.cmake_installed
        @ONLY
    )

    configure_file (
        ${CMAKE_MODULE_PATH}/HpccConfig.cmake_local.in
        ${CMAKE_BINARY_DIR}/HpccConfig.cmake
        @ONLY
    )

    include ( CMakeExportBuildSettings )
    export_library_dependencies ( ${CMAKE_BINARY_DIR}/HpccDepends.cmake )

    install ( FILES ${CMAKE_BINARY_DIR}/HpccConfig.cmake_installed RENAME HpccConfig.cmake DESTINATION lib COMPONENT Include )
    install ( FILES ${CMAKE_BINARY_DIR}/HpccDepends.cmake DESTINATION lib COMPONENT Include )
    install ( FILES ${CMAKE_MODULE_PATH}/HpccUse.cmake DESTINATION lib COMPONENT Include )
endif()
